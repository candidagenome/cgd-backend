#!/usr/bin/env python3
"""
Load GO path data from OBO ontology file.

This script parses the Gene Ontology OBO file and loads the
child/parent relationship information into the go_path table.

Based on loadGoPath.pl by Shuai Weng (March 2002)
Modified for CGD BUD by Prachi Shah (Dec 2007)

Usage:
    python load_go_path.py

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory containing GO files
    LOG_DIR: Log directory
    CURATOR_EMAIL: Email for notifications
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class GOPathLoader:
    """Load GO path data from OBO file."""

    def __init__(self, session):
        self.session = session

        # Data structures
        self.parent: dict[str, list[str]] = {}
        self.relationship: dict[str, str] = {}
        self.go_no_cache: dict[str, int] = {}
        self.inserted_keys: set[str] = set()
        self.insert_count = 0

    def get_go_no(self, goid: str) -> int | None:
        """Get go_no for a GO ID."""
        if goid in self.go_no_cache:
            return self.go_no_cache[goid]

        query = text(f"""
            SELECT go_no
            FROM {DB_SCHEMA}.go
            WHERE goid = :goid
        """)

        result = self.session.execute(query, {"goid": goid}).first()

        if result:
            self.go_no_cache[goid] = result[0]
            return result[0]
        return None

    def parse_ontology(self, obo_file: Path) -> None:
        """
        Parse the OBO ontology file.

        Args:
            obo_file: Path to gene_ontology.obo file
        """
        logger.info(f"Parsing ontology file: {obo_file}")

        with open(obo_file, "r") as f:
            entry = ""
            term_start = False

            for line in f:
                if line.strip() == "[Term]":
                    term_start = True
                    if entry:
                        self._process_entry(entry)
                    entry = ""
                    continue

                if line.strip() == "[Typedef]":
                    # End of terms section
                    if entry:
                        self._process_entry(entry)
                    break

                if term_start:
                    entry += line

            # Process last entry
            if entry:
                self._process_entry(entry)

        logger.info(f"Parsed {len(self.parent)} GO terms with parent relationships")

    def _process_entry(self, entry: str) -> None:
        """Process a single OBO term entry."""
        # Skip obsolete terms
        if "is_obsolete: true" in entry.lower():
            return

        goid = None
        parents = []

        for line in entry.split("\n"):
            line = line.strip()

            # Get GO ID
            if line.startswith("id: GO:"):
                match = line.replace("id: GO:", "").strip()
                # Remove leading zeros
                goid = str(int(match))

            # Get is_a parent
            elif line.startswith("is_a: GO:"):
                parts = line.split()
                if len(parts) >= 2:
                    parent_str = parts[1].replace("GO:", "")
                    parent_id = str(int(parent_str))

                    # Skip if parent is same as child (cycle)
                    if parent_id != goid:
                        parents.append(parent_id)
                        self.relationship[f"{goid}::{parent_id}"] = "is a"

            # Get part_of parent
            elif line.startswith("relationship: part_of GO:"):
                parts = line.split()
                for i, part in enumerate(parts):
                    if part.startswith("GO:"):
                        parent_str = part.replace("GO:", "")
                        parent_id = str(int(parent_str))
                        parents.append(parent_id)
                        self.relationship[f"{goid}::{parent_id}"] = "part of"
                        break

        if goid and parents:
            self.parent[goid] = parents

    def create_go_path_data(self, output_file: Path) -> None:
        """
        Create GO path data file.

        Args:
            output_file: Path to output data file
        """
        logger.info(f"Creating GO path data file: {output_file}")

        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            # Write header
            f.write("child_gono\tancestor_gono\tgeneration\tancestor_path\trelationship\n")

            for child_goid in sorted(self.parent.keys()):
                for parent_goid in self.parent[child_goid]:
                    generation = 1
                    relationship = self.relationship.get(
                        f"{child_goid}::{parent_goid}", ""
                    )
                    ancestor_path = parent_goid

                    key = f"{child_goid}_{ancestor_path}"
                    if key not in self.inserted_keys:
                        child_go_no = self.get_go_no(child_goid)
                        parent_go_no = self.get_go_no(parent_goid)

                        if child_go_no and parent_go_no:
                            f.write(
                                f"{child_go_no}\t{parent_go_no}\t{generation}\t"
                                f"{ancestor_path}\t{relationship}\n"
                            )
                            self.inserted_keys.add(key)
                            self.insert_count += 1

                    # Find ancestors recursively
                    if parent_goid in self.parent:
                        self._find_ancestors(
                            f, child_goid, parent_goid,
                            generation + 1, ancestor_path
                        )

        logger.info(f"Created {self.insert_count} GO path entries")

    def _find_ancestors(
        self,
        f,
        child_goid: str,
        parent_goid: str,
        generation: int,
        ancestor_list: str
    ) -> None:
        """Recursively find all ancestors."""
        if parent_goid not in self.parent:
            return

        for ancestor_goid in self.parent[parent_goid]:
            ancestor_path = f"{ancestor_list}::{ancestor_goid}"

            key = f"{child_goid}_{ancestor_path}"
            if key not in self.inserted_keys:
                child_go_no = self.get_go_no(child_goid)
                ancestor_go_no = self.get_go_no(ancestor_goid)

                if child_go_no and ancestor_go_no:
                    f.write(
                        f"{child_go_no}\t{ancestor_go_no}\t{generation}\t"
                        f"{ancestor_path}\t\n"
                    )
                    self.inserted_keys.add(key)
                    self.insert_count += 1

            # Continue recursion
            if ancestor_goid in self.parent:
                self._find_ancestors(
                    f, child_goid, ancestor_goid,
                    generation + 1, ancestor_path
                )

    def load_go_path(self, data_file: Path) -> int:
        """
        Load GO path data into database.

        Args:
            data_file: Path to data file

        Returns:
            Number of rows loaded
        """
        logger.info("Loading GO_PATH table...")

        # Truncate table
        truncate = text(f"DELETE FROM {DB_SCHEMA}.go_path")
        self.session.execute(truncate)
        self.session.commit()
        logger.info("Truncated GO_PATH table")

        # Load data
        count = 0
        insert = text(f"""
            INSERT INTO {DB_SCHEMA}.go_path
            (child_go_no, parent_go_no, generation, ancestor_path, relationship_type)
            VALUES (:child, :parent, :gen, :path, :rel)
        """)

        with open(data_file, "r") as f:
            # Skip header
            next(f)

            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 4:
                    child_go_no = int(parts[0])
                    parent_go_no = int(parts[1])
                    generation = int(parts[2])
                    ancestor_path = parts[3]
                    relationship = parts[4] if len(parts) > 4 else None

                    self.session.execute(insert, {
                        "child": child_go_no,
                        "parent": parent_go_no,
                        "gen": generation,
                        "path": ancestor_path,
                        "rel": relationship,
                    })
                    count += 1

                    # Commit in batches
                    if count % 10000 == 0:
                        self.session.commit()
                        logger.info(f"Loaded {count} rows...")

        self.session.commit()
        logger.info(f"Loaded {count} rows into GO_PATH table")
        return count


def load_go_path(obo_file: Path | None = None) -> bool:
    """
    Main function to load GO path data.

    Args:
        obo_file: Path to OBO file (default: DATA_DIR/GO/gene_ontology.obo)

    Returns:
        True on success, False on failure
    """
    if obo_file is None:
        obo_file = DATA_DIR / "GO" / "gene_ontology.obo"

    if not obo_file.exists():
        logger.error(f"OBO file not found: {obo_file}")
        return False

    # Output file
    data_file = DATA_DIR / "GO" / "GO_PATH.data"

    logger.info("Starting GO path loading...")
    logger.info(f"OBO file: {obo_file}")

    try:
        with SessionLocal() as session:
            loader = GOPathLoader(session)

            # Parse ontology
            loader.parse_ontology(obo_file)

            # Create data file
            loader.create_go_path_data(data_file)

            # Load into database
            count = loader.load_go_path(data_file)

            logger.info(f"GO path loading complete: {count} rows")
            return count > 0

    except Exception as e:
        logger.exception(f"Error loading GO path: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load GO path data from OBO ontology file"
    )
    parser.add_argument(
        "--obo",
        type=Path,
        help="Path to gene_ontology.obo file",
    )

    args = parser.parse_args()

    success = load_go_path(args.obo)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
