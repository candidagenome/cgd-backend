#!/usr/bin/env python3
"""
Load GO parent/child relationships into go_path table.

This script parses the Gene Ontology OBO file and loads the hierarchical
relationships (is_a, part_of) into the go_path table. It builds the
complete ancestor path for each GO term.

Based on loadGoPath.pl by Shuai Weng (March 2002).

Usage:
    python load_go_path.py
    python load_go_path.py --obo /path/to/gene_ontology.obo

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for GO data files
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email for error notifications
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")

# Default OBO file location
DEFAULT_OBO_FILE = DATA_DIR / "GO" / "gene_ontology.obo"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def send_error_email(subject: str, message: str) -> None:
    """Send error notification email."""
    if not CURATOR_EMAIL:
        logger.warning("CURATOR_EMAIL not set, skipping email notification")
        return
    logger.error(f"Email notification: {subject}")
    logger.error(f"Message: {message}")


class GoPathLoader:
    """Load GO hierarchical relationships into go_path table."""

    def __init__(self, session):
        self.session = session
        self.parent: dict[str, list[str]] = defaultdict(list)
        self.relationship: dict[str, str] = {}
        self.go_no_cache: dict[str, int | None] = {}
        self.insert_count = 0

    def get_go_no_for_goid(self, goid: str) -> int | None:
        """Get go_no for a goid from the database."""
        # Check cache first
        if goid in self.go_no_cache:
            return self.go_no_cache[goid]

        query = text(f"""
            SELECT go_no
            FROM   {DB_SCHEMA}.GO
            WHERE  goid = :goid
        """)
        result = self.session.execute(query, {"goid": goid}).fetchone()

        go_no = result[0] if result else None
        self.go_no_cache[goid] = go_no
        return go_no

    def parse_ontology(self, obo_file: Path) -> None:
        """
        Parse the OBO file to extract parent/child relationships.

        Extracts is_a and part_of relationships from the OBO file.
        """
        logger.info(f"Parsing ontology file: {obo_file}")

        if not obo_file.exists():
            raise FileNotFoundError(f"OBO file not found: {obo_file}")

        with open(obo_file) as f:
            entry_lines: list[str] = []
            in_term = False

            for line in f:
                line = line.strip()

                # Start of a Term stanza
                if line == "[Term]":
                    if entry_lines:
                        self._process_entry(entry_lines)
                    entry_lines = []
                    in_term = True
                    continue

                # Start of Typedef stanza - stop processing
                if line == "[Typedef]":
                    if entry_lines:
                        self._process_entry(entry_lines)
                    break

                # Collect lines within Term stanza
                if in_term:
                    entry_lines.append(line)

            # Process last entry if any
            if entry_lines:
                self._process_entry(entry_lines)

        logger.info(f"Parsed {len(self.parent)} GO terms with parent relationships")

    def _process_entry(self, lines: list[str]) -> None:
        """Process a single GO term entry to extract relationships."""
        goid = None
        is_obsolete = False

        for line in lines:
            if line.startswith("is_obsolete: true"):
                is_obsolete = True
                break

            if line.startswith("id: GO:"):
                # Extract the numeric part of the GO ID
                goid = line[4:].strip().replace("GO:", "").lstrip("0")

        if is_obsolete or not goid:
            return

        # Extract relationships
        for line in lines:
            if line.startswith("is_a: GO:"):
                # Extract parent GO ID
                parent_part = line[6:].split("!")[0].strip()
                parent_goid = parent_part.replace("GO:", "").lstrip("0")

                # Skip cycles
                if parent_goid == goid:
                    continue

                self.parent[goid].append(parent_goid)
                self.relationship[f"{goid}::{parent_goid}"] = "is a"

            elif line.startswith("relationship: part_of GO:"):
                # Extract part_of relationship
                parts = line.split()
                for part in parts:
                    if part.startswith("GO:"):
                        parent_goid = part.replace("GO:", "").lstrip("0")
                        if parent_goid != goid:
                            self.parent[goid].append(parent_goid)
                            self.relationship[f"{goid}::{parent_goid}"] = "part of"
                        break

    def create_go_path_data(self, output_file: Path) -> int:
        """
        Create the GO path data file for loading.

        Builds complete ancestor paths for each GO term.

        Returns the number of records created.
        """
        logger.info(f"Creating GO path data file: {output_file}")

        seen_keys: set[str] = set()
        records = []

        for child_goid in sorted(self.parent.keys()):
            for parent_goid in self.parent[child_goid]:
                generation = 1
                relationship = self.relationship.get(f"{child_goid}::{parent_goid}", "")
                ancestor_path = parent_goid

                key = f"{child_goid}_{ancestor_path}"
                if key not in seen_keys:
                    child_go_no = self.get_go_no_for_goid(child_goid)
                    parent_go_no = self.get_go_no_for_goid(parent_goid)

                    if child_go_no and parent_go_no:
                        records.append({
                            "child_go_no": child_go_no,
                            "ancestor_go_no": parent_go_no,
                            "generation": generation,
                            "ancestor_path": ancestor_path,
                            "relationship": relationship,
                        })
                        seen_keys.add(key)

                # Recursively find all ancestors
                if parent_goid in self.parent:
                    self._find_ancestors(
                        child_goid,
                        parent_goid,
                        generation + 1,
                        ancestor_path,
                        seen_keys,
                        records,
                    )

        # Write to file
        with open(output_file, "w") as f:
            # Header
            f.write("child_go_no\tancestor_go_no\tgeneration\tancestor_path\trelationship\n")

            for rec in records:
                f.write(
                    f"{rec['child_go_no']}\t{rec['ancestor_go_no']}\t"
                    f"{rec['generation']}\t{rec['ancestor_path']}\t{rec['relationship']}\n"
                )

        logger.info(f"Created {len(records)} GO path records")
        return len(records)

    def _find_ancestors(
        self,
        child_goid: str,
        parent_goid: str,
        generation: int,
        ancestor_list: str,
        seen_keys: set[str],
        records: list[dict],
    ) -> None:
        """Recursively find all ancestors of a GO term."""
        if parent_goid not in self.parent:
            return

        for ancestor_goid in self.parent[parent_goid]:
            ancestor_path = f"{ancestor_list}::{ancestor_goid}"
            key = f"{child_goid}_{ancestor_path}"

            if key not in seen_keys:
                child_go_no = self.get_go_no_for_goid(child_goid)
                ancestor_go_no = self.get_go_no_for_goid(ancestor_goid)

                if child_go_no and ancestor_go_no:
                    records.append({
                        "child_go_no": child_go_no,
                        "ancestor_go_no": ancestor_go_no,
                        "generation": generation,
                        "ancestor_path": ancestor_path,
                        "relationship": "",  # No direct relationship for distant ancestors
                    })
                    seen_keys.add(key)

            # Continue recursion
            if ancestor_goid in self.parent:
                self._find_ancestors(
                    child_goid,
                    ancestor_goid,
                    generation + 1,
                    ancestor_path,
                    seen_keys,
                    records,
                )

    def load_go_path_table(self, data_file: Path) -> int:
        """
        Load the GO path data into the database.

        Returns the number of records loaded.
        """
        logger.info("Loading GO_PATH table...")

        # Truncate table
        logger.info("Truncating GO_PATH table...")
        self.session.execute(text(f"DELETE FROM {DB_SCHEMA}.GO_PATH"))

        # Read data file and insert
        count = 0
        with open(data_file) as f:
            # Skip header
            next(f)

            for line in f:
                parts = line.strip().split("\t")
                if len(parts) < 4:
                    continue

                child_go_no = int(parts[0])
                ancestor_go_no = int(parts[1])
                generation = int(parts[2])
                ancestor_path = parts[3]
                relationship = parts[4] if len(parts) > 4 else None

                # Get next sequence value
                seq_query = text(f"SELECT {DB_SCHEMA}.GO_PATH_SEQ.NEXTVAL FROM DUAL")
                try:
                    go_path_no = self.session.execute(seq_query).scalar()
                except Exception:
                    # If sequence doesn't exist, use count
                    go_path_no = count + 1

                insert_query = text(f"""
                    INSERT INTO {DB_SCHEMA}.GO_PATH
                        (GO_PATH_NO, CHILD_GO_NO, ANCESTOR_GO_NO, GENERATION,
                         ANCESTOR_PATH, RELATIONSHIP_TYPE)
                    VALUES
                        (:go_path_no, :child_go_no, :ancestor_go_no, :generation,
                         :ancestor_path, :relationship)
                """)

                self.session.execute(
                    insert_query,
                    {
                        "go_path_no": go_path_no,
                        "child_go_no": child_go_no,
                        "ancestor_go_no": ancestor_go_no,
                        "generation": generation,
                        "ancestor_path": ancestor_path,
                        "relationship": relationship,
                    },
                )
                count += 1

                if count % 10000 == 0:
                    logger.info(f"Inserted {count} records...")

        self.session.commit()
        logger.info(f"Loaded {count} records into GO_PATH table")
        return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load GO parent/child relationships into go_path table"
    )
    parser.add_argument(
        "--obo",
        type=Path,
        default=DEFAULT_OBO_FILE,
        help=f"Path to gene_ontology.obo file (default: {DEFAULT_OBO_FILE})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DATA_DIR / "GO",
        help="Directory for output data file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse OBO and create data file without loading to database",
    )

    args = parser.parse_args()

    # Set up file logging
    log_dir = LOG_DIR / "load"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "goPath.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting GO path loading at {datetime.now()}")

    try:
        with SessionLocal() as session:
            loader = GoPathLoader(session)

            # Parse OBO file
            loader.parse_ontology(args.obo)

            # Create data file
            args.output_dir.mkdir(parents=True, exist_ok=True)
            data_file = args.output_dir / "GO_PATH.data"
            record_count = loader.create_go_path_data(data_file)

            if args.dry_run:
                logger.info(f"DRY RUN - created {record_count} records in {data_file}")
                return 0

            # Load into database
            loaded_count = loader.load_go_path_table(data_file)

            logger.info(f"Successfully loaded {loaded_count} rows into go_path table")
            logger.info(f"Completed at {datetime.now()}")

        return 0

    except Exception as e:
        error_msg = f"Error loading GO path: {e}"
        logger.error(error_msg)
        send_error_email("Error loading GO_PATH table", error_msg)
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
