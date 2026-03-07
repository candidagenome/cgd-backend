#!/usr/bin/env python3
"""
Update TAX_RELATIONSHIP table from taxonomy hierarchy file.

This script compares a new taxonomy hierarchy file with the existing
TAX_RELATIONSHIP table and updates the database accordingly.

Original Perl: updateTaxRelationship (by Shuai Weng, Jan 2004)
Converted to Python: 2024

Usage:
    python update_tax_relationship.py hierarchy.data --created-by DBUSER
"""

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from cgd.db.engine import SessionLocal

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))


def setup_logging(verbose: bool = False, log_file: Path = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def create_hierarchy_file_from_db(session: Session, output_file: Path) -> None:
    """
    Export current TAX_RELATIONSHIP table to a file.

    Args:
        session: Database session
        output_file: Output file path
    """
    result = session.execute(
        text(f"""
            SELECT child_taxon_id, parent_taxon_id, generation
            FROM {DB_SCHEMA}.tax_relationship
            ORDER BY child_taxon_id, generation
        """)
    )

    with open(output_file, 'w') as f:
        for child_id, parent_id, generation in result:
            f.write(f"{child_id}\t{parent_id}\t{generation}\n")

    logger.info(f"Exported current tax_relationship to {output_file}")


def get_differences(new_file: Path, db_file: Path) -> tuple[dict, dict]:
    """
    Compare new hierarchy file with database export.

    Args:
        new_file: New hierarchy file
        db_file: Database export file

    Returns:
        Tuple of (generation_in_file, generation_in_db) dicts
    """
    generation_in_file: dict[str, int] = {}
    generation_in_db: dict[str, int] = {}

    # Use diff to find differences
    try:
        result = subprocess.run(
            ['diff', str(new_file), str(db_file)],
            capture_output=True,
            text=True,
        )
        diff_output = result.stdout
    except Exception as e:
        logger.error(f"Error running diff: {e}")
        return generation_in_file, generation_in_db

    for line in diff_output.split('\n'):
        if not line or not line.startswith(('<', '>')):
            continue

        parts = line[2:].split('\t')
        if len(parts) < 3:
            continue

        child_id = parts[0].strip()
        parent_id = parts[1].strip()
        generation = int(parts[2].strip())
        key = f"{child_id}:{parent_id}"

        if line.startswith('< '):
            # Line in new file
            generation_in_file[key] = generation
        elif line.startswith('> '):
            # Line in database
            generation_in_db[key] = generation

    return generation_in_file, generation_in_db


def update_tax_relationship(
    session: Session,
    data_file: Path,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Update TAX_RELATIONSHIP table.

    Args:
        session: Database session
        data_file: New taxonomy hierarchy file
        created_by: Username for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'inserted': 0,
        'deleted': 0,
        'updated': 0,
    }

    # Export current database to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.db', delete=False) as tmp:
        db_file = Path(tmp.name)

    try:
        create_hierarchy_file_from_db(session, db_file)

        # Get differences
        generation_in_file, generation_in_db = get_differences(data_file, db_file)

        logger.info(f"Found {len(generation_in_file)} entries in new file")
        logger.info(f"Found {len(generation_in_db)} entries in database")

        # Process entries in new file
        for key, generation in generation_in_file.items():
            # Skip if same generation in both
            if generation_in_db.get(key) == generation:
                continue

            child_id, parent_id = key.split(':')

            # If exists in DB with different generation, delete old first
            if key in generation_in_db:
                try:
                    session.execute(
                        text(f"""
                            DELETE FROM {DB_SCHEMA}.tax_relationship
                            WHERE parent_taxon_id = :parent
                            AND child_taxon_id = :child
                            AND generation = :gen
                        """),
                        {
                            "parent": int(parent_id),
                            "child": int(child_id),
                            "gen": generation_in_db[key],
                        }
                    )
                    stats['deleted'] += 1
                    logger.debug(
                        f"Deleted: parent={parent_id}, child={child_id}, gen={generation_in_db[key]}"
                    )
                except Exception as e:
                    logger.error(f"Error deleting {key}: {e}")
                    continue

            # Insert new entry
            try:
                session.execute(
                    text(f"""
                        INSERT INTO {DB_SCHEMA}.tax_relationship
                        (parent_taxon_id, child_taxon_id, generation)
                        VALUES (:parent, :child, :gen)
                    """),
                    {
                        "parent": int(parent_id),
                        "child": int(child_id),
                        "gen": generation,
                    }
                )
                stats['inserted'] += 1
                logger.debug(
                    f"Inserted: parent={parent_id}, child={child_id}, gen={generation}"
                )
            except Exception as e:
                logger.error(f"Error inserting {key}: {e}")

        # Delete entries only in database (not in new file)
        for key, generation in generation_in_db.items():
            if key in generation_in_file:
                continue

            child_id, parent_id = key.split(':')

            try:
                session.execute(
                    text(f"""
                        DELETE FROM {DB_SCHEMA}.tax_relationship
                        WHERE parent_taxon_id = :parent
                        AND child_taxon_id = :child
                        AND generation = :gen
                    """),
                    {
                        "parent": int(parent_id),
                        "child": int(child_id),
                        "gen": generation,
                    }
                )
                stats['deleted'] += 1
                logger.debug(
                    f"Deleted obsolete: parent={parent_id}, child={child_id}, gen={generation}"
                )
            except Exception as e:
                logger.error(f"Error deleting obsolete {key}: {e}")

        if not dry_run:
            session.commit()
        else:
            session.rollback()

    finally:
        # Cleanup temp file
        if db_file.exists():
            db_file.unlink()

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update TAX_RELATIONSHIP table from hierarchy file"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="Taxonomy hierarchy file (child_id, parent_id, generation)",
    )
    parser.add_argument(
        "--created-by",
        required=True,
        help="Username for audit trail",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't commit changes",
    )

    args = parser.parse_args()

    # Validate input
    if not args.data_file.exists():
        print(f"Error: Data file not found: {args.data_file}")
        sys.exit(1)

    log_file = args.log_file
    if not log_file:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / "updateTaxRelationship.log"

    setup_logging(args.verbose, log_file)

    logger.info(f"Started at {datetime.now()}")

    try:
        with SessionLocal() as session:
            stats = update_tax_relationship(
                session,
                args.data_file,
                args.created_by,
                args.dry_run,
            )

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Inserted: {stats['inserted']}")
            logger.info(f"  Deleted: {stats['deleted']}")
            logger.info("=" * 50)

            if args.dry_run:
                logger.info("Dry run - no changes committed")

    except Exception as e:
        logger.exception(f"Error: {e}")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
