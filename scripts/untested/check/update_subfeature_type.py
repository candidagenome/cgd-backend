#!/usr/bin/env python3
"""
Update subfeature_type for subfeatures.

This script updates subfeature_type values in the database, typically
to rename or correct subfeature type classifications.

Original Perl: updateSubfeature_type.pl
Converted to Python: 2024
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Subfeature, SubfeatureType

load_dotenv()

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def find_subfeatures_with_inverted_coords(
    session: Session,
    subfeature_type: str,
) -> list[int]:
    """
    Find subfeatures with inverted coordinates (stop < start).

    Args:
        session: Database session
        subfeature_type: Subfeature type to filter

    Returns:
        List of subfeature numbers
    """
    result = session.execute(
        text("""
            SELECT s.subfeature_no
            FROM subfeature s
            JOIN subfeature_type t ON s.subfeature_no = t.subfeature_no
            WHERE s.stop_coord < s.start_coord
              AND t.subfeature_type = :subfeature_type
        """),
        {"subfeature_type": subfeature_type}
    )
    return [row[0] for row in result]


def find_subfeatures_by_type(
    session: Session,
    subfeature_type: str,
) -> list[int]:
    """
    Find all subfeatures with a given type.

    Args:
        session: Database session
        subfeature_type: Subfeature type to filter

    Returns:
        List of subfeature numbers
    """
    result = session.query(SubfeatureType.subfeature_no).filter(
        SubfeatureType.subfeature_type == subfeature_type
    ).all()
    return [row[0] for row in result]


def update_subfeature_type(
    session: Session,
    subfeature_no: int,
    old_type: str,
    new_type: str,
    created_by: str,
) -> bool:
    """
    Update subfeature type.

    Args:
        session: Database session
        subfeature_no: Subfeature number
        old_type: Current type to replace
        new_type: New type value
        created_by: User making the change

    Returns:
        True if updated, False otherwise
    """
    # Check if new type already exists
    existing_new = session.query(SubfeatureType).filter(
        and_(
            SubfeatureType.subfeature_no == subfeature_no,
            SubfeatureType.subfeature_type == new_type,
        )
    ).first()

    if existing_new:
        # Just delete the old type
        session.query(SubfeatureType).filter(
            and_(
                SubfeatureType.subfeature_no == subfeature_no,
                SubfeatureType.subfeature_type == old_type,
            )
        ).delete()
        return True

    # Update the type
    old_record = session.query(SubfeatureType).filter(
        and_(
            SubfeatureType.subfeature_no == subfeature_no,
            SubfeatureType.subfeature_type == old_type,
        )
    ).first()

    if old_record:
        old_record.subfeature_type = new_type
        return True

    return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update subfeature_type for subfeatures"
    )
    parser.add_argument(
        "old_type",
        help="Current subfeature type to change",
    )
    parser.add_argument(
        "new_type",
        help="New subfeature type value",
    )
    parser.add_argument(
        "--inverted-only",
        action="store_true",
        help="Only update subfeatures with inverted coords (stop < start)",
    )
    parser.add_argument(
        "--created-by",
        default="SYSTEM",
        help="User name for audit trail (default: SYSTEM)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying database",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")
    logger.info(f"Old type: '{args.old_type}'")
    logger.info(f"New type: '{args.new_type}'")

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    stats = {
        "found": 0,
        "updated": 0,
        "errors": 0,
    }

    try:
        with SessionLocal() as session:
            # Find subfeatures to update
            if args.inverted_only:
                subfeature_nos = find_subfeatures_with_inverted_coords(
                    session, args.old_type
                )
                logger.info(f"Found {len(subfeature_nos)} subfeatures with inverted coords")
            else:
                subfeature_nos = find_subfeatures_by_type(session, args.old_type)
                logger.info(f"Found {len(subfeature_nos)} subfeatures with type '{args.old_type}'")

            stats["found"] = len(subfeature_nos)

            for subfeat_no in subfeature_nos:
                try:
                    if update_subfeature_type(
                        session,
                        subfeat_no,
                        args.old_type,
                        args.new_type,
                        args.created_by,
                    ):
                        logger.debug(f"Updated subfeature {subfeat_no}")
                        stats["updated"] += 1
                except Exception as e:
                    logger.error(f"Error updating subfeature {subfeat_no}: {e}")
                    stats["errors"] += 1

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Found: {stats['found']}")
            logger.info(f"  Updated: {stats['updated']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
