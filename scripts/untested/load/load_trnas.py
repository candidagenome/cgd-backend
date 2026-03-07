#!/usr/bin/env python3
"""
Load tRNA loci into the database.

This script loads tRNA information from a tRNA info file, creating entries
in both the FEATURE and FEATURE_TYPE tables.

Input file format (tab-delimited):
- Column 1: tRNA name
- Column 2: Contig information
- Column 3: Description

Each tRNA is created as a new feature with feature_type 'tRNA'.

Original Perl: loadtRNAs.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature

load_dotenv()

logger = logging.getLogger(__name__)


def setup_logging(log_file: Path = None, verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def feature_exists(session: Session, feature_name: str) -> bool:
    """
    Check if a feature with the given name already exists.

    Args:
        session: Database session
        feature_name: Name to check

    Returns:
        True if feature exists
    """
    existing = session.query(Feature).filter(
        Feature.feature_name == feature_name
    ).first()
    return existing is not None


def create_feature(
    session: Session,
    name: str,
    contig_info: str,
    description: str,
    created_by: str,
) -> int:
    """
    Create a new feature entry.

    Args:
        session: Database session
        name: Feature name
        contig_info: Contig location information
        description: tRNA description
        created_by: User creating the record

    Returns:
        feature_no of created feature
    """
    brief_id = f"{description} predicted by tRNAscan-SE"

    new_feature = Feature(
        feature_name=name,
        is_on_pmap="N",
        created_by=created_by[:12],
        brief_id=brief_id[:100] if brief_id else None,
        contigs=contig_info,
    )
    session.add(new_feature)
    session.flush()

    return new_feature.feature_no


def create_feature_type(
    session: Session,
    feature_no: int,
    feature_type: str,
    created_by: str,
) -> None:
    """
    Create a feature_type entry.

    Args:
        session: Database session
        feature_no: Feature number
        feature_type: Type of feature (e.g., 'tRNA')
        created_by: User creating the record
    """
    query = text("""
        INSERT INTO feature_type (feature_no, feature_type, date_created, created_by)
        VALUES (:feature_no, :feature_type, CURRENT_TIMESTAMP, :created_by)
    """)

    session.execute(query, {
        "feature_no": feature_no,
        "feature_type": feature_type,
        "created_by": created_by[:12],
    })


def parse_trna_file(filepath: Path) -> list[dict]:
    """
    Parse tRNA info file.

    Args:
        filepath: Path to tRNA info file

    Returns:
        List of dictionaries with name, contig_info, description
    """
    entries = []

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 3:
                logger.warning(f"Skipping malformed line: {line}")
                continue

            name = parts[0].strip()
            contig_info = parts[1].strip()
            description = parts[2].strip()

            if name:
                entries.append({
                    "name": name,
                    "contig_info": contig_info,
                    "description": description,
                })

    logger.info(f"Parsed {len(entries)} tRNA entries from input file")
    return entries


def load_trnas(
    session: Session,
    entries: list[dict],
    created_by: str,
) -> dict:
    """
    Load tRNA entries into the database.

    Args:
        session: Database session
        entries: List of tRNA entry dictionaries
        created_by: User creating the records

    Returns:
        Dictionary with statistics
    """
    stats = {
        "features_created": 0,
        "features_skipped": 0,
        "errors": [],
    }

    for entry in entries:
        name = entry["name"]
        contig_info = entry["contig_info"]
        description = entry["description"]

        # Check if feature already exists
        if feature_exists(session, name):
            logger.warning(f"Feature already exists: {name}")
            stats["features_skipped"] += 1
            continue

        try:
            # Create feature
            feature_no = create_feature(
                session, name, contig_info, description, created_by
            )

            # Create feature_type entry
            create_feature_type(session, feature_no, "tRNA", created_by)

            stats["features_created"] += 1
            logger.info(f"Created tRNA feature: {name}")

        except Exception as e:
            error_msg = f"Error creating feature {name}: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load tRNA loci into the database"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input tRNA info file (tab-delimited: name, contig_info, description)",
    )
    parser.add_argument(
        "--created-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user name for created_by field",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Path to log file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse file but don't modify database",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    entries = parse_trna_file(args.input_file)

    if not entries:
        logger.warning("No tRNA entries found in input file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would create {len(entries)} tRNA features")
        return

    try:
        with SessionLocal() as session:
            stats = load_trnas(session, entries, args.created_by)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  tRNA features created: {stats['features_created']}")
            if stats["features_skipped"] > 0:
                logger.warning(f"  Features skipped (existing): {stats['features_skipped']}")
            if stats["errors"]:
                logger.error(f"  Errors: {len(stats['errors'])}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading tRNAs: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
