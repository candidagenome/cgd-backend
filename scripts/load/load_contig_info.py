#!/usr/bin/env python3
"""
Load or update contig information for features.

This script updates the contig field in the FEATURE table based on
feature location data or an input mapping file.

Input file format (if provided): Tab-delimited with columns:
- Column 1: Feature name (ORF)
- Column 2: Contig identifier

If no input file is provided, contig information is derived from
the feature's location data (seq_no -> ftp_file/contig).

Original Perl: loadContigInfo.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, Organism

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


def get_organism_no(session: Session, organism_abbrev: str) -> int | None:
    """Get organism_no for the given abbreviation."""
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == organism_abbrev
    ).first()
    return organism.organism_no if organism else None


def parse_contig_mapping(filepath: Path) -> dict[str, str]:
    """
    Parse contig mapping file.

    Args:
        filepath: Path to mapping file

    Returns:
        Dictionary mapping feature_name to contig
    """
    mapping = {}

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                logger.warning(f"Line {line_num}: Invalid format, skipping")
                continue

            feature_name = parts[0].strip()
            contig = parts[1].strip()

            if feature_name and contig:
                mapping[feature_name] = contig

    logger.info(f"Parsed {len(mapping)} contig mappings from file")
    return mapping


def get_contig_from_location(session: Session, feature_no: int) -> str | None:
    """
    Get contig identifier from feature location.

    Args:
        session: Database session
        feature_no: Feature number

    Returns:
        Contig identifier or None
    """
    # Query feat_location to get the root_seq_no, then seq to get contig info
    query = text("""
        SELECT s.ftp_file
        FROM MULTI.feat_location fl
        JOIN MULTI.seq s ON fl.root_seq_no = s.seq_no
        WHERE fl.feature_no = :feature_no
        AND fl.root_seq_no IS NOT NULL
    """)

    result = session.execute(query, {"feature_no": feature_no}).fetchone()

    if result and result[0]:
        # Extract contig from ftp_file path
        ftp_file = result[0]
        # Typically formatted as strain:contig or just contig name
        if ":" in ftp_file:
            return ftp_file.split(":")[-1]
        return ftp_file

    return None


def update_feature_contig(
    session: Session,
    feature_no: int,
    contig: str,
) -> bool:
    """
    Update the contig field for a feature.

    Args:
        session: Database session
        feature_no: Feature number
        contig: Contig identifier

    Returns:
        True if updated, False if no change needed
    """
    feature = session.query(Feature).filter(
        Feature.feature_no == feature_no
    ).first()

    if not feature:
        return False

    # Check if contig field exists and needs update
    # Note: The Feature model may need a 'contig' column
    current_contig = getattr(feature, "contig", None)

    if current_contig == contig:
        return False

    # Update using raw SQL since contig may not be in the model
    query = text("""
        UPDATE MULTI.feature
        SET contig = :contig
        WHERE feature_no = :feature_no
    """)

    session.execute(query, {"contig": contig, "feature_no": feature_no})
    return True


def load_contig_info_from_file(
    session: Session,
    mapping: dict[str, str],
    organism_no: int = None,
) -> dict:
    """
    Load contig info from mapping file.

    Args:
        session: Database session
        mapping: Dictionary mapping feature_name to contig
        organism_no: Optional organism number to filter features

    Returns:
        Dictionary with statistics
    """
    stats = {
        "features_processed": 0,
        "features_updated": 0,
        "features_not_found": 0,
    }

    for feature_name, contig in mapping.items():
        # Find feature
        query = session.query(Feature).filter(
            Feature.feature_name == feature_name
        )
        if organism_no:
            query = query.filter(Feature.organism_no == organism_no)

        feature = query.first()

        if not feature:
            logger.debug(f"Feature not found: {feature_name}")
            stats["features_not_found"] += 1
            continue

        stats["features_processed"] += 1

        if update_feature_contig(session, feature.feature_no, contig):
            stats["features_updated"] += 1
            logger.debug(f"Updated contig for {feature_name}: {contig}")

    return stats


def load_contig_info_from_locations(
    session: Session,
    organism_no: int,
) -> dict:
    """
    Load contig info from feature locations.

    Args:
        session: Database session
        organism_no: Organism number to filter features

    Returns:
        Dictionary with statistics
    """
    stats = {
        "features_processed": 0,
        "features_updated": 0,
        "features_skipped": 0,
    }

    # Get all features for the organism
    features = session.query(Feature).filter(
        Feature.organism_no == organism_no
    ).all()

    for feature in features:
        stats["features_processed"] += 1

        contig = get_contig_from_location(session, feature.feature_no)

        if not contig:
            stats["features_skipped"] += 1
            continue

        if update_feature_contig(session, feature.feature_no, contig):
            stats["features_updated"] += 1
            logger.debug(f"Updated contig for {feature.feature_name}: {contig}")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load or update contig information for features"
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        help="Input file with feature-to-contig mapping (optional)",
    )
    parser.add_argument(
        "--strain",
        help="Strain abbreviation to filter features",
    )
    parser.add_argument(
        "--from-locations",
        action="store_true",
        help="Derive contig from feature locations instead of file",
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

    logger.info(f"Started at {datetime.now()}")

    # Validate arguments
    if not args.input_file and not args.from_locations:
        logger.error("Must specify either --input-file or --from-locations")
        sys.exit(1)

    if args.from_locations and not args.strain:
        logger.error("--from-locations requires --strain")
        sys.exit(1)

    if args.input_file and not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    # Parse input file if provided
    mapping = {}
    if args.input_file:
        mapping = parse_contig_mapping(args.input_file)
        if not mapping:
            logger.warning("No valid mappings found in input file")
            return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        if args.input_file:
            logger.info(f"Would process {len(mapping)} contig mappings")
        else:
            logger.info("Would derive contig info from feature locations")
        return

    try:
        with SessionLocal() as session:
            organism_no = None
            if args.strain:
                organism_no = get_organism_no(session, args.strain)
                if not organism_no:
                    logger.error(f"Organism not found: {args.strain}")
                    sys.exit(1)
                logger.info(f"Filtering by organism_no: {organism_no}")

            if args.from_locations:
                stats = load_contig_info_from_locations(session, organism_no)
            else:
                stats = load_contig_info_from_file(session, mapping, organism_no)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Features processed: {stats['features_processed']}")
            logger.info(f"  Features updated: {stats['features_updated']}")
            if "features_not_found" in stats and stats["features_not_found"] > 0:
                logger.warning(
                    f"  Features not found: {stats['features_not_found']}"
                )
            if "features_skipped" in stats and stats["features_skipped"] > 0:
                logger.info(f"  Features skipped: {stats['features_skipped']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading contig info: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
