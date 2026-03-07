#!/usr/bin/env python3
"""
Load new feature URLs into database.

This script loads data into URL and FEAT_URL tables
for external links on feature/locus pages.

Original Perl: newFEATurl.pl
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
from cgd.models.models import FeatUrl, Feature, Url

load_dotenv()

logger = logging.getLogger(__name__)


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


def get_feature_map(session: Session) -> dict:
    """
    Get mapping of feature_name to feature_no.

    Returns:
        Dict mapping feature_name to feature_no
    """
    features = session.query(Feature).all()
    return {f.feature_name: f.feature_no for f in features}


def get_features_by_type(
    session: Session,
    feature_type: str,
) -> list:
    """
    Get feature_nos for a given feature type.

    Args:
        session: Database session
        feature_type: Feature type or 'AllFeatures'/'ChrFeatures'

    Returns:
        List of feature_nos
    """
    if feature_type == 'AllFeatures':
        # All features that have locus pages
        result = session.execute(
            text("""
                SELECT feature_no FROM feature
                WHERE feature_type IN (
                    SELECT DISTINCT feature_type FROM feature
                    WHERE feature_type NOT LIKE 'not%'
                )
            """)
        )
    elif feature_type == 'ChrFeatures':
        # Chromosomal features only
        result = session.execute(
            text("""
                SELECT feature_no FROM feature
                WHERE feature_type IN (
                    SELECT DISTINCT feature_type FROM feature
                    WHERE feature_type NOT LIKE 'not%'
                )
                AND feature_type NOT LIKE 'not_%'
            """)
        )
    else:
        # Specific feature type
        result = session.execute(
            text("""
                SELECT feature_no FROM feature
                WHERE feature_type = :feat_type
            """),
            {"feat_type": feature_type}
        )

    return [row[0] for row in result]


def get_or_create_url(
    session: Session,
    url: str,
    url_type: str,
    source: str,
    substitution_value: str,
    created_by: str,
) -> int:
    """
    Get or create URL entry.

    Returns:
        url_no
    """
    url_obj = session.query(Url).filter(Url.url == url).first()

    if url_obj:
        return url_obj.url_no

    url_obj = Url(
        url=url,
        url_type=url_type,
        source=source,
        substitution_value=substitution_value,
        created_by=created_by,
    )
    session.add(url_obj)
    session.flush()

    logger.info(f"URL inserted: {url}")
    return url_obj.url_no


def load_feat_url(
    session: Session,
    url_no: int,
    feature_no: int,
) -> bool:
    """
    Load FEAT_URL entry.

    Returns:
        True if inserted, False if already exists
    """
    # Check if already exists
    existing = session.query(FeatUrl).filter(
        and_(
            FeatUrl.feature_no == feature_no,
            FeatUrl.url_no == url_no,
        )
    ).first()

    if existing:
        return False

    feat_url = FeatUrl(
        feature_no=feature_no,
        url_no=url_no,
    )
    session.add(feat_url)
    session.flush()

    return True


def load_feat_urls_from_file(
    session: Session,
    data_file: Path,
    url_no: int,
    dry_run: bool = False,
) -> dict:
    """
    Load FEAT_URLs for features listed in file.

    Args:
        session: Database session
        data_file: File with feature names
        url_no: URL number to link
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'inserted': 0,
        'already_exists': 0,
        'bad_features': 0,
        'errors': 0,
    }

    # Get feature mapping
    feature_map = get_feature_map(session)

    with open(data_file) as f:
        for line_num, line in enumerate(f, 1):
            feat_name = line.strip()
            if not feat_name:
                continue

            stats['processed'] += 1

            feat_no = feature_map.get(feat_name)
            if not feat_no:
                logger.warning(f"Bad feature name: {feat_name} (line {line_num})")
                stats['bad_features'] += 1
                continue

            try:
                if load_feat_url(session, url_no, feat_no):
                    logger.debug(f"Loaded URL {url_no} for {feat_name} ({feat_no})")
                    stats['inserted'] += 1
                else:
                    stats['already_exists'] += 1

            except Exception as e:
                logger.error(f"Error loading FEAT_URL for {feat_name}: {e}")
                stats['errors'] += 1

    return stats


def load_feat_urls_by_type(
    session: Session,
    feature_type: str,
    url_no: int,
    dry_run: bool = False,
) -> dict:
    """
    Load FEAT_URLs for all features of a type.

    Args:
        session: Database session
        feature_type: Feature type or 'AllFeatures'/'ChrFeatures'
        url_no: URL number to link
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'inserted': 0,
        'already_exists': 0,
        'errors': 0,
    }

    # Get feature_nos for type
    feature_nos = get_features_by_type(session, feature_type)
    logger.info(f"Found {len(feature_nos)} features of type '{feature_type}'")

    for feat_no in feature_nos:
        stats['processed'] += 1

        try:
            if load_feat_url(session, url_no, feat_no):
                logger.debug(f"Loaded URL {url_no} for feature_no {feat_no}")
                stats['inserted'] += 1
            else:
                stats['already_exists'] += 1

        except Exception as e:
            logger.error(f"Error loading FEAT_URL for feature_no {feat_no}: {e}")
            stats['errors'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load new feature URLs into database"
    )
    parser.add_argument(
        "--url",
        help="Template URL (use _SUBSTITUTE_THIS_ placeholder)",
    )
    parser.add_argument(
        "--url-no",
        type=int,
        help="Existing URL number to use",
    )
    parser.add_argument(
        "--url-source",
        help="URL source (required if creating new URL)",
    )
    parser.add_argument(
        "--url-type",
        help="URL type (required if creating new URL)",
    )
    parser.add_argument(
        "--feature-file",
        type=Path,
        help="File with list of feature names",
    )
    parser.add_argument(
        "--feature-type",
        help="Feature type (or AllFeatures/ChrFeatures)",
    )
    parser.add_argument(
        "--created-by",
        default="SCRIPT",
        help="User name for audit (default: SCRIPT)",
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
        help="Show what would be done without modifying database",
    )

    args = parser.parse_args()

    setup_logging(args.verbose, args.log_file)

    logger.info(f"Started at {datetime.now()}")

    # Validate input
    if not args.url_no and not args.url:
        logger.error("Either --url or --url-no must be provided")
        sys.exit(1)

    if args.url and (not args.url_source or not args.url_type):
        logger.error("--url-source and --url-type required when creating new URL")
        sys.exit(1)

    if not args.feature_file and not args.feature_type:
        logger.error("Either --feature-file or --feature-type must be provided")
        sys.exit(1)

    if args.feature_file and not args.feature_file.exists():
        logger.error(f"Feature file not found: {args.feature_file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            # Get or create URL
            if args.url_no:
                url_no = args.url_no
                logger.info(f"Using existing URL: {url_no}")
            else:
                url_no = get_or_create_url(
                    session,
                    args.url,
                    args.url_type,
                    args.url_source,
                    "FEATURE",
                    args.created_by,
                )

            # Load FEAT_URLs
            if args.feature_file:
                stats = load_feat_urls_from_file(
                    session,
                    args.feature_file,
                    url_no,
                    args.dry_run,
                )
            else:
                stats = load_feat_urls_by_type(
                    session,
                    args.feature_type,
                    url_no,
                    args.dry_run,
                )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Processed: {stats['processed']}")
            logger.info(f"  Inserted: {stats['inserted']}")
            logger.info(f"  Already exists: {stats['already_exists']}")
            if 'bad_features' in stats:
                logger.info(f"  Bad features: {stats['bad_features']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
