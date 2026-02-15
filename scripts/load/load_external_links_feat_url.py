#!/usr/bin/env python3
"""
Load external link information via FEAT_URL table.

This script loads external link information into the following tables:
- URL: The base URL template
- WEB_DISPLAY: How the link is displayed on web pages
- FEAT_URL: Direct feature-to-URL associations

Input file format:
- Single column with feature names (one per line)
- First line is header (skipped)
- Multiple features can be separated by : or ;

Original Perl: loadExternalLinks_FeatUrl.pl
Author: Prachi Shah (Mar 16, 2010)
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, FeatUrl, Url, WebDisplay

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


def get_or_create_url(
    session: Session,
    url_template: str,
    source: str,
    url_type: str,
    created_by: str,
) -> int:
    """
    Get existing URL or create new one.

    Args:
        session: Database session
        url_template: URL template string
        source: URL source
        url_type: Type of URL
        created_by: User creating the record

    Returns:
        url_no for the URL
    """
    existing = session.query(Url).filter(Url.url == url_template).first()
    if existing:
        logger.debug(f"Found existing URL with url_no={existing.url_no}")
        return existing.url_no

    new_url = Url(
        source=source,
        url_type=url_type,
        url=url_template,
        substitution_value="FEATURE",
        created_by=created_by[:12],
    )
    session.add(new_url)
    session.flush()

    logger.info(f"Created new URL with url_no={new_url.url_no}")
    return new_url.url_no


def get_or_create_web_display(
    session: Session,
    url_no: int,
    label_location: str,
    label_name: str,
    created_by: str,
) -> None:
    """
    Get existing WebDisplay or create new one.

    Args:
        session: Database session
        url_no: Foreign key to URL table
        label_location: Location on the page
        label_name: Display label name
        created_by: User creating the record
    """
    existing = session.query(WebDisplay).filter(
        and_(
            WebDisplay.web_page_name == "Locus",
            WebDisplay.label_location == label_location,
            WebDisplay.url_no == url_no,
            WebDisplay.label_name == label_name,
            WebDisplay.label_type == "Text",
            WebDisplay.is_default == "N",
        )
    ).first()

    if existing:
        logger.debug("WebDisplay entry already exists")
        return

    new_wd = WebDisplay(
        url_no=url_no,
        web_page_name="Locus",
        label_location=label_location,
        label_type="Text",
        label_name=label_name,
        is_default="N",
        created_by=created_by[:12],
    )
    session.add(new_wd)
    logger.info("Created new WebDisplay entry")


def find_feature_by_name(session: Session, name: str) -> Feature | None:
    """
    Find a feature by feature_name.

    Args:
        session: Database session
        name: Feature name to search for

    Returns:
        Feature object or None
    """
    return session.query(Feature).filter(
        Feature.feature_name == name.strip()
    ).first()


def create_feat_url_if_not_exists(
    session: Session,
    feature_no: int,
    url_no: int,
) -> bool:
    """
    Create FEAT_URL entry if it doesn't exist.

    Args:
        session: Database session
        feature_no: Foreign key to FEATURE
        url_no: Foreign key to URL

    Returns:
        True if created, False if already existed
    """
    existing = session.query(FeatUrl).filter(
        and_(
            FeatUrl.feature_no == feature_no,
            FeatUrl.url_no == url_no,
        )
    ).first()

    if existing:
        logger.debug("FEAT_URL entry already exists")
        return False

    new_entry = FeatUrl(
        feature_no=feature_no,
        url_no=url_no,
    )
    session.add(new_entry)
    return True


def parse_input_file(filepath: Path) -> list[str]:
    """
    Parse the input file containing feature names.

    Args:
        filepath: Path to input file

    Returns:
        List of feature names
    """
    features = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            # Skip header (first line)
            if line_num == 1:
                continue

            line = line.strip()
            if not line:
                continue

            # Handle multiple features separated by : or ;
            for name in re.split(r"[:;]", line):
                name = name.strip()
                if name:
                    features.append(name)

    logger.info(f"Parsed {len(features)} feature names from input file")
    return features


def load_feat_urls(
    session: Session,
    features: list[str],
    url_no: int,
) -> dict:
    """
    Load FEAT_URL entries for features.

    Args:
        session: Database session
        features: List of feature names
        url_no: URL number to link to

    Returns:
        Dictionary with statistics
    """
    stats = {
        "features_processed": 0,
        "feat_urls_created": 0,
        "features_not_found": 0,
        "warnings": [],
    }

    for feature_name in features:
        feature = find_feature_by_name(session, feature_name)

        if not feature:
            warning = f"Could not find feature in database: '{feature_name}'"
            logger.warning(warning)
            stats["warnings"].append(warning)
            stats["features_not_found"] += 1
            continue

        if not feature.feature_no:
            warning = f"No feature_no for feature: '{feature_name}'"
            logger.warning(warning)
            stats["warnings"].append(warning)
            stats["features_not_found"] += 1
            continue

        stats["features_processed"] += 1

        if create_feat_url_if_not_exists(session, feature.feature_no, url_no):
            stats["feat_urls_created"] += 1
            logger.info(f"Created FEAT_URL for '{feature_name}'")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load external link information via FEAT_URL table"
    )
    parser.add_argument(
        "url",
        help="URL template",
    )
    parser.add_argument(
        "source",
        help="URL source database name",
    )
    parser.add_argument(
        "url_type",
        help="Type of URL",
    )
    parser.add_argument(
        "label_name",
        help="Display label name",
    )
    parser.add_argument(
        "label_location",
        help="Location on the web page",
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input data file (one feature name per line)",
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

    logger.info(f"URL: {args.url}")
    logger.info(f"Source: {args.source}")
    logger.info(f"URL type: {args.url_type}")
    logger.info(f"Label name: {args.label_name}")
    logger.info(f"Label location: {args.label_location}")
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    features = parse_input_file(args.input_file)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(features)} features")
        return

    try:
        with SessionLocal() as session:
            # Create/get URL
            url_no = get_or_create_url(
                session,
                args.url,
                args.source,
                args.url_type,
                args.created_by,
            )

            # Create/get WebDisplay
            get_or_create_web_display(
                session,
                url_no,
                args.label_location,
                args.label_name,
                args.created_by,
            )

            # Load FEAT_URL entries
            stats = load_feat_urls(session, features, url_no)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Features processed: {stats['features_processed']}")
            logger.info(f"  FEAT_URLs created: {stats['feat_urls_created']}")
            if stats["features_not_found"] > 0:
                logger.warning(f"  Features not found: {stats['features_not_found']}")
            logger.info("=" * 50)

            if stats["warnings"]:
                logger.info(f"\nThere were {len(stats['warnings'])} warnings")

    except Exception as e:
        logger.error(f"Error loading external links: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
