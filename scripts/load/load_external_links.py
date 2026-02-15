#!/usr/bin/env python3
"""
Load external link information into the database.

This script loads external link information into the following tables:
- URL: The base URL template
- WEB_DISPLAY: How the link is displayed on web pages
- DBXREF: Database cross-references
- DBXREF_URL: Links between DBXREF and URL
- DBXREF_FEAT: Links between DBXREF and features (genes)

Input file format (tab-delimited):
- Column 1: CGD feature names (semicolon or colon separated for multiple)
- Column 2: External database ID
- Column 3: Description (optional)

Note: First line of input file is assumed to be a header and is skipped.

Original Perl: loadExternalLinks.pl
Author: Prachi Shah (Apr 3, 2008)
Converted to Python: 2024
"""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Dbxref, DbxrefFeat, DbxrefUrl, Feature, Url, WebDisplay

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
    created_by: str,
) -> int:
    """
    Get existing URL or create new one.

    Args:
        session: Database session
        url_template: URL template string
        source: URL source
        created_by: User creating the record

    Returns:
        url_no for the URL
    """
    # Check if URL exists
    existing = session.query(Url).filter(Url.url == url_template).first()
    if existing:
        logger.debug(f"Found existing URL with url_no={existing.url_no}")
        return existing.url_no

    # Create new URL
    new_url = Url(
        source=source,
        url_type="query by ID assigned by database",
        url=url_template,
        substitution_value="DBXREF",
        created_by=created_by[:12],
    )
    session.add(new_url)
    session.flush()  # Get the generated url_no

    logger.info(f"Created new URL with url_no={new_url.url_no}")
    return new_url.url_no


def get_or_create_web_display(
    session: Session,
    url_no: int,
    web_page_name: str,
    label_location: str,
    label_name: str,
    created_by: str,
) -> None:
    """
    Get existing WebDisplay or create new one.

    Args:
        session: Database session
        url_no: Foreign key to URL table
        web_page_name: Name of web page (e.g., "Locus")
        label_location: Location on the page
        label_name: Display label name
        created_by: User creating the record
    """
    # Check if web_display exists
    existing = session.query(WebDisplay).filter(
        and_(
            WebDisplay.web_page_name == web_page_name,
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

    # Create new WebDisplay
    new_wd = WebDisplay(
        url_no=url_no,
        web_page_name=web_page_name,
        label_location=label_location,
        label_type="Text",
        label_name=label_name,
        is_default="N",
        created_by=created_by[:12],
    )
    session.add(new_wd)
    logger.info("Created new WebDisplay entry")


def get_or_create_dbxref(
    session: Session,
    source: str,
    dbxref_type: str,
    dbxref_id: str,
    description: str | None,
    created_by: str,
) -> int:
    """
    Get existing Dbxref or create new one.

    Args:
        session: Database session
        source: Source database
        dbxref_type: Type of cross-reference
        dbxref_id: External database ID
        description: Optional description
        created_by: User creating the record

    Returns:
        dbxref_no for the Dbxref
    """
    # Check if dbxref exists
    existing = session.query(Dbxref).filter(
        and_(
            Dbxref.source == source,
            Dbxref.dbxref_type == dbxref_type,
            Dbxref.dbxref_id == dbxref_id,
        )
    ).first()

    if existing:
        logger.debug(f"Found existing Dbxref with dbxref_no={existing.dbxref_no}")
        return existing.dbxref_no

    # Create new Dbxref
    new_dbxref = Dbxref(
        source=source,
        dbxref_type=dbxref_type,
        dbxref_id=dbxref_id,
        description=description,
        created_by=created_by[:12],
    )
    session.add(new_dbxref)
    session.flush()

    logger.info(f"Created DBXREF for {dbxref_id} with dbxref_no={new_dbxref.dbxref_no}")
    return new_dbxref.dbxref_no


def create_dbxref_url_if_not_exists(
    session: Session,
    dbxref_no: int,
    url_no: int,
) -> bool:
    """
    Create DBXREF_URL entry if it doesn't exist.

    Args:
        session: Database session
        dbxref_no: Foreign key to DBXREF
        url_no: Foreign key to URL

    Returns:
        True if created, False if already existed
    """
    existing = session.query(DbxrefUrl).filter(
        and_(
            DbxrefUrl.dbxref_no == dbxref_no,
            DbxrefUrl.url_no == url_no,
        )
    ).first()

    if existing:
        logger.debug("DBXREF_URL entry already exists")
        return False

    new_entry = DbxrefUrl(
        dbxref_no=dbxref_no,
        url_no=url_no,
    )
    session.add(new_entry)
    logger.debug(f"Created DBXREF_URL for dbxref_no={dbxref_no}")
    return True


def find_feature_by_name(session: Session, name: str) -> Feature | None:
    """
    Find a feature by gene_name or feature_name.

    Args:
        session: Database session
        name: Name to search for

    Returns:
        Feature object or None
    """
    name = name.strip()

    # Try feature_name first
    feature = session.query(Feature).filter(Feature.feature_name == name).first()
    if feature:
        return feature

    # Try gene_name
    feature = session.query(Feature).filter(Feature.gene_name == name).first()
    return feature


def create_dbxref_feat_if_not_exists(
    session: Session,
    dbxref_no: int,
    feature_no: int,
) -> bool:
    """
    Create DBXREF_FEAT entry if it doesn't exist.

    Args:
        session: Database session
        dbxref_no: Foreign key to DBXREF
        feature_no: Foreign key to FEATURE

    Returns:
        True if created, False if already existed
    """
    existing = session.query(DbxrefFeat).filter(
        and_(
            DbxrefFeat.dbxref_no == dbxref_no,
            DbxrefFeat.feature_no == feature_no,
        )
    ).first()

    if existing:
        logger.debug("DBXREF_FEAT entry already exists")
        return False

    new_entry = DbxrefFeat(
        dbxref_no=dbxref_no,
        feature_no=feature_no,
    )
    session.add(new_entry)
    logger.debug(f"Created DBXREF_FEAT for dbxref_no={dbxref_no}, feature_no={feature_no}")
    return True


def parse_input_file(filepath: Path) -> list[dict]:
    """
    Parse the input tab-delimited file.

    Args:
        filepath: Path to input file

    Returns:
        List of dictionaries with cgd_feat, external_id, description
    """
    entries = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            # Skip header (first line)
            if line_num == 1:
                continue

            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                logger.warning(f"Line {line_num}: insufficient columns, skipping")
                continue

            entry = {
                "cgd_feat": parts[0] if len(parts) > 0 else "",
                "external_id": parts[1] if len(parts) > 1 else "",
                "description": parts[2] if len(parts) > 2 else None,
            }
            entries.append(entry)

    logger.info(f"Parsed {len(entries)} entries from input file")
    return entries


def load_external_links(
    session: Session,
    entries: list[dict],
    url_template: str,
    source: str,
    label_name: str,
    label_location: str,
    dbxref_type: str,
    web_page_name: str,
    created_by: str,
) -> dict:
    """
    Load external links into the database.

    Args:
        session: Database session
        entries: List of entry dictionaries
        url_template: URL template
        source: Source database name
        label_name: Display label
        label_location: Location on page
        dbxref_type: Type of DBXREF
        web_page_name: Web page name
        created_by: User performing the load

    Returns:
        Dictionary with statistics
    """
    stats = {
        "entries_processed": 0,
        "dbxrefs_created": 0,
        "dbxref_urls_created": 0,
        "dbxref_feats_created": 0,
        "features_not_found": 0,
        "warnings": [],
    }

    # Create/get URL
    url_no = get_or_create_url(session, url_template, source, created_by)

    # Create/get WebDisplay
    get_or_create_web_display(
        session, url_no, web_page_name, label_location, label_name, created_by
    )

    # Process each entry
    for entry in entries:
        external_id = entry["external_id"]
        cgd_feat = entry["cgd_feat"]
        description = entry["description"]

        if not external_id:
            continue

        stats["entries_processed"] += 1

        # Create/get DBXREF
        dbxref_no = get_or_create_dbxref(
            session, source, dbxref_type, external_id, description, created_by
        )
        if dbxref_no:
            stats["dbxrefs_created"] += 1

        # Create DBXREF_URL
        if create_dbxref_url_if_not_exists(session, dbxref_no, url_no):
            stats["dbxref_urls_created"] += 1

        # Process features if any
        if cgd_feat:
            # Split by colon or semicolon
            import re
            genes = re.split(r"[:;]", cgd_feat)

            for gene in genes:
                gene = gene.strip()
                if not gene:
                    continue

                feature = find_feature_by_name(session, gene)
                if not feature:
                    warning = f"Could not find gene in database: '{gene}'"
                    logger.warning(warning)
                    stats["warnings"].append(warning)
                    stats["features_not_found"] += 1
                    continue

                if create_dbxref_feat_if_not_exists(session, dbxref_no, feature.feature_no):
                    stats["dbxref_feats_created"] += 1
                    logger.info(f"Created DBXREF_FEAT for {dbxref_no}: '{gene}'")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load external link information into the database"
    )
    parser.add_argument(
        "url",
        help="URL template (use 'DBXREF' as placeholder for the external ID)",
    )
    parser.add_argument(
        "source",
        help="URL source database name",
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
        "dbxref_type",
        help="Type of database cross-reference",
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input data file (tab-delimited)",
    )
    parser.add_argument(
        "created_by",
        help="Database user name",
    )
    parser.add_argument(
        "--web-page-name",
        default="Locus",
        help="Web page name (default: Locus)",
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
    logger.info(f"Label name: {args.label_name}")
    logger.info(f"Label location: {args.label_location}")
    logger.info(f"DBXREF type: {args.dbxref_type}")
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Web page: {args.web_page_name}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    entries = parse_input_file(args.input_file)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(entries)} entries")
        return

    try:
        with SessionLocal() as session:
            stats = load_external_links(
                session,
                entries,
                args.url,
                args.source,
                args.label_name,
                args.label_location,
                args.dbxref_type,
                args.web_page_name,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Entries processed: {stats['entries_processed']}")
            logger.info(f"  DBXREFs created: {stats['dbxrefs_created']}")
            logger.info(f"  DBXREF_URLs created: {stats['dbxref_urls_created']}")
            logger.info(f"  DBXREF_FEATs created: {stats['dbxref_feats_created']}")
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
