#!/usr/bin/env python3
"""
Load external links into database.

This script loads external link information into the database:
- DBXREF, DBXREF_URL, DBXREF_FEAT, URL, and WEB_DISPLAY tables

Used for loading pathway links, external database cross-references, etc.

Original Perl: loadExternalLinks.pl
Converted to Python: 2024
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Dbxref, DbxrefFeat, DbxrefUrl, Feature, Url, WebDisplay

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


def get_or_create_url(
    session: Session,
    url: str,
    source: str,
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

    # Create new URL
    url_obj = Url(
        url=url,
        source=source,
        url_type='query by ID assigned by database',
        substitution_value='DBXREF',
        created_by=created_by,
    )
    session.add(url_obj)
    session.flush()

    logger.info(f"URL inserted: {url}")
    return url_obj.url_no


def get_or_create_web_display(
    session: Session,
    url_no: int,
    label_name: str,
    created_by: str,
) -> None:
    """Create web_display entry if not exists."""
    existing = session.query(WebDisplay).filter(
        and_(
            WebDisplay.web_page_name == 'Locus',
            WebDisplay.label_location == 'External Links',
            WebDisplay.url_no == url_no,
            WebDisplay.label_name == label_name,
            WebDisplay.label_type == 'Text',
            WebDisplay.is_default == 'N',
        )
    ).first()

    if existing:
        return

    web_display = WebDisplay(
        url_no=url_no,
        web_page_name='Locus',
        label_location='External Links',
        label_type='Text',
        label_name=label_name,
        is_default='N',
        created_by=created_by,
    )
    session.add(web_display)
    session.flush()

    logger.info(f"Web_display inserted for label: {label_name}")


def find_feature(session: Session, query: str) -> Feature | None:
    """
    Find feature by name or gene name.

    Args:
        session: Database session
        query: Feature name or gene name to search

    Returns:
        Feature object or None
    """
    # Try exact match on feature_name
    feature = session.query(Feature).filter(
        Feature.feature_name == query.upper()
    ).first()

    if feature:
        return feature

    # Try gene_name
    feature = session.query(Feature).filter(
        Feature.gene_name == query.upper()
    ).first()

    return feature


def load_external_links(
    session: Session,
    data_file: Path,
    url: str,
    source: str,
    label_name: str,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Load external links from file.

    Args:
        session: Database session
        data_file: Data file (TSV: external_id, feature_names)
        url: URL template for the external resource
        source: Source name
        label_name: Label name for display
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'dbxref_inserted': 0,
        'dbxref_feat_inserted': 0,
        'warnings': 0,
        'errors': 0,
    }

    # Get or create URL
    url_no = get_or_create_url(session, url, source, created_by)

    # Create web_display entry
    get_or_create_web_display(session, url_no, label_name, created_by)

    with open(data_file) as f:
        for line_num, line in enumerate(f, 1):
            # Skip header
            if line_num == 1:
                continue

            line = line.strip()
            if not line:
                continue

            parts = line.split(None, 1)  # Split on whitespace
            if not parts:
                continue

            external_id = parts[0]
            features_str = parts[1] if len(parts) > 1 else ""

            stats['processed'] += 1

            # Insert DBXREF
            try:
                dbxref = Dbxref(
                    source=source,
                    dbxref_type=f'{label_name} ID',
                    dbxref_id=external_id,
                    created_by=created_by,
                )
                session.add(dbxref)
                session.flush()

                dbxref_no = dbxref.dbxref_no
                logger.debug(f"DBXREF inserted: {external_id}")
                stats['dbxref_inserted'] += 1

            except Exception as e:
                logger.error(f"Error inserting DBXREF {external_id}: {e}")
                stats['errors'] += 1
                continue

            # Insert DBXREF_URL
            try:
                dbxref_url = DbxrefUrl(
                    dbxref_no=dbxref_no,
                    url_no=url_no,
                )
                session.add(dbxref_url)
                session.flush()

            except Exception as e:
                logger.error(f"Error inserting DBXREF_URL: {e}")
                stats['errors'] += 1
                continue

            # Insert DBXREF_FEAT for associated features
            if features_str:
                # Features can be separated by : or |
                features = features_str.replace(':', '|').split('|')

                for feat_query in features:
                    feat_query = feat_query.strip()
                    if not feat_query:
                        continue

                    feature = find_feature(session, feat_query)

                    if not feature:
                        logger.warning(f"Feature not found: {feat_query}")
                        stats['warnings'] += 1
                        continue

                    if not feature.feature_no:
                        logger.warning(f"No feature_no for: {feat_query}")
                        stats['warnings'] += 1
                        continue

                    try:
                        dbxref_feat = DbxrefFeat(
                            dbxref_no=dbxref_no,
                            feature_no=feature.feature_no,
                        )
                        session.add(dbxref_feat)
                        session.flush()

                        logger.debug(
                            f"DBXREF_FEAT inserted: {external_id} -> {feat_query}"
                        )
                        stats['dbxref_feat_inserted'] += 1

                    except Exception as e:
                        logger.error(
                            f"Error inserting DBXREF_FEAT for {feat_query}: {e}"
                        )
                        stats['errors'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load external links into database"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="Data file (TSV: external_id, feature_names)",
    )
    parser.add_argument(
        "--url",
        required=True,
        help="URL template (use DBXREF as placeholder)",
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Source name (e.g., 'KEGG', 'Reactome')",
    )
    parser.add_argument(
        "--label",
        required=True,
        help="Label name for display",
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
    if not args.data_file.exists():
        logger.error(f"Data file not found: {args.data_file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            stats = load_external_links(
                session,
                args.data_file,
                args.url,
                args.source,
                args.label,
                args.created_by,
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
            logger.info(f"  DBXREF inserted: {stats['dbxref_inserted']}")
            logger.info(f"  DBXREF_FEAT inserted: {stats['dbxref_feat_inserted']}")
            logger.info(f"  Warnings: {stats['warnings']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
