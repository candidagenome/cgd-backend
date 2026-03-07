#!/usr/bin/env python3
"""
Load new DBXREF URLs into database.

This script loads data into URL, DBXREF, DBXREF_URL, and DBXREF_FEAT tables
for external database cross-references.

Original Perl: newDBXREFurl.pl
Converted to Python: 2024
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Dbxref, DbxrefFeat, DbxrefUrl, Feature, Url

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


def get_dbxref_no(
    session: Session,
    dbxref_id: str,
    source: str,
) -> int | None:
    """Get dbxref_no for existing DBXREF."""
    dbxref = session.query(Dbxref).filter(
        and_(
            Dbxref.dbxref_id == dbxref_id,
            Dbxref.source == source,
        )
    ).first()

    return dbxref.dbxref_no if dbxref else None


def load_dbxref_urls(
    session: Session,
    data_file: Path,
    url_no: int,
    dbxref_source: str,
    dbxref_type: str,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Load DBXREF URLs from file.

    Args:
        session: Database session
        data_file: Data file (TSV: feature_name, dbxref_id, description)
        url_no: URL number to link
        dbxref_source: Source for DBXREF entries
        dbxref_type: Type for DBXREF entries
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'dbxref_inserted': 0,
        'dbxref_url_inserted': 0,
        'dbxref_feat_inserted': 0,
        'bad_features': 0,
        'errors': 0,
    }

    # Get feature mapping
    feature_map = get_feature_map(session)
    logger.info(f"Loaded {len(feature_map)} features")

    with open(data_file) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 2:
                continue

            feat_name = parts[0]
            dbxref_id = parts[1]
            description = parts[2] if len(parts) > 2 else ""

            stats['processed'] += 1

            # Get feature_no
            feat_no = feature_map.get(feat_name)
            if not feat_no:
                logger.warning(f"Bad feature name: {feat_name} (line {line_num})")
                stats['bad_features'] += 1
                continue

            # Get or create DBXREF
            dbxref_no = get_dbxref_no(session, dbxref_id, dbxref_source)

            if not dbxref_no:
                try:
                    dbxref = Dbxref(
                        dbxref_id=dbxref_id,
                        source=dbxref_source,
                        dbxref_type=dbxref_type,
                        description=description if description else None,
                        created_by=created_by,
                    )
                    session.add(dbxref)
                    session.flush()
                    dbxref_no = dbxref.dbxref_no
                    stats['dbxref_inserted'] += 1
                    logger.debug(f"DBXREF inserted: {dbxref_id}")

                except Exception as e:
                    logger.error(f"Error inserting DBXREF {dbxref_id}: {e}")
                    stats['errors'] += 1
                    continue

            # Insert DBXREF_URL
            try:
                # Check if already exists
                existing = session.query(DbxrefUrl).filter(
                    and_(
                        DbxrefUrl.dbxref_no == dbxref_no,
                        DbxrefUrl.url_no == url_no,
                    )
                ).first()

                if not existing:
                    dbxref_url = DbxrefUrl(
                        dbxref_no=dbxref_no,
                        url_no=url_no,
                    )
                    session.add(dbxref_url)
                    session.flush()
                    stats['dbxref_url_inserted'] += 1
                    logger.debug(f"DBXREF_URL inserted: {dbxref_no} -> {url_no}")

            except Exception as e:
                logger.error(f"Error inserting DBXREF_URL: {e}")
                stats['errors'] += 1
                continue

            # Insert DBXREF_FEAT
            try:
                # Check if already exists
                existing = session.query(DbxrefFeat).filter(
                    and_(
                        DbxrefFeat.dbxref_no == dbxref_no,
                        DbxrefFeat.feature_no == feat_no,
                    )
                ).first()

                if not existing:
                    dbxref_feat = DbxrefFeat(
                        dbxref_no=dbxref_no,
                        feature_no=feat_no,
                    )
                    session.add(dbxref_feat)
                    session.flush()
                    stats['dbxref_feat_inserted'] += 1
                    logger.debug(f"DBXREF_FEAT inserted: {dbxref_no} -> {feat_no}")
                else:
                    logger.debug(
                        f"DBXREF_FEAT already exists: {dbxref_no} -> {feat_no}"
                    )

            except Exception as e:
                logger.error(f"Error inserting DBXREF_FEAT: {e}")
                stats['errors'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load new DBXREF URLs into database"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="Data file (TSV: feature_name, dbxref_id, description)",
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
        "--dbxref-source",
        required=True,
        help="DBXREF source (e.g., 'NCBI')",
    )
    parser.add_argument(
        "--dbxref-type",
        required=True,
        help="DBXREF type (e.g., 'DNA version ID')",
    )
    parser.add_argument(
        "--created-by",
        required=True,
        help="User name for audit",
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

    if not args.url_no and not args.url:
        logger.error("Either --url or --url-no must be provided")
        sys.exit(1)

    if args.url and (not args.url_source or not args.url_type):
        logger.error("--url-source and --url-type required when creating new URL")
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
                    "DBXREF",
                    args.created_by,
                )

            stats = load_dbxref_urls(
                session,
                args.data_file,
                url_no,
                args.dbxref_source,
                args.dbxref_type,
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
            logger.info(f"  DBXREF_URL inserted: {stats['dbxref_url_inserted']}")
            logger.info(f"  DBXREF_FEAT inserted: {stats['dbxref_feat_inserted']}")
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
