#!/usr/bin/env python3
"""
Load headlines and name descriptions into the database.

This script loads headlines and name_descriptions for features,
and creates ref_link entries for associated references.

Original Perl: loadHeadlines.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, RefLink, Reference

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


def cleanup_text(text: str) -> str:
    """Clean up text by removing quotes and fixing apostrophes."""
    if not text:
        return ""

    text = text.strip()
    # Remove surrounding quotes
    text = re.sub(r'^"', '', text)
    text = re.sub(r'"$', '', text)
    # Fix double apostrophes
    text = text.replace("5''", "5'")
    text = text.replace("3''", "3'")

    return text


def get_reference_no(
    session: Session,
    ref_str: str,
    project_acronym: str = 'CGD',
) -> int | None:
    """
    Get reference_no from reference string.

    Args:
        session: Database session
        ref_str: Reference string (e.g., "PMID:12345" or "CGD:123")
        project_acronym: Project acronym for direct reference IDs

    Returns:
        reference_no or None if not found
    """
    ref_str = ref_str.strip()

    # Check for project reference ID (e.g., "CGD:123")
    match = re.match(rf'^{project_acronym}:(\d+)$', ref_str)
    if match:
        return int(match.group(1))

    # Check for PMID
    match = re.match(r'^PMID:(\d+)$', ref_str)
    if match:
        pmid = int(match.group(1))
        ref = session.query(Reference).filter(
            Reference.pubmed == pmid
        ).first()

        if ref:
            return ref.reference_no
        else:
            logger.warning(f"No reference found for PMID: {pmid}")
            return None

    logger.warning(f"Invalid reference format: {ref_str}")
    return None


def insert_ref_links(
    session: Session,
    ref_ids: list,
    col_name: str,
    feature_no: int,
    project_acronym: str,
    created_by: str,
) -> int:
    """
    Insert ref_link entries for references.

    Args:
        session: Database session
        ref_ids: List of reference strings
        col_name: Column name (NAME_DESCRIPTION or HEADLINE)
        feature_no: Feature number
        project_acronym: Project acronym
        created_by: User name for audit

    Returns:
        Number of ref_links inserted
    """
    num_inserted = 0

    for ref_str in ref_ids:
        ref_str = ref_str.strip()
        if not ref_str:
            continue

        reference_no = get_reference_no(session, ref_str, project_acronym)
        if reference_no is None:
            continue

        # Check if ref_link already exists
        existing = session.query(RefLink).filter(
            and_(
                RefLink.reference_no == reference_no,
                RefLink.tab_name == 'FEATURE',
                RefLink.col_name == col_name,
                RefLink.primary_key == feature_no,
            )
        ).first()

        if existing:
            logger.debug(
                f"Ref_link exists: reference_no={reference_no}, "
                f"feature_no={feature_no}, column={col_name}"
            )
            continue

        # Insert new ref_link
        ref_link = RefLink(
            reference_no=reference_no,
            tab_name='FEATURE',
            col_name=col_name,
            primary_key=feature_no,
            created_by=created_by,
        )
        session.add(ref_link)

        logger.info(
            f"Inserted ref_link: reference_no={reference_no}, "
            f"feature_no={feature_no}, column={col_name}"
        )
        num_inserted += 1

    return num_inserted


def load_headlines(
    session: Session,
    data_file: Path,
    created_by: str,
    project_acronym: str = 'CGD',
    dry_run: bool = False,
) -> dict:
    """
    Load headlines and name descriptions from file.

    Args:
        session: Database session
        data_file: Data file path
        created_by: User name for audit
        project_acronym: Project acronym
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'features_updated': 0,
        'ref_links_inserted': 0,
        'not_found': 0,
        'errors': 0,
    }

    with open(data_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 5:
                logger.warning(f"Invalid line format: {line}")
                continue

            feat_name = parts[0].upper()
            name_desc = cleanup_text(parts[1])
            nd_refs = parts[2] if len(parts) > 2 else ""
            headline = cleanup_text(parts[3]) if len(parts) > 3 else ""
            hl_refs = parts[4] if len(parts) > 4 else ""

            stats['processed'] += 1

            # Find feature
            feature = session.query(Feature).filter(
                Feature.feature_name == feat_name
            ).first()

            if not feature:
                logger.warning(f"Feature not found: {feat_name}")
                stats['not_found'] += 1
                continue

            # Update feature
            updated = False
            if headline:
                feature.headline = headline
                updated = True

            if name_desc:
                feature.name_description = name_desc
                updated = True

            if updated:
                try:
                    if not dry_run:
                        session.flush()
                    logger.info(
                        f"Updated feature: {feat_name} (feature_no={feature.feature_no})"
                    )
                    stats['features_updated'] += 1
                except Exception as e:
                    logger.error(f"Error updating feature {feat_name}: {e}")
                    stats['errors'] += 1

            # Insert ref_links for name_description
            if nd_refs:
                nd_ref_list = nd_refs.split('|')
                count = insert_ref_links(
                    session, nd_ref_list, 'NAME_DESCRIPTION',
                    feature.feature_no, project_acronym, created_by
                )
                stats['ref_links_inserted'] += count

            # Insert ref_links for headline
            if hl_refs:
                hl_ref_list = hl_refs.split('|')
                count = insert_ref_links(
                    session, hl_ref_list, 'HEADLINE',
                    feature.feature_no, project_acronym, created_by
                )
                stats['ref_links_inserted'] += count

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load headlines and name descriptions into database"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="Data file (TSV: FeatureName, NameDesc, NDRefs, Headline, HLRefs)",
    )
    parser.add_argument(
        "--created-by",
        default="SCRIPT",
        help="User name for audit (default: SCRIPT)",
    )
    parser.add_argument(
        "--project-acronym",
        default="CGD",
        help="Project acronym for reference IDs (default: CGD)",
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
            stats = load_headlines(
                session,
                args.data_file,
                args.created_by,
                args.project_acronym,
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
            logger.info(f"  Features updated: {stats['features_updated']}")
            logger.info(f"  Ref_links inserted: {stats['ref_links_inserted']}")
            logger.info(f"  Not found: {stats['not_found']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
