#!/usr/bin/env python3
"""
Update PDF status column in reference table.

This script updates the pdf_status column for references:
- PMIDs with PDF available are updated to 'YP'
- PMIDs without PDF are updated to 'N'

Original Perl: loadPDFStatus.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Reference

load_dotenv()

logger = logging.getLogger(__name__)

# PDF status codes
CODE_PDF = 'YP'   # PDF available
CODE_TEXT = 'YT'  # Text conversion available
CODE_NONE = 'N'   # PDF not available


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


def load_pdf_list(data_file: Path) -> set:
    """
    Load list of PMIDs with PDF available.

    Args:
        data_file: File containing PDF filenames (PMID.pdf format)

    Returns:
        Set of PMIDs with PDF available
    """
    pmids = set()

    with open(data_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Extract PMID from filename like "12345.pdf"
            match = re.search(r'(\d+)\.pdf', line)
            if match:
                pmids.add(int(match.group(1)))

    return pmids


def update_pdf_status(
    session: Session,
    pdf_pmids: set,
    dry_run: bool = False,
) -> dict:
    """
    Update pdf_status for all references.

    Args:
        session: Database session
        pdf_pmids: Set of PMIDs with PDF available
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'pdf_updated': 0,
        'pdf_skipped': 0,
        'pdf_errors': 0,
        'no_pdf_updated': 0,
        'no_pdf_skipped': 0,
        'no_pdf_errors': 0,
        'not_found': 0,
    }

    # Update PMIDs with PDF available
    for pmid in pdf_pmids:
        ref = session.query(Reference).filter(
            Reference.pubmed == pmid
        ).first()

        if not ref:
            logger.debug(f"PMID {pmid} not found in database")
            stats['not_found'] += 1
            continue

        # Skip if already marked as PDF or text available
        if ref.pdf_status in (CODE_PDF, CODE_TEXT):
            logger.debug(f"Skip: PMID {pmid} pdf_status={ref.pdf_status}")
            stats['pdf_skipped'] += 1
            continue

        try:
            ref.pdf_status = CODE_PDF
            if not dry_run:
                session.flush()
            logger.info(f"Updated PMID {pmid} pdf_status to {CODE_PDF}")
            stats['pdf_updated'] += 1
        except Exception as e:
            logger.error(f"Error updating PMID {pmid}: {e}")
            stats['pdf_errors'] += 1

    # Update PMIDs without PDF (mark as N if not already)
    all_refs = session.query(Reference).filter(
        Reference.pubmed.isnot(None)
    ).all()

    for ref in all_refs:
        if ref.pubmed in pdf_pmids:
            continue

        if ref.pdf_status == CODE_NONE:
            stats['no_pdf_skipped'] += 1
            continue

        try:
            ref.pdf_status = CODE_NONE
            if not dry_run:
                session.flush()
            logger.debug(f"Updated PMID {ref.pubmed} pdf_status to {CODE_NONE}")
            stats['no_pdf_updated'] += 1
        except Exception as e:
            logger.error(f"Error updating PMID {ref.pubmed}: {e}")
            stats['no_pdf_errors'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update PDF status column in reference table"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="File containing PDF filenames (PMID.pdf format)",
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

    # Load PDF list
    pdf_pmids = load_pdf_list(args.data_file)
    logger.info(f"Loaded {len(pdf_pmids)} PMIDs with PDF")

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            stats = update_pdf_status(session, pdf_pmids, args.dry_run)

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  PMIDs with PDF updated: {stats['pdf_updated']}")
            logger.info(f"  PMIDs with PDF skipped: {stats['pdf_skipped']}")
            logger.info(f"  PMIDs with PDF errors: {stats['pdf_errors']}")
            logger.info(f"  PMIDs without PDF updated: {stats['no_pdf_updated']}")
            logger.info(f"  PMIDs without PDF skipped: {stats['no_pdf_skipped']}")
            logger.info(f"  PMIDs without PDF errors: {stats['no_pdf_errors']}")
            logger.info(f"  PMIDs not found: {stats['not_found']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
