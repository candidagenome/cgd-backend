#!/usr/bin/env python3
"""
Update PDF status for references by PMID.

This script updates the REFERENCE table PDF_STATUS field for a list of PMIDs.
Use this after downloading PDFs for papers to update their status.

Valid PDF statuses:
- N:   Need a PDF for this reference
- NAA: Unable to download the PDF for this reference automatically
- NAM: Unable to download the PDF for this reference manually
- NAP: Looking for a PDF for this reference is not applicable
- Y:   A PDF was downloaded successfully for this reference
- YF:  The PDF was downloaded but there is a problem converting into text
- YT:  The PDF was converted into text successfully and is now in the vault

Input file format:
- One PMID per line

Original Perl: update_PDFstatus_4_PMIDs.pl
Author: Prachi Shah (Mar 19, 2009)
Converted to Python: 2024
"""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Reference

load_dotenv()

logger = logging.getLogger(__name__)

# Valid PDF status values
VALID_STATUSES = {
    "N": "Need a PDF for this reference",
    "NAA": "Unable to download the PDF for this reference automatically",
    "NAM": "Unable to download the PDF for this reference manually",
    "NAP": "Looking for a PDF for this reference is not applicable",
    "Y": "A PDF was downloaded successfully for this reference",
    "YF": "The PDF was downloaded but there is a problem converting into text",
    "YT": "The PDF was converted into text successfully and is now in the vault",
}


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


def parse_pmid_file(filepath: Path) -> list[str]:
    """
    Parse file containing PMIDs.

    Args:
        filepath: Path to file with one PMID per line

    Returns:
        List of PMIDs
    """
    pmids = []

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # Extract numeric portion of PMID
            import re
            match = re.search(r"(\d+)", line)
            if match:
                pmids.append(match.group(1))

    logger.info(f"Parsed {len(pmids)} PMIDs from input file")
    return pmids


def update_pdf_status(
    session,
    pmids: list[str],
    status: str,
) -> dict:
    """
    Update PDF status for references by PMID.

    Args:
        session: Database session
        pmids: List of PMIDs to update
        status: New PDF status value

    Returns:
        Dictionary with statistics
    """
    stats = {
        "total": len(pmids),
        "updated": 0,
        "not_found": 0,
        "already_set": 0,
        "errors": [],
    }

    for pmid in pmids:
        ref = session.query(Reference).filter(
            Reference.pubmed == pmid
        ).first()

        if not ref:
            logger.warning(f"No reference found for PMID: {pmid}")
            stats["not_found"] += 1
            continue

        if ref.pdf_status == status:
            logger.debug(f"PMID {pmid} already has status '{status}'")
            stats["already_set"] += 1
            continue

        try:
            old_status = ref.pdf_status
            ref.pdf_status = status
            stats["updated"] += 1
            logger.info(f"Updated PMID {pmid}: '{old_status}' -> '{status}'")
        except Exception as e:
            error_msg = f"Error updating PMID {pmid}: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)

    return stats


def main():
    """Main entry point."""
    status_help = "\n".join(
        f"  {code}: {desc}" for code, desc in VALID_STATUSES.items()
    )

    parser = argparse.ArgumentParser(
        description="Update PDF status for references by PMID",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Valid PDF statuses:\n{status_help}",
    )
    parser.add_argument(
        "pmid_file",
        type=Path,
        help="File containing PMIDs (one per line)",
    )
    parser.add_argument(
        "status",
        choices=list(VALID_STATUSES.keys()),
        help="PDF status to set",
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
    if not args.pmid_file.exists():
        logger.error(f"Input file not found: {args.pmid_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.pmid_file}")
    logger.info(f"New status: {args.status} ({VALID_STATUSES[args.status]})")

    # Parse input file
    pmids = parse_pmid_file(args.pmid_file)

    if not pmids:
        logger.warning("No PMIDs found in input file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would update {len(pmids)} references to status '{args.status}'")
        return

    try:
        with SessionLocal() as session:
            stats = update_pdf_status(session, pmids, args.status)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Update Summary:")
            logger.info(f"  Total PMIDs in file: {stats['total']}")
            logger.info(f"  References updated: {stats['updated']}")
            logger.info(f"  Already had status: {stats['already_set']}")
            if stats["not_found"] > 0:
                logger.warning(f"  References not found: {stats['not_found']}")
            if stats["errors"]:
                logger.error(f"  Errors: {len(stats['errors'])}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error updating PDF status: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
