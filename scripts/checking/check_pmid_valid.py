#!/usr/bin/env python3
"""
Validate PubMed IDs in the database.

This script checks if PubMed IDs in the database are valid by querying
NCBI's Entrez eutils service.

Original Perl: checkPmidValid.pl (Stan Dong)
Converted to Python: 2024

Usage:
    python check_pmid_valid.py --email user@example.com
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from Bio import Entrez
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
BATCH_SIZE = 200
DEFAULT_EMAIL = "cgd-admin@stanford.edu"


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


def get_pubmed_ids_from_db(session: Session) -> list[int]:
    """
    Get all PubMed IDs from the database.

    Args:
        session: Database session

    Returns:
        List of PubMed IDs
    """
    result = session.execute(
        text(f"""
            SELECT DISTINCT pubmed
            FROM {DB_SCHEMA}.reference
            WHERE pubmed IS NOT NULL
        """)
    )

    pubmed_ids = [row[0] for row in result if row[0]]
    logger.info(f"Found {len(pubmed_ids)} PubMed IDs in database")
    return pubmed_ids


def validate_pubmed_ids(
    pubmed_ids: list[int],
    email: str,
    batch_size: int = BATCH_SIZE,
) -> list[int]:
    """
    Validate PubMed IDs using NCBI Entrez.

    Args:
        pubmed_ids: List of PubMed IDs to validate
        email: Email for NCBI API
        batch_size: Number of IDs per batch

    Returns:
        List of invalid PubMed IDs
    """
    Entrez.email = email
    invalid_ids = []

    for i in range(0, len(pubmed_ids), batch_size):
        batch = pubmed_ids[i:i + batch_size]
        batch_str = [str(p) for p in batch]

        logger.debug(f"Checking batch {i}-{i + len(batch)}")

        try:
            # Search for the IDs
            handle = Entrez.esearch(
                db="pubmed",
                term=','.join(batch_str),
                retmax=batch_size,
            )
            results = Entrez.read(handle)
            handle.close()

            # Get the valid IDs returned
            valid_ids = set(int(uid) for uid in results.get('IdList', []))

            # Find invalid IDs (those not returned by search)
            for pmid in batch:
                if pmid not in valid_ids:
                    invalid_ids.append(pmid)
                    logger.warning(f"Invalid PubMed ID: {pmid}")

            # Be nice to NCBI
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"Error checking batch {i}: {e}")

    return invalid_ids


def check_pubmed_ids(
    email: str,
    dry_run: bool = False,
) -> dict:
    """
    Check all PubMed IDs in database for validity.

    Args:
        email: Email for NCBI API
        dry_run: If True, don't send notifications

    Returns:
        Stats dict
    """
    stats = {
        'total_ids': 0,
        'invalid_ids': [],
        'errors': 0,
    }

    with SessionLocal() as session:
        pubmed_ids = get_pubmed_ids_from_db(session)
        stats['total_ids'] = len(pubmed_ids)

        if not pubmed_ids:
            logger.info("No PubMed IDs to check")
            return stats

        invalid_ids = validate_pubmed_ids(pubmed_ids, email)
        stats['invalid_ids'] = invalid_ids

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate PubMed IDs in database using NCBI Entrez"
    )
    parser.add_argument(
        "--email",
        default=DEFAULT_EMAIL,
        help=f"Email for NCBI API (default: {DEFAULT_EMAIL})",
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
        help="Don't send notifications",
    )

    args = parser.parse_args()

    setup_logging(args.verbose, args.log_file)

    logger.info(f"Started at {datetime.now()}")

    try:
        stats = check_pubmed_ids(args.email, args.dry_run)

        logger.info("=" * 50)
        logger.info("Summary:")
        logger.info(f"  Total PubMed IDs checked: {stats['total_ids']}")
        logger.info(f"  Invalid IDs found: {len(stats['invalid_ids'])}")

        if stats['invalid_ids']:
            logger.info("Invalid PubMed IDs:")
            for pmid in stats['invalid_ids']:
                logger.info(f"  {pmid}")

        logger.info("=" * 50)

    except Exception as e:
        logger.exception(f"Error: {e}")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
