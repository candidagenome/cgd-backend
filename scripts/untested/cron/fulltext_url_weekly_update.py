#!/usr/bin/env python3
"""
Weekly update of full text URLs from NCBI.

This script retrieves full text URLs for recently added references using
NCBI's ELink utility and stores them in the database.

It processes references added within the last 10 days, retrieves their
full text URLs from NCBI LINKOUT, and updates the URL and REF_URL tables.

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
    CURATOR_EMAIL: Email for notifications
    NCBI_EMAIL: Email for NCBI E-utilities
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "admin@candidagenome.org")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
ADMIN_USER = os.getenv("ADMIN_USER", "ADMIN")

# NCBI E-utilities
ELINK_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"

# How many days back to look for new references
DAYS_LOOKBACK = 10

# Batch size for NCBI queries
BATCH_SIZE = 500

# Configure logging
LOG_FILE = LOG_DIR / "load" / "NCBIfulltextURL.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def send_notification(message: str) -> None:
    """Send notification email."""
    if CURATOR_EMAIL:
        logger.info(f"Notification: {message}")
        # In production, implement actual email sending


def get_recent_pubmed_ids(session, days: int = DAYS_LOOKBACK) -> list[int]:
    """
    Get PubMed IDs for references added in the last N days.

    Args:
        session: Database session
        days: Number of days to look back

    Returns:
        List of PubMed IDs
    """
    query = text(f"""
        SELECT DISTINCT pubmed
        FROM {DB_SCHEMA}.reference
        WHERE pubmed IS NOT NULL
        AND (SYSDATE - date_created) < :days
    """)

    result = session.execute(query, {"days": days})
    return [row[0] for row in result if row[0]]


def get_fulltext_urls_from_ncbi(pmids: list[int]) -> dict[int, list[str]]:
    """
    Get full text URLs from NCBI ELink.

    Args:
        pmids: List of PubMed IDs

    Returns:
        Dictionary mapping PMID to list of URLs
    """
    if not pmids:
        return {}

    params = {
        "dbfrom": "pubmed",
        "cmd": "llinks",
        "id": ",".join(str(p) for p in pmids),
        "email": NCBI_EMAIL,
        "tool": PROJECT_ACRONYM,
    }

    try:
        response = requests.get(ELINK_URL, params=params, timeout=60)
        response.raise_for_status()

        # Parse response for full text URLs
        # This is a simplified parser - in production, use proper XML parsing
        urls_by_pmid: dict[int, list[str]] = {}

        content = response.text

        # Look for URL patterns in the response
        import re

        # Find IdUrlSet blocks
        for pmid in pmids:
            urls_by_pmid[pmid] = []

            # Look for full text provider URLs
            # Pattern varies by provider
            patterns = [
                rf'<Id>{pmid}</Id>.*?<Url[^>]*>([^<]+)</Url>',
                rf'<ObjUrl>.*?<Url>([^<]+)</Url>.*?</ObjUrl>',
            ]

            for pattern in patterns:
                matches = re.findall(pattern, content, re.DOTALL)
                for url in matches:
                    # Filter for full text URLs (exclude PubMed, abstract-only links)
                    if _is_fulltext_url(url):
                        urls_by_pmid[pmid].append(url)

        return urls_by_pmid

    except requests.RequestException as e:
        logger.error(f"Error fetching from NCBI: {e}")
        return {}


def _is_fulltext_url(url: str) -> bool:
    """Check if URL appears to be a full text link."""
    # Skip known non-fulltext URLs
    skip_patterns = [
        "pubmed",
        "ncbi.nlm.nih.gov/pubmed",
        "abstract",
        "ncbi.nlm.nih.gov/pmc",  # PMC is handled separately
    ]

    url_lower = url.lower()
    for pattern in skip_patterns:
        if pattern in url_lower:
            return False

    # Accept URLs from known full text providers
    fulltext_patterns = [
        "doi.org",
        "sciencedirect",
        "springer",
        "wiley",
        "nature.com",
        "cell.com",
        "asm.org",
        "plos",
        "frontiersin",
        "mdpi",
        "oxford",
        "cambridge",
        "tandfonline",
        "karger",
        "jbc.org",
        "pnas.org",
    ]

    for pattern in fulltext_patterns:
        if pattern in url_lower:
            return True

    return False


def get_existing_urls(session) -> tuple[dict, dict, dict]:
    """
    Get existing URLs, ref_url associations, and reference numbers.

    Returns:
        Tuple of (url_dict, ref_url_dict, ref_no_dict)
    """
    # Get existing LINKOUT URLs
    url_query = text(f"""
        SELECT url, url_no
        FROM {DB_SCHEMA}.url
        WHERE url_type = 'Reference LINKOUT'
    """)
    url_result = session.execute(url_query)
    url_dict = {row[0]: row[1] for row in url_result}

    # Get existing ref_url associations
    ref_url_query = text(f"""
        SELECT reference_no, url_no
        FROM {DB_SCHEMA}.ref_url
    """)
    ref_url_result = session.execute(ref_url_query)
    ref_url_dict = {f"{row[0]}::{row[1]}": True for row in ref_url_result}

    # Get reference numbers by pubmed
    ref_no_query = text(f"""
        SELECT pubmed, reference_no
        FROM {DB_SCHEMA}.reference
        WHERE pubmed IS NOT NULL
    """)
    ref_no_result = session.execute(ref_no_query)
    ref_no_dict = {row[0]: row[1] for row in ref_no_result}

    return url_dict, ref_url_dict, ref_no_dict


def update_fulltext_urls() -> dict:
    """
    Main function to update full text URLs.

    Returns:
        Statistics dictionary
    """
    stats = {
        "pmids_checked": 0,
        "urls_found": 0,
        "urls_inserted": 0,
        "ref_urls_inserted": 0,
        "errors": 0,
    }

    logger.info(f"Starting full text URL weekly update at {datetime.now()}")

    try:
        with SessionLocal() as session:
            # Get recent PubMed IDs
            pmids = get_recent_pubmed_ids(session, DAYS_LOOKBACK)
            stats["pmids_checked"] = len(pmids)

            if not pmids:
                logger.info("No new references found in the last 10 days")
                send_notification("No new fulltext URLs retrieved from NCBI this week.")
                return stats

            logger.info(f"Found {len(pmids)} recent references to check")

            # Get existing data
            url_dict, ref_url_dict, ref_no_dict = get_existing_urls(session)

            # Process in batches
            all_urls: dict[int, list[str]] = {}

            for i in range(0, len(pmids), BATCH_SIZE):
                batch = pmids[i : i + BATCH_SIZE]
                logger.info(f"Processing batch {i // BATCH_SIZE + 1}")

                urls = get_fulltext_urls_from_ncbi(batch)
                all_urls.update(urls)

                # Rate limit
                time.sleep(0.5)

            # Count URLs found
            for pmid, urls in all_urls.items():
                stats["urls_found"] += len(urls)

            # Insert new URLs
            for pmid, urls in all_urls.items():
                ref_no = ref_no_dict.get(pmid)
                if not ref_no:
                    continue

                for url in urls:
                    try:
                        # Insert URL if new
                        if url not in url_dict:
                            insert_url = text(f"""
                                INSERT INTO {DB_SCHEMA}.url
                                (url, url_type, source, date_created, created_by)
                                VALUES (:url, 'Reference LINKOUT', :source, SYSDATE, :user)
                            """)

                            session.execute(
                                insert_url,
                                {"url": url, "source": PROJECT_ACRONYM, "user": ADMIN_USER},
                            )
                            session.commit()
                            stats["urls_inserted"] += 1

                            # Get the new url_no
                            get_url_no = text(f"""
                                SELECT url_no FROM {DB_SCHEMA}.url
                                WHERE url = :url AND url_type = 'Reference LINKOUT'
                            """)
                            result = session.execute(get_url_no, {"url": url}).first()
                            if result:
                                url_dict[url] = result[0]

                        url_no = url_dict.get(url)
                        if not url_no:
                            continue

                        # Insert ref_url if new
                        check_key = f"{ref_no}::{url_no}"
                        if check_key not in ref_url_dict:
                            insert_ref_url = text(f"""
                                INSERT INTO {DB_SCHEMA}.ref_url (reference_no, url_no)
                                VALUES (:ref_no, :url_no)
                            """)

                            session.execute(
                                insert_ref_url,
                                {"ref_no": ref_no, "url_no": url_no},
                            )
                            session.commit()
                            stats["ref_urls_inserted"] += 1
                            ref_url_dict[check_key] = True

                    except Exception as e:
                        session.rollback()
                        logger.error(f"Error inserting URL for PMID {pmid}: {e}")
                        stats["errors"] += 1

    except Exception as e:
        logger.exception(f"Error in update_fulltext_urls: {e}")
        stats["errors"] += 1

    return stats


def main() -> int:
    """Main entry point."""
    logger.info("=" * 60)
    logger.info(f"Full Text URL Weekly Update - {datetime.now()}")
    logger.info("=" * 60)

    stats = update_fulltext_urls()

    # Log summary
    logger.info("")
    logger.info("SUMMARY:")
    logger.info(f"  PubMed IDs checked: {stats['pmids_checked']}")
    logger.info(f"  URLs found: {stats['urls_found']}")
    logger.info(f"  URLs inserted: {stats['urls_inserted']}")
    logger.info(f"  Ref-URL links inserted: {stats['ref_urls_inserted']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("")
    logger.info(f"Finished at {datetime.now()}")

    if stats["urls_found"] == 0:
        send_notification("No new fulltext URLs retrieved from NCBI this week.")

    return 0 if stats["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
