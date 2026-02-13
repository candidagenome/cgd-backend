#!/usr/bin/env python3
"""
Check and update ePub ahead-of-print references.

This script checks references marked as 'Epub ahead of print' in the database
and updates them to 'Published' when they become fully published in PubMed.

It fetches the latest publication status from NCBI and updates citation info.

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Directory for log files (default: /tmp)
    NCBI_EMAIL: Email for NCBI E-utilities (required by NCBI)
"""

import logging
import os
import re
import sys
import time
from datetime import datetime
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
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
LOG_FILE = LOG_DIR / "EPubUpdate.log"
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "admin@candidagenome.org")

# NCBI E-utilities base URL
NCBI_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def get_medline_content(pmid: int) -> str | None:
    """
    Fetch Medline content from NCBI for a PubMed ID.

    Args:
        pmid: PubMed ID

    Returns:
        Medline format content or None on error
    """
    params = {
        "db": "pubmed",
        "id": str(pmid),
        "rettype": "medline",
        "retmode": "text",
        "email": NCBI_EMAIL,
    }

    try:
        response = requests.get(NCBI_EFETCH_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.text

    except requests.RequestException as e:
        logger.error(f"Error fetching PMID {pmid}: {e}")
        return None


def parse_field(content: str, field: str) -> str | None:
    """Parse a field from Medline format content."""
    pattern = rf"^{field}\s*-\s*(.+?)(?=^[A-Z]{{2,4}}\s*-|\Z)"
    match = re.search(pattern, content, re.MULTILINE | re.DOTALL)
    if match:
        return " ".join(match.group(1).split())
    return None


def parse_publication_status(content: str) -> str | None:
    """Parse publication status (PST field) from Medline content."""
    return parse_field(content, "PST")


def parse_authors(content: str) -> list[str]:
    """Parse authors from Medline content."""
    authors = []
    for match in re.finditer(r"^AU\s*-\s*(.+)$", content, re.MULTILINE):
        authors.append(match.group(1).strip())
    return authors


def parse_date_published(content: str) -> str | None:
    """Parse date published (DP field) from Medline content."""
    return parse_field(content, "DP")


def parse_year(date_published: str | None) -> str | None:
    """Extract year from date published string."""
    if not date_published:
        return None
    match = re.match(r"(\d{4})", date_published)
    return match.group(1) if match else None


def create_citation(
    year: str | None,
    title: str | None,
    journal: str | None,
    volume: str | None,
    issue: str | None,
    pages: str | None,
    authors: list[str],
) -> str | None:
    """Create a citation string from publication details."""
    if not all([year, title, journal]):
        return None

    # Format authors
    if len(authors) == 0:
        author_str = ""
    elif len(authors) == 1:
        author_str = authors[0]
    elif len(authors) == 2:
        author_str = f"{authors[0]} and {authors[1]}"
    else:
        author_str = f"{authors[0]}, et al."

    # Build citation
    parts = [author_str, f"({year})", title, journal]

    if volume:
        vol_str = volume
        if issue:
            vol_str += f"({issue})"
        parts.append(vol_str)

    if pages:
        parts.append(pages)

    return " ".join(filter(None, parts))


def check_epubs() -> dict:
    """
    Check and update ePub ahead-of-print references.

    Returns:
        Dictionary with counts of checked, updated, unchanged, skipped, errors
    """
    stats = {
        "checked": 0,
        "updated": 0,
        "unchanged": 0,
        "skipped": 0,
        "errors": 0,
    }

    logger.info("Starting ePub check")
    logger.info("Retrieving Epub info from database")

    try:
        with SessionLocal() as session:
            # Get all ePub references
            epub_query = text(f"""
                SELECT pubmed
                FROM {DB_SCHEMA}.reference
                WHERE status = 'Epub ahead of print'
            """)
            result = session.execute(epub_query)
            epubs = [row[0] for row in result]

            logger.info(f"Found {len(epubs)} Epub references to check")

            for pmid in epubs:
                stats["checked"] += 1

                # Rate limit NCBI requests
                time.sleep(0.34)  # ~3 requests per second max

                content = get_medline_content(pmid)
                if not content:
                    logger.warning(f"Could not fetch content for PMID {pmid}")
                    stats["errors"] += 1
                    continue

                pst = parse_publication_status(content)

                # Check if still ahead of print
                if pst and "aheadofprint" in pst.lower():
                    logger.info(f"PMID {pmid} is still ahead of print, no update")
                    stats["unchanged"] += 1
                    continue

                logger.info(f"PMID {pmid} is published, updating")

                # Parse publication details
                date_published = parse_date_published(content)
                year = parse_year(date_published)
                authors = parse_authors(content)
                title = parse_field(content, "TI")
                journal = parse_field(content, "TA")
                volume = parse_field(content, "VI")
                issue = parse_field(content, "IP")
                pages = parse_field(content, "PG")
                date_revised = parse_field(content, "LR")

                # Handle year spanning two years (e.g., "2023-2024")
                if year and re.match(r"^\d{4}-\d{4}$", year):
                    year = year[:4]

                citation = create_citation(
                    year, title, journal, volume, issue, pages, authors
                )

                if not citation:
                    logger.warning(f"PMID {pmid}: cannot create citation, skipping")
                    stats["skipped"] += 1
                    continue

                # Update the reference
                try:
                    update_query = text(f"""
                        UPDATE {DB_SCHEMA}.reference
                        SET status = 'Published',
                            citation = :citation,
                            year = :year,
                            date_published = :date_published,
                            date_revised = :date_revised,
                            issue = :issue,
                            page = :pages,
                            volume = :volume,
                            title = :title,
                            pdf_status = 'N'
                        WHERE pubmed = :pmid
                    """)

                    session.execute(
                        update_query,
                        {
                            "citation": citation,
                            "year": year,
                            "date_published": date_published,
                            "date_revised": date_revised,
                            "issue": issue,
                            "pages": pages,
                            "volume": volume,
                            "title": title,
                            "pmid": pmid,
                        },
                    )
                    session.commit()

                    logger.info(f"PMID {pmid} successfully updated to 'Published'")
                    stats["updated"] += 1

                except Exception as e:
                    session.rollback()
                    logger.error(f"PMID {pmid} error during update: {e}")
                    stats["errors"] += 1

    except Exception as e:
        logger.exception(f"Error in check_epubs: {e}")

    return stats


def main() -> int:
    """Main entry point."""
    logger.info(f"Program {__file__}: Starting {datetime.now()}")

    stats = check_epubs()

    # Log summary
    logger.info("")
    logger.info("=" * 50)
    logger.info("SUMMARY")
    logger.info("=" * 50)
    logger.info(f"{stats['checked']} Epubs checked")
    logger.info(f"{stats['updated']} successfully updated to 'Published'")
    logger.info(f"{stats['unchanged']} remain unchanged")
    logger.info(f"{stats['skipped']} skipped (citation cannot be created)")
    logger.info(f"{stats['errors']} generated errors during update")
    logger.info("")
    logger.info(f"Exiting: {datetime.now()}")

    return 0 if stats["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
