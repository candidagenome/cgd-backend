#!/usr/bin/env python3
"""
Update full text URLs for references from NCBI.

This script retrieves full text URLs for recently added references using
NCBI's ELink utility and stores them in the URL and REF_URL tables.

Based on fullTextUrlWeeklyUpdate.pl by Stan Dong (11/2001)

Usage:
    python fulltext_url_update.py
    python fulltext_url_update.py --days 10

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Data directory
    LOG_DIR: Log directory
    NCBI_EMAIL: Email for NCBI E-utilities
    ADMIN_USER: Admin username
    CURATOR_EMAIL: Email for notifications
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
"""

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from Bio import Entrez
from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "admin@candidagenome.org")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL", "")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
CONF_DIR = Path(os.getenv("CONF_DIR", "/etc/cgd"))

# Default number of days to look back for recent references
DEFAULT_DAYS = 10

# Batch size for NCBI queries
BATCH_SIZE = 500

# Configure Entrez
Entrez.email = NCBI_EMAIL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def read_bad_urls(conf_dir: Path) -> set[str]:
    """
    Read list of bad URLs to skip.

    Args:
        conf_dir: Configuration directory

    Returns:
        Set of bad URL patterns
    """
    bad_url_file = conf_dir / "badURL.lookup"
    bad_urls = set()

    if bad_url_file.exists():
        with open(bad_url_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    bad_urls.add(line)

    return bad_urls


def is_bad_url(url: str, bad_urls: set[str]) -> bool:
    """Check if URL matches any bad URL pattern."""
    for pattern in bad_urls:
        if pattern in url:
            return True
    return False


def get_fulltext_url_by_elink(pmid: int) -> str | None:
    """
    Get full text URL for a PubMed ID using ELink.

    Args:
        pmid: PubMed ID

    Returns:
        Full text URL or None
    """
    try:
        handle = Entrez.elink(
            dbfrom="pubmed",
            id=str(pmid),
            cmd="prlinks"
        )
        record = Entrez.read(handle)
        handle.close()

        # Extract URL from LinkSet
        for linkset in record:
            if "IdUrlList" in linkset:
                for id_url in linkset["IdUrlList"].get("IdUrlSet", []):
                    if "ObjUrl" in id_url:
                        for obj_url in id_url["ObjUrl"]:
                            url = obj_url.get("Url")
                            if url:
                                return url
        return None

    except Exception as e:
        logger.debug(f"ELink error for pmid={pmid}: {e}")
        return None


class FullTextUrlUpdater:
    """Update full text URLs for references."""

    def __init__(self, session, days: int = DEFAULT_DAYS):
        self.session = session
        self.days = days
        self.bad_urls = read_bad_urls(CONF_DIR)

        # Counters
        self.url_insert_count = 0
        self.url_bad_count = 0
        self.ref_url_insert_count = 0
        self.ref_url_bad_count = 0
        self.pubmed_url_delete_count = 0
        self.pubmed_url_delete_bad_count = 0

        # Caches
        self.linkout_urls: dict[str, int] = {}  # url -> url_no
        self.ref_urls: set[str] = set()  # "ref_no::url_no"
        self.ref_no_map: dict[int, int] = {}  # pubmed -> reference_no

    def get_recent_pubmeds(self) -> list[int]:
        """Get PubMed IDs for references added in the last N days."""
        query = text(f"""
            SELECT DISTINCT pubmed
            FROM {DB_SCHEMA}.reference
            WHERE date_created > :cutoff_date
            AND pubmed IS NOT NULL
        """)

        cutoff = datetime.now() - timedelta(days=self.days)
        result = self.session.execute(query, {"cutoff_date": cutoff})

        pmids = [row[0] for row in result if row[0]]
        logger.info(f"Found {len(pmids)} recent references (last {self.days} days)")
        return pmids

    def load_existing_data(self) -> None:
        """Load existing URLs and ref_url associations."""
        # Load existing LINKOUT URLs
        url_query = text(f"""
            SELECT url, url_no FROM {DB_SCHEMA}.url
            WHERE url_type = 'Reference LINKOUT'
        """)
        result = self.session.execute(url_query)
        for url, url_no in result:
            self.linkout_urls[url] = url_no

        # Load existing ref_url associations
        ref_url_query = text(f"""
            SELECT reference_no, url_no FROM {DB_SCHEMA}.ref_url
        """)
        result = self.session.execute(ref_url_query)
        for ref_no, url_no in result:
            self.ref_urls.add(f"{ref_no}::{url_no}")

        # Load reference_no -> pubmed mapping
        ref_no_query = text(f"""
            SELECT reference_no, pubmed FROM {DB_SCHEMA}.reference
            WHERE pubmed IS NOT NULL
        """)
        result = self.session.execute(ref_no_query)
        for ref_no, pubmed in result:
            self.ref_no_map[pubmed] = ref_no

        logger.info(f"Loaded {len(self.linkout_urls)} existing LINKOUT URLs")
        logger.info(f"Loaded {len(self.ref_urls)} existing ref_url associations")

    def get_url_no(self, url: str) -> int | None:
        """Get url_no for a URL."""
        query = text(f"""
            SELECT url_no FROM {DB_SCHEMA}.url
            WHERE url_type = 'Reference LINKOUT' AND url = :url
        """)
        result = self.session.execute(query, {"url": url}).first()
        return result[0] if result else None

    def insert_url(self, url: str) -> int | None:
        """Insert a new URL and return url_no."""
        try:
            insert_sql = text(f"""
                INSERT INTO {DB_SCHEMA}.url
                (url, url_type, source, date_created, created_by)
                VALUES (:url, 'Reference LINKOUT', :source, CURRENT_TIMESTAMP, :user)
            """)
            self.session.execute(insert_sql, {
                "url": url,
                "source": PROJECT_ACRONYM,
                "user": ADMIN_USER,
            })
            self.session.commit()

            url_no = self.get_url_no(url)
            self.url_insert_count += 1
            self.linkout_urls[url] = url_no
            return url_no

        except Exception as e:
            logger.error(f"Error inserting URL {url}: {e}")
            self.session.rollback()
            self.url_bad_count += 1
            return None

    def insert_ref_url(self, ref_no: int, url_no: int) -> bool:
        """Insert a ref_url association."""
        key = f"{ref_no}::{url_no}"
        if key in self.ref_urls:
            return True

        try:
            insert_sql = text(f"""
                INSERT INTO {DB_SCHEMA}.ref_url (reference_no, url_no)
                VALUES (:ref_no, :url_no)
            """)
            self.session.execute(insert_sql, {"ref_no": ref_no, "url_no": url_no})
            self.session.commit()

            self.ref_url_insert_count += 1
            self.ref_urls.add(key)
            return True

        except Exception as e:
            logger.error(f"Error inserting ref_url {ref_no}::{url_no}: {e}")
            self.session.rollback()
            self.ref_url_bad_count += 1
            return False

    def remove_pubmed_urls(self, ref_no: int) -> None:
        """Remove PubMed URLs (full text) for a reference if LINKOUT exists."""
        # Get URLs to delete
        query = text(f"""
            SELECT u.url_no
            FROM {DB_SCHEMA}.url u
            JOIN {DB_SCHEMA}.ref_url r ON r.url_no = u.url_no
            WHERE r.reference_no = :ref_no
            AND u.url_type = 'Reference full text'
        """)

        result = self.session.execute(query, {"ref_no": ref_no})
        urls_to_delete = [row[0] for row in result]

        for url_no in urls_to_delete:
            try:
                delete_sql = text(f"""
                    DELETE FROM {DB_SCHEMA}.url WHERE url_no = :url_no
                """)
                self.session.execute(delete_sql, {"url_no": url_no})
                self.session.commit()
                self.pubmed_url_delete_count += 1

            except Exception as e:
                logger.error(f"Error deleting URL {url_no}: {e}")
                self.session.rollback()
                self.pubmed_url_delete_bad_count += 1

    def process_pmids(self, pmids: list[int]) -> dict[int, str]:
        """
        Process PubMed IDs and retrieve full text URLs.

        Args:
            pmids: List of PubMed IDs

        Returns:
            Dict of pmid -> url
        """
        urls_found: dict[int, str] = {}

        for i, pmid in enumerate(pmids):
            if (i + 1) % 100 == 0:
                logger.info(f"Processing {i + 1}/{len(pmids)}...")

            url = get_fulltext_url_by_elink(pmid)

            if url and not is_bad_url(url, self.bad_urls):
                urls_found[pmid] = url

        logger.info(f"Found {len(urls_found)} full text URLs")
        return urls_found

    def load_urls(self, urls_found: dict[int, str]) -> None:
        """Load URLs into database."""
        for pmid, url in urls_found.items():
            ref_no = self.ref_no_map.get(pmid)
            if not ref_no:
                continue

            # Get or insert URL
            url_no = self.linkout_urls.get(url)
            if not url_no:
                url_no = self.insert_url(url)
                if not url_no:
                    continue

            # Remove any existing PubMed URLs
            self.remove_pubmed_urls(ref_no)

            # Insert ref_url association
            self.insert_ref_url(ref_no, url_no)

    def dump_new_urls(self) -> int:
        """
        Dump new URLs for full text retrieval.

        Returns:
            Number of URLs dumped
        """
        output_dir = DATA_DIR / "WeeklyUpdate"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "urlThisWeek.tab"

        # Get date 6 days ago
        cutoff = datetime.now() - timedelta(days=6)

        query = text(f"""
            SELECT r.pubmed, u.url_no, u.url
            FROM {DB_SCHEMA}.reference r
            JOIN {DB_SCHEMA}.ref_url ru ON ru.reference_no = r.reference_no
            JOIN {DB_SCHEMA}.url u ON u.url_no = ru.url_no
            WHERE u.url_type = 'Reference LINKOUT'
            AND u.date_created > :cutoff_date
        """)

        result = self.session.execute(query, {"cutoff_date": cutoff})

        count = 0
        with open(output_file, "w") as f:
            for row in result:
                f.write(f"{row[0]}\t{row[1]}\t{row[2]}\n")
                count += 1

        logger.info(f"Dumped {count} new URLs to {output_file}")
        return count

    def run(self) -> dict:
        """Run the full update process."""
        # Load existing data
        self.load_existing_data()

        # Get recent references
        pmids = self.get_recent_pubmeds()
        if not pmids:
            logger.info("No recent references found")
            return {"urls_found": 0}

        # Retrieve full text URLs
        urls_found = self.process_pmids(pmids)

        if not urls_found:
            logger.info("No new full text URLs found")
            return {"urls_found": 0}

        # Load URLs into database
        self.load_urls(urls_found)

        # Dump new URLs for full text retrieval
        new_url_count = self.dump_new_urls()

        return {
            "urls_found": len(urls_found),
            "url_inserts": self.url_insert_count,
            "url_errors": self.url_bad_count,
            "ref_url_inserts": self.ref_url_insert_count,
            "ref_url_errors": self.ref_url_bad_count,
            "pubmed_deletes": self.pubmed_url_delete_count,
            "new_urls_dumped": new_url_count,
        }


def update_fulltext_urls(days: int = DEFAULT_DAYS) -> bool:
    """
    Main function to update full text URLs.

    Args:
        days: Number of days to look back for recent references

    Returns:
        True on success, False on failure
    """
    # Set up logging to file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "load" / "NCBIfulltextURL.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info("*" * 50)
    logger.info(f"Started at {datetime.now()}")

    try:
        with SessionLocal() as session:
            updater = FullTextUrlUpdater(session, days=days)
            results = updater.run()

            logger.info("\nSummary:")
            logger.info(f"  URLs found: {results.get('urls_found', 0)}")
            logger.info(f"  URL inserts: {results.get('url_inserts', 0)}")
            logger.info(f"  URL errors: {results.get('url_errors', 0)}")
            logger.info(f"  Ref_url inserts: {results.get('ref_url_inserts', 0)}")
            logger.info(f"  Ref_url errors: {results.get('ref_url_errors', 0)}")
            logger.info(f"  PubMed URLs deleted: {results.get('pubmed_deletes', 0)}")
            logger.info(f"  New URLs dumped: {results.get('new_urls_dumped', 0)}")

            logger.info(f"Finished at {datetime.now()}")
            return True

    except Exception as e:
        logger.exception(f"Error updating full text URLs: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update full text URLs for references from NCBI"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS})",
    )

    args = parser.parse_args()

    success = update_fulltext_urls(days=args.days)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
