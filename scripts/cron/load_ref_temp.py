#!/usr/bin/env python3
"""
Load recent PubMed references into ref_temp table.

This script searches PubMed for recent papers (past week) containing
species-specific terms and loads them into the ref_temp table for
curator review. It excludes papers already in reference or ref_bad tables.

Based on loadRefTemp.pl by Stan Dong (Feb 2006)

Usage:
    python load_ref_temp.py --query "Candida albicans" --exclude "Biomphalaria,Arachis"

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Log directory
    TMP_DIR: Temporary directory
    NCBI_EMAIL: Email for NCBI E-utilities
    ADMIN_USER: Admin username for database operations
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from Bio import Entrez, Medline
from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "admin@candidagenome.org")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")

# How many days back to search
RELDATE = 10

# Configure Entrez
Entrez.email = NCBI_EMAIL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_citation(record: dict) -> str | None:
    """Create a citation string from a PubMed record."""
    authors = record.get("AU", [])
    title = record.get("TI", "")
    journal = record.get("TA", "")
    year = record.get("DP", "")[:4] if record.get("DP") else ""
    volume = record.get("VI", "")
    issue = record.get("IP", "")
    pages = record.get("PG", "")

    if not authors or not title:
        return None

    # Format authors
    if len(authors) == 1:
        author_str = authors[0]
    elif len(authors) == 2:
        author_str = f"{authors[0]} and {authors[1]}"
    else:
        author_str = f"{authors[0]} et al."

    # Build citation
    citation = f"{author_str} ({year}) {title}"

    if journal:
        journal_part = journal
        if volume:
            journal_part += f" {volume}"
        if issue:
            journal_part += f"({issue})"
        if pages:
            journal_part += f":{pages}"
        citation += f" {journal_part}"

    # Truncate if too long
    if len(citation) > 480:
        citation = citation[:477] + "..."

    return citation


def get_fulltext_url(pmid: int) -> str | None:
    """Get full text URL for a PubMed ID using ELink."""
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
                for id_url in linkset["IdUrlList"]["IdUrlSet"]:
                    if "ObjUrl" in id_url:
                        for obj_url in id_url["ObjUrl"]:
                            if "Url" in obj_url:
                                return obj_url["Url"]
        return None
    except Exception:
        return None


class RefTempLoader:
    """Load recent PubMed references into ref_temp table."""

    def __init__(self, session, species_query: str, exclude_list: list[str] | None = None):
        self.session = session
        self.species_query = species_query
        self.exclude_list = exclude_list or []

        # Counters
        self.insert_count = 0
        self.fail_count = 0
        self.exclude_count = 0

    def build_query(self) -> str:
        """Build the PubMed search query."""
        # Add [TW] (text word) to each query term
        query_words = self.species_query.split()
        formatted_words = []
        for word in query_words:
            if word.lower() in ("and", "or", "not"):
                formatted_words.append(word)
            else:
                formatted_words.append(f"{word}[TW]")

        query = " ".join(formatted_words)

        # Add exclude terms
        if self.exclude_list:
            exclude_terms = []
            for word in self.exclude_list:
                word = word.strip()
                if word:
                    exclude_terms.append(f"{word}[TW]")
            if exclude_terms:
                query += " NOT " + " NOT ".join(exclude_terms)

        return query

    def search_pubmed(self) -> list[int]:
        """Search PubMed for recent papers."""
        query = self.build_query()
        logger.info(f"Searching PubMed with query: {query}")

        try:
            handle = Entrez.esearch(
                db="pubmed",
                term=query,
                reldate=RELDATE,
                datetype="pdat",
                usehistory="y",
                retmax=10000
            )
            record = Entrez.read(handle)
            handle.close()

            pmids = [int(pmid) for pmid in record.get("IdList", [])]
            logger.info(f"Found {len(pmids)} PubMed IDs")
            return pmids

        except Exception as e:
            logger.error(f"Error searching PubMed: {e}")
            return []

    def fetch_records(self, pmids: list[int]) -> dict[int, dict]:
        """Fetch Medline records for PubMed IDs."""
        if not pmids:
            return {}

        records = {}
        batch_size = 200

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i + batch_size]
            try:
                handle = Entrez.efetch(
                    db="pubmed",
                    id=",".join(str(p) for p in batch),
                    rettype="medline",
                    retmode="text"
                )
                for record in Medline.parse(handle):
                    pmid = int(record.get("PMID", 0))
                    if pmid:
                        records[pmid] = record
                handle.close()
            except Exception as e:
                logger.error(f"Error fetching records: {e}")

        logger.info(f"Fetched {len(records)} Medline records")
        return records

    def is_in_reference(self, pmid: int) -> bool:
        """Check if PMID is already in reference table."""
        query = text(f"""
            SELECT reference_no FROM {DB_SCHEMA}.reference
            WHERE pubmed = :pmid
        """)
        result = self.session.execute(query, {"pmid": pmid}).first()
        return result is not None

    def is_in_ref_bad(self, pmid: int) -> bool:
        """Check if PMID is in ref_bad table."""
        query = text(f"""
            SELECT ref_bad_no FROM {DB_SCHEMA}.ref_bad
            WHERE pubmed = :pmid
        """)
        result = self.session.execute(query, {"pmid": pmid}).first()
        return result is not None

    def is_in_ref_temp(self, pmid: int) -> bool:
        """Check if PMID is already in ref_temp table."""
        query = text(f"""
            SELECT ref_temp_no FROM {DB_SCHEMA}.ref_temp
            WHERE pubmed = :pmid
        """)
        result = self.session.execute(query, {"pmid": pmid}).first()
        return result is not None

    def load_records(self, records: dict[int, dict]) -> None:
        """Load records into ref_temp table."""
        insert_query = text(f"""
            INSERT INTO {DB_SCHEMA}.ref_temp
            (pubmed, citation, fulltext_url, abstract, created_by)
            VALUES (:pmid, :citation, :url, :abstract, :user)
        """)

        for pmid in sorted(records.keys(), reverse=True):
            record = records[pmid]

            # Check if already exists
            if self.is_in_reference(pmid):
                logger.debug(f"IN_REFERENCE: {pmid} skipped")
                self.exclude_count += 1
                continue

            if self.is_in_ref_bad(pmid):
                logger.debug(f"IN_REF_BAD: {pmid} skipped")
                self.exclude_count += 1
                continue

            if self.is_in_ref_temp(pmid):
                logger.debug(f"IN_REF_TEMP: {pmid} skipped")
                self.exclude_count += 1
                continue

            # Create citation
            citation = create_citation(record)
            if not citation:
                logger.warning(f"Could not create citation for {pmid}")
                self.fail_count += 1
                continue

            # Get full text URL
            url = get_fulltext_url(pmid)

            # Get abstract
            abstract = record.get("AB", "")
            if len(abstract) > 4000:
                abstract = abstract[:3950] + "...ABSTRACT TRUNCATED AT 3950 CHARACTERS."

            try:
                self.session.execute(insert_query, {
                    "pmid": pmid,
                    "citation": citation,
                    "url": url,
                    "abstract": abstract,
                    "user": ADMIN_USER,
                })
                self.session.commit()
                logger.info(f"Inserted pmid={pmid}")
                self.insert_count += 1

            except Exception as e:
                logger.error(f"Error inserting pmid={pmid}: {e}")
                self.session.rollback()
                self.fail_count += 1

    def run(self) -> dict:
        """Run the full loading process."""
        pmids = self.search_pubmed()
        records = self.fetch_records(pmids)
        self.load_records(records)

        return {
            "success": self.insert_count,
            "failed": self.fail_count,
            "excluded": self.exclude_count,
        }


def load_ref_temp(species_query: str, exclude_list: list[str] | None = None) -> bool:
    """
    Main function to load recent PubMed references.

    Args:
        species_query: Species query string for PubMed
        exclude_list: List of terms to exclude from search

    Returns:
        True on success, False on failure
    """
    # Set up log file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "load" / "loadRefTemp.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info("*" * 40)
    logger.info(f"Started at {datetime.now()}")
    logger.info(f"Query: {species_query}")
    if exclude_list:
        logger.info(f"Exclude: {exclude_list}")

    try:
        with SessionLocal() as session:
            loader = RefTempLoader(session, species_query, exclude_list)
            results = loader.run()

            logger.info(f"\nSuccess count: {results['success']}")
            logger.info(f"Fail count: {results['failed']}")
            logger.info(f"Exclude count: {results['excluded']}")
            logger.info(f"Ended at {datetime.now()}")

            return results["success"] > 0 or results["excluded"] > 0

    except Exception as e:
        logger.exception(f"Error loading ref_temp: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load recent PubMed references into ref_temp table"
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Species query string (e.g., 'Candida albicans')",
    )
    parser.add_argument(
        "--exclude",
        help="Comma-separated list of terms to exclude",
    )

    args = parser.parse_args()

    exclude_list = None
    if args.exclude:
        exclude_list = [x.strip() for x in args.exclude.split(",")]

    success = load_ref_temp(args.query, exclude_list)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
