#!/usr/bin/env python3
"""
Load recent PubMed references into ref_temp table.

This script searches PubMed for recent papers and loads them
into the ref_temp table for triage and curation.

Original Perl: loadRefTemp.pl
Converted to Python: 2024
"""

import argparse
import logging
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import requests
from Bio import Entrez, Medline
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import RefBad, RefTemp, Reference

load_dotenv()

logger = logging.getLogger(__name__)

# Search parameters
RELDATE = 7  # Search back one week


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


def search_pubmed(
    search_terms: list,
    reldate: int = RELDATE,
    email: str = "admin@candidagenome.org",
) -> list:
    """
    Search PubMed for recent papers.

    Args:
        search_terms: List of search terms
        reldate: Number of days to search back
        email: Email for NCBI

    Returns:
        List of PMIDs
    """
    Entrez.email = email

    # Build query
    query = " OR ".join(search_terms)
    logger.info(f"Searching PubMed: {query}")

    # Search
    handle = Entrez.esearch(
        db="pubmed",
        term=query,
        reldate=reldate,
        datetype="pdat",
        usehistory="y",
    )
    results = Entrez.read(handle)
    handle.close()

    count = int(results["Count"])
    logger.info(f"Found {count} papers")

    if count == 0:
        return []

    # Fetch all PMIDs
    pmids = results["IdList"]

    return pmids


def fetch_pubmed_records(pmids: list, email: str) -> dict:
    """
    Fetch PubMed records for given PMIDs.

    Args:
        pmids: List of PMIDs
        email: Email for NCBI

    Returns:
        Dict mapping PMID to record info
    """
    Entrez.email = email

    records = {}

    # Fetch in batches
    batch_size = 100
    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i + batch_size]
        logger.debug(f"Fetching batch {i // batch_size + 1}")

        handle = Entrez.efetch(
            db="pubmed",
            id=",".join(batch),
            rettype="medline",
            retmode="text",
        )

        for record in Medline.parse(handle):
            pmid = record.get("PMID")
            if not pmid:
                continue

            # Extract fields
            title = record.get("TI", "")
            abstract = record.get("AB", "")
            journal = record.get("JT", record.get("TA", ""))
            volume = record.get("VI", "")
            issue = record.get("IP", "")
            pages = record.get("PG", "")
            year = record.get("DP", "")[:4] if record.get("DP") else ""
            authors = record.get("AU", [])

            # Create citation
            citation = create_citation(
                year, title, journal, volume, issue, pages, authors
            )

            records[pmid] = {
                "pmid": pmid,
                "title": title,
                "abstract": abstract,
                "journal": journal,
                "volume": volume,
                "issue": issue,
                "pages": pages,
                "year": year,
                "authors": authors,
                "citation": citation,
            }

        handle.close()

    return records


def create_citation(
    year: str,
    title: str,
    journal: str,
    volume: str,
    issue: str,
    pages: str,
    authors: list,
) -> str:
    """Create a formatted citation string."""
    parts = []

    # Authors
    if authors:
        if len(authors) > 3:
            parts.append(f"{authors[0]} et al.")
        else:
            parts.append(", ".join(authors))

    # Year
    if year:
        parts.append(f"({year})")

    # Title
    if title:
        parts.append(title)

    # Journal
    journal_parts = []
    if journal:
        journal_parts.append(journal)
    if volume:
        journal_parts.append(volume)
        if issue:
            journal_parts[-1] += f"({issue})"
    if pages:
        journal_parts.append(pages)

    if journal_parts:
        parts.append(" ".join(journal_parts))

    return " ".join(parts)


def get_fulltext_urls(pmids: list, email: str) -> dict:
    """
    Get fulltext URLs for PMIDs using NCBI elink.

    Args:
        pmids: List of PMIDs
        email: Email for NCBI

    Returns:
        Dict mapping PMID to URL
    """
    Entrez.email = email
    urls = {}

    try:
        handle = Entrez.elink(
            dbfrom="pubmed",
            db="pubmed",
            id=pmids,
            cmd="llinks",
        )
        results = Entrez.read(handle)
        handle.close()

        for linkset in results:
            pmid = linkset.get("IdList", [None])[0]
            if pmid and "LinkSetDb" in linkset:
                for linkdb in linkset["LinkSetDb"]:
                    if "Link" in linkdb:
                        for link in linkdb["Link"]:
                            url = link.get("Url")
                            if url:
                                urls[pmid] = url
                                break

    except Exception as e:
        logger.warning(f"Error getting fulltext URLs: {e}")

    return urls


def load_ref_temp(
    session: Session,
    records: dict,
    fulltext_urls: dict,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Load records into ref_temp table.

    Args:
        session: Database session
        records: Dict of PubMed records
        fulltext_urls: Dict mapping PMID to URL
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'total': len(records),
        'inserted': 0,
        'in_reference': 0,
        'in_ref_bad': 0,
        'in_ref_temp': 0,
        'errors': 0,
    }

    for pmid in sorted(records.keys(), reverse=True):
        record = records[pmid]
        pmid_int = int(pmid)

        # Check if already in reference
        existing = session.query(Reference).filter(
            Reference.pubmed == pmid_int
        ).first()
        if existing:
            logger.debug(f"IN_REFERENCE: {pmid} skipped")
            stats['in_reference'] += 1
            continue

        # Check if in ref_bad
        bad = session.query(RefBad).filter(
            RefBad.pubmed == pmid_int
        ).first()
        if bad:
            logger.debug(f"IN_REF_BAD: {pmid} skipped")
            stats['in_ref_bad'] += 1
            continue

        # Check if already in ref_temp
        temp = session.query(RefTemp).filter(
            RefTemp.pubmed == pmid_int
        ).first()
        if temp:
            logger.debug(f"IN_REF_TEMP: {pmid} skipped")
            stats['in_ref_temp'] += 1
            continue

        # Truncate abstract if too long
        abstract = record.get("abstract", "")
        if len(abstract) > 4000:
            abstract = abstract[:3950] + "...ABSTRACT TRUNCATED AT 3950 CHARACTERS."

        # Get fulltext URL
        url = fulltext_urls.get(pmid, "")

        try:
            ref_temp = RefTemp(
                pubmed=pmid_int,
                citation=record.get("citation", ""),
                fulltext_url=url,
                abstract=abstract,
                created_by=created_by,
            )
            session.add(ref_temp)

            if not dry_run:
                session.flush()

            logger.info(f"Inserted: PMID {pmid}")
            stats['inserted'] += 1

        except Exception as e:
            logger.error(f"Error inserting PMID {pmid}: {e}")
            stats['errors'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load recent PubMed references into ref_temp table"
    )
    parser.add_argument(
        "--search-terms",
        nargs="+",
        default=["Candida", "albicans"],
        help="Search terms for PubMed (default: Candida albicans)",
    )
    parser.add_argument(
        "--reldate",
        type=int,
        default=RELDATE,
        help=f"Days to search back (default: {RELDATE})",
    )
    parser.add_argument(
        "--email",
        default="admin@candidagenome.org",
        help="Email for NCBI API",
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

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    # Search PubMed
    pmids = search_pubmed(args.search_terms, args.reldate, args.email)

    if not pmids:
        logger.info("No papers found")
        return

    # Fetch records
    logger.info(f"Fetching {len(pmids)} records")
    records = fetch_pubmed_records(pmids, args.email)

    # Get fulltext URLs
    logger.info("Getting fulltext URLs")
    fulltext_urls = get_fulltext_urls(pmids, args.email)

    try:
        with SessionLocal() as session:
            stats = load_ref_temp(
                session,
                records,
                fulltext_urls,
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
            logger.info(f"  Total found: {stats['total']}")
            logger.info(f"  Inserted: {stats['inserted']}")
            logger.info(f"  Already in reference: {stats['in_reference']}")
            logger.info(f"  Already in ref_bad: {stats['in_ref_bad']}")
            logger.info(f"  Already in ref_temp: {stats['in_ref_temp']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
