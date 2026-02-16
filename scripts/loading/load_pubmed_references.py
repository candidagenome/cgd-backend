#!/usr/bin/env python3
"""
Load PubMed references into database.

This script searches PubMed for organism-related papers and loads
references, authors, journals, abstracts, and publication types
into the database.

Original Perl: LoadPubMedReferences.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from Bio import Entrez, Medline
from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import (
    Abstract,
    Author,
    Journal,
    RefAuthor,
    RefBad,
    Reference,
    RefReftype,
    RefType,
)

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
DEFAULT_EMAIL = "cgd-admin@stanford.edu"
DEFAULT_SEARCH_TERMS = ["Candida", "albicans"]
DEFAULT_RELDATE = 30  # Days to look back
DEFAULT_SOURCE = "NCBI"


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


def get_existing_pubmeds(session: Session) -> set:
    """Get set of existing PubMed IDs in the reference table."""
    result = session.execute(
        text("SELECT pubmed FROM reference WHERE pubmed IS NOT NULL")
    )
    return {row[0] for row in result}


def get_bad_pubmeds(session: Session) -> set:
    """Get set of PubMed IDs that should not be loaded."""
    result = session.query(RefBad.pubmed).all()
    return {row[0] for row in result}


def get_or_create_journal(
    session: Session,
    full_name: str,
    abbreviation: str,
    issn: str,
    essn: str,
    created_by: str,
) -> int:
    """
    Get or create journal entry.

    Returns:
        journal_no
    """
    # Try to find by abbreviation first
    journal = None
    if abbreviation:
        journal = session.query(Journal).filter(
            Journal.abbreviation == abbreviation
        ).first()

    # Try by full name
    if not journal and full_name:
        journal = session.query(Journal).filter(
            Journal.full_name == full_name
        ).first()

    # Try by ISSN
    if not journal and issn:
        journal = session.query(Journal).filter(
            Journal.issn == issn
        ).first()

    if journal:
        return journal.journal_no

    # Create new journal
    journal = Journal(
        full_name=full_name if full_name else None,
        abbreviation=abbreviation if abbreviation else None,
        issn=issn if issn else None,
        essn=essn if essn else None,
        created_by=created_by,
    )
    session.add(journal)
    session.flush()

    logger.debug(f"Created journal: {abbreviation or full_name}")
    return journal.journal_no


def get_or_create_author(
    session: Session,
    author_name: str,
    created_by: str,
) -> int:
    """
    Get or create author entry.

    Returns:
        author_no
    """
    author = session.query(Author).filter(
        Author.author_name == author_name
    ).first()

    if author:
        return author.author_no

    author = Author(
        author_name=author_name,
        created_by=created_by,
    )
    session.add(author)
    session.flush()

    return author.author_no


def get_or_create_ref_type(
    session: Session,
    source: str,
    ref_type: str,
    created_by: str,
) -> int:
    """
    Get or create reference type entry.

    Returns:
        ref_type_no
    """
    ref_type_obj = session.query(RefType).filter(
        and_(
            RefType.source == source,
            RefType.ref_type == ref_type,
        )
    ).first()

    if ref_type_obj:
        return ref_type_obj.ref_type_no

    ref_type_obj = RefType(
        source=source,
        ref_type=ref_type,
        created_by=created_by,
    )
    session.add(ref_type_obj)
    session.flush()

    return ref_type_obj.ref_type_no


def parse_date(date_str: str) -> tuple[int, str, int]:
    """
    Parse PubMed date string.

    Args:
        date_str: Date string like "2024 Jan 15" or "2024 Jan"

    Returns:
        Tuple of (year, month, day) where month is abbreviated name
    """
    parts = date_str.split()
    year = int(parts[0]) if parts else 0
    month = parts[1] if len(parts) > 1 else 'Jan'
    day = int(parts[2]) if len(parts) > 2 else 1

    return year, month, day


def format_citation(record: dict) -> str:
    """Format a Medline record into a citation string."""
    authors = record.get('AU', [])
    title = record.get('TI', '')
    journal = record.get('TA', '')
    year = record.get('DP', '').split()[0] if record.get('DP') else ''
    volume = record.get('VI', '')
    issue = record.get('IP', '')
    pages = record.get('PG', '')

    # Format authors
    if len(authors) > 3:
        author_str = f"{authors[0]} et al."
    elif authors:
        author_str = ', '.join(authors)
    else:
        author_str = ''

    # Build citation
    citation_parts = []
    if author_str:
        citation_parts.append(author_str)
    if title:
        citation_parts.append(title)
    if journal:
        journal_part = journal
        if year:
            journal_part += f" ({year})"
        if volume:
            journal_part += f" {volume}"
            if issue:
                journal_part += f"({issue})"
        if pages:
            journal_part += f":{pages}"
        citation_parts.append(journal_part)

    return ' '.join(citation_parts)[:500]  # Truncate if too long


def load_reference(
    session: Session,
    record: dict,
    source: str,
    created_by: str,
) -> int | None:
    """
    Load a single reference from a Medline record.

    Args:
        session: Database session
        record: Medline record dict
        source: Source for entries
        created_by: User name for audit

    Returns:
        reference_no or None if error
    """
    pmid = int(record.get('PMID', 0))
    if not pmid:
        return None

    # Extract fields
    title = record.get('TI', '')[:2000] if record.get('TI') else None
    volume = record.get('VI', '')[:40] if record.get('VI') else None
    issue = record.get('IP', '')[:40] if record.get('IP') else None
    pages = record.get('PG', '')[:40] if record.get('PG') else None
    abstract_text = record.get('AB', '')

    # Parse date
    date_str = record.get('DP', '')
    year, month, day = parse_date(date_str) if date_str else (None, None, None)

    # Format citation
    citation = format_citation(record)

    # Get or create journal
    journal_no = None
    journal_name = record.get('JT', '')
    journal_abbrev = record.get('TA', '')
    issn = record.get('IS', '').split()[0] if record.get('IS') else None

    if journal_name or journal_abbrev:
        journal_no = get_or_create_journal(
            session,
            journal_name[:200] if journal_name else None,
            journal_abbrev[:140] if journal_abbrev else None,
            issn[:20] if issn else None,
            None,  # essn
            created_by,
        )

    # Create reference
    try:
        reference = Reference(
            source=source,
            pubmed=pmid,
            citation=citation,
            title=title,
            volume=volume,
            issue=issue,
            page=pages,
            year=year,
            journal_no=journal_no,
            created_by=created_by,
        )
        session.add(reference)
        session.flush()

        reference_no = reference.reference_no
        logger.debug(f"Created reference: PMID {pmid}")

    except Exception as e:
        logger.error(f"Error creating reference for PMID {pmid}: {e}")
        return None

    # Add authors
    authors = record.get('AU', [])
    for author_order, author_name in enumerate(authors, 1):
        try:
            author_no = get_or_create_author(session, author_name[:100], created_by)
            ref_author = RefAuthor(
                reference_no=reference_no,
                author_no=author_no,
                author_order=author_order,
            )
            session.add(ref_author)
        except Exception as e:
            logger.warning(f"Error adding author {author_name} for PMID {pmid}: {e}")

    # Add abstract
    if abstract_text:
        try:
            abstract = Abstract(
                reference_no=reference_no,
                abstract=abstract_text[:4000],
            )
            session.add(abstract)
        except Exception as e:
            logger.warning(f"Error adding abstract for PMID {pmid}: {e}")

    # Add publication types
    pub_types = record.get('PT', [])
    for pt in pub_types:
        try:
            ref_type_no = get_or_create_ref_type(session, 'NCBI', pt[:40], created_by)
            ref_reftype = RefReftype(
                reference_no=reference_no,
                ref_type_no=ref_type_no,
            )
            session.add(ref_reftype)
        except Exception as e:
            logger.warning(f"Error adding pub type {pt} for PMID {pmid}: {e}")

    session.flush()
    return reference_no


def search_pubmed(
    search_terms: list[str],
    reldate: int,
    email: str,
    max_results: int = 10000,
) -> list[int]:
    """
    Search PubMed for papers matching search terms.

    Args:
        search_terms: List of search terms
        reldate: Number of days to look back
        email: Email for NCBI API
        max_results: Maximum number of results

    Returns:
        List of PubMed IDs
    """
    Entrez.email = email

    # Build search query
    query = ' AND '.join(f'"{term}"' for term in search_terms)

    try:
        handle = Entrez.esearch(
            db="pubmed",
            term=query,
            reldate=reldate,
            datetype="edat",
            retmax=max_results,
        )
        result = Entrez.read(handle)
        handle.close()

        pmids = [int(pmid) for pmid in result.get('IdList', [])]
        logger.info(f"Found {len(pmids)} papers matching '{query}'")
        return pmids

    except Exception as e:
        logger.error(f"PubMed search error: {e}")
        return []


def fetch_pubmed_records(
    pmids: list[int],
    email: str,
    batch_size: int = 100,
) -> list[dict]:
    """
    Fetch Medline records for PubMed IDs.

    Args:
        pmids: List of PubMed IDs
        email: Email for NCBI API
        batch_size: Number of records to fetch at once

    Returns:
        List of Medline record dicts
    """
    Entrez.email = email
    records = []

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i + batch_size]
        try:
            handle = Entrez.efetch(
                db="pubmed",
                id=','.join(str(p) for p in batch),
                rettype="medline",
                retmode="text",
            )
            batch_records = list(Medline.parse(handle))
            handle.close()
            records.extend(batch_records)

            logger.debug(f"Fetched {len(batch_records)} records ({i + len(batch)}/{len(pmids)})")

            # Be nice to NCBI
            time.sleep(0.34)

        except Exception as e:
            logger.error(f"Error fetching batch {i}-{i + batch_size}: {e}")

    return records


def load_pubmed_references(
    session: Session,
    search_terms: list[str],
    reldate: int,
    email: str,
    source: str,
    created_by: str,
    max_results: int = 10000,
    dry_run: bool = False,
) -> dict:
    """
    Search PubMed and load new references.

    Args:
        session: Database session
        search_terms: Search terms for PubMed
        reldate: Number of days to look back
        email: Email for NCBI API
        source: Source for reference entries
        created_by: User name for audit
        max_results: Maximum number of results
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'searched': 0,
        'new_pmids': 0,
        'references_created': 0,
        'already_exists': 0,
        'in_bad_list': 0,
        'errors': 0,
    }

    # Get existing PMIDs
    existing_pmids = get_existing_pubmeds(session)
    logger.info(f"Found {len(existing_pmids)} existing references")

    bad_pmids = get_bad_pubmeds(session)
    logger.info(f"Found {len(bad_pmids)} bad PMIDs to skip")

    # Search PubMed
    pmids = search_pubmed(search_terms, reldate, email, max_results)
    stats['searched'] = len(pmids)

    # Filter out existing and bad PMIDs
    new_pmids = []
    for pmid in pmids:
        if pmid in existing_pmids:
            stats['already_exists'] += 1
        elif pmid in bad_pmids:
            stats['in_bad_list'] += 1
        else:
            new_pmids.append(pmid)

    stats['new_pmids'] = len(new_pmids)
    logger.info(f"Found {len(new_pmids)} new PMIDs to load")

    if not new_pmids:
        return stats

    # Fetch records
    records = fetch_pubmed_records(new_pmids, email)
    logger.info(f"Fetched {len(records)} Medline records")

    # Load references
    for record in records:
        try:
            ref_no = load_reference(session, record, source, created_by)
            if ref_no:
                stats['references_created'] += 1
        except Exception as e:
            pmid = record.get('PMID', 'unknown')
            logger.error(f"Error loading PMID {pmid}: {e}")
            stats['errors'] += 1

        # Flush periodically
        if stats['references_created'] % 50 == 0:
            session.flush()

    session.flush()
    return stats


def load_from_file(
    session: Session,
    pmid_file: Path,
    email: str,
    source: str,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Load references from a file of PubMed IDs.

    Args:
        session: Database session
        pmid_file: File with PubMed IDs (one per line)
        email: Email for NCBI API
        source: Source for reference entries
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'searched': 0,
        'new_pmids': 0,
        'references_created': 0,
        'already_exists': 0,
        'in_bad_list': 0,
        'errors': 0,
    }

    # Read PMIDs from file
    pmids = []
    with open(pmid_file) as f:
        for line in f:
            line = line.strip()
            if line and line.isdigit():
                pmids.append(int(line))

    stats['searched'] = len(pmids)
    logger.info(f"Read {len(pmids)} PMIDs from file")

    # Get existing PMIDs
    existing_pmids = get_existing_pubmeds(session)
    bad_pmids = get_bad_pubmeds(session)

    # Filter
    new_pmids = []
    for pmid in pmids:
        if pmid in existing_pmids:
            stats['already_exists'] += 1
        elif pmid in bad_pmids:
            stats['in_bad_list'] += 1
        else:
            new_pmids.append(pmid)

    stats['new_pmids'] = len(new_pmids)

    if not new_pmids:
        return stats

    # Fetch and load
    records = fetch_pubmed_records(new_pmids, email)
    for record in records:
        try:
            ref_no = load_reference(session, record, source, created_by)
            if ref_no:
                stats['references_created'] += 1
        except Exception as e:
            pmid = record.get('PMID', 'unknown')
            logger.error(f"Error loading PMID {pmid}: {e}")
            stats['errors'] += 1

        if stats['references_created'] % 50 == 0:
            session.flush()

    session.flush()
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load PubMed references into database"
    )
    parser.add_argument(
        "--search-terms",
        nargs="+",
        default=DEFAULT_SEARCH_TERMS,
        help=f"Search terms for PubMed (default: {DEFAULT_SEARCH_TERMS})",
    )
    parser.add_argument(
        "--reldate",
        type=int,
        default=DEFAULT_RELDATE,
        help=f"Days to look back (default: {DEFAULT_RELDATE})",
    )
    parser.add_argument(
        "--pmid-file",
        type=Path,
        help="File with PubMed IDs to load (one per line)",
    )
    parser.add_argument(
        "--email",
        default=DEFAULT_EMAIL,
        help=f"Email for NCBI API (default: {DEFAULT_EMAIL})",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Source for reference entries (default: {DEFAULT_SOURCE})",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=10000,
        help="Maximum number of results (default: 10000)",
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

    try:
        with SessionLocal() as session:
            if args.pmid_file:
                if not args.pmid_file.exists():
                    logger.error(f"PMID file not found: {args.pmid_file}")
                    sys.exit(1)
                stats = load_from_file(
                    session,
                    args.pmid_file,
                    args.email,
                    args.source,
                    args.created_by,
                    args.dry_run,
                )
            else:
                stats = load_pubmed_references(
                    session,
                    args.search_terms,
                    args.reldate,
                    args.email,
                    args.source,
                    args.created_by,
                    args.max_results,
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
            logger.info(f"  Searched: {stats['searched']}")
            logger.info(f"  New PMIDs: {stats['new_pmids']}")
            logger.info(f"  References created: {stats['references_created']}")
            logger.info(f"  Already exists: {stats['already_exists']}")
            logger.info(f"  In bad list: {stats['in_bad_list']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
