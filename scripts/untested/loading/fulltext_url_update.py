#!/usr/bin/env python3
"""
Update fulltext URLs in reference table.

This script updates the fulltext_url column in the reference table
by fetching the latest PubMed Central (PMC) data for linked articles.

Original Perl: fullTextUrlWeeklyUpdate.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from Bio import Entrez
from dotenv import load_dotenv
from sqlalchemy import and_, or_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Reference

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
DEFAULT_EMAIL = "cgd-admin@stanford.edu"
NCBI_PMC_URL = "https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
PUBMED_CENTRAL_SEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"


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


def get_references_without_fulltext(session: Session, limit: int = None) -> list:
    """
    Get references that don't have fulltext URLs.

    Args:
        session: Database session
        limit: Maximum number of references to return

    Returns:
        List of Reference objects with pubmed IDs
    """
    query = session.query(Reference).filter(
        and_(
            Reference.pubmed.isnot(None),
            or_(
                Reference.fulltext_url.is_(None),
                Reference.fulltext_url == '',
            ),
        )
    )

    if limit:
        query = query.limit(limit)

    return query.all()


def get_all_references_with_pubmed(session: Session, limit: int = None) -> list:
    """
    Get all references with PubMed IDs.

    Args:
        session: Database session
        limit: Maximum number of references to return

    Returns:
        List of Reference objects
    """
    query = session.query(Reference).filter(
        Reference.pubmed.isnot(None)
    )

    if limit:
        query = query.limit(limit)

    return query.all()


def fetch_pmc_ids_for_pubmeds(
    pubmed_ids: list[int],
    email: str,
    batch_size: int = 100,
) -> dict:
    """
    Fetch PMC IDs linked to PubMed IDs.

    Args:
        pubmed_ids: List of PubMed IDs
        email: Email for NCBI API
        batch_size: Number of IDs per batch

    Returns:
        Dict mapping pubmed_id to pmc_id
    """
    Entrez.email = email
    pmc_map = {}

    for i in range(0, len(pubmed_ids), batch_size):
        batch = pubmed_ids[i:i + batch_size]
        try:
            # Get links from PubMed to PMC
            handle = Entrez.elink(
                dbfrom="pubmed",
                db="pmc",
                id=[str(p) for p in batch],
            )
            results = Entrez.read(handle)
            handle.close()

            for result in results:
                pubmed_id = result.get('IdList', [''])[0]
                if not pubmed_id:
                    continue

                for linkset in result.get('LinkSetDb', []):
                    if linkset.get('DbTo') == 'pmc':
                        links = linkset.get('Link', [])
                        if links:
                            pmc_id = links[0]['Id']
                            pmc_map[int(pubmed_id)] = f"PMC{pmc_id}"

            logger.debug(f"Processed batch {i}-{i + len(batch)}")
            time.sleep(0.34)  # Be nice to NCBI

        except Exception as e:
            logger.error(f"Error fetching PMC links for batch {i}: {e}")

    return pmc_map


def get_doi_url(pubmed_id: int, email: str) -> str | None:
    """
    Get DOI-based URL for a PubMed article.

    Args:
        pubmed_id: PubMed ID
        email: Email for NCBI API

    Returns:
        DOI URL or None
    """
    Entrez.email = email

    try:
        handle = Entrez.efetch(
            db="pubmed",
            id=str(pubmed_id),
            rettype="xml",
            retmode="xml",
        )
        records = Entrez.read(handle)
        handle.close()

        # Extract DOI from article IDs
        articles = records.get('PubmedArticle', [])
        if articles:
            article = articles[0]
            article_ids = article.get('PubmedData', {}).get('ArticleIdList', [])
            for aid in article_ids:
                if aid.attributes.get('IdType') == 'doi':
                    return f"https://doi.org/{str(aid)}"

    except Exception as e:
        logger.debug(f"Error fetching DOI for PMID {pubmed_id}: {e}")

    return None


def update_fulltext_urls(
    session: Session,
    email: str,
    update_all: bool = False,
    created_by: str = "SCRIPT",
    dry_run: bool = False,
    limit: int = None,
) -> dict:
    """
    Update fulltext URLs for references.

    Args:
        session: Database session
        email: Email for NCBI API
        update_all: If True, update all references (not just missing)
        created_by: User name for audit
        dry_run: If True, don't commit changes
        limit: Maximum number of references to process

    Returns:
        Statistics dict
    """
    stats = {
        'total_references': 0,
        'pmc_urls_added': 0,
        'doi_urls_added': 0,
        'already_has_url': 0,
        'no_fulltext_found': 0,
        'errors': 0,
    }

    # Get references to update
    if update_all:
        references = get_all_references_with_pubmed(session, limit)
    else:
        references = get_references_without_fulltext(session, limit)

    stats['total_references'] = len(references)
    logger.info(f"Found {len(references)} references to process")

    if not references:
        return stats

    # Get PubMed IDs
    pubmed_ids = [r.pubmed for r in references]
    pubmed_to_ref = {r.pubmed: r for r in references}

    # Fetch PMC IDs
    logger.info("Fetching PMC IDs from NCBI...")
    pmc_map = fetch_pmc_ids_for_pubmeds(pubmed_ids, email)
    logger.info(f"Found {len(pmc_map)} PMC IDs")

    # Update references
    for pubmed_id, ref in pubmed_to_ref.items():
        try:
            # Skip if already has URL and not updating all
            if ref.fulltext_url and not update_all:
                stats['already_has_url'] += 1
                continue

            # Check for PMC ID
            pmc_id = pmc_map.get(pubmed_id)
            if pmc_id:
                url = NCBI_PMC_URL.format(pmcid=pmc_id)
                ref.fulltext_url = url
                stats['pmc_urls_added'] += 1
                logger.debug(f"PMID {pubmed_id}: PMC URL added")
            else:
                # Try to get DOI URL
                doi_url = get_doi_url(pubmed_id, email)
                if doi_url:
                    ref.fulltext_url = doi_url
                    stats['doi_urls_added'] += 1
                    logger.debug(f"PMID {pubmed_id}: DOI URL added")
                else:
                    stats['no_fulltext_found'] += 1
                    logger.debug(f"PMID {pubmed_id}: No fulltext URL found")

        except Exception as e:
            logger.error(f"Error updating PMID {pubmed_id}: {e}")
            stats['errors'] += 1

        # Flush periodically
        if (stats['pmc_urls_added'] + stats['doi_urls_added']) % 50 == 0:
            session.flush()

    session.flush()
    return stats


def load_from_file(
    session: Session,
    data_file: Path,
    dry_run: bool = False,
) -> dict:
    """
    Update fulltext URLs from a file.

    Expected file format (tab-delimited):
    pubmed_id, fulltext_url

    Args:
        session: Database session
        data_file: Data file path
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'total_references': 0,
        'pmc_urls_added': 0,
        'doi_urls_added': 0,
        'already_has_url': 0,
        'no_fulltext_found': 0,
        'references_not_found': 0,
        'errors': 0,
    }

    # Read file and build mapping
    url_map = {}
    with open(data_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('\t')
            if len(parts) >= 2:
                try:
                    pubmed_id = int(parts[0].strip())
                    url = parts[1].strip()
                    if pubmed_id and url:
                        url_map[pubmed_id] = url
                except ValueError:
                    continue

    stats['total_references'] = len(url_map)
    logger.info(f"Loaded {len(url_map)} URLs from file")

    # Get references
    pubmed_ids = list(url_map.keys())
    references = session.query(Reference).filter(
        Reference.pubmed.in_(pubmed_ids)
    ).all()

    ref_map = {r.pubmed: r for r in references}
    logger.info(f"Found {len(references)} matching references in database")

    # Update references
    for pubmed_id, url in url_map.items():
        ref = ref_map.get(pubmed_id)
        if not ref:
            stats['references_not_found'] += 1
            continue

        try:
            if ref.fulltext_url == url:
                stats['already_has_url'] += 1
            else:
                ref.fulltext_url = url
                if 'pmc' in url.lower():
                    stats['pmc_urls_added'] += 1
                elif 'doi' in url.lower():
                    stats['doi_urls_added'] += 1
                else:
                    stats['pmc_urls_added'] += 1  # Generic counter

        except Exception as e:
            logger.error(f"Error updating PMID {pubmed_id}: {e}")
            stats['errors'] += 1

    session.flush()
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update fulltext URLs in reference table"
    )
    parser.add_argument(
        "--email",
        default=DEFAULT_EMAIL,
        help=f"Email for NCBI API (default: {DEFAULT_EMAIL})",
    )
    parser.add_argument(
        "--update-all",
        action="store_true",
        help="Update all references (not just those missing URLs)",
    )
    parser.add_argument(
        "--data-file",
        type=Path,
        help="File with pubmed_id and URL mappings (tab-delimited)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of references to process",
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
            if args.data_file:
                if not args.data_file.exists():
                    logger.error(f"Data file not found: {args.data_file}")
                    sys.exit(1)
                stats = load_from_file(
                    session,
                    args.data_file,
                    args.dry_run,
                )
            else:
                stats = update_fulltext_urls(
                    session,
                    args.email,
                    args.update_all,
                    args.created_by,
                    args.dry_run,
                    args.limit,
                )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Total references: {stats['total_references']}")
            logger.info(f"  PMC URLs added: {stats['pmc_urls_added']}")
            logger.info(f"  DOI URLs added: {stats['doi_urls_added']}")
            logger.info(f"  Already has URL: {stats['already_has_url']}")
            logger.info(f"  No fulltext found: {stats['no_fulltext_found']}")
            if 'references_not_found' in stats:
                logger.info(f"  References not found: {stats['references_not_found']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
