#!/usr/bin/env python3
"""
Load gene-specific PubMed references to REF_TEMP.

This script searches PubMed for gene-specific references and loads
them to the REF_TEMP table for curator review.

Based on load_geneRefs_2_Reftemp.pl.

Usage:
    python load_gene_refs_to_reftemp.py local
    python load_gene_refs_to_reftemp.py remote
    python load_gene_refs_to_reftemp.py --help

Arguments:
    source: 'local' for local database genes, 'remote' for remote DB genes

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Directory for log files
    NCBI_API_KEY: NCBI E-utilities API key (optional)
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
ADMIN_USER = os.getenv("ADMIN_USER", "ADMIN").upper()
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "")

# Species configuration
GENUS = os.getenv("GENUS", "Candida")
SPECIES = os.getenv("SPECIES", "albicans")

# NCBI E-utilities base URLs
ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
ELINK_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Words to exclude from search queries (too generic)
STOP_WORDS = {"abpA"}


def get_query_terms(session, which_genes: str) -> dict[str, set[str]]:
    """
    Get gene names and aliases to use as query terms.

    Returns dict mapping feature_name to set of query terms.
    """
    query_terms: dict[str, set[str]] = {}

    # Get feature names, gene names, and aliases
    query = text(f"""
        SELECT f.feature_name, f.gene_name, a.alias_name
        FROM {DB_SCHEMA}.feature f
        LEFT JOIN {DB_SCHEMA}.feat_alias fa ON f.feature_no = fa.feature_no
        LEFT JOIN {DB_SCHEMA}.alias a ON fa.alias_no = a.alias_no
        WHERE f.feature_type IN (
            SELECT col_value
            FROM {DB_SCHEMA}.web_metadata
            WHERE application_name = 'Locus Page'
            AND tab_name = 'FEATURE'
            AND col_name = 'FEATURE_TYPE'
        )
    """)

    for row in session.execute(query).fetchall():
        feature_name, gene_name, alias_name = row

        if feature_name not in query_terms:
            query_terms[feature_name] = set()

        if gene_name and gene_name not in STOP_WORDS:
            query_terms[feature_name].add(gene_name)

        if alias_name and alias_name not in STOP_WORDS:
            query_terms[feature_name].add(alias_name)

    # Get gene products used for search
    query = text(f"""
        SELECT f.feature_name, g.gene_product
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_gp fg ON f.feature_no = fg.feature_no
        JOIN {DB_SCHEMA}.gene_product g ON fg.gene_product_no = g.gene_product_no
        WHERE fg.used_for_search = 'Y'
    """)

    for row in session.execute(query).fetchall():
        feature_name, gene_product = row

        if feature_name not in query_terms:
            query_terms[feature_name] = set()

        if gene_product and gene_product not in STOP_WORDS:
            query_terms[feature_name].add(gene_product)

    return query_terms


def search_pubmed(query: str, max_results: int = 1000) -> list[str]:
    """
    Search PubMed for a query and return list of PMIDs.
    """
    params = {
        "db": "pubmed",
        "term": query,
        "retmax": max_results,
        "retmode": "json",
    }

    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    try:
        response = requests.get(ESEARCH_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        return data.get("esearchresult", {}).get("idlist", [])

    except Exception as e:
        logger.error(f"PubMed search error: {e}")
        return []


def fetch_pubmed_record(pmid: str) -> dict | None:
    """
    Fetch PubMed record details for a PMID.
    """
    params = {
        "db": "pubmed",
        "id": pmid,
        "retmode": "xml",
    }

    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    try:
        response = requests.get(EFETCH_URL, params=params, timeout=30)
        response.raise_for_status()

        # Parse XML response
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.content)

        article = root.find(".//PubmedArticle")
        if article is None:
            return None

        # Extract fields
        record = {"pmid": pmid}

        # Title
        title_elem = article.find(".//ArticleTitle")
        record["title"] = title_elem.text if title_elem is not None else ""

        # Abstract
        abstract_elem = article.find(".//AbstractText")
        record["abstract"] = abstract_elem.text if abstract_elem is not None else ""

        # Authors
        authors = []
        for author in article.findall(".//Author"):
            last = author.find("LastName")
            init = author.find("Initials")
            if last is not None and init is not None:
                authors.append(f"{last.text} {init.text}")
        record["authors"] = authors

        # Journal
        journal_elem = article.find(".//Journal/Title")
        record["journal"] = journal_elem.text if journal_elem is not None else ""

        # Year
        year_elem = article.find(".//PubDate/Year")
        record["year"] = year_elem.text if year_elem is not None else ""

        # Volume
        volume_elem = article.find(".//Volume")
        record["volume"] = volume_elem.text if volume_elem is not None else ""

        # Issue
        issue_elem = article.find(".//Issue")
        record["issue"] = issue_elem.text if issue_elem is not None else ""

        # Pages
        pages_elem = article.find(".//MedlinePgn")
        record["pages"] = pages_elem.text if pages_elem is not None else ""

        return record

    except Exception as e:
        logger.error(f"PubMed fetch error for {pmid}: {e}")
        return None


def create_citation(record: dict) -> str:
    """Create citation string from PubMed record."""
    parts = []

    if record.get("authors"):
        if len(record["authors"]) > 3:
            parts.append(f"{record['authors'][0]}, et al.")
        else:
            parts.append(", ".join(record["authors"]))

    if record.get("year"):
        parts.append(f"({record['year']})")

    if record.get("title"):
        parts.append(record["title"])

    if record.get("journal"):
        journal_part = record["journal"]
        if record.get("volume"):
            journal_part += f" {record['volume']}"
            if record.get("issue"):
                journal_part += f"({record['issue']})"
        if record.get("pages"):
            journal_part += f":{record['pages']}"
        parts.append(journal_part)

    return " ".join(parts)


def get_fulltext_url(pmid: str) -> str | None:
    """Get full text URL for a PMID using ELink."""
    params = {
        "dbfrom": "pubmed",
        "id": pmid,
        "cmd": "llinks",
        "retmode": "json",
    }

    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    try:
        response = requests.get(ELINK_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Extract URL from response
        linksets = data.get("linksets", [])
        for linkset in linksets:
            urls = linkset.get("idurllist", [])
            for url_info in urls:
                obj_urls = url_info.get("objurls", [])
                for obj_url in obj_urls:
                    url = obj_url.get("url", {}).get("value")
                    if url:
                        return url

        return None

    except Exception as e:
        logger.debug(f"ELink error for {pmid}: {e}")
        return None


def pmid_exists_in_db(session, pmid: str) -> bool:
    """Check if PMID exists in reference, ref_bad, or ref_temp tables."""
    # Check reference table
    query = text(f"""
        SELECT 1 FROM {DB_SCHEMA}.reference WHERE pubmed = :pmid
    """)
    if session.execute(query, {"pmid": pmid}).fetchone():
        return True

    # Check ref_bad table
    query = text(f"""
        SELECT 1 FROM {DB_SCHEMA}.ref_bad WHERE pubmed = :pmid
    """)
    if session.execute(query, {"pmid": pmid}).fetchone():
        return True

    # Check ref_temp table
    query = text(f"""
        SELECT 1 FROM {DB_SCHEMA}.ref_temp WHERE pubmed = :pmid
    """)
    if session.execute(query, {"pmid": pmid}).fetchone():
        return True

    return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load gene-specific PubMed references to REF_TEMP"
    )
    parser.add_argument(
        "source",
        choices=["local", "remote"],
        help="Source of gene names: 'local' or 'remote'",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of genes to process (0 = no limit)",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    which_genes = args.source

    # Set up log file
    log_file = LOG_DIR / f"{which_genes}_load_geneRefs_2_reftemp.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info(f"Starting gene reference search at {datetime.now()}")

    try:
        with SessionLocal() as session:
            # Get query terms for all genes
            logger.info("Retrieving gene names and aliases...")
            query_terms = get_query_terms(session, which_genes)
            logger.info(f"Found {len(query_terms)} features with query terms")

            # Search PubMed for each gene
            all_pmids: set[str] = set()
            gene_count = 0

            for feature_name, terms in query_terms.items():
                if args.limit and gene_count >= args.limit:
                    break

                if not terms:
                    continue

                # Build search query
                terms_query = " OR ".join(f'"{term}"' for term in terms)
                search_query = f"({GENUS}[TW] AND {SPECIES}[TW]) AND ({terms_query})"

                pmids = search_pubmed(search_query)
                all_pmids.update(pmids)

                gene_count += 1

                if gene_count % 100 == 0:
                    logger.info(f"Processed {gene_count} genes, found {len(all_pmids)} PMIDs")

                # Rate limiting
                time.sleep(0.34)  # ~3 requests per second

            logger.info(f"Total unique PMIDs found: {len(all_pmids)}")

            # Load PMIDs to REF_TEMP
            insert_count = 0
            skip_count = 0
            fail_count = 0

            for pmid in sorted(all_pmids, reverse=True):
                # Check if already exists
                if pmid_exists_in_db(session, pmid):
                    logger.debug(f"PMID {pmid} already exists, skipping")
                    skip_count += 1
                    continue

                # Fetch record details
                record = fetch_pubmed_record(pmid)
                if not record:
                    logger.warning(f"Could not fetch record for PMID {pmid}")
                    fail_count += 1
                    continue

                # Create citation
                citation = create_citation(record)

                # Get full text URL
                fulltext_url = get_fulltext_url(pmid)

                # Truncate abstract if too long
                abstract = record.get("abstract", "") or ""
                if len(abstract) > 4000:
                    abstract = abstract[:3950] + "...ABSTRACT TRUNCATED AT 3950 CHARACTERS."

                # Insert into ref_temp
                try:
                    insert_query = text(f"""
                        INSERT INTO {DB_SCHEMA}.ref_temp
                        (pubmed, citation, fulltext_url, abstract, created_by)
                        VALUES (:pmid, :citation, :url, :abstract, :user)
                    """)

                    session.execute(
                        insert_query,
                        {
                            "pmid": pmid,
                            "citation": citation,
                            "url": fulltext_url,
                            "abstract": abstract,
                            "user": ADMIN_USER,
                        },
                    )
                    session.commit()

                    insert_count += 1
                    logger.info(f"Inserted PMID {pmid}")

                except Exception as e:
                    logger.error(f"Error inserting PMID {pmid}: {e}")
                    session.rollback()
                    fail_count += 1

                # Rate limiting
                time.sleep(0.34)

            logger.info(f"\nSuccess count: {insert_count}")
            logger.info(f"Skip count: {skip_count}")
            logger.info(f"Fail count: {fail_count}")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    logger.info(f"Completed at {datetime.now()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
