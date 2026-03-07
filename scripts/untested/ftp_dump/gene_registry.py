#!/usr/bin/env python3
"""
Create gene registry FTP files.

This script generates gene name list files in both tab-delimited and
human-readable text formats.

Based on generegistry.pl.

Usage:
    python gene_registry.py
    python gene_registry.py --debug
    python gene_registry.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    FTP_DIR: FTP directory for output files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)

Output Files:
    registry.genenames.tab - Tab-delimited file
    registry.genenames.txt - Human-readable text file
"""

import argparse
import logging
import os
import re
import shutil
import sys
import textwrap
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output directory
DATA_DIR = FTP_DIR / "data_download" / "gene_registry"

# File names
TAB_FNAME = "registry.genenames.tab"
TEXT_FNAME = "registry.genenames.txt"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def delete_unwanted_chars(text_str: str | None) -> str:
    """Remove unwanted characters from text."""
    if not text_str:
        return ""

    # Remove control characters and normalize whitespace
    text_str = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", text_str)
    text_str = re.sub(r"\s+", " ", text_str)
    return text_str.strip()


def get_phenotype_text(session) -> dict[int, str]:
    """Get free text phenotypes for all features."""
    query = text(f"""
        SELECT f.feature_no, p.phenotype
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_pheno fp ON f.feature_no = fp.feature_no
        JOIN {DB_SCHEMA}.phenotype p ON fp.phenotype_no = p.phenotype_no
        WHERE fp.phenotype_type = 'Free text'
    """)

    phenotypes: dict[int, list[str]] = {}
    for row in session.execute(query).fetchall():
        feat_no, pheno = row
        pheno = delete_unwanted_chars(pheno)

        if not pheno:
            continue

        if feat_no not in phenotypes:
            phenotypes[feat_no] = []
        phenotypes[feat_no].append(pheno)

    return {k: "|".join(v) for k, v in phenotypes.items()}


def get_gene_info(session) -> list[dict]:
    """
    Get gene information for registry files.

    Returns list of dicts with gene information, consolidated by feature.
    """
    query = text(f"""
        SELECT DISTINCT f.feature_no, f.gene_name, a.alias_name,
               f.headline, gp.gene_product, f.feature_no as pheno_key,
               CASE WHEN f.feature_type = 'ORF' THEN f.feature_name ELSE NULL END as orf_name,
               f.dbxref_id
        FROM {DB_SCHEMA}.feature f
        LEFT JOIN {DB_SCHEMA}.feat_alias fa ON f.feature_no = fa.feature_no
        LEFT JOIN {DB_SCHEMA}.alias a ON fa.alias_no = a.alias_no
        LEFT JOIN {DB_SCHEMA}.feat_gp fg ON f.feature_no = fg.feature_no
        LEFT JOIN {DB_SCHEMA}.gene_product gp ON fg.gene_product_no = gp.gene_product_no
        WHERE f.gene_name IS NOT NULL
        ORDER BY f.gene_name
    """)

    results = session.execute(query).fetchall()

    # Consolidate by feature_no and dbxref_id
    consolidated: dict[tuple, dict] = {}

    for row in results:
        (feat_no, gene_name, alias_name, headline, gene_product,
         pheno_key, orf_name, dbxref_id) = row

        key = (feat_no, dbxref_id)

        if key not in consolidated:
            consolidated[key] = {
                "feature_no": feat_no,
                "gene_name": gene_name or "",
                "aliases": set(),
                "headline": headline or "",
                "gene_products": set(),
                "pheno_key": pheno_key,
                "orf_name": orf_name or "",
                "dbxref_id": dbxref_id or "",
            }

        if alias_name:
            consolidated[key]["aliases"].add(alias_name)
        if gene_product:
            consolidated[key]["gene_products"].add(gene_product)

    # Convert sets to pipe-delimited strings
    result = []
    for data in consolidated.values():
        data["aliases"] = "|".join(sorted(data["aliases"]))
        data["gene_products"] = "|".join(sorted(data["gene_products"]))
        result.append(data)

    # Sort by gene name
    result.sort(key=lambda x: x["gene_name"].upper())

    return result


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create gene registry FTP files"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Create output directory
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Archive paths
    archive_dir = DATA_DIR / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Get current date for archiving
    now = datetime.now()
    date_stamp = now.strftime("%Y%m%d")

    # Output files
    tab_file = DATA_DIR / TAB_FNAME
    text_file = DATA_DIR / TEXT_FNAME

    # Archive existing files
    if tab_file.exists():
        archive_file = archive_dir / f"{TAB_FNAME}.{date_stamp}"
        shutil.move(str(tab_file), str(archive_file))
        logger.info(f"Archived {tab_file} to {archive_file}")

    if text_file.exists():
        archive_file = archive_dir / f"{TEXT_FNAME}.{date_stamp}"
        shutil.move(str(text_file), str(archive_file))
        logger.info(f"Archived {text_file} to {archive_file}")

    try:
        with SessionLocal() as session:
            # Get phenotype text
            logger.info("Loading phenotype data...")
            phenotypes = get_phenotype_text(session)

            # Get gene info
            logger.info("Loading gene information...")
            genes = get_gene_info(session)
            logger.info(f"Found {len(genes)} genes with names")

            # Title fields for text file
            titles = [
                "Locus_No:\t",
                "Locus_Name:\t",
                "Alias_Name:\t",
                "Description:\t",
                "Gene_Product:\t",
                "Phenotype:\t",
                "ORF_Name:\t",
                f"{PROJECT_ACRONYM}ID:\t\t",
            ]

            # Write files
            count = 0
            with open(tab_file, "w") as tf, open(text_file, "w") as xf:
                for gene in genes:
                    # Get phenotype for this feature
                    pheno = phenotypes.get(gene["pheno_key"], "")

                    # Build data row
                    row = [
                        gene["gene_name"],
                        gene["aliases"],
                        gene["headline"],
                        gene["gene_products"],
                        pheno,
                        gene["orf_name"],
                        gene["dbxref_id"],
                    ]

                    # Write tab file
                    tf.write("\t".join(row) + "\n")

                    # Write text file
                    for i, value in enumerate(row):
                        if value and value.strip():
                            xf.write(titles[i + 1])
                            # Wrap long lines
                            wrapped = textwrap.fill(
                                value,
                                width=64,
                                initial_indent="",
                                subsequent_indent="\t\t",
                            )
                            xf.write(wrapped + "\n")

                    xf.write("\n")
                    count += 1

            logger.info(f"Wrote {count} gene entries")
            logger.info(f"Created {tab_file}")
            logger.info(f"Created {text_file}")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
