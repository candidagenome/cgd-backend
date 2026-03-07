#!/usr/bin/env python3
"""
Dump phenotype data for all features.

This script exports all phenotype data for features of a specified organism
to a tab-delimited file for download.

Usage:
    python dump_phenotype_data.py <organism_abbreviation>
    python dump_phenotype_data.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    HTML_ROOT_DIR: Root directory for HTML/download files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_organism_info(session, org_abbrev: str) -> dict | None:
    """
    Get organism information from the database.

    Args:
        session: Database session
        org_abbrev: Organism abbreviation

    Returns:
        Dictionary with organism info or None if not found
    """
    query = text(f"""
        SELECT organism_no, organism_name, organism_abbrev
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :org_abbrev
    """)

    result = session.execute(query, {"org_abbrev": org_abbrev}).first()

    if result:
        return {
            "organism_no": result[0],
            "organism_name": result[1],
            "organism_abbrev": result[2],
        }
    return None


def get_species_abbrev(session, organism_no: int) -> str | None:
    """
    Get the species-level abbreviation for an organism.

    Args:
        session: Database session
        organism_no: Organism number

    Returns:
        Species abbreviation or None
    """
    # Get parent at species level through taxonomy
    query = text(f"""
        SELECT o2.organism_abbrev
        FROM {DB_SCHEMA}.organism o1
        JOIN {DB_SCHEMA}.organism o2 ON o1.parent_organism_no = o2.organism_no
        WHERE o1.organism_no = :organism_no
        AND o2.tax_rank = 'Species'
    """)

    result = session.execute(query, {"organism_no": organism_no}).first()

    if result:
        return result[0]

    # If no parent found, might be species itself
    query2 = text(f"""
        SELECT organism_abbrev
        FROM {DB_SCHEMA}.organism
        WHERE organism_no = :organism_no
        AND tax_rank = 'Species'
    """)

    result2 = session.execute(query2, {"organism_no": organism_no}).first()
    return result2[0] if result2 else None


def dump_phenotype_data(session, org_abbrev: str, output_file: Path) -> int:
    """
    Dump phenotype data to a file.

    Args:
        session: Database session
        org_abbrev: Organism abbreviation
        output_file: Path to output file

    Returns:
        Number of records written
    """
    # Query to get phenotype data
    query = text(f"""
        SELECT
            f.feature_name,
            f.gene_name,
            p.observable,
            p.qualifier,
            p.mutant_type,
            p.strain_background,
            p.experiment_type,
            p.experiment_comment,
            p.allele,
            p.allele_comment,
            p.reporter,
            p.reporter_comment,
            p.details,
            r.pubmed,
            r.citation
        FROM {DB_SCHEMA}.phenotype p
        JOIN {DB_SCHEMA}.feature f ON p.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        LEFT JOIN {DB_SCHEMA}.reference r ON p.reference_no = r.reference_no
        WHERE o.organism_abbrev = :org_abbrev
        ORDER BY f.feature_name, p.observable
    """)

    result = session.execute(query, {"org_abbrev": org_abbrev})

    # Write header
    headers = [
        "Feature_Name",
        "Gene_Name",
        "Observable",
        "Qualifier",
        "Mutant_Type",
        "Strain_Background",
        "Experiment_Type",
        "Experiment_Comment",
        "Allele",
        "Allele_Comment",
        "Reporter",
        "Reporter_Comment",
        "Details",
        "PubMed_ID",
        "Citation",
    ]

    count = 0
    with open(output_file, "w") as f:
        f.write("\t".join(headers) + "\n")

        for row in result:
            values = [str(v) if v is not None else "" for v in row]
            f.write("\t".join(values) + "\n")
            count += 1

    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump phenotype data for all features of an organism"
    )
    parser.add_argument(
        "organism",
        help="Organism abbreviation (e.g., C_albicans_SC5314)",
    )

    args = parser.parse_args()
    org_abbrev = args.organism

    # Set up logging to file
    log_file = LOG_DIR / f"dumpPhenotypeData_{org_abbrev}.log"
    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Dumping Phenotype data for organism: {org_abbrev}")
    logger.info(f"Start execution: {datetime.now()}")

    try:
        with SessionLocal() as session:
            # Verify organism exists
            org_info = get_organism_info(session, org_abbrev)
            if not org_info:
                logger.error(f"Organism '{org_abbrev}' not found in database")
                return 1

            # Get species abbreviation for output filename
            species_abbrev = get_species_abbrev(session, org_info["organism_no"])
            if not species_abbrev:
                species_abbrev = org_abbrev

            # Set up output directory and file
            dump_dir = HTML_ROOT_DIR / "download" / "phenotype"
            dump_dir.mkdir(parents=True, exist_ok=True)

            output_file = dump_dir / f"{species_abbrev}_phenotype_data.tab"

            # Dump data
            count = dump_phenotype_data(session, org_abbrev, output_file)

            logger.info(f"Wrote {count} phenotype records to {output_file}")
            logger.info(f"End execution: {datetime.now()}")

            return 0

    except Exception as e:
        logger.exception(f"Error dumping phenotype data: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
