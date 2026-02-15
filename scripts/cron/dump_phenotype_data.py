#!/usr/bin/env python3
"""
Dump phenotype data for all features of a given organism.

This script exports all phenotype annotation data to a tab-delimited file
for download. It was created as an alternative to batch download which
times out when requesting phenotype results for all features on a chromosome.

Based on dumpPhenotypeData.pl by Adil Lotia (May 6, 2009).

Usage:
    python dump_phenotype_data.py <organism_abbrev>
    python dump_phenotype_data.py A_nidulans_FGSC_A4
    python dump_phenotype_data.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
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

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_organism_info(session, org_abbrev: str) -> dict | None:
    """Get organism information from database."""
    query = text(f"""
        SELECT o.organism_no, o.organism_abbrev, o.organism_name, o.taxon_id
        FROM {DB_SCHEMA}.organism o
        WHERE o.organism_abbrev = :org_abbrev
    """)
    result = session.execute(query, {"org_abbrev": org_abbrev}).fetchone()

    if not result:
        return None

    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "organism_name": result[2],
        "taxon_id": result[3],
    }


def get_species_abbrev(session, org_abbrev: str) -> str | None:
    """
    Get the species-level organism abbreviation.

    For strain-level organisms, returns the parent species abbreviation.
    For species-level organisms, returns the same abbreviation.
    """
    # First try to get parent species through taxonomy hierarchy
    query = text(f"""
        SELECT parent.organism_abbrev
        FROM {DB_SCHEMA}.organism child
        JOIN {DB_SCHEMA}.organism parent
            ON child.parent_organism_no = parent.organism_no
        WHERE child.organism_abbrev = :org_abbrev
        AND parent.tax_rank = 'Species'
    """)
    result = session.execute(query, {"org_abbrev": org_abbrev}).fetchone()

    if result:
        return result[0]

    # If no parent species found, check if this organism is itself a species
    query = text(f"""
        SELECT organism_abbrev
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :org_abbrev
        AND tax_rank = 'Species'
    """)
    result = session.execute(query, {"org_abbrev": org_abbrev}).fetchone()

    if result:
        return result[0]

    # Fallback: use the organism abbreviation as-is
    return org_abbrev


def get_phenotype_data(session, org_abbrev: str) -> list[dict]:
    """
    Get all phenotype data for features of a given organism.

    Returns a list of dictionaries containing phenotype annotation data.
    """
    query = text(f"""
        SELECT
            f.feature_name,
            f.gene_name,
            f.dbxref_id,
            p.observable,
            p.qualifier,
            p.experiment_type,
            p.mutant_type,
            e.strain_background,
            e.allele,
            e.allele_details,
            e.reporter,
            e.reporter_details,
            e.chemical,
            e.condition,
            e.experiment_details,
            r.pubmed,
            r.citation
        FROM {DB_SCHEMA}.pheno_annotation pa
        JOIN {DB_SCHEMA}.feature f ON pa.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.phenotype p ON pa.phenotype_no = p.phenotype_no
        LEFT JOIN {DB_SCHEMA}.experiment e ON pa.experiment_no = e.experiment_no
        LEFT JOIN {DB_SCHEMA}.reference r ON e.reference_no = r.reference_no
        WHERE f.organism_abbrev = :org_abbrev
        ORDER BY f.feature_name, p.observable
    """)

    results = session.execute(query, {"org_abbrev": org_abbrev}).fetchall()

    phenotypes = []
    for row in results:
        phenotypes.append({
            "feature_name": row[0],
            "gene_name": row[1] or "",
            "dbxref_id": row[2] or "",
            "observable": row[3] or "",
            "qualifier": row[4] or "",
            "experiment_type": row[5] or "",
            "mutant_type": row[6] or "",
            "strain_background": row[7] or "",
            "allele": row[8] or "",
            "allele_details": row[9] or "",
            "reporter": row[10] or "",
            "reporter_details": row[11] or "",
            "chemical": row[12] or "",
            "condition": row[13] or "",
            "experiment_details": row[14] or "",
            "pubmed": row[15] or "",
            "citation": row[16] or "",
        })

    return phenotypes


def write_phenotype_file(phenotypes: list[dict], output_file: Path) -> int:
    """
    Write phenotype data to a tab-delimited file.

    Returns the number of records written.
    """
    # Column headers
    headers = [
        "Feature Name",
        "Gene Name",
        "DBXREF ID",
        "Observable",
        "Qualifier",
        "Experiment Type",
        "Mutant Type",
        "Strain Background",
        "Allele",
        "Allele Details",
        "Reporter",
        "Reporter Details",
        "Chemical",
        "Condition",
        "Experiment Details",
        "PubMed ID",
        "Citation",
    ]

    with open(output_file, "w") as f:
        # Write header
        f.write("\t".join(headers) + "\n")

        # Write data rows
        for p in phenotypes:
            row = [
                p["feature_name"],
                p["gene_name"],
                p["dbxref_id"],
                p["observable"],
                p["qualifier"],
                p["experiment_type"],
                p["mutant_type"],
                p["strain_background"],
                p["allele"],
                p["allele_details"],
                p["reporter"],
                p["reporter_details"],
                p["chemical"],
                p["condition"],
                p["experiment_details"],
                str(p["pubmed"]) if p["pubmed"] else "",
                p["citation"],
            ]
            f.write("\t".join(row) + "\n")

    return len(phenotypes)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump phenotype data for all features of a given organism"
    )
    parser.add_argument(
        "organism_abbrev",
        help="Organism abbreviation (e.g., A_nidulans_FGSC_A4, C_albicans_SC5314)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: HTML_ROOT_DIR/download/phenotype/)",
    )

    args = parser.parse_args()

    org_abbrev = args.organism_abbrev
    output_dir = args.output_dir or HTML_ROOT_DIR / "download" / "phenotype"

    # Set up file logging
    log_file = LOG_DIR / f"dumpPhenotypeData_{org_abbrev}.log"
    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Dumping phenotype data for {org_abbrev}")
    logger.info(f"Start execution: {datetime.now()}")

    try:
        with SessionLocal() as session:
            # Verify organism exists
            org_info = get_organism_info(session, org_abbrev)
            if not org_info:
                logger.error(f"Organism {org_abbrev} not found in database")
                return 1

            logger.info(f"Found organism: {org_info['organism_name']}")

            # Get species abbreviation for output filename
            species_abbrev = get_species_abbrev(session, org_abbrev)
            if not species_abbrev:
                species_abbrev = org_abbrev

            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)

            if not output_dir.exists():
                logger.error(f"Output directory does not exist: {output_dir}")
                return 1

            output_file = output_dir / f"{species_abbrev}_phenotype_data.tab"

            # Get phenotype data
            logger.info("Retrieving phenotype data from database...")
            phenotypes = get_phenotype_data(session, org_abbrev)

            if not phenotypes:
                logger.warning(f"No phenotype data found for {org_abbrev}")
                return 0

            # Write to file
            logger.info(f"Writing {len(phenotypes)} records to {output_file}")
            count = write_phenotype_file(phenotypes, output_file)

            logger.info(f"Successfully wrote {count} phenotype records")
            logger.info(f"End execution: {datetime.now()}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
