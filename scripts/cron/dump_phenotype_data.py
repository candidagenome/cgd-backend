#!/usr/bin/env python3
from __future__ import annotations

"""
Dump phenotype data for all features of a given organism.

This script exports all phenotype annotation data to a tab-delimited file
for download. It was created as an alternative to batch download which
times out when requesting phenotype results for all features on a chromosome.

Based on dumpPhenotypeData.pl by Adil Lotia (May 6, 2009).

Output format (tab-delimited):
1) Feature Name (Mandatory)
2) Feature Type (Mandatory)
3) Gene Name (Optional)
4) CGDID (Mandatory)
5) Reference (CGD_REF Required, PMID optional) - PMID: ####|CGD_REF: ####
6) Experiment Type (Mandatory)
7) Mutant Type (Mandatory)
8) Allele (Optional)
9) Strain background (Mandatory)
10) Phenotype (Mandatory) - qualifier + observable
11) Chemical (Optional)
12) Condition (Optional)
13) Details (Optional)
14) Reporter (Optional)
15) Anatomical Structure (Optional)
16) Virulence Model (Optional)
17) Species

Usage:
    python dump_phenotype_data.py <organism_abbrev>
    python dump_phenotype_data.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
    DATA_DIR: Directory for data files (default: PROJECT_ROOT/data)
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import bindparam, text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directories to path
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", str(PROJECT_ROOT / "data")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))

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


def get_phenotype_data(session, organism_no: int, species_name: str) -> list[dict]:
    """
    Get all phenotype data for features of a given organism.

    Returns a list of dictionaries containing phenotype annotation data.
    """
    # Get base phenotype annotation data
    base_query = text(f"""
        SELECT
            pa.pheno_annotation_no,
            pa.experiment_no,
            f.feature_name,
            f.feature_type,
            f.gene_name,
            f.dbxref_id,
            p.experiment_type,
            p.mutant_type,
            p.qualifier,
            p.observable
        FROM {DB_SCHEMA}.pheno_annotation pa
        JOIN {DB_SCHEMA}.feature f ON pa.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.phenotype p ON pa.phenotype_no = p.phenotype_no
        WHERE f.organism_no = :organism_no
        ORDER BY f.feature_name, p.observable
    """)

    base_results = session.execute(base_query, {"organism_no": organism_no}).fetchall()

    if not base_results:
        return []

    # Collect all pheno_annotation_nos and experiment_nos
    pa_nos = []
    exp_nos = []
    for row in base_results:
        pa_nos.append(row[0])
        if row[1]:
            exp_nos.append(row[1])

    # Oracle IN clause limit is 1000, so batch the queries
    BATCH_SIZE = 900

    # Get references via ref_link (one annotation can have multiple refs)
    ref_map: dict[int, list[tuple]] = defaultdict(list)
    for i in range(0, len(pa_nos), BATCH_SIZE):
        batch = pa_nos[i:i + BATCH_SIZE]
        ref_query = text(f"""
            SELECT rl.primary_key, r.pubmed, r.dbxref_id
            FROM {DB_SCHEMA}.ref_link rl
            JOIN {DB_SCHEMA}.reference r ON rl.reference_no = r.reference_no
            WHERE rl.tab_name = 'PHENO_ANNOTATION'
            AND rl.col_name = 'PHENO_ANNOTATION_NO'
            AND rl.primary_key IN :pa_nos
        """).bindparams(bindparam("pa_nos", expanding=True))
        ref_results = session.execute(ref_query, {"pa_nos": batch}).fetchall()
        for row in ref_results:
            ref_map[row[0]].append((row[1], row[2]))

    # Get experiment properties
    prop_map: dict[int, dict[str, str]] = defaultdict(dict)
    exp_nos_unique = list(set(exp_nos))
    for i in range(0, len(exp_nos_unique), BATCH_SIZE):
        batch = exp_nos_unique[i:i + BATCH_SIZE]
        prop_query = text(f"""
            SELECT ee.experiment_no, ep.property_type, ep.property_value
            FROM {DB_SCHEMA}.expt_exptprop ee
            JOIN {DB_SCHEMA}.expt_property ep ON ee.expt_property_no = ep.expt_property_no
            WHERE ee.experiment_no IN :exp_nos
        """).bindparams(bindparam("exp_nos", expanding=True))
        prop_results = session.execute(prop_query, {"exp_nos": batch}).fetchall()

        for row in prop_results:
            exp_no, prop_type, prop_value = row
            # Map property types to our columns
            prop_map[exp_no][prop_type] = prop_value or ""

    # Build final phenotype records
    phenotypes = []
    for row in base_results:
        pa_no = row[0]
        exp_no = row[1]
        feature_name = row[2]
        feature_type = row[3]
        gene_name = row[4] or ""
        dbxref_id = row[5] or ""
        experiment_type = row[6] or ""
        mutant_type = row[7] or ""
        qualifier = row[8] or ""
        observable = row[9] or ""

        # Build phenotype string (qualifier + observable)
        phenotype = f"{qualifier} {observable}".strip() if qualifier else observable

        # Build reference string: PMID: ####|CGD_REF: ####
        refs = ref_map.get(pa_no, [])
        ref_parts = []
        for pubmed, cgd_ref in refs:
            parts = []
            if pubmed:
                parts.append(f"PMID: {pubmed}")
            if cgd_ref:
                parts.append(f"CGD_REF: {cgd_ref}")
            if parts:
                ref_parts.append("|".join(parts))
        reference = "; ".join(ref_parts) if ref_parts else ""

        # Get experiment properties
        props = prop_map.get(exp_no, {})

        phenotypes.append({
            "feature_name": feature_name,
            "feature_type": feature_type,
            "gene_name": gene_name,
            "cgdid": dbxref_id,
            "reference": reference,
            "experiment_type": experiment_type,
            "mutant_type": mutant_type,
            "allele": props.get("Allele", ""),
            "strain_background": props.get("strain_background", ""),
            "phenotype": phenotype,
            "chemical": props.get("Chemical_pending", "") or props.get("chebi_ontology", ""),
            "condition": props.get("Condition", ""),
            "details": props.get("Details", ""),
            "reporter": props.get("Reporter", ""),
            "anatomical_structure": props.get("fungal_anatomy_ontology", ""),
            "virulence_model": props.get("virulence_model", ""),
            "species": species_name,
        })

    return phenotypes


def write_phenotype_file(phenotypes: list[dict], output_file: Path) -> int:
    """
    Write phenotype data to a tab-delimited file.

    Returns the number of records written.
    """
    # Column headers matching official format
    headers = [
        "Feature Name",
        "Feature Type",
        "Gene Name",
        "CGDID",
        "Reference",
        "Experiment Type",
        "Mutant Type",
        "Allele",
        "Strain Background",
        "Phenotype",
        "Chemical",
        "Condition",
        "Details",
        "Reporter",
        "Anatomical Structure",
        "Virulence Model",
        "Species",
    ]

    with open(output_file, "w") as f:
        # Write header
        f.write("\t".join(headers) + "\n")

        # Write data rows
        for p in phenotypes:
            row = [
                p["feature_name"],
                p["feature_type"],
                p["gene_name"],
                p["cgdid"],
                p["reference"],
                p["experiment_type"],
                p["mutant_type"],
                p["allele"],
                p["strain_background"],
                p["phenotype"],
                p["chemical"],
                p["condition"],
                p["details"],
                p["reporter"],
                p["anatomical_structure"],
                p["virulence_model"],
                p["species"],
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
        help="Organism abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: DATA_DIR/phenotype/)",
    )

    args = parser.parse_args()

    org_abbrev = args.organism_abbrev
    output_dir = args.output_dir or DATA_DIR / "phenotype"

    # Ensure log directory exists
    LOG_DIR.mkdir(parents=True, exist_ok=True)

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

            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)

            output_file = output_dir / f"{org_abbrev}_phenotype_data.tab"

            # Get phenotype data
            logger.info("Retrieving phenotype data from database...")
            phenotypes = get_phenotype_data(
                session, org_info["organism_no"], org_info["organism_name"]
            )

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
