#!/usr/bin/env python3
"""
Dump GO annotations to gene_association file.

This script exports GO annotations from the database into the standard
GAF (Gene Association File) format for submission to the Gene Ontology
Consortium.

Based on dumpAnnotation.pl.

Usage:
    python dump_go_annotation.py
    python dump_go_annotation.py --debug
    python dump_go_annotation.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    FTP_DIR: FTP directory for output files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)
    TAXON_ID: Organism taxon ID
"""

import argparse
import gzip
import hashlib
import logging
import os
import shutil
import subprocess
import sys
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
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
TAXON_ID = os.getenv("TAXON_ID", "5476")  # Default to C. albicans

# Output configuration
DATABASE_NAME = f"Candida Genome Database ({PROJECT_ACRONYM})"
URL = os.getenv("HTM_ROOT_URL", "http://www.candidagenome.org")
EMAIL = os.getenv("CURATORS_EMAIL", "candida-curator@lists.stanford.edu")
FUNDING = "NHGRI at US NIH, grant number 5-P41-HG001315"
NAME_TYPE = "gene"

# File paths
CVS_DIR = FTP_DIR / "go" / "gene-associations" / "submission"
DB_CODE_FILE = DATA_DIR / "GO_DB_code_mapping"

# Column indices for annotation data
DBID = 0
NAME = 1
QUALIFIER = 2
GOID = 3
REFERENCE = 4
EVIDENCE = 5
SUPPORT_EVIDENCE = 6
ASPECT = 7
GENE_PRODUCT = 8
ALIAS = 9
TAG = 10
TAXON = 11
DATE = 12
SOURCE = 13
LAST_FIELD = 13

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def format_goid(goid: str | int) -> str:
    """Format GO ID with proper prefix and padding."""
    goid_str = str(goid)
    if goid_str.startswith("GO:"):
        return goid_str

    # Pad to 7 digits
    goid_str = goid_str.zfill(7)
    return f"GO:{goid_str}"


def get_feature_qualifiers(session) -> dict[str, str]:
    """Get feature qualifiers (Verified, Uncharacterized, etc.) for all features."""
    query = text(f"""
        SELECT UPPER(f.feature_name), fp.property_value
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
        WHERE fp.property_type = 'Feature Qualifier'
    """)

    qualifiers = {}
    for row in session.execute(query).fetchall():
        qualifiers[row[0]] = row[1]

    return qualifiers


def get_feature_aliases(session) -> dict[int, str]:
    """Get aliases for all features."""
    query = text(f"""
        SELECT FA.feature_no, A.alias_name
        FROM {DB_SCHEMA}.feat_alias FA
        JOIN {DB_SCHEMA}.alias A ON FA.alias_no = A.alias_no
        ORDER BY 1, 2
    """)

    aliases: dict[int, list[str]] = {}
    for row in session.execute(query).fetchall():
        feat_no, alias_nm = row
        if feat_no not in aliases:
            aliases[feat_no] = []
        aliases[feat_no].append(alias_nm)

    # Join with pipe separator
    return {k: "|".join(v) for k, v in aliases.items()}


def get_feature_gene_products(session) -> dict[int, str]:
    """Get gene products for all features."""
    query = text(f"""
        SELECT FGP.feature_no, GP.gene_product
        FROM {DB_SCHEMA}.feat_gp FGP
        JOIN {DB_SCHEMA}.gene_product GP ON FGP.gene_product_no = GP.gene_product_no
        ORDER BY 1, 2
    """)

    products: dict[int, list[str]] = {}
    for row in session.execute(query).fetchall():
        feat_no, gp = row
        if feat_no not in products:
            products[feat_no] = []
        products[feat_no].append(gp)

    # Join with comma separator
    return {k: ", ".join(v) for k, v in products.items()}


def load_db_code_mapping(code_file: Path) -> dict[str, str]:
    """Load mapping from database source to GO code."""
    mapping = {}

    if not code_file.exists():
        logger.warning(f"DB code mapping file not found: {code_file}")
        return mapping

    with open(code_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) >= 2:
                go_code, db_source = parts[0], parts[1]
                mapping[db_source.upper()] = go_code

    return mapping


def get_go_support_evidence(session, db_codes: dict[str, str]) -> dict[int, str]:
    """Get GO support evidence for all go_ref records."""
    query = text(f"""
        SELECT GD.go_ref_no, GD.support_type, D.source, D.dbxref_id
        FROM {DB_SCHEMA}.goref_dbxref GD
        JOIN {DB_SCHEMA}.dbxref D ON GD.dbxref_no = D.dbxref_no
        ORDER BY 1, 2
    """)

    support: dict[int, list[str]] = {}
    for row in session.execute(query).fetchall():
        go_ref_no, support_type, source, dbxref_id = row

        go_code = db_codes.get(source.upper() if source else "", source or "")

        if go_code and go_code.startswith("GO"):
            # Format as GO ID
            formatted_id = format_goid(dbxref_id)
        else:
            formatted_id = f"{go_code}:{dbxref_id}" if go_code else str(dbxref_id)

        if go_ref_no not in support:
            support[go_ref_no] = []
        support[go_ref_no].append(formatted_id)

    return {k: "|".join(v) for k, v in support.items()}


def get_go_qualifiers(session) -> dict[int, str]:
    """Get GO qualifiers for all go_ref records."""
    query = text(f"""
        SELECT go_ref_no, qualifier
        FROM {DB_SCHEMA}.go_qualifier
        ORDER BY 1, 2
    """)

    qualifiers: dict[int, list[str]] = {}
    for row in session.execute(query).fetchall():
        go_ref_no, qualifier = row
        if go_ref_no not in qualifiers:
            qualifiers[go_ref_no] = []
        qualifiers[go_ref_no].append(qualifier)

    return {k: "|".join(v) for k, v in qualifiers.items()}


def get_reserved_gene_features(session) -> set[int]:
    """Get feature numbers associated with reserved gene names."""
    query = text(f"""
        SELECT feature_no
        FROM {DB_SCHEMA}.gene_reservation
        WHERE date_standardized IS NULL
    """)

    return {row[0] for row in session.execute(query).fetchall()}


def get_go_annotations(session) -> list[tuple]:
    """Get all GO annotations."""
    query = text(f"""
        SELECT F.feature_no, F.feature_name, F.gene_name, F.dbxref_id,
               G.goid, G.go_aspect, GA.go_evidence,
               GR.go_ref_no, R.dbxref_id, R.pubmed, GR.date_created
        FROM {DB_SCHEMA}.feature F
        JOIN {DB_SCHEMA}.go_annotation GA ON F.feature_no = GA.feature_no
        JOIN {DB_SCHEMA}.go G ON GA.go_no = G.go_no
        JOIN {DB_SCHEMA}.go_ref GR ON GA.go_annotation_no = GR.go_annotation_no
        JOIN {DB_SCHEMA}.reference R ON GR.reference_no = R.reference_no
        ORDER BY F.gene_name, F.feature_name, G.go_aspect, G.goid
    """)

    return session.execute(query).fetchall()


def compute_checksum(file_path: Path) -> str:
    """Compute checksum of file contents (excluding headers)."""
    checksum = hashlib.md5()

    with open(file_path, "rb") as f:
        for line in f:
            # Only include lines with tabs (data lines, not headers)
            if b"\t" in line:
                checksum.update(line)

    return checksum.hexdigest()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GO annotations to gene_association file"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file path (overrides default)",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Output file
    new_file = f"gene_association.{PROJECT_ACRONYM.lower()}"
    new_gz = f"{new_file}.gz"

    data_file = args.output or DATA_DIR / new_file
    data_file.parent.mkdir(parents=True, exist_ok=True)

    # Get current date
    date_str = datetime.now().strftime("%m/%d/%Y")

    try:
        with SessionLocal() as session:
            logger.info("Loading reference data...")

            # Load reference data
            qualifiers = get_feature_qualifiers(session)
            aliases = get_feature_aliases(session)
            gene_products = get_feature_gene_products(session)
            db_codes = load_db_code_mapping(DB_CODE_FILE)
            support_evidence = get_go_support_evidence(session, db_codes)
            go_qualifiers = get_go_qualifiers(session)
            reserved_genes = get_reserved_gene_features(session)

            logger.info("Retrieving GO annotations...")
            annotations = get_go_annotations(session)
            logger.info(f"Found {len(annotations)} annotations")

            # Write output file
            with open(data_file, "w") as f:
                # Write header
                f.write(f"!gaf-version: 2.2\n")
                f.write(f"!Date: {date_str}\n")
                f.write(f"!From: {DATABASE_NAME}\n")
                f.write(f"!URL: {URL}\n")
                f.write(f"!Contact Email: {EMAIL}\n")
                f.write(f"!Funding: {FUNDING}\n")
                f.write("!\n")

                # Track features we've seen
                feat_no_list = []
                found_feat_no: set[int] = set()
                annotation_data: dict[int, list[list[str]]] = {}

                for row in annotations:
                    (feat_no, feat_nm, gene_nm, dbid, goid, aspect, evidence,
                     go_ref_no, ref_dbid, pubmed, date_created) = row

                    # Skip deleted/merged features
                    qualifier = qualifiers.get(feat_nm.upper(), "")
                    if any(x in qualifier.lower() for x in ["deleted", "merged", "dubious"]):
                        continue

                    # Clear gene name for reserved genes
                    if feat_no in reserved_genes:
                        gene_nm = ""

                    # Track feature order
                    if feat_no not in found_feat_no:
                        feat_no_list.append(feat_no)

                        # Prepend feature name to aliases if different from gene name
                        if not gene_nm or gene_nm != feat_nm:
                            if feat_no in aliases:
                                aliases[feat_no] = f"{feat_nm}|{aliases[feat_no]}"
                            else:
                                aliases[feat_no] = feat_nm

                        found_feat_no.add(feat_no)
                        annotation_data[feat_no] = []

                    # Format fields
                    formatted_goid = format_goid(goid)
                    date_str_formatted = date_created.strftime("%Y%m%d") if date_created else ""

                    reference = f"{PROJECT_ACRONYM}_REF:{ref_dbid}"
                    if pubmed:
                        reference += f"|PMID:{pubmed}"

                    # Build annotation row
                    ann_row = [""] * (LAST_FIELD + 1)
                    ann_row[DBID] = str(dbid)
                    ann_row[NAME] = gene_nm or feat_nm
                    ann_row[QUALIFIER] = go_qualifiers.get(go_ref_no, "") or ""
                    ann_row[GOID] = formatted_goid
                    ann_row[REFERENCE] = reference
                    ann_row[EVIDENCE] = evidence or ""
                    ann_row[SUPPORT_EVIDENCE] = support_evidence.get(go_ref_no, "") or ""
                    ann_row[ASPECT] = aspect or ""
                    ann_row[GENE_PRODUCT] = gene_products.get(feat_no, "") or ""
                    ann_row[ALIAS] = aliases.get(feat_no, "") or ""
                    ann_row[TAG] = NAME_TYPE
                    ann_row[TAXON] = f"taxon:{TAXON_ID}"
                    ann_row[DATE] = date_str_formatted
                    ann_row[SOURCE] = PROJECT_ACRONYM

                    annotation_data[feat_no].append(ann_row)

                # Write annotations
                for feat_no in feat_no_list:
                    for ann_row in annotation_data[feat_no]:
                        f.write(PROJECT_ACRONYM + "\t")
                        f.write("\t".join(ann_row) + "\n")

            logger.info(f"Wrote annotations to {data_file}")

            # Compare with existing file if it exists
            CVS_DIR.mkdir(parents=True, exist_ok=True)
            existing_gz = CVS_DIR / new_gz

            if existing_gz.exists():
                # Decompress existing file
                old_file = DATA_DIR / f"gene_association.{PROJECT_ACRONYM.lower()}.old"
                with gzip.open(existing_gz, "rb") as f_in:
                    with open(old_file, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # Compare checksums
                old_checksum = compute_checksum(old_file)
                new_checksum = compute_checksum(data_file)

                old_file.unlink()

                if old_checksum == new_checksum:
                    logger.info("No changes detected, skipping update")
                    data_file.unlink()
                    return 0

            # Gzip and copy to CVS directory
            gz_file = DATA_DIR / new_gz
            with open(data_file, "rb") as f_in:
                with gzip.open(gz_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            shutil.copy(str(gz_file), str(existing_gz))
            logger.info(f"Updated {existing_gz}")

            # Clean up
            data_file.unlink()
            gz_file.unlink()

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
