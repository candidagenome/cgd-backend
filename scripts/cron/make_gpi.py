#!/usr/bin/env python3
from __future__ import annotations

"""
Generate GPI (Gene Product Information) files for GO annotation.

This script generates GPI 2.0 format files for submission to the GO Consortium.
It outputs feature information including IDs, names, descriptions, aliases,
SO type codes, taxon IDs, and UniProt cross-references.

Based on makeGPI.pl by Shuai Weng

Usage:
    python make_gpi.py C_albicans_SC5314
    python make_gpi.py --all

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DOWNLOAD_DIR: Directory for output files
"""

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data")))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output directories
GPI_DIR = DOWNLOAD_DIR / "go"
ARCHIVE_DIR = GPI_DIR / "archive"

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_FEATURES = 100  # Absolute minimum features expected per strain
MAX_FEATURE_CHANGE_PERCENT = 10.0  # Maximum allowed change from previous file

# SO type codes for different feature types
SO_CODE_FOR_TYPE = {
    "ORF": "SO:0001217",
    "ncRNA": "SO:0001263",
    "rRNA": "SO:0001263",
    "snRNA": "SO:0001263",
    "snoRNA": "SO:0001263",
    "tRNA": "SO:0001263",
    "pseudogene": "SO:0000336",
}

# Strain configurations
STRAIN_CONFIGS = {
    "C_albicans_SC5314": {
        "seq_source": "C. albicans SC5314 Assembly 22",
        "taxon_id": "237561",
    },
    "C_dubliniensis_CD36": {
        "seq_source": "C. dubliniensis CD36",
        "taxon_id": "573826",
    },
    "C_glabrata_CBS138": {
        "seq_source": "C. glabrata CBS138",
        "taxon_id": "284593",
    },
    "C_parapsilosis_CDC317": {
        "seq_source": "C. parapsilosis CDC317",
        "taxon_id": "578454",
    },
    "C_auris_B8441": {
        "seq_source": "C. auris B8441",
        "taxon_id": "498019",
    },
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def send_slack_message(message: str, is_error: bool = False) -> None:
    """Send a message to Slack webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping notification")
        return

    emoji = ":x:" if is_error else ":white_check_mark:"
    env_prefix = f"[{ENV_STATE.upper()}] " if ENV_STATE != "prod" else ""

    payload = {
        "text": f"{emoji} {env_prefix}GPI Generation: {message}"
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Slack notification failed: {response.status}")
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")


def count_features_in_file(file_path: Path) -> int:
    """Count non-header lines (features) in a GPI file."""
    if not file_path.exists():
        return 0

    count = 0
    with open(file_path) as f:
        for line in f:
            if not line.startswith("!"):
                count += 1
    return count


def validate_output_file(
    new_file: Path,
    existing_file: Path | None = None,
    strain_abbrev: str = "",
) -> tuple[bool, str]:
    """
    Validate the generated GPI file.

    Returns:
        Tuple of (is_valid, message)
    """
    # Check file exists and has content
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_features_in_file(new_file)

    # Check minimum features
    if new_count < MIN_FEATURES:
        return False, (
            f"Too few features for {strain_abbrev}: {new_count} "
            f"(minimum: {MIN_FEATURES})"
        )

    # Check against existing file if present
    if existing_file and existing_file.exists():
        existing_count = count_features_in_file(existing_file)
        if existing_count > 0:
            change_pct = abs(new_count - existing_count) / existing_count * 100
            if change_pct > MAX_FEATURE_CHANGE_PERCENT:
                return False, (
                    f"Feature count changed too much for {strain_abbrev}: "
                    f"{existing_count} -> {new_count} ({change_pct:.1f}% "
                    f"change, max: {MAX_FEATURE_CHANGE_PERCENT}%)"
                )

    return True, f"Validation passed for {strain_abbrev}: {new_count} features"


def get_genome_version(session, seq_source: str) -> str | None:
    """Get current genome version for the sequence source."""
    query = text(f"""
        SELECT gv.genome_version
        FROM {DB_SCHEMA}.genome_version gv
        WHERE gv.is_ver_current = 'Y'
        AND gv.genome_version_no IN (
            SELECT DISTINCT s.genome_version_no
            FROM {DB_SCHEMA}.seq s
            WHERE s.is_seq_current = 'Y'
            AND s.source = :seq_source
        )
    """)

    result = session.execute(query, {"seq_source": seq_source}).fetchone()
    return result[0] if result else None


def get_features(session, seq_source: str) -> list[dict]:
    """Get features for GPI output."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.dbxref_id, f.feature_type,
               f.gene_name, f.headline
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        JOIN {DB_SCHEMA}.genome_version gv ON (s.genome_version_no = gv.genome_version_no AND gv.is_ver_current = 'Y')
        WHERE f.feature_type IN ('ORF', 'ncRNA', 'rRNA', 'snRNA', 'snoRNA', 'tRNA', 'pseudogene')
    """)

    result = session.execute(query, {"seq_source": seq_source})
    features = []

    for row in result:
        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "dbxref_id": row[2],
            "feature_type": row[3],
            "gene_name": row[4],
            "headline": row[5],
        })

    return features


def get_aliases(session, feature_no: int) -> list[str]:
    """Get aliases for a feature."""
    query = text(f"""
        SELECT a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON (fa.alias_no = a.alias_no AND fa.feature_no = :feature_no)
    """)

    result = session.execute(query, {"feature_no": feature_no})
    return [row[0] for row in result if row[0]]


def get_uniprot_ids(session, feature_no: int) -> list[str]:
    """Get UniProt IDs for a feature."""
    query = text(f"""
        SELECT d.dbxref_id
        FROM {DB_SCHEMA}.dbxref d
        JOIN {DB_SCHEMA}.dbxref_feat df ON (d.dbxref_no = df.dbxref_no AND df.feature_no = :feature_no)
        WHERE d.dbxref_type IN ('SwissProt', 'UniProtKB')
    """)

    result = session.execute(query, {"feature_no": feature_no})
    return [row[0] for row in result if row[0]]


def clean_description(headline: str | None) -> str:
    """Clean headline for GPI description field."""
    if not headline:
        return ""

    # Extract first part before semicolon
    desc = headline
    if ";" in headline:
        desc = headline.split(";")[0]

    # Remove HTML tags
    desc = desc.replace("<i>", "").replace("</i>", "")
    desc = desc.replace("<sub>", "").replace("</sub>", "")
    desc = desc.replace("<sup>", "").replace("</sup>", "")

    return desc.strip()


def generate_gpi(
    strain_abbrev: str,
    output_dir: Path | None = None,
) -> bool:
    """
    Generate GPI file for a strain.

    Args:
        strain_abbrev: Strain abbreviation (e.g., C_albicans_SC5314)
        output_dir: Output directory (default: DOWNLOAD_DIR/go)

    Returns:
        True on success, False on failure
    """
    if strain_abbrev not in STRAIN_CONFIGS:
        logger.error(f"Unknown strain: {strain_abbrev}")
        return False

    config = STRAIN_CONFIGS[strain_abbrev]
    seq_source = config["seq_source"]
    taxon_id = config["taxon_id"]

    if output_dir is None:
        output_dir = GPI_DIR

    output_dir.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    gpi_filename = f"{strain_abbrev}.gpi"
    final_file = output_dir / gpi_filename
    today = datetime.now().strftime("%Y-%m-%d")
    today_tag = datetime.now().strftime("%Y%m%d")

    # Create temp directory for safe generation
    temp_dir = Path(tempfile.mkdtemp(prefix="gpi_"))
    temp_file = temp_dir / gpi_filename

    try:
        with SessionLocal() as session:
            # Get genome version
            genome_version = get_genome_version(session, seq_source)
            if not genome_version:
                logger.warning(f"No genome version found for {seq_source}")
                genome_version = "unknown"

            # Get features
            features = get_features(session, seq_source)
            logger.info(f"Found {len(features)} features for {strain_abbrev}")

            # Write GPI file to temp location
            with open(temp_file, "w") as f:
                # Write header
                f.write("!gpi-version: 2.0\n")
                f.write(f"!generated-by: {PROJECT_ACRONYM}\n")
                f.write(f"!date-generated: {today}\n")
                f.write("!URL: http://www.candidagenome.org\n")
                f.write(f"!Project-release: {seq_source} genome version {genome_version}\n")

                # Write feature lines
                for feat in features:
                    feature_no = feat["feature_no"]
                    feature_name = feat["feature_name"] or ""
                    dbxref_id = f"{PROJECT_ACRONYM}:{feat['dbxref_id']}"
                    feature_type = feat["feature_type"]
                    gene_name = feat["gene_name"] or ""
                    headline = feat["headline"]

                    # Get SO code
                    so_code = SO_CODE_FOR_TYPE.get(feature_type, "")

                    # Get taxon
                    taxon = f"NCBITaxon:{taxon_id}"

                    # Clean description
                    description = clean_description(headline)

                    # Get aliases and build name list
                    aliases = get_aliases(session, feature_no)
                    name_list = gene_name
                    for alias in aliases:
                        if name_list:
                            name_list += " | "
                        name_list += alias

                    # Get UniProt IDs (only for ORFs)
                    up_list = ""
                    if feature_type == "ORF":
                        uniprot_ids = get_uniprot_ids(session, feature_no)
                        up_list = " | ".join(f"UniProtKB:{up}" for up in uniprot_ids)

                    # Write GPI line (tab-separated)
                    # Columns: DB_Object_ID, DB_Object_Symbol, DB_Object_Name,
                    #          DB_Object_Synonym(s), DB_Object_Type, Taxon,
                    #          Parent_Object_ID, DB_Xref(s), Gene_Product_Properties
                    f.write(f"{dbxref_id}\t{feature_name}\t{description}\t{name_list}\t"
                            f"{so_code}\t{taxon}\t\t{dbxref_id}\t\t{up_list}\t\n")

            logger.info(f"Generated temp file: {temp_file}")

            # Validate the generated file
            is_valid, validation_msg = validate_output_file(
                temp_file,
                final_file if final_file.exists() else None,
                strain_abbrev,
            )

            if not is_valid:
                error_msg = f"Validation failed: {validation_msg}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            # Archive existing file before replacing
            if final_file.exists():
                archive_file = ARCHIVE_DIR / f"{strain_abbrev}_{today_tag}.gpi"
                try:
                    shutil.copy(str(final_file), str(archive_file))
                    logger.info(f"Archived {final_file} to {archive_file}")
                except Exception as e:
                    logger.warning(f"Could not archive {final_file}: {e}")

            # Copy validated file to final location
            shutil.copy(str(temp_file), str(final_file))
            logger.info(f"Copied validated file to {final_file}")

            print(f"Generated: {final_file} ({len(features)} features)")

            # Send success notification
            send_slack_message(
                f"Successfully generated {gpi_filename} with "
                f"{len(features)} features"
            )

            return True

    except Exception as e:
        logger.exception(f"Error generating GPI for {strain_abbrev}: {e}")
        send_slack_message(
            f"Error generating GPI for {strain_abbrev}: {e}",
            is_error=True
        )
        return False

    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate GPI files for GO annotation"
    )
    parser.add_argument(
        "strain",
        nargs="?",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate GPI files for all strains",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory",
    )

    args = parser.parse_args()

    if args.all:
        strains = list(STRAIN_CONFIGS.keys())
    elif args.strain:
        strains = [args.strain]
    else:
        parser.print_help()
        return 1

    success = True
    for strain in strains:
        if not generate_gpi(strain, args.output_dir):
            success = False

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
