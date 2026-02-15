#!/usr/bin/env python3
"""
Dump Paula Sundstrom files with ORF mappings and GO annotations.

This script creates two tab-delimited files:
1. Short description file: orf6.#, orf19.#, gene name, short description
2. Long description with GO: orf6.#, orf19.#, gene name, full description, GO terms

Based on dumpPaulaSundstromFiles.pl.

Usage:
    python dump_paula_sundstrom_files.py
    python dump_paula_sundstrom_files.py --debug

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import logging
import os
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
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
HTML_ROOT = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))

# Output directory
OUTPUT_DIR = HTML_ROOT / "download" / "misc"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Organism abbreviation
STRAIN_ABBREV = "C_albicans_SC5314"


def get_organism_no(session, strain_abbrev: str) -> int | None:
    """Get organism_no for a strain abbreviation."""
    query = text(f"""
        SELECT organism_no
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :abbrev
    """)

    result = session.execute(query, {"abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_features_for_organism(
    session, organism_no: int
) -> dict[int, str]:
    """Get features (ORFs and transposable elements) for an organism."""
    query = text(f"""
        SELECT feature_no, feature_name
        FROM {DB_SCHEMA}.feature
        WHERE feature_type IN ('ORF', 'transposable_element_gene')
        AND organism_no = :org_no
    """)

    features = {}
    for row in session.execute(query, {"org_no": organism_no}).fetchall():
        feature_no, feature_name = row
        features[feature_no] = feature_name

    return features


def get_feature_info(
    session, feature_name: str
) -> dict:
    """Get detailed information for a feature including aliases and GO terms."""
    info = {
        "feature_name": feature_name,
        "gene_name": "",
        "headline": "",
        "aliases": [],
        "biological_process": [],
        "cellular_component": [],
        "molecular_function": [],
    }

    # Get gene name and headline
    query = text(f"""
        SELECT gene_name, headline
        FROM {DB_SCHEMA}.feature
        WHERE feature_name = :name
    """)

    result = session.execute(query, {"name": feature_name}).fetchone()
    if result:
        info["gene_name"] = result[0] or ""
        info["headline"] = result[1] or ""

    # Get aliases
    query = text(f"""
        SELECT alias_name
        FROM {DB_SCHEMA}.feature_alias
        WHERE feature_name = :name
    """)

    for row in session.execute(query, {"name": feature_name}).fetchall():
        if row[0]:
            info["aliases"].append(row[0])

    # Get GO annotations
    go_aspects = {
        "P": "biological_process",
        "C": "cellular_component",
        "F": "molecular_function",
    }

    query = text(f"""
        SELECT g.term, g.aspect
        FROM {DB_SCHEMA}.go g
        JOIN {DB_SCHEMA}.go_annotation ga ON g.go_no = ga.go_no
        JOIN {DB_SCHEMA}.feature f ON ga.feature_no = f.feature_no
        WHERE f.feature_name = :name
    """)

    for row in session.execute(query, {"name": feature_name}).fetchall():
        term, aspect = row
        if term and aspect in go_aspects:
            info[go_aspects[aspect]].append(term)

    return info


def gzip_file(filepath: Path) -> None:
    """Compress a file with gzip."""
    with open(filepath, "rb") as f_in:
        with gzip.open(str(filepath) + ".gz", "wb", compresslevel=9) as f_out:
            f_out.write(f_in.read())
    filepath.unlink()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump Paula Sundstrom files with ORF mappings and GO annotations"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Set up output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Output files
    short_file = OUTPUT_DIR / "orf6_short_desc.txt"
    long_file = OUTPUT_DIR / "orf6_long_desc_plus_GO.txt"

    try:
        with SessionLocal() as session:
            # Get organism
            organism_no = get_organism_no(session, STRAIN_ABBREV)
            if not organism_no:
                logger.error(f"No organism found for {STRAIN_ABBREV}")
                return 1

            logger.info(f"Processing organism: {STRAIN_ABBREV}")

            # Get all features
            features = get_features_for_organism(session, organism_no)
            logger.info(f"Found {len(features)} features")

            # Build mapping of orf6 aliases to features
            orf6_features: dict[str, dict] = {}

            for feature_no, feature_name in sorted(
                features.items(), key=lambda x: x[1]
            ):
                info = get_feature_info(session, feature_name)

                # Look for orf6 aliases
                for alias in info["aliases"]:
                    if alias.startswith("orf6"):
                        orf6_features[alias] = info
                        break

            logger.info(f"Found {len(orf6_features)} features with orf6 aliases")

            # Write output files
            with open(short_file, "w") as f_short, open(long_file, "w") as f_long:
                for orf6 in sorted(orf6_features.keys()):
                    info = orf6_features[orf6]

                    # Short description (up to first semicolon)
                    short_desc = info["headline"].split(";")[0] if info["headline"] else ""

                    # Write short file
                    f_short.write(
                        f"{orf6}\t{info['feature_name']}\t"
                        f"{info['gene_name']}\t{short_desc}\n"
                    )

                    # GO terms joined by ||
                    bp_terms = "||".join(info["biological_process"])
                    cc_terms = "||".join(info["cellular_component"])
                    mf_terms = "||".join(info["molecular_function"])

                    # Write long file
                    f_long.write(
                        f"{orf6}\t{info['feature_name']}\t"
                        f"{info['gene_name']}\t{info['headline']}\t"
                        f"{bp_terms}\t{cc_terms}\t{mf_terms}\n"
                    )

            # Gzip output files
            gzip_file(short_file)
            gzip_file(long_file)

            logger.info(f"Created {short_file}.gz")
            logger.info(f"Created {long_file}.gz")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
