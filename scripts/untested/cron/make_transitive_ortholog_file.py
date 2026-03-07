#!/usr/bin/env python3
"""
Create transitive ortholog mapping file between two strains.

This script creates ortholog mappings between strain2 and strain3 by transitivity
through strain1. If strain1 has orthologs to both strain2 and strain3, then
strain2 and strain3 are considered orthologs by transitivity.

Based on makeTransitiveOrthologFile.pl by CGD team.

Usage:
    python make_transitive_ortholog_file.py <strain1_strain2.txt> <strain1_strain3.txt> <strain2_abbrev> <output_file>
    python make_transitive_ortholog_file.py calb_cglab.txt calb_scer.txt C_glabrata_CBS138 cglab_scer_orthologs.txt

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
"""

import argparse
import logging
import os
import sys
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_organism_no(session, strain_abbrev: str) -> int | None:
    """Get organism number for a strain abbreviation."""
    query = text(f"""
        SELECT organism_no
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_feature_sysname_by_dbxref(session, organism_no: int) -> dict[str, str]:
    """
    Get mapping from dbxref_id to feature_name for an organism.

    Returns dict mapping dbxref_id -> feature_name (systematic name)
    """
    query = text(f"""
        SELECT feature_name, dbxref_id
        FROM {DB_SCHEMA}.feature
        WHERE organism_no = :organism_no
        AND dbxref_id IS NOT NULL
    """)

    result = {}
    for row in session.execute(query, {"organism_no": organism_no}).fetchall():
        feature_name, dbxref_id = row
        if dbxref_id:
            result[dbxref_id] = feature_name

    return result


def parse_pair_file(filepath: Path) -> dict[str, set[str]]:
    """
    Parse pairwise ortholog file.

    Args:
        filepath: Path to pairwise ortholog file

    Returns:
        Dict mapping query name -> set of target names/IDs
    """
    mappings: dict[str, set[str]] = {}

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                continue

            # First column may have multiple queries separated by semicolons
            queries = [q.strip() for q in parts[0].split(";")]
            target = parts[1]

            for q in queries:
                if q:
                    if q not in mappings:
                        mappings[q] = set()
                    mappings[q].add(target)

    return mappings


def make_transitive_ortholog_file(
    session,
    pair1_file: Path,
    pair2_file: Path,
    strain2_abbrev: str,
    output_file: Path,
) -> int:
    """
    Create transitive ortholog file.

    Args:
        session: Database session
        pair1_file: Path to strain1-strain2 ortholog file
        pair2_file: Path to strain1-strain3 ortholog file
        strain2_abbrev: Abbreviation for strain2 (to get systematic names)
        output_file: Output file path

    Returns:
        Number of ortholog pairs written
    """
    # Validate strain2
    organism_no = get_organism_no(session, strain2_abbrev)
    if not organism_no:
        raise ValueError(f"Organism not found in database: {strain2_abbrev}")

    logger.info(f"Loading feature names for {strain2_abbrev}")
    sysname_for_dbxref = get_feature_sysname_by_dbxref(session, organism_no)
    logger.info(f"Found {len(sysname_for_dbxref)} features")

    # Parse pairwise files
    # pair1: strain1 -> strain2 (DBXREFs)
    # pair2: strain1 -> strain3 (names)
    logger.info(f"Parsing {pair1_file}")
    strain2_dbxrefs_for_strain1 = parse_pair_file(pair1_file)
    logger.info(f"Found {len(strain2_dbxrefs_for_strain1)} strain1 entries in pair1")

    logger.info(f"Parsing {pair2_file}")
    strain3_names_for_strain1 = parse_pair_file(pair2_file)
    logger.info(f"Found {len(strain3_names_for_strain1)} strain1 entries in pair2")

    # Generate transitive pairs
    pairs_written = 0

    with open(output_file, "w") as f:
        f.write(f"# Transitive ortholog mapping\n")
        f.write(f"# Created from: {pair1_file.name} and {pair2_file.name}\n")
        f.write(f"# Columns: {strain2_abbrev}_feature_name\\tortholog_name\n")

        for strain1_name, strain2_dbxrefs in strain2_dbxrefs_for_strain1.items():
            # Check if strain1 also has orthologs in strain3
            if strain1_name not in strain3_names_for_strain1:
                continue

            strain3_names = strain3_names_for_strain1[strain1_name]

            for strain2_dbxref in strain2_dbxrefs:
                # Get systematic name for strain2 dbxref
                if strain2_dbxref not in sysname_for_dbxref:
                    continue

                strain2_sysname = sysname_for_dbxref[strain2_dbxref]

                for strain3_name in strain3_names:
                    f.write(f"{strain2_sysname}\t{strain3_name}\n")
                    pairs_written += 1

    return pairs_written


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create transitive ortholog mapping file"
    )
    parser.add_argument(
        "pair1_file",
        type=Path,
        help="Path to strain1-strain2 ortholog file",
    )
    parser.add_argument(
        "pair2_file",
        type=Path,
        help="Path to strain1-strain3 ortholog file",
    )
    parser.add_argument(
        "strain2_abbrev",
        help="Strain2 abbreviation (e.g., C_glabrata_CBS138)",
    )
    parser.add_argument(
        "output_file",
        type=Path,
        help="Output file path",
    )

    args = parser.parse_args()

    # Validate input files
    if not args.pair1_file.exists():
        logger.error(f"File not found: {args.pair1_file}")
        return 1

    if not args.pair2_file.exists():
        logger.error(f"File not found: {args.pair2_file}")
        return 1

    logger.info("Creating transitive ortholog file")
    logger.info(f"  Pair1 file: {args.pair1_file}")
    logger.info(f"  Pair2 file: {args.pair2_file}")
    logger.info(f"  Strain2: {args.strain2_abbrev}")
    logger.info(f"  Output: {args.output_file}")

    try:
        with SessionLocal() as session:
            count = make_transitive_ortholog_file(
                session,
                args.pair1_file,
                args.pair2_file,
                args.strain2_abbrev,
                args.output_file,
            )

            logger.info(f"Wrote {count} ortholog pairs to {args.output_file}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
