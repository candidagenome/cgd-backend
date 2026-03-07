#!/usr/bin/env python3
"""
Create pairwise ortholog files from orthogroups data.

This script reads ortholog group data (e.g., from Synergy/MCL clustering)
and creates pairwise ortholog mappings between two specified strains.
The output is a tab-delimited file mapping features between the two organisms.

Based on makePairwiseOrthogroupFile.pl by CGD team.

Usage:
    python make_pairwise_orthogroup_file.py <orthogroups_file> <strain1> <strain2>
    python make_pairwise_orthogroup_file.py orthogroups.txt C_albicans_SC5314 S_cerevisiae

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    DATA_DIR: Data directory
    HTML_ROOT_DIR: Root directory for download files
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
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Mapping of orthogroup tags to strain abbreviations
STRAIN_ABBREV_FOR_OG_TAG = {
    "Calb": "C_albicans_SC5314",
    "Cgla": "C_glabrata_CBS138",
    "Cgui": "C_guilliermondii_ATCC_6260",
    "Clus": "C_lusitaniae_ATCC_42720",
    "Cpar": "C_parapsilosis_CDC_317",
    "Ctro": "C_tropicalis_MYA-3404",
    "Dhan": "D_hansenii_CBS767",
    "Lelo": "L_elongisporus_NRLL_YB-4239",
    "Scer": "S_cerevisiae",
}

# Reverse mapping
OG_TAG_FOR_STRAIN_ABBREV = {v: k for k, v in STRAIN_ABBREV_FOR_OG_TAG.items()}


def normalize_strain_name(strain: str) -> str:
    """Normalize strain name to standard abbreviation."""
    strain_lower = strain.lower()
    if strain_lower in ("sgd", "scer", "s_cer", "s_cerevisiae", "sacc"):
        return "S_cerevisiae"
    return strain


def get_organism_features(session, organism_no: int) -> dict[str, dict]:
    """
    Get feature information for an organism.

    Returns dict mapping systematic name to {dbxref_id, gene_name}
    """
    query = text(f"""
        SELECT f.feature_name, f.dbxref_id, f.gene_name
        FROM {DB_SCHEMA}.feature f
        WHERE f.organism_no = :organism_no
        AND f.feature_type IN ('ORF', 'ncRNA', 'tRNA', 'rRNA', 'snRNA', 'snoRNA')
    """)

    result = {}
    for row in session.execute(query, {"organism_no": organism_no}).fetchall():
        feat_name = row[0]
        result[feat_name] = {
            "dbxref_id": row[1],
            "gene_name": row[2] or feat_name,
        }

    return result


def get_organism_no(session, strain_abbrev: str) -> int | None:
    """Get organism number for a strain abbreviation."""
    query = text(f"""
        SELECT organism_no
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_genome_version(session, strain_abbrev: str) -> str | None:
    """Get current genome version for a strain."""
    query = text(f"""
        SELECT gv.genome_version
        FROM {DB_SCHEMA}.genome_version gv
        JOIN {DB_SCHEMA}.seq s ON gv.genome_version_no = s.genome_version_no
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE gv.is_ver_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND f.organism_abbrev = :strain_abbrev
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def parse_orthogroups_file(
    filepath: Path,
    strain1: str,
    strain2: str,
) -> list[tuple[list[str], list[str]]]:
    """
    Parse orthogroups file and extract pairwise relationships.

    Args:
        filepath: Path to orthogroups file
        strain1: First strain abbreviation
        strain2: Second strain abbreviation

    Returns:
        List of (strain1_features, strain2_features) tuples for each orthogroup
    """
    og_tag1 = OG_TAG_FOR_STRAIN_ABBREV.get(strain1)
    og_tag2 = OG_TAG_FOR_STRAIN_ABBREV.get(strain2)

    if not og_tag1 or not og_tag2:
        raise ValueError(f"Unknown strain abbreviation: {strain1} or {strain2}")

    pairs = []

    with open(filepath) as f:
        for line in f:
            line = line.strip()

            # Skip non-data lines (orthogroups format starts with cluster number)
            if not line or not line[0].isdigit():
                continue

            parts = line.split("\t")
            if len(parts) < 4:
                continue

            # Skip first 3 columns (cluster info), remaining are orthologs
            orthologs = parts[3:]

            strain1_features = []
            strain2_features = []

            for orth in orthologs:
                if "|" not in orth:
                    continue

                og_tag, sys_name = orth.split("|", 1)

                if og_tag not in STRAIN_ABBREV_FOR_OG_TAG:
                    continue

                if STRAIN_ABBREV_FOR_OG_TAG[og_tag] == strain1:
                    strain1_features.append(sys_name)
                elif STRAIN_ABBREV_FOR_OG_TAG[og_tag] == strain2:
                    strain2_features.append(sys_name)

            if strain1_features and strain2_features:
                pairs.append((strain1_features, strain2_features))

    return pairs


def generate_file_header(
    strain1: str,
    strain2: str,
    gv1: str | None,
    gv2: str | None,
) -> str:
    """Generate header for the output file."""
    lines = [
        f"# Pairwise ortholog mapping between {strain1} and {strain2}",
        f"# Generated by: make_pairwise_orthogroup_file.py",
        f"# Date: {datetime.now().strftime('%Y-%m-%d')}",
    ]
    if gv1:
        lines.append(f"# {strain1} genome version: {gv1}")
    if gv2:
        lines.append(f"# {strain2} genome version: {gv2}")
    lines.append("#")
    lines.append(f"# Columns: {strain1}_DBID\t{strain1}_name\t{strain2}_DBID\t{strain2}_name")
    return "\n".join(lines) + "\n"


def make_pairwise_file(
    session,
    orthogroups_file: Path,
    strain1: str,
    strain2: str,
    output_file: Path,
) -> tuple[int, int, int]:
    """
    Create pairwise ortholog file.

    Returns:
        Tuple of (clusters_count, strain1_count, strain2_count)
    """
    # Get organism info
    org_no1 = get_organism_no(session, strain1)
    org_no2 = get_organism_no(session, strain2)

    if not org_no1:
        raise ValueError(f"Organism not found: {strain1}")
    if not org_no2:
        raise ValueError(f"Organism not found: {strain2}")

    # Get feature info
    logger.info(f"Getting feature info for {strain1}...")
    features1 = get_organism_features(session, org_no1)
    logger.info(f"Found {len(features1)} features for {strain1}")

    logger.info(f"Getting feature info for {strain2}...")
    features2 = get_organism_features(session, org_no2)
    logger.info(f"Found {len(features2)} features for {strain2}")

    # Get genome versions
    gv1 = get_genome_version(session, strain1)
    gv2 = get_genome_version(session, strain2)

    # Parse orthogroups
    logger.info(f"Parsing orthogroups file: {orthogroups_file}")
    pairs = parse_orthogroups_file(orthogroups_file, strain1, strain2)
    logger.info(f"Found {len(pairs)} ortholog clusters")

    # Write output
    seen1 = set()
    seen2 = set()
    cluster_count = 0

    with open(output_file, "w") as f:
        f.write(generate_file_header(strain1, strain2, gv1, gv2))

        for strain1_feats, strain2_feats in pairs:
            cluster_count += 1

            for feat1 in strain1_feats:
                for feat2 in strain2_feats:
                    info1 = features1.get(feat1, {})
                    info2 = features2.get(feat2, {})

                    dbid1 = info1.get("dbxref_id", feat1)
                    name1 = info1.get("gene_name", feat1)
                    dbid2 = info2.get("dbxref_id", feat2)
                    name2 = info2.get("gene_name", feat2)

                    f.write(f"{dbid1}\t{name1}\t{dbid2}\t{name2}\n")

                    seen1.add(feat1)
                    seen2.add(feat2)

    return cluster_count, len(seen1), len(seen2)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create pairwise ortholog files from orthogroups data"
    )
    parser.add_argument(
        "orthogroups_file",
        type=Path,
        help="Path to orthogroups file",
    )
    parser.add_argument(
        "strain1",
        help="First strain abbreviation",
    )
    parser.add_argument(
        "strain2",
        help="Second strain abbreviation",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output file path (default: auto-generated)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=HTML_ROOT_DIR / "download" / "homology" / "orthologs",
        help="Output directory (used if --output not specified)",
    )

    args = parser.parse_args()

    # Normalize strain names
    strain1 = normalize_strain_name(args.strain1)
    strain2 = normalize_strain_name(args.strain2)

    # Validate strains
    if strain1 not in OG_TAG_FOR_STRAIN_ABBREV:
        logger.error(f"Unknown strain: {strain1}")
        logger.error(f"Supported strains: {', '.join(OG_TAG_FOR_STRAIN_ABBREV.keys())}")
        return 1

    if strain2 not in OG_TAG_FOR_STRAIN_ABBREV:
        logger.error(f"Unknown strain: {strain2}")
        logger.error(f"Supported strains: {', '.join(OG_TAG_FOR_STRAIN_ABBREV.keys())}")
        return 1

    # Check orthogroups file
    if not args.orthogroups_file.exists():
        logger.error(f"Orthogroups file not found: {args.orthogroups_file}")
        return 1

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        output_dir = args.output_dir / f"{strain1}_{strain2}_by_synergy"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{strain1}_{strain2}_orthologs.txt"

    # Set up logging to file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{strain1}_{strain2}_orthologs.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Creating pairwise ortholog file")
    logger.info(f"Strain 1: {strain1}")
    logger.info(f"Strain 2: {strain2}")
    logger.info(f"Orthogroups file: {args.orthogroups_file}")
    logger.info(f"Output file: {output_file}")

    try:
        with SessionLocal() as session:
            clusters, count1, count2 = make_pairwise_file(
                session,
                args.orthogroups_file,
                strain1,
                strain2,
                output_file,
            )

            logger.info(f"Summary:")
            logger.info(f"  Ortholog clusters: {clusters}")
            logger.info(f"  {strain1} features: {count1}")
            logger.info(f"  {strain2} features: {count2}")
            logger.info(f"Output written to: {output_file}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
