#!/usr/bin/env python3
"""
Generate ORF coordinate files for web-primer.

This script generates data files for the web-primer user interface:
- orf_coordinates.table: ORF coordinates
- orf2locus.table: ORF to gene name mapping
- locus2orf.table: Gene name to ORF mapping

Based on make_orf_coordinates_webprimer.pl by Stan Dong,
rewritten March 2006 to access info directly from database.

Usage:
    python make_orf_coordinates_webprimer.py
    python make_orf_coordinates_webprimer.py --output-dir /var/data/cgd/web-primer

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
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
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_non_standard_genes(session) -> set[str]:
    """Get gene names that are not standardized."""
    query = text(f"""
        SELECT f.gene_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.gene_reservation g ON g.feature_no = f.feature_no
        WHERE g.date_standardized IS NULL
    """)

    non_standard = set()
    for row in session.execute(query).fetchall():
        if row[0]:
            non_standard.add(row[0])

    return non_standard


def get_strains_with_seq_source(session) -> list[tuple[str, str]]:
    """Get all strains with their seq_source."""
    strains = []

    # Get all strains
    strain_query = text(f"""
        SELECT organism_no, organism_abbrev
        FROM {DB_SCHEMA}.organism
    """)

    for row in session.execute(strain_query).fetchall():
        org_no = row[0]
        strain_abbrev = row[1]

        # Get seq_source for this strain
        seq_query = text(f"""
            SELECT DISTINCT s.source
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            WHERE s.is_seq_current = 'Y'
            AND f.organism_abbrev = :strain
            FETCH FIRST 1 ROW ONLY
        """)
        result = session.execute(seq_query, {"strain": strain_abbrev}).fetchone()

        if result and result[0]:
            strains.append((strain_abbrev, result[0]))

    return strains


def get_orf_coordinates(
    session, seq_source: str, non_standard_genes: set[str]
) -> list[dict]:
    """Get ORF coordinates for a given seq_source."""
    query = text(f"""
        SELECT f1.feature_name, f1.gene_name, l.start_coord,
               l.stop_coord, l.strand, f2.feature_name as chromosome
        FROM {DB_SCHEMA}.feature f1
        JOIN {DB_SCHEMA}.feat_relationship fr ON f1.feature_no = fr.child_feature_no
        JOIN {DB_SCHEMA}.feature f2 ON fr.parent_feature_no = f2.feature_no
        JOIN {DB_SCHEMA}.seq s ON s.feature_no = f2.feature_no
        JOIN {DB_SCHEMA}.feat_location l ON (
            f1.feature_no = l.feature_no
            AND l.root_seq_no = s.seq_no
        )
        WHERE f1.feature_type = 'ORF'
        AND f2.feature_type IN ('chromosome', 'contig')
        AND f1.feature_no NOT IN (
            SELECT feature_no
            FROM {DB_SCHEMA}.feat_property
            WHERE property_type = 'feature_qualifier'
            AND property_value LIKE 'Deleted%'
        )
        AND l.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND (s.source = :seq_source OR f2.feature_name LIKE '%mtDNA')
        ORDER BY f1.feature_name
    """)

    orfs = []

    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        if not row[0]:
            continue

        feature_name = row[0]
        gene_name = row[1]
        start = row[2]
        stop = row[3]
        strand = row[4]
        chromosome = row[5]

        # Remove non-standard gene names
        if gene_name and gene_name in non_standard_genes:
            gene_name = None

        # Ensure start < stop
        if start and stop and start > stop:
            start, stop = stop, start

        orfs.append({
            "feature_name": feature_name,
            "gene_name": gene_name,
            "start": start,
            "stop": stop,
            "strand": strand,
            "chromosome": chromosome,
        })

    return orfs


def write_output_files(
    orfs: list[dict],
    coord_file: Path,
    orf2locus_file: Path,
    locus2orf_file: Path,
) -> tuple[int, int, int]:
    """Write the output files and return counts."""
    coord_count = 0
    orf2locus_count = 0
    locus2orf_count = 0

    with open(coord_file, "w") as f_coord, \
         open(orf2locus_file, "w") as f_orf2locus, \
         open(locus2orf_file, "w") as f_locus2orf:

        for orf in orfs:
            # Write coordinates
            f_coord.write(
                f"{orf['feature_name']}\t"
                f"{orf['start']}\t"
                f"{orf['stop']}\t"
                f"{orf['chromosome']}\t"
                f"{orf['strand']}\n"
            )
            coord_count += 1

            # Write mappings if gene name exists
            if orf["gene_name"]:
                f_orf2locus.write(
                    f"{orf['feature_name']}\t{orf['gene_name']}\n"
                )
                orf2locus_count += 1

                f_locus2orf.write(
                    f"{orf['gene_name']}\t{orf['feature_name']}\n"
                )
                locus2orf_count += 1

    return coord_count, orf2locus_count, locus2orf_count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate ORF coordinate files for web-primer"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for files",
    )

    args = parser.parse_args()

    output_dir = args.output_dir or (DATA_DIR / "web-primer")
    output_dir.mkdir(parents=True, exist_ok=True)

    coord_file = output_dir / "orf_coordinates.table"
    orf2locus_file = output_dir / "orf2locus.table"
    locus2orf_file = output_dir / "locus2orf.table"

    logger.info("Generating web-primer coordinate files")

    try:
        with SessionLocal() as session:
            # Get non-standard genes
            non_standard = get_non_standard_genes(session)
            logger.info(f"Found {len(non_standard)} non-standard gene names")

            # Get strains with seq_source
            strains = get_strains_with_seq_source(session)
            logger.info(f"Found {len(strains)} strains with sequence data")

            # Collect all ORFs
            all_orfs: list[dict] = []

            for strain_abbrev, seq_source in strains:
                orfs = get_orf_coordinates(session, seq_source, non_standard)
                all_orfs.extend(orfs)
                logger.info(
                    f"Found {len(orfs)} ORFs for {strain_abbrev} "
                    f"(seq_source: {seq_source})"
                )

            # Write output files
            coord_count, orf2locus_count, locus2orf_count = write_output_files(
                all_orfs, coord_file, orf2locus_file, locus2orf_file
            )

            logger.info(f"Wrote {coord_count} records to {coord_file}")
            logger.info(f"Wrote {orf2locus_count} records to {orf2locus_file}")
            logger.info(f"Wrote {locus2orf_count} records to {locus2orf_file}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
