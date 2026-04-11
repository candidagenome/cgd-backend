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
    python make_orf_coordinates_webprimer.py --output-dir /data/web-primer

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DOWNLOAD_DIR: Directory for data files
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
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

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_non_standard_genes(session) -> set[str]:
    """Get gene names that are not standardized."""
    try:
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
    except Exception:
        # Table may not exist in all schemas
        return set()


def get_strains_with_seq_source(session) -> list[tuple[int, str, str]]:
    """Get all strains with their seq_source."""
    strains = []

    # Get all strains
    strain_query = text(f"""
        SELECT organism_no, organism_abbrev
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev IS NOT NULL
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
            AND f.organism_no = :org_no
            ORDER BY s.source DESC
            FETCH FIRST 1 ROW ONLY
        """)
        result = session.execute(seq_query, {"org_no": org_no}).fetchone()

        if result and result[0]:
            strains.append((org_no, strain_abbrev, result[0]))

    return strains


def get_orf_coordinates(
    session, organism_no: int, seq_source: str, non_standard_genes: set[str]
) -> list[dict]:
    """Get ORF coordinates for a given organism and seq_source."""
    query = text(f"""
        SELECT f1.feature_name, f1.gene_name, l.start_coord,
               l.stop_coord, l.strand, f2.feature_name as chromosome
        FROM {DB_SCHEMA}.feature f1
        JOIN {DB_SCHEMA}.feat_relationship fr ON f1.feature_no = fr.child_feature_no
        JOIN {DB_SCHEMA}.feature f2 ON fr.parent_feature_no = f2.feature_no
        JOIN {DB_SCHEMA}.feat_location l ON f1.feature_no = l.feature_no
        JOIN {DB_SCHEMA}.seq s ON l.root_seq_no = s.seq_no
        WHERE f1.feature_type = 'ORF'
        AND f1.organism_no = :organism_no
        AND f2.feature_type IN ('chromosome', 'contig')
        AND f1.feature_no NOT IN (
            SELECT feature_no
            FROM {DB_SCHEMA}.feat_property
            WHERE property_type = 'feature_qualifier'
            AND property_value LIKE 'Deleted%'
        )
        AND l.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND (s.source = :seq_source OR UPPER(f2.feature_name) LIKE '%MTDNA%' OR UPPER(f2.feature_name) LIKE '%MITO%')
        AND fr.rank = 1
        ORDER BY f1.feature_name
    """)

    orfs = []

    for row in session.execute(query, {"organism_no": organism_no, "seq_source": seq_source}).fetchall():
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
        help="Output directory for files (default: DOWNLOAD_DIR/web-primer)",
    )

    args = parser.parse_args()

    output_dir = args.output_dir or (DOWNLOAD_DIR / "web-primer")
    output_dir.mkdir(parents=True, exist_ok=True)

    coord_file = output_dir / "orf_coordinates.table"
    orf2locus_file = output_dir / "orf2locus.table"
    locus2orf_file = output_dir / "locus2orf.table"

    # Set up file logging
    log_file = LOG_DIR / "make_orf_coordinates_webprimer.log"
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info("Generating web-primer coordinate files")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Started: {datetime.now()}")

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

            for org_no, strain_abbrev, seq_source in strains:
                orfs = get_orf_coordinates(session, org_no, seq_source, non_standard)
                all_orfs.extend(orfs)
                logger.info(
                    f"  {strain_abbrev}: {len(orfs)} ORFs (seq_source: {seq_source})"
                )

            # Write output files
            coord_count, orf2locus_count, locus2orf_count = write_output_files(
                all_orfs, coord_file, orf2locus_file, locus2orf_file
            )

            logger.info(f"Wrote {coord_count} records to {coord_file.name}")
            logger.info(f"Wrote {orf2locus_count} records to {orf2locus_file.name}")
            logger.info(f"Wrote {locus2orf_count} records to {locus2orf_file.name}")
            logger.info(f"Completed: {datetime.now()}")

            # Print summary for shell wrapper
            print(f"ORF coordinates: {coord_count}")
            print(f"ORF to locus mappings: {orf2locus_count}")
            print(f"Locus to ORF mappings: {locus2orf_count}")

        return 0

    except Exception as e:
        logger.exception(f"Error: {e}")
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
