#!/usr/bin/env python3
"""
Diagnostic script to identify and report sequence-related data issues.

This script checks for:
1. Mismatched seq_type values (genomic vs 'Genomic DNA')
2. NULL or missing root_seq_no in feat_location
3. CDS features without proper feat_location entries
4. Strand inconsistencies between parent genes and CDS

Usage:
    python diagnose_seq_issues.py [--fix]

The --fix flag will attempt to fix identified issues.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ORGANISM_NAME = "Candida tropicalis MYA-3404"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_chromosome_seq_types(session):
    """Check for chromosome sequences with inconsistent seq_type values."""
    logger.info("Checking chromosome sequence seq_type values...")

    # Check for 'Genomic DNA' seq_type on chromosome/contig features
    query = text(f"""
        SELECT f.feature_name, s.seq_type, s.seq_no
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_name = :name
        AND f.feature_type = 'contig'
        AND s.is_seq_current = 'Y'
    """)
    results = session.execute(query, {"name": ORGANISM_NAME}).fetchall()

    issues = []
    for row in results:
        feature_name, seq_type, seq_no = row
        if seq_type != 'genomic':
            issues.append({
                "feature_name": feature_name,
                "seq_no": seq_no,
                "current_seq_type": seq_type,
                "expected_seq_type": "genomic"
            })

    if issues:
        logger.warning(f"Found {len(issues)} chromosome sequences with incorrect seq_type:")
        for issue in issues:
            logger.warning(f"  {issue['feature_name']}: '{issue['current_seq_type']}' should be 'genomic'")
    else:
        logger.info("All chromosome sequences have correct seq_type='genomic'")

    return issues


def check_null_root_seq_no(session):
    """Check for feat_location entries with NULL root_seq_no."""
    logger.info("Checking for NULL root_seq_no in feat_location...")

    query = text(f"""
        SELECT f.feature_name, fl.feat_location_no
        FROM {DB_SCHEMA}.feat_location fl
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_name = :name
        AND f.feature_type = 'ORF'
        AND fl.is_loc_current = 'Y'
        AND fl.root_seq_no IS NULL
    """)
    results = session.execute(query, {"name": ORGANISM_NAME}).fetchall()

    if results:
        logger.warning(f"Found {len(results)} ORF feat_locations with NULL root_seq_no")
        for row in results[:10]:  # Show first 10
            logger.warning(f"  {row[0]} (feat_location_no={row[1]})")
        if len(results) > 10:
            logger.warning(f"  ... and {len(results) - 10} more")
    else:
        logger.info("All ORF feat_locations have non-NULL root_seq_no")

    return results


def check_cds_feat_locations(session):
    """Check for CDS features missing feat_location entries."""
    logger.info("Checking CDS features for missing feat_location entries...")

    # Count total CDS
    total_query = text(f"""
        SELECT COUNT(*)
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_name = :name
        AND f.feature_type = 'CDS'
    """)
    total = session.execute(total_query, {"name": ORGANISM_NAME}).scalar()

    # Count CDS with feat_location
    with_loc_query = text(f"""
        SELECT COUNT(*)
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        JOIN {DB_SCHEMA}.feat_location fl ON fl.feature_no = f.feature_no
        WHERE o.organism_name = :name
        AND f.feature_type = 'CDS'
        AND fl.is_loc_current = 'Y'
    """)
    with_loc = session.execute(with_loc_query, {"name": ORGANISM_NAME}).scalar()

    missing = total - with_loc
    if missing > 0:
        logger.warning(f"Found {missing} CDS features without feat_location entries ({with_loc}/{total} have locations)")
    else:
        logger.info(f"All {total} CDS features have feat_location entries")

    return {"total_cds": total, "with_location": with_loc, "missing": missing}


def check_root_seq_validity(session):
    """Check if root_seq_no values point to valid chromosome sequences."""
    logger.info("Checking root_seq_no validity...")

    # Find feat_locations where root_seq_no doesn't point to a valid sequence
    query = text(f"""
        SELECT COUNT(*)
        FROM {DB_SCHEMA}.feat_location fl
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_name = :name
        AND f.feature_type = 'ORF'
        AND fl.is_loc_current = 'Y'
        AND fl.root_seq_no IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM {DB_SCHEMA}.seq s
            WHERE s.seq_no = fl.root_seq_no
            AND s.is_seq_current = 'Y'
        )
    """)
    invalid_count = session.execute(query, {"name": ORGANISM_NAME}).scalar()

    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} feat_locations with invalid root_seq_no")
    else:
        logger.info("All root_seq_no values point to valid sequences")

    return invalid_count


def check_strand_consistency(session):
    """Check strand consistency between parent genes and CDS subfeatures."""
    logger.info("Checking strand consistency...")

    query = text(f"""
        SELECT
            parent_f.feature_name as gene_name,
            parent_fl.strand as gene_strand,
            child_f.feature_name as cds_name,
            child_fl.strand as cds_strand
        FROM {DB_SCHEMA}.feat_relationship fr
        JOIN {DB_SCHEMA}.feature parent_f ON fr.parent_feature_no = parent_f.feature_no
        JOIN {DB_SCHEMA}.feature child_f ON fr.child_feature_no = child_f.feature_no
        JOIN {DB_SCHEMA}.organism o ON parent_f.organism_no = o.organism_no
        JOIN {DB_SCHEMA}.feat_location parent_fl ON parent_fl.feature_no = parent_f.feature_no
        JOIN {DB_SCHEMA}.feat_location child_fl ON child_fl.feature_no = child_f.feature_no
        WHERE o.organism_name = :name
        AND parent_f.feature_type = 'ORF'
        AND child_f.feature_type = 'CDS'
        AND parent_fl.is_loc_current = 'Y'
        AND child_fl.is_loc_current = 'Y'
        AND parent_fl.strand != child_fl.strand
        AND ROWNUM <= 100
    """)
    results = session.execute(query, {"name": ORGANISM_NAME}).fetchall()

    if results:
        logger.warning(f"Found strand inconsistencies between genes and CDS:")
        for row in results[:10]:
            logger.warning(f"  Gene {row[0]} (strand={row[1]}) -> CDS {row[2]} (strand={row[3]})")
        if len(results) > 10:
            logger.warning(f"  ... and more")
    else:
        logger.info("All CDS strands match their parent gene strands")

    return results


def sample_transcript_data(session):
    """Sample a gene and show its transcript computation data."""
    logger.info("Sampling transcript computation data...")

    # First check strand distribution
    strand_query = text(f"""
        SELECT fl.strand, COUNT(*) as cnt
        FROM {DB_SCHEMA}.feat_location fl
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_name = :name
        AND f.feature_type = 'ORF'
        AND fl.is_loc_current = 'Y'
        GROUP BY fl.strand
    """)
    strand_results = session.execute(strand_query, {"name": ORGANISM_NAME}).fetchall()
    logger.info("Strand distribution:")
    for row in strand_results:
        logger.info(f"  {row[0]}: {row[1]} genes")

    # Find any gene with CDS
    query = text(f"""
        SELECT f.feature_name, f.feature_no, fl.strand, fl.start_coord, fl.stop_coord, fl.root_seq_no
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        JOIN {DB_SCHEMA}.feat_location fl ON fl.feature_no = f.feature_no
        WHERE o.organism_name = :name
        AND f.feature_type = 'ORF'
        AND fl.is_loc_current = 'Y'
        AND ROWNUM = 1
    """)
    gene = session.execute(query, {"name": ORGANISM_NAME}).first()

    if not gene:
        logger.info("No ORF genes found")
        return None

    feature_name, feature_no, strand, start, stop, root_seq_no = gene
    logger.info(f"Sample gene: {feature_name}")
    logger.info(f"  Strand: {strand}, Coords: {start}-{stop}, root_seq_no: {root_seq_no}")

    # Get CDS subfeatures
    cds_query = text(f"""
        SELECT
            child_f.feature_name,
            child_fl.start_coord,
            child_fl.stop_coord,
            child_fl.strand,
            child_fl.root_seq_no
        FROM {DB_SCHEMA}.feat_relationship fr
        JOIN {DB_SCHEMA}.feature child_f ON fr.child_feature_no = child_f.feature_no
        JOIN {DB_SCHEMA}.feat_location child_fl ON child_fl.feature_no = child_f.feature_no
        WHERE fr.parent_feature_no = :fno
        AND child_f.feature_type = 'CDS'
        AND child_fl.is_loc_current = 'Y'
        ORDER BY child_fl.start_coord
    """)
    cds_results = session.execute(cds_query, {"fno": feature_no}).fetchall()

    logger.info(f"  CDS subfeatures: {len(cds_results)}")
    for cds in cds_results:
        logger.info(f"    {cds[0]}: {cds[1]}-{cds[2]} (strand={cds[3]}, root_seq_no={cds[4]})")

    # Get chromosome sequence info
    if root_seq_no:
        chr_query = text(f"""
            SELECT f.feature_name, s.seq_type, LENGTH(s.residues) as seq_len
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
            WHERE s.seq_no = :sno
        """)
        chr_info = session.execute(chr_query, {"sno": root_seq_no}).first()
        if chr_info:
            logger.info(f"  Chromosome: {chr_info[0]} (seq_type={chr_info[1]}, length={chr_info[2]})")
        else:
            logger.warning(f"  root_seq_no {root_seq_no} does not point to a valid sequence!")

    return gene


def fix_chromosome_seq_types(session, issues):
    """Fix chromosome seq_type values from 'Genomic DNA' to 'genomic'."""
    if not issues:
        logger.info("No seq_type issues to fix")
        return 0

    logger.info(f"Fixing {len(issues)} chromosome seq_type values...")

    # Use direct SQL update since the trigger might not block this
    for issue in issues:
        try:
            update = text(f"""
                UPDATE {DB_SCHEMA}.seq
                SET seq_type = 'genomic'
                WHERE seq_no = :seq_no
            """)
            session.execute(update, {"seq_no": issue["seq_no"]})
            logger.info(f"  Fixed {issue['feature_name']}: 'Genomic DNA' -> 'genomic'")
        except Exception as e:
            logger.error(f"  Failed to fix {issue['feature_name']}: {e}")
            session.rollback()
            return 0

    session.commit()
    logger.info(f"Successfully fixed {len(issues)} seq_type values")
    return len(issues)


def run_diagnostics(session):
    """Run all diagnostic checks."""
    logger.info("=" * 60)
    logger.info(f"Running diagnostics for {ORGANISM_NAME}")
    logger.info("=" * 60)

    issues = {
        "seq_type_issues": check_chromosome_seq_types(session),
        "null_root_seq": check_null_root_seq_no(session),
        "cds_missing_loc": check_cds_feat_locations(session),
        "invalid_root_seq": check_root_seq_validity(session),
        "strand_issues": check_strand_consistency(session),
    }

    logger.info("=" * 60)
    sample_transcript_data(session)

    logger.info("=" * 60)
    logger.info("Summary:")

    has_issues = False
    if issues["seq_type_issues"]:
        logger.warning(f"  - {len(issues['seq_type_issues'])} chromosome seq_type issues")
        has_issues = True
    if issues["null_root_seq"]:
        logger.warning(f"  - {len(issues['null_root_seq'])} NULL root_seq_no in feat_location")
        has_issues = True
    if issues["cds_missing_loc"]["missing"] > 0:
        logger.warning(f"  - {issues['cds_missing_loc']['missing']} CDS missing feat_location")
        has_issues = True
    if issues["invalid_root_seq"] > 0:
        logger.warning(f"  - {issues['invalid_root_seq']} invalid root_seq_no references")
        has_issues = True
    if issues["strand_issues"]:
        logger.warning(f"  - {len(issues['strand_issues'])} strand inconsistencies")
        has_issues = True

    if not has_issues:
        logger.info("  No issues found!")

    return issues


def main():
    parser = argparse.ArgumentParser(description="Diagnose sequence data issues")
    parser.add_argument("--fix", action="store_true", help="Fix identified issues")
    args = parser.parse_args()

    with SessionLocal() as session:
        issues = run_diagnostics(session)

        if args.fix:
            logger.info("=" * 60)
            logger.info("Applying fixes...")

            # Fix seq_type issues
            if issues["seq_type_issues"]:
                fix_chromosome_seq_types(session, issues["seq_type_issues"])

            # Other fixes would require reloading data
            if issues["null_root_seq"] or issues["cds_missing_loc"]["missing"] > 0:
                logger.warning("Some issues require reloading data using the fixed loading scripts.")

    logger.info("Done!")


if __name__ == "__main__":
    main()
