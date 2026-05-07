#!/usr/bin/env python3
"""
Fix strand values in feat_location for C. tropicalis genes.

The loading scripts stored all genes with strand='W' instead of correctly
converting '-' to 'C' for minus strand genes.

This script reads the GFF file to get correct strand values and updates
the feat_location entries.

Usage:
    python fix_strand_values.py --gff FILE [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict

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


def parse_gff_strands(gff_file: Path) -> Dict[str, str]:
    """Parse GFF file and extract gene_id -> strand mapping."""
    gene_strands = {}
    gene_to_protein = {}

    with open(gff_file, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            line = line.strip()
            if not line:
                continue

            fields = line.split('\t')
            if len(fields) < 9:
                continue

            seqid, source, feature_type, start, end, score, strand, phase, attributes = fields

            attr_dict = {}
            for attr in attributes.split(';'):
                if '=' in attr:
                    key, value = attr.split('=', 1)
                    attr_dict[key.strip()] = value.strip()

            # Extract protein_id mapping from CDS features
            if feature_type == 'CDS':
                protein_id = attr_dict.get('protein_id', '')
                gene_id = attr_dict.get('gene_id', '')
                if protein_id and gene_id:
                    gene_to_protein[gene_id] = protein_id

            # Process gene features
            if feature_type == 'gene':
                gene_id = attr_dict.get('ID', '').split(',')[0]
                if gene_id:
                    gene_strands[gene_id] = strand

    # Also map protein_id to strand
    for gene_id, protein_id in gene_to_protein.items():
        if gene_id in gene_strands:
            gene_strands[protein_id] = gene_strands[gene_id]

    return gene_strands


def get_organism_no(session) -> int:
    """Get organism_no for C. tropicalis."""
    query = text(f"SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_name = :name")
    result = session.execute(query, {"name": ORGANISM_NAME}).first()
    if not result:
        raise ValueError(f"Organism not found: {ORGANISM_NAME}")
    return result[0]


def fix_strand_values(session, gff_file: Path, dry_run: bool = False):
    """Fix strand values in feat_location based on GFF file."""

    logger.info(f"Parsing GFF file: {gff_file}")
    gene_strands = parse_gff_strands(gff_file)

    minus_strand_count = sum(1 for s in gene_strands.values() if s == '-')
    plus_strand_count = sum(1 for s in gene_strands.values() if s == '+')
    logger.info(f"Found {len(gene_strands)} gene/protein IDs in GFF")
    logger.info(f"  Plus strand (+): {plus_strand_count}")
    logger.info(f"  Minus strand (-): {minus_strand_count}")

    organism_no = get_organism_no(session)

    # Fix ORF features first
    logger.info("Checking ORF features...")
    fix_feature_type(session, gene_strands, 'ORF', dry_run)

    # Also fix CDS features (they inherit strand from parent)
    logger.info("Checking CDS features...")
    fix_cds_strands(session, gene_strands, dry_run)


def fix_feature_type(session, gene_strands: Dict[str, str], feature_type: str, dry_run: bool):
    """Fix strand values for a specific feature type."""
    # Get all features with their current strand
    query = text(f"""
        SELECT f.feature_name, f.feature_no, fl.feat_location_no, fl.strand,
               fl.root_seq_no, fl.start_coord, fl.stop_coord
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        JOIN {DB_SCHEMA}.feat_location fl ON fl.feature_no = f.feature_no
        WHERE o.organism_name = :name
        AND f.feature_type = :ftype
        AND fl.is_loc_current = 'Y'
    """)
    results = session.execute(query, {"name": ORGANISM_NAME, "ftype": feature_type}).fetchall()

    logger.info(f"Found {len(results)} {feature_type} features in database")

    # Check which ones need fixing
    to_fix = []
    not_found = []
    already_correct = []

    for row in results:
        feature_name, feature_no, feat_location_no, current_strand, root_seq_no, start_coord, stop_coord = row

        gff_strand = gene_strands.get(feature_name)
        if not gff_strand:
            not_found.append(feature_name)
            continue

        # Convert GFF strand to database format
        expected_strand = 'W' if gff_strand == '+' else 'C'

        if current_strand != expected_strand:
            to_fix.append({
                'feature_name': feature_name,
                'feature_no': feature_no,
                'feat_location_no': feat_location_no,
                'current_strand': current_strand,
                'expected_strand': expected_strand,
                'root_seq_no': root_seq_no,
                'start_coord': start_coord,
                'stop_coord': stop_coord,
            })
        else:
            already_correct.append(feature_name)

    logger.info(f"Already correct: {len(already_correct)}")
    logger.info(f"Need fixing: {len(to_fix)}")
    logger.info(f"Not found in GFF: {len(not_found)}")

    if not to_fix:
        logger.info("No strand values need fixing")
        return

    if dry_run:
        logger.info("[DRY RUN] Would fix the following:")
        for item in to_fix[:10]:
            logger.info(f"  {item['feature_name']}: {item['current_strand']} -> {item['expected_strand']}")
        if len(to_fix) > 10:
            logger.info(f"  ... and {len(to_fix) - 10} more")
        return

    # Fix by deleting and re-inserting feat_location entries
    # (UPDATE might be blocked by triggers)
    logger.info("Fixing strand values...")
    fixed = 0
    failed = 0

    for item in to_fix:
        try:
            # Delete existing feat_location
            delete = text(f"""
                DELETE FROM {DB_SCHEMA}.feat_location
                WHERE feat_location_no = :fln
            """)
            session.execute(delete, {"fln": item['feat_location_no']})

            # Insert with correct strand
            insert = text(f"""
                INSERT INTO {DB_SCHEMA}.feat_location (
                    feature_no, root_seq_no, coord_version, start_coord, stop_coord,
                    strand, is_loc_current, created_by
                ) VALUES (
                    :fno, :root_seq_no, SYSDATE, :start, :stop, :strand, 'Y', 'CGDADMIN'
                )
            """)
            session.execute(insert, {
                "fno": item['feature_no'],
                "root_seq_no": item['root_seq_no'],
                "start": item['start_coord'],
                "stop": item['stop_coord'],
                "strand": item['expected_strand'],
            })

            fixed += 1

            if fixed % 500 == 0:
                logger.info(f"  Fixed {fixed}/{len(to_fix)}...")
                session.commit()

        except Exception as e:
            logger.error(f"  Failed to fix {item['feature_name']}: {e}")
            failed += 1
            session.rollback()

    session.commit()
    logger.info(f"Fixed {fixed} feat_location entries, {failed} failed")


def fix_cds_strands(session, gene_strands: Dict[str, str], dry_run: bool):
    """Fix CDS strand values based on parent gene strand."""
    # Get CDS features with their parent's expected strand
    query = text(f"""
        SELECT
            child_f.feature_name as cds_name,
            child_f.feature_no as cds_feature_no,
            child_fl.feat_location_no,
            child_fl.strand as current_strand,
            child_fl.root_seq_no,
            child_fl.start_coord,
            child_fl.stop_coord,
            parent_f.feature_name as parent_name
        FROM {DB_SCHEMA}.feat_relationship fr
        JOIN {DB_SCHEMA}.feature parent_f ON fr.parent_feature_no = parent_f.feature_no
        JOIN {DB_SCHEMA}.feature child_f ON fr.child_feature_no = child_f.feature_no
        JOIN {DB_SCHEMA}.organism o ON parent_f.organism_no = o.organism_no
        JOIN {DB_SCHEMA}.feat_location child_fl ON child_fl.feature_no = child_f.feature_no
        WHERE o.organism_name = :name
        AND parent_f.feature_type = 'ORF'
        AND child_f.feature_type = 'CDS'
        AND child_fl.is_loc_current = 'Y'
    """)
    results = session.execute(query, {"name": ORGANISM_NAME}).fetchall()

    logger.info(f"Found {len(results)} CDS features in database")

    to_fix = []
    already_correct = 0

    for row in results:
        cds_name, cds_feature_no, feat_location_no, current_strand, root_seq_no, start_coord, stop_coord, parent_name = row

        # Get expected strand from parent
        gff_strand = gene_strands.get(parent_name)
        if not gff_strand:
            continue

        expected_strand = 'W' if gff_strand == '+' else 'C'

        if current_strand != expected_strand:
            to_fix.append({
                'feature_name': cds_name,
                'feature_no': cds_feature_no,
                'feat_location_no': feat_location_no,
                'current_strand': current_strand,
                'expected_strand': expected_strand,
                'root_seq_no': root_seq_no,
                'start_coord': start_coord,
                'stop_coord': stop_coord,
            })
        else:
            already_correct += 1

    logger.info(f"CDS already correct: {already_correct}")
    logger.info(f"CDS need fixing: {len(to_fix)}")

    if not to_fix:
        return

    if dry_run:
        logger.info("[DRY RUN] Would fix CDS:")
        for item in to_fix[:5]:
            logger.info(f"  {item['feature_name']}: {item['current_strand']} -> {item['expected_strand']}")
        if len(to_fix) > 5:
            logger.info(f"  ... and {len(to_fix) - 5} more")
        return

    # Fix CDS strands
    logger.info("Fixing CDS strand values...")
    fixed = 0

    for item in to_fix:
        try:
            delete = text(f"DELETE FROM {DB_SCHEMA}.feat_location WHERE feat_location_no = :fln")
            session.execute(delete, {"fln": item['feat_location_no']})

            insert = text(f"""
                INSERT INTO {DB_SCHEMA}.feat_location (
                    feature_no, root_seq_no, coord_version, start_coord, stop_coord,
                    strand, is_loc_current, created_by
                ) VALUES (
                    :fno, :root_seq_no, SYSDATE, :start, :stop, :strand, 'Y', 'CGDADMIN'
                )
            """)
            session.execute(insert, {
                "fno": item['feature_no'],
                "root_seq_no": item['root_seq_no'],
                "start": item['start_coord'],
                "stop": item['stop_coord'],
                "strand": item['expected_strand'],
            })
            fixed += 1

            if fixed % 1000 == 0:
                logger.info(f"  Fixed {fixed}/{len(to_fix)} CDS...")
                session.commit()

        except Exception as e:
            logger.error(f"  Failed to fix {item['feature_name']}: {e}")
            session.rollback()

    session.commit()
    logger.info(f"Fixed {fixed} CDS feat_location entries")


def main():
    parser = argparse.ArgumentParser(description="Fix strand values in feat_location")
    parser.add_argument("--gff", required=True, type=Path, help="GFF file with correct strand values")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes only")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Fixing C. tropicalis strand values")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        fix_strand_values(session, args.gff, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
