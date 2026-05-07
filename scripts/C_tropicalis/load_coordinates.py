#!/usr/bin/env python3
"""
Load C. tropicalis feature coordinates and intron data.

This script loads:
1. Chromosome/scaffold features and sequences
2. Feature locations (chromosomal coordinates) into feat_location
3. Exon and intron coordinates into subfeature/subfeature_type tables

Usage:
    python load_coordinates.py --gff FILE --genomic FILE [--dry-run]

Data files needed:
    - GFF file with gene annotations (Ctrop_liftover3_sorted.gff)
    - Genomic FASTA file (GCA_013177555.1_ASM1317755v1_genomic.fna)

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()

ORGANISM_NAME = "Candida tropicalis MYA-3404"
SOURCE = "C. tropicalis MYA-3404"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def ensure_code(session, table_name: str, col_name: str, code_value: str,
                description: str, dry_run: bool = False) -> bool:
    """Ensure a code value exists in the code table."""
    query = text(f"""
        SELECT code_no FROM {DB_SCHEMA}.code
        WHERE tab_name = :tab_name AND col_name = :col_name AND code_value = :code_value
    """)
    result = session.execute(query, {
        "tab_name": table_name, "col_name": col_name, "code_value": code_value
    }).first()

    if result:
        return True

    if dry_run:
        logger.info(f"[DRY RUN] Would create code for {table_name}.{col_name}: {code_value}")
        return True

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.code (tab_name, col_name, code_value, description, created_by)
        VALUES (:tab_name, :col_name, :code_value, :description, :created_by)
    """)
    session.execute(insert, {
        "tab_name": table_name, "col_name": col_name, "code_value": code_value,
        "description": description, "created_by": ADMIN_USER,
    })
    session.commit()
    logger.info(f"Created code: {table_name}.{col_name}: {code_value}")
    return True


def parse_genomic_fasta(fasta_file: Path) -> Dict[str, str]:
    """Parse genomic FASTA into dict of {chromosome_id: sequence}."""
    sequences = {}
    current_id = None
    current_seq = []

    with open(fasta_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('>'):
                if current_id:
                    sequences[current_id] = ''.join(current_seq).upper()
                current_id = line[1:].split()[0]
                current_seq = []
            else:
                current_seq.append(line)

        if current_id:
            sequences[current_id] = ''.join(current_seq).upper()

    return sequences


def parse_gff_full(gff_file: Path) -> Tuple[List[Dict], Dict[str, List[Dict]], Dict[str, str]]:
    """
    Parse GFF file and extract gene features, CDS/exon features, and protein ID mapping.

    Returns:
        Tuple of (genes, gene_to_exons dict, gene_to_protein mapping)
    """
    genes = []
    gene_to_exons = defaultdict(list)
    gene_to_protein = {}
    mrna_to_gene = {}

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

            # Track mRNA -> gene mapping
            if feature_type == 'mRNA':
                mrna_id = attr_dict.get('ID', '')
                parent = attr_dict.get('Parent', '')
                if mrna_id and parent:
                    mrna_to_gene[mrna_id] = parent

            # Extract protein_id mapping from CDS features
            if feature_type == 'CDS':
                protein_id = attr_dict.get('protein_id', '')
                gene_id = attr_dict.get('gene_id', '')
                parent = attr_dict.get('Parent', '')

                if protein_id and gene_id:
                    gene_to_protein[gene_id] = protein_id

                # Get actual gene_id from parent chain
                actual_gene_id = gene_id or mrna_to_gene.get(parent, parent)

                # Store CDS coordinates for exon/intron calculation
                if actual_gene_id:
                    gene_to_exons[actual_gene_id].append({
                        'start': int(start),
                        'end': int(end),
                        'strand': strand,
                    })

            # Process gene features
            if feature_type == 'gene':
                gene_id = attr_dict.get('ID', '').split(',')[0]
                if not gene_id:
                    continue

                genes.append({
                    'chromosome': seqid,
                    'gene_id': gene_id,
                    'start': int(start),
                    'end': int(end),
                    'strand': strand,
                })

    # Sort exons by start coordinate for each gene
    for gene_id in gene_to_exons:
        gene_to_exons[gene_id].sort(key=lambda x: x['start'])

    return genes, dict(gene_to_exons), gene_to_protein


def get_organism_and_genome(session) -> Tuple[int, int]:
    """Get organism_no and genome_version_no for C. tropicalis."""
    query = text(f"SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_name = :name")
    result = session.execute(query, {"name": ORGANISM_NAME}).first()
    if not result:
        raise ValueError(f"Organism not found: {ORGANISM_NAME}")
    organism_no = result[0]

    query = text(f"""
        SELECT genome_version_no FROM {DB_SCHEMA}.genome_version
        WHERE organism_no = :org_no AND is_ver_current = 'Y'
    """)
    result = session.execute(query, {"org_no": organism_no}).first()
    if not result:
        raise ValueError(f"No genome version for organism_no={organism_no}")

    return organism_no, result[0]


def get_or_create_chromosome_feature(session, organism_no: int, chrom_name: str,
                                      dry_run: bool = False) -> Optional[int]:
    """Get or create a chromosome/scaffold feature."""
    query = text(f"""
        SELECT feature_no FROM {DB_SCHEMA}.feature
        WHERE feature_name = :name AND organism_no = :org_no
    """)
    result = session.execute(query, {"name": chrom_name, "org_no": organism_no}).first()

    if result:
        return result[0]

    if dry_run:
        logger.info(f"[DRY RUN] Would create chromosome feature: {chrom_name}")
        return None

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.feature (
            organism_no, feature_name, feature_type, source, created_by
        ) VALUES (
            :org_no, :name, 'contig', :source, :created_by
        )
    """)
    session.execute(insert, {
        "org_no": organism_no, "name": chrom_name,
        "source": SOURCE, "created_by": ADMIN_USER,
    })

    result = session.execute(query, {"name": chrom_name, "org_no": organism_no}).first()
    return result[0] if result else None


def get_or_create_chromosome_seq(session, feature_no: int, genome_version_no: int,
                                  sequence: str, dry_run: bool = False) -> Optional[int]:
    """Get or create a chromosome/scaffold sequence."""
    # Check both lowercase and mixed-case seq_type for backward compatibility
    query = text(f"""
        SELECT seq_no FROM {DB_SCHEMA}.seq
        WHERE feature_no = :fno AND seq_type IN ('genomic', 'Genomic DNA') AND is_seq_current = 'Y'
    """)
    result = session.execute(query, {"fno": feature_no}).first()

    if result:
        return result[0]

    if dry_run:
        return None

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.seq (
            feature_no, genome_version_no, seq_version, seq_type, source,
            is_seq_current, seq_length, residues, created_by
        ) VALUES (
            :fno, :gvno, SYSDATE, 'genomic', :source, 'Y', :len, :res, :created_by
        )
    """)
    session.execute(insert, {
        "fno": feature_no, "gvno": genome_version_no, "source": SOURCE,
        "len": len(sequence), "res": sequence, "created_by": ADMIN_USER,
    })

    result = session.execute(query, {"fno": feature_no}).first()
    return result[0] if result else None


def get_feature_no(session, feature_name: str) -> Optional[int]:
    """Get feature_no by feature_name."""
    query = text(f"SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name = :name")
    result = session.execute(query, {"name": feature_name}).first()
    return result[0] if result else None


def feat_location_exists(session, feature_no: int) -> bool:
    """Check if feature already has a current location."""
    query = text(f"""
        SELECT feat_location_no FROM {DB_SCHEMA}.feat_location
        WHERE feature_no = :fno AND is_loc_current = 'Y'
    """)
    result = session.execute(query, {"fno": feature_no}).first()
    return result is not None


def create_feat_location(session, feature_no: int, root_seq_no: int,
                          start_coord: int, stop_coord: int, strand: str) -> bool:
    """Create a feat_location entry."""
    # Convert strand format: + -> W, - -> C
    db_strand = 'W' if strand == '+' else 'C'

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.feat_location (
            feature_no, root_seq_no, coord_version, start_coord, stop_coord,
            strand, is_loc_current, created_by
        ) VALUES (
            :fno, :root_seq_no, SYSDATE, :start, :stop, :strand, 'Y', :created_by
        )
    """)
    session.execute(insert, {
        "fno": feature_no, "root_seq_no": root_seq_no,
        "start": start_coord, "stop": stop_coord,
        "strand": db_strand, "created_by": ADMIN_USER,
    })
    return True


def subfeature_exists(session, feature_no: int, start_coord: int, stop_coord: int) -> bool:
    """Check if subfeature already exists."""
    query = text(f"""
        SELECT subfeature_no FROM {DB_SCHEMA}.subfeature
        WHERE feature_no = :fno AND start_coord = :start AND stop_coord = :stop
    """)
    result = session.execute(query, {"fno": feature_no, "start": start_coord, "stop": stop_coord}).first()
    return result is not None


def create_subfeature(session, feature_no: int, start_coord: int, stop_coord: int,
                       subfeature_type: str) -> bool:
    """Create a subfeature entry with its type."""
    # Insert subfeature
    insert_sf = text(f"""
        INSERT INTO {DB_SCHEMA}.subfeature (feature_no, start_coord, stop_coord, created_by)
        VALUES (:fno, :start, :stop, :created_by)
    """)
    session.execute(insert_sf, {
        "fno": feature_no, "start": start_coord, "stop": stop_coord, "created_by": ADMIN_USER,
    })

    # Get subfeature_no
    query = text(f"""
        SELECT subfeature_no FROM {DB_SCHEMA}.subfeature
        WHERE feature_no = :fno AND start_coord = :start AND stop_coord = :stop
    """)
    result = session.execute(query, {"fno": feature_no, "start": start_coord, "stop": stop_coord}).first()

    if result:
        subfeature_no = result[0]
        # Insert subfeature_type
        insert_type = text(f"""
            INSERT INTO {DB_SCHEMA}.subfeature_type (subfeature_no, subfeature_type, created_by)
            VALUES (:sfno, :sftype, :created_by)
        """)
        session.execute(insert_type, {
            "sfno": subfeature_no, "sftype": subfeature_type, "created_by": ADMIN_USER,
        })
        return True

    return False


def calculate_introns(exons: List[Dict], gene_start: int, strand: str) -> List[Dict]:
    """
    Calculate intron positions from sorted exon list.

    Args:
        exons: List of exon dicts with 'start' and 'end' coordinates (absolute)
        gene_start: Gene start coordinate (for relative position calculation)
        strand: Gene strand ('+' or '-')

    Returns:
        List of intron dicts with 'start' and 'end' relative to gene
    """
    if len(exons) < 2:
        return []

    introns = []
    sorted_exons = sorted(exons, key=lambda x: x['start'])

    for i in range(len(sorted_exons) - 1):
        # Intron is between current exon end and next exon start
        intron_start = sorted_exons[i]['end'] + 1
        intron_end = sorted_exons[i + 1]['start'] - 1

        if intron_start <= intron_end:
            # Convert to relative coordinates (1-based relative to gene start)
            rel_start = intron_start - gene_start + 1
            rel_end = intron_end - gene_start + 1

            introns.append({
                'start': rel_start,
                'end': rel_end,
            })

    return introns


def calculate_exons_relative(exons: List[Dict], gene_start: int) -> List[Dict]:
    """
    Convert exon coordinates to relative positions.

    Args:
        exons: List of exon dicts with absolute 'start' and 'end' coordinates
        gene_start: Gene start coordinate

    Returns:
        List of exon dicts with relative coordinates
    """
    relative_exons = []
    for exon in sorted(exons, key=lambda x: x['start']):
        rel_start = exon['start'] - gene_start + 1
        rel_end = exon['end'] - gene_start + 1
        relative_exons.append({
            'start': rel_start,
            'end': rel_end,
        })
    return relative_exons


def load_coordinates(session, gff_file: Path, genomic_file: Path, dry_run: bool = False,
                      skip_subfeatures: bool = False):
    """Load feature coordinates and introns."""
    # Ensure required codes
    ensure_code(session, "FEATURE", "FEATURE_TYPE", "contig", "Contig/scaffold feature", dry_run)
    ensure_code(session, "SEQ", "SEQ_TYPE", "genomic", "Genomic DNA sequence", dry_run)
    if not skip_subfeatures:
        ensure_code(session, "SUBFEATURE_TYPE", "SUBFEATURE_TYPE", "Exon", "Coding exon", dry_run)
        ensure_code(session, "SUBFEATURE_TYPE", "SUBFEATURE_TYPE", "Intron", "Intron", dry_run)

    organism_no, genome_version_no = get_organism_and_genome(session)
    logger.info(f"Loading for organism_no={organism_no}, genome_version_no={genome_version_no}")

    # Parse files
    logger.info(f"Parsing genomic FASTA: {genomic_file}")
    genomic_seqs = parse_genomic_fasta(genomic_file)
    logger.info(f"Found {len(genomic_seqs)} chromosomes/scaffolds")

    logger.info(f"Parsing GFF: {gff_file}")
    genes, gene_to_exons, gene_to_protein = parse_gff_full(gff_file)
    logger.info(f"Found {len(genes)} genes, {len(gene_to_exons)} genes with exon data")

    # Step 1: Create chromosome/scaffold features and sequences
    logger.info("Creating chromosome/scaffold features and sequences...")
    chrom_to_seq_no = {}

    for chrom_name, chrom_seq in genomic_seqs.items():
        chrom_feature_no = get_or_create_chromosome_feature(
            session, organism_no, chrom_name, dry_run
        )
        if chrom_feature_no and not dry_run:
            seq_no = get_or_create_chromosome_seq(
                session, chrom_feature_no, genome_version_no, chrom_seq, dry_run
            )
            if seq_no:
                chrom_to_seq_no[chrom_name] = seq_no

    if not dry_run:
        session.commit()
    logger.info(f"Created/found {len(chrom_to_seq_no)} chromosome sequences")

    # Step 2: Create feat_location entries
    logger.info("Creating feature locations...")
    locations_created = 0
    locations_skipped = 0
    features_not_found = 0

    for i, gene in enumerate(genes):
        gene_id = gene['gene_id']
        protein_id = gene_to_protein.get(gene_id)

        # Find feature
        feature_no = None
        if protein_id:
            feature_no = get_feature_no(session, protein_id)
        if not feature_no:
            feature_no = get_feature_no(session, gene_id)

        if not feature_no:
            features_not_found += 1
            continue

        # Get root_seq_no for this chromosome
        chrom = gene['chromosome']
        root_seq_no = chrom_to_seq_no.get(chrom)
        if not root_seq_no:
            continue

        # Check if location exists
        if feat_location_exists(session, feature_no):
            locations_skipped += 1
            continue

        if not dry_run:
            create_feat_location(
                session, feature_no, root_seq_no,
                gene['start'], gene['end'], gene['strand']
            )
            locations_created += 1

        if (i + 1) % 500 == 0:
            logger.info(f"Processed {i + 1}/{len(genes)} genes (locations)...")
            if not dry_run:
                session.commit()

    if not dry_run:
        session.commit()
    logger.info(f"Feature locations created: {locations_created}, skipped: {locations_skipped}")

    # Step 3: Create subfeature entries (exons and introns)
    exons_created = 0
    introns_created = 0

    if skip_subfeatures:
        logger.info("Skipping exon and intron subfeatures (table not available or --skip-subfeatures)")
    else:
        logger.info("Creating exon and intron subfeatures...")
        for i, gene in enumerate(genes):
            gene_id = gene['gene_id']
            protein_id = gene_to_protein.get(gene_id)

            # Find feature
            feature_no = None
            if protein_id:
                feature_no = get_feature_no(session, protein_id)
            if not feature_no:
                feature_no = get_feature_no(session, gene_id)

            if not feature_no:
                continue

            # Get exons for this gene
            exons = gene_to_exons.get(gene_id, [])
            if not exons:
                continue

            gene_start = gene['start']

            # Create exon subfeatures
            relative_exons = calculate_exons_relative(exons, gene_start)
            for exon in relative_exons:
                if not subfeature_exists(session, feature_no, exon['start'], exon['end']):
                    if not dry_run:
                        create_subfeature(session, feature_no, exon['start'], exon['end'], 'Exon')
                        exons_created += 1

            # Create intron subfeatures
            introns = calculate_introns(exons, gene_start, gene['strand'])
            for intron in introns:
                if not subfeature_exists(session, feature_no, intron['start'], intron['end']):
                    if not dry_run:
                        create_subfeature(session, feature_no, intron['start'], intron['end'], 'Intron')
                        introns_created += 1

            if (i + 1) % 500 == 0:
                logger.info(f"Processed {i + 1}/{len(genes)} genes (subfeatures)...")
                if not dry_run:
                    session.commit()

        if not dry_run:
            session.commit()

    logger.info("=" * 60)
    logger.info(f"Features not found: {features_not_found}")
    logger.info(f"Feature locations created: {locations_created}")
    logger.info(f"Feature locations skipped (already exist): {locations_skipped}")
    logger.info(f"Exon subfeatures created: {exons_created}")
    logger.info(f"Intron subfeatures created: {introns_created}")


def table_exists(session, table_name: str) -> bool:
    """Check if a table exists in the database."""
    query = text(f"""
        SELECT COUNT(*) FROM all_tables
        WHERE owner = :schema AND table_name = :table_name
    """)
    result = session.execute(query, {"schema": DB_SCHEMA, "table_name": table_name.upper()}).first()
    return result[0] > 0 if result else False


def main():
    parser = argparse.ArgumentParser(description="Load C. tropicalis coordinates and introns")
    parser.add_argument("--gff", required=True, type=Path, help="GFF file")
    parser.add_argument("--genomic", required=True, type=Path, help="Genomic FASTA file")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--skip-subfeatures", action="store_true", help="Skip exon/intron loading")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis coordinates and introns")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        # Check if subfeature table exists
        skip_subfeatures = args.skip_subfeatures
        if not skip_subfeatures and not table_exists(session, "SUBFEATURE"):
            logger.warning("SUBFEATURE table does not exist - skipping exon/intron loading")
            skip_subfeatures = True

        load_coordinates(session, args.gff, args.genomic, args.dry_run, skip_subfeatures)

    logger.info("Done!")


if __name__ == "__main__":
    main()
