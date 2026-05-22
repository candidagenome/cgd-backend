#!/usr/bin/env python3
"""
Load C. auris B8441 Mitochondrial Genome Data into CGD Database.

This script imports the mitochondrial genome (GenBank: MT849287.1, PMID: 33193142)
for Candida auris B8441 strain into the CGD database. It creates:

1. A chromosome/contig Feature record for the mitochondrial genome
2. A Seq record for the chromosome sequence
3. A GenomeVersion record for the mitochondrial genome
4. Feature records for all genes (CDS, tRNA, rRNA)
5. FeatLocation records for each feature

Usage:
    # Dry run (no database changes)
    python scripts/load_c_auris_mitochondrial_genome.py --dry-run

    # Load data
    python scripts/load_c_auris_mitochondrial_genome.py

    # With custom GenBank file
    python scripts/load_c_auris_mitochondrial_genome.py --genbank-file /path/to/MT849287.gb

Requirements:
    - BioPython (pip install biopython)
    - Database connection configured via environment variables or config

Author: CGD Curation Team
Date: 2024
Reference: PMID 33193142 (Misas et al.)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from Bio import SeqIO
from Bio.SeqRecord import SeqRecord
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker, Session

# Load environment variables from .env file
load_dotenv()

from cgd.models.models import (
    Organism,
    Feature,
    Seq,
    GenomeVersion,
    FeatLocation,
    Dbxref,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
ORGANISM_ABBREV = "C_auris_B8441"
MITO_FEATURE_NAME = "MT849287.1_C_auris_B8441_mito"
MITO_DISPLAY_NAME = "Mito"
GENBANK_ACCESSION = "MT849287.1"
PMID = "33193142"
SEQ_LENGTH = 28212
SOURCE = "CGD"  # Source for CGD-curated data

# Default GenBank file URL
GENBANK_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&id=MT849287.1&rettype=gb"


def get_database_url() -> str:
    """Get database URL from environment or config."""
    # Try environment variables first
    db_url = os.environ.get("CGD_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if db_url:
        return db_url

    # Try to load from config
    try:
        from cgd.config import settings
        return settings.database_url
    except ImportError:
        pass

    # Default development database
    logger.warning("Using default database URL. Set CGD_DATABASE_URL for production.")
    return "oracle+oracledb://user:pass@localhost:1521/cgd"


def get_next_sequence_value(db: Session, sequence_name: str) -> int:
    """
    Get the next value from an Oracle sequence.

    Args:
        db: Database session
        sequence_name: Name of the Oracle sequence

    Returns:
        Next sequence value
    """
    result = db.execute(
        text(f"SELECT {sequence_name}.NEXTVAL FROM DUAL")
    ).scalar()
    return result


def get_organism(db: Session, organism_abbrev: str) -> Optional[Organism]:
    """Get organism by abbreviation."""
    return (
        db.query(Organism)
        .filter(Organism.organism_abbrev == organism_abbrev)
        .first()
    )


def check_existing_mito_chromosome(db: Session, organism_no: int) -> Optional[Feature]:
    """Check if mitochondrial chromosome already exists."""
    return (
        db.query(Feature)
        .filter(
            Feature.organism_no == organism_no,
            Feature.feature_name == MITO_FEATURE_NAME,
        )
        .first()
    )


def download_genbank_file(output_path: Path) -> bool:
    """
    Download GenBank file from NCBI.

    Args:
        output_path: Path to save the GenBank file

    Returns:
        True if successful, False otherwise
    """
    import urllib.request

    logger.info(f"Downloading GenBank file from NCBI...")
    try:
        urllib.request.urlretrieve(GENBANK_URL, output_path)
        logger.info(f"Downloaded to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to download GenBank file: {e}")
        return False


def parse_genbank_file(genbank_path: Path) -> Optional[SeqRecord]:
    """
    Parse GenBank file.

    Args:
        genbank_path: Path to GenBank file

    Returns:
        SeqRecord object or None if parsing fails
    """
    try:
        record = SeqIO.read(genbank_path, "genbank")
        logger.info(f"Parsed GenBank record: {record.id}")
        logger.info(f"  Sequence length: {len(record.seq)} bp")
        logger.info(f"  Features: {len(record.features)}")
        return record
    except Exception as e:
        logger.error(f"Failed to parse GenBank file: {e}")
        return None


def create_genome_version(
    db: Session,
    organism_no: int,
    dry_run: bool = True,
) -> Optional[int]:
    """
    Create a GenomeVersion record for the mitochondrial genome.

    Args:
        db: Database session
        organism_no: Organism number
        dry_run: If True, don't commit changes

    Returns:
        genome_version_no or None
    """
    # Check if mito genome version already exists
    existing = (
        db.query(GenomeVersion)
        .filter(
            GenomeVersion.organism_no == organism_no,
            GenomeVersion.genome_version.like("mito%"),
        )
        .first()
    )

    if existing:
        logger.info(f"Mitochondrial genome version already exists: {existing.genome_version}")
        return existing.genome_version_no

    # Create new genome version
    genome_version_str = "mito-s01-m01-r01"
    description = f"Mitochondrial genome sequence (GenBank: {GENBANK_ACCESSION}, PMID: {PMID})"

    logger.info(f"Creating genome version: {genome_version_str}")
    logger.info(f"  Description: {description}")

    if dry_run:
        logger.info("  [DRY RUN] Would create GenomeVersion record")
        return None

    # Get next sequence value
    gv_no = get_next_sequence_value(db, "MULTI.genome_version_seq")

    genome_version = GenomeVersion(
        genome_version_no=gv_no,
        genome_version=genome_version_str,
        organism_no=organism_no,
        is_ver_current="Y",
        date_created=datetime.now(),
        description=description,
    )
    db.add(genome_version)
    db.flush()

    logger.info(f"  Created GenomeVersion: {genome_version_str} (no={gv_no})")
    return gv_no


def create_mito_chromosome(
    db: Session,
    organism_no: int,
    genome_version_no: int,
    sequence: str,
    dry_run: bool = True,
) -> Optional[int]:
    """
    Create Feature and Seq records for the mitochondrial chromosome.

    Args:
        db: Database session
        organism_no: Organism number
        genome_version_no: Genome version number
        sequence: DNA sequence
        dry_run: If True, don't commit changes

    Returns:
        feature_no or None
    """
    logger.info(f"Creating mitochondrial chromosome feature: {MITO_FEATURE_NAME}")
    logger.info(f"  Sequence length: {len(sequence)} bp")

    if dry_run:
        logger.info("  [DRY RUN] Would create Feature and Seq records")
        return None

    # Get next sequence values
    feature_no = get_next_sequence_value(db, "MULTI.feature_seq")
    seq_no = get_next_sequence_value(db, "MULTI.seq_seq")

    # Generate dbxref_id for the chromosome
    dbxref_id = f"CAL_MITO_{GENBANK_ACCESSION}"

    # Create Feature record
    feature = Feature(
        feature_no=feature_no,
        organism_no=organism_no,
        feature_name=MITO_FEATURE_NAME,
        dbxref_id=dbxref_id,
        feature_type="chromosome",  # or "mitochondrion" depending on CGD conventions
        source=SOURCE,
        date_created=datetime.now(),
        gene_name=None,
        name_description=None,
        headline=f"Mitochondrial genome ({GENBANK_ACCESSION})",
    )
    db.add(feature)
    db.flush()

    logger.info(f"  Created Feature: {MITO_FEATURE_NAME} (no={feature_no})")

    # Create Seq record for the chromosome sequence
    seq = Seq(
        seq_no=seq_no,
        feature_no=feature_no,
        genome_version_no=genome_version_no,
        seq_version=datetime.now(),
        seq_type="Genomic",
        source=SOURCE,
        is_seq_current="Y",
        date_created=datetime.now(),
        seq_length=len(sequence),
        residues=sequence.upper(),
        ftp_file=None,
    )
    db.add(seq)
    db.flush()

    logger.info(f"  Created Seq: length={len(sequence)} (no={seq_no})")

    return feature_no


def create_feature_location(
    db: Session,
    feature_no: int,
    root_seq_no: int,
    start: int,
    stop: int,
    strand: str,
    dry_run: bool = True,
) -> Optional[int]:
    """
    Create FeatLocation record for a feature.

    Args:
        db: Database session
        feature_no: Feature number
        root_seq_no: Root sequence number (chromosome)
        start: Start coordinate (1-based)
        stop: Stop coordinate (1-based)
        strand: 'W' for Watson (forward) or 'C' for Crick (reverse)
        dry_run: If True, don't commit changes

    Returns:
        feat_location_no or None
    """
    if dry_run:
        return None

    fl_no = get_next_sequence_value(db, "MULTI.feat_location_seq")

    feat_location = FeatLocation(
        feat_location_no=fl_no,
        feature_no=feature_no,
        root_seq_no=root_seq_no,
        coord_version=datetime.now(),
        start_coord=start,
        stop_coord=stop,
        strand=strand,
        is_loc_current="Y",
        date_created=datetime.now(),
        seq_no=None,
    )
    db.add(feat_location)
    db.flush()

    return fl_no


def get_strand_code(strand_int: int) -> str:
    """Convert BioPython strand integer to CGD strand code."""
    # BioPython: 1 = forward, -1 = reverse, 0 or None = unknown
    if strand_int == 1:
        return "W"  # Watson (forward)
    elif strand_int == -1:
        return "C"  # Crick (reverse)
    else:
        return "W"  # Default to Watson


def create_gene_features(
    db: Session,
    genbank_record: SeqRecord,
    organism_no: int,
    genome_version_no: int,
    mito_seq_no: int,
    dry_run: bool = True,
) -> dict:
    """
    Create Feature records for all genes in the GenBank record.

    Args:
        db: Database session
        genbank_record: Parsed GenBank record
        organism_no: Organism number
        genome_version_no: Genome version number
        mito_seq_no: Seq number of the mitochondrial chromosome
        dry_run: If True, don't commit changes

    Returns:
        Dictionary with counts of created features
    """
    counts = {
        "CDS": 0,
        "tRNA": 0,
        "rRNA": 0,
        "other": 0,
    }

    logger.info("Processing gene features from GenBank record...")

    for feature in genbank_record.features:
        feature_type = feature.type

        # Skip source and gene features (we create our own gene structure)
        if feature_type in ("source", "gene"):
            continue

        # Map GenBank feature types to CGD feature types
        if feature_type == "CDS":
            cgd_feature_type = "ORF"
            counts["CDS"] += 1
        elif feature_type == "tRNA":
            cgd_feature_type = "tRNA"
            counts["tRNA"] += 1
        elif feature_type == "rRNA":
            cgd_feature_type = "rRNA"
            counts["rRNA"] += 1
        else:
            logger.debug(f"Skipping feature type: {feature_type}")
            counts["other"] += 1
            continue

        # Extract feature information
        qualifiers = feature.qualifiers
        gene_name = qualifiers.get("gene", [None])[0]
        product = qualifiers.get("product", [None])[0]
        protein_id = qualifiers.get("protein_id", [None])[0]
        locus_tag = qualifiers.get("locus_tag", [None])[0]

        # Get location
        location = feature.location
        start = int(location.start) + 1  # Convert to 1-based
        stop = int(location.end)
        strand = get_strand_code(location.strand)

        # Generate feature name
        if gene_name:
            feature_name = f"{gene_name}_mito"
        elif locus_tag:
            feature_name = f"{locus_tag}_mito"
        elif protein_id:
            feature_name = f"{protein_id.replace('.', '_')}_mito"
        else:
            feature_name = f"mito_{feature_type}_{start}_{stop}"

        # Generate dbxref_id
        if protein_id:
            dbxref_id = f"MITO_{protein_id.replace('.', '_')}"
        else:
            dbxref_id = f"MITO_{feature_name.upper()}"

        # Generate headline
        if product:
            headline = product
        elif gene_name:
            headline = f"{gene_name} gene"
        else:
            headline = f"Mitochondrial {feature_type}"

        logger.info(f"  {cgd_feature_type}: {feature_name}")
        logger.info(f"    Location: {start}..{stop} ({strand})")
        logger.info(f"    Product: {headline}")

        if dry_run:
            logger.info(f"    [DRY RUN] Would create {cgd_feature_type} feature")
            continue

        # Create Feature record
        feature_no = get_next_sequence_value(db, "MULTI.feature_seq")

        db_feature = Feature(
            feature_no=feature_no,
            organism_no=organism_no,
            feature_name=feature_name,
            dbxref_id=dbxref_id,
            feature_type=cgd_feature_type,
            source=SOURCE,
            date_created=datetime.now(),
            gene_name=gene_name,
            name_description=None,
            headline=headline,
        )
        db.add(db_feature)
        db.flush()

        # Create FeatLocation record
        create_feature_location(
            db=db,
            feature_no=feature_no,
            root_seq_no=mito_seq_no,
            start=start,
            stop=stop,
            strand=strand,
            dry_run=dry_run,
        )

        # For CDS features, also create Seq records for protein sequence
        if feature_type == "CDS" and "translation" in qualifiers:
            protein_seq = qualifiers["translation"][0]
            seq_no = get_next_sequence_value(db, "MULTI.seq_seq")

            protein = Seq(
                seq_no=seq_no,
                feature_no=feature_no,
                genome_version_no=genome_version_no,
                seq_version=datetime.now(),
                seq_type="Protein",
                source=SOURCE,
                is_seq_current="Y",
                date_created=datetime.now(),
                seq_length=len(protein_seq),
                residues=protein_seq,
                ftp_file=None,
            )
            db.add(protein)
            db.flush()

    logger.info(f"Feature counts: {counts}")
    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load C. auris mitochondrial genome into CGD database"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--genbank-file",
        type=Path,
        help="Path to GenBank file (will download if not provided)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Only parse the GenBank file without connecting to database",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("C. auris Mitochondrial Genome Import Script")
    logger.info("=" * 60)
    logger.info(f"GenBank Accession: {GENBANK_ACCESSION}")
    logger.info(f"Reference PMID: {PMID}")
    logger.info(f"Sequence Length: {SEQ_LENGTH} bp")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("")

    # Get or download GenBank file
    genbank_path = args.genbank_file
    if not genbank_path:
        genbank_path = Path("/tmp/MT849287.gb")
        if not genbank_path.exists():
            if not download_genbank_file(genbank_path):
                logger.error("Failed to download GenBank file")
                sys.exit(1)
        else:
            logger.info(f"Using cached GenBank file: {genbank_path}")

    # Parse GenBank file
    genbank_record = parse_genbank_file(genbank_path)
    if not genbank_record:
        logger.error("Failed to parse GenBank file")
        sys.exit(1)

    # If parse-only mode, just report the features and exit
    if args.parse_only:
        logger.info("")
        logger.info("=" * 60)
        logger.info("PARSE-ONLY MODE - Analyzing GenBank features")
        logger.info("=" * 60)

        # Count features by type
        feature_counts = {}
        for feature in genbank_record.features:
            ftype = feature.type
            feature_counts[ftype] = feature_counts.get(ftype, 0) + 1

        logger.info("Feature counts by type:")
        for ftype, count in sorted(feature_counts.items()):
            logger.info(f"  {ftype}: {count}")

        logger.info("")
        logger.info("Detailed features (CDS, tRNA, rRNA):")
        for feature in genbank_record.features:
            if feature.type in ("CDS", "tRNA", "rRNA"):
                qualifiers = feature.qualifiers
                gene_name = qualifiers.get("gene", ["-"])[0]
                product = qualifiers.get("product", ["-"])[0]
                location = feature.location
                start = int(location.start) + 1
                stop = int(location.end)
                strand = "+" if location.strand == 1 else "-"
                logger.info(f"  {feature.type}: {gene_name} ({start}..{stop}, {strand})")
                logger.info(f"    Product: {product}")

        logger.info("")
        logger.info("=" * 60)
        logger.info("PARSE COMPLETE")
        logger.info("=" * 60)
        sys.exit(0)

    # Connect to database
    db_url = get_database_url()
    logger.info(f"Connecting to database...")

    try:
        engine = create_engine(db_url)
        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Get organism
        organism = get_organism(db, ORGANISM_ABBREV)
        if not organism:
            logger.error(f"Organism not found: {ORGANISM_ABBREV}")
            sys.exit(1)

        logger.info(f"Found organism: {organism.organism_name} (no={organism.organism_no})")

        # Check for existing mitochondrial chromosome
        existing_mito = check_existing_mito_chromosome(db, organism.organism_no)
        if existing_mito:
            logger.warning(f"Mitochondrial chromosome already exists: {existing_mito.feature_name}")
            logger.warning("To re-import, first delete the existing records")
            sys.exit(1)

        # Create genome version
        genome_version_no = create_genome_version(
            db=db,
            organism_no=organism.organism_no,
            dry_run=args.dry_run,
        )

        # Create mitochondrial chromosome feature and sequence
        mito_feature_no = create_mito_chromosome(
            db=db,
            organism_no=organism.organism_no,
            genome_version_no=genome_version_no,
            sequence=str(genbank_record.seq),
            dry_run=args.dry_run,
        )

        if not args.dry_run and mito_feature_no:
            # Get the Seq record for the mitochondrial chromosome
            mito_seq = (
                db.query(Seq)
                .filter(
                    Seq.feature_no == mito_feature_no,
                    Seq.seq_type == "Genomic",
                )
                .first()
            )

            if mito_seq:
                # Create gene features
                counts = create_gene_features(
                    db=db,
                    genbank_record=genbank_record,
                    organism_no=organism.organism_no,
                    genome_version_no=genome_version_no,
                    mito_seq_no=mito_seq.seq_no,
                    dry_run=args.dry_run,
                )
        else:
            # Dry run - still process features for reporting
            counts = create_gene_features(
                db=db,
                genbank_record=genbank_record,
                organism_no=organism.organism_no,
                genome_version_no=genome_version_no,
                mito_seq_no=None,
                dry_run=args.dry_run,
            )

        # Commit or rollback
        if args.dry_run:
            logger.info("")
            logger.info("=" * 60)
            logger.info("DRY RUN COMPLETE - No changes made to database")
            logger.info("=" * 60)
            db.rollback()
        else:
            db.commit()
            logger.info("")
            logger.info("=" * 60)
            logger.info("IMPORT COMPLETE - Changes committed to database")
            logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error during import: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
