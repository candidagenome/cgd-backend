#!/usr/bin/env python3
"""
Load mitochondrial contig sequences from FASTA file.

This script loads mitochondrial contig sequences from a FASTA file into
the FEATURE, SEQ, and FEAT_LOCATION tables. It creates or updates
contig features with their genomic sequences.

Input: FASTA file with contig sequences

Original Perl: loadMitoContigs_Anidulans.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from Bio import SeqIO
from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, FeatLocation, Organism, Seq

load_dotenv()

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def get_organism(session: Session, organism_abbrev: str) -> Organism:
    """
    Get organism by abbreviation.

    Args:
        session: Database session
        organism_abbrev: Organism abbreviation

    Returns:
        Organism object

    Raises:
        ValueError: If organism not found
    """
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == organism_abbrev
    ).first()

    if not organism:
        raise ValueError(f"Organism not found: {organism_abbrev}")

    return organism


def get_or_create_feature(
    session: Session,
    feature_name: str,
    feature_type: str,
    organism_no: int,
    source: str,
    created_by: str,
) -> tuple[int, bool]:
    """
    Get existing feature or create new one.

    Args:
        session: Database session
        feature_name: Feature name
        feature_type: Feature type
        organism_no: Organism number
        source: Source of the feature
        created_by: User creating the record

    Returns:
        Tuple of (feature_no, is_new)
    """
    feature = session.query(Feature).filter(
        Feature.feature_name == feature_name
    ).first()

    if feature:
        logger.debug(f"Feature exists: {feature_name}")
        return feature.feature_no, False

    # Generate next dbxref_id
    result = session.execute(
        text("SELECT MULTI.FEATURE_SEQ.NEXTVAL FROM DUAL")
    )
    next_id = result.scalar()
    dbxref_id = f"CGD{next_id:010d}"

    new_feature = Feature(
        organism_no=organism_no,
        feature_name=feature_name,
        dbxref_id=dbxref_id,
        feature_type=feature_type,
        source=source,
        created_by=created_by[:12],
    )
    session.add(new_feature)
    session.flush()

    logger.info(f"FEATURE inserted for {feature_name}")
    return new_feature.feature_no, True


def get_or_create_seq(
    session: Session,
    feature_no: int,
    feature_name: str,
    sequence: str,
    seq_length: int,
    source: str,
    genome_version_no: int,
    created_by: str,
) -> int:
    """
    Get existing sequence or create/update one.

    Args:
        session: Database session
        feature_no: Feature number
        feature_name: Feature name (for logging)
        sequence: Sequence residues
        seq_length: Sequence length
        source: Sequence source
        genome_version_no: Genome version number
        created_by: User creating the record

    Returns:
        seq_no
    """
    existing = session.query(Seq).filter(
        and_(
            Seq.feature_no == feature_no,
            Seq.seq_type == "genomic",
            Seq.is_seq_current == "Y",
            Seq.source == source,
        )
    ).first()

    if existing:
        # Check if sequence changed
        if existing.residues.upper() != sequence.upper():
            existing.seq_length = seq_length
            existing.residues = sequence
            session.flush()
            logger.info(f"SEQ updated for {feature_name}")
        else:
            logger.debug(f"SEQ unchanged for {feature_name}")
        return existing.seq_no

    # Create new sequence
    new_seq = Seq(
        feature_no=feature_no,
        genome_version_no=genome_version_no,
        seq_version=datetime.now(),
        seq_type="genomic",
        source=source,
        is_seq_current="Y",
        seq_length=seq_length,
        residues=sequence,
        created_by=created_by[:12],
    )
    session.add(new_seq)
    session.flush()

    logger.info(f"SEQ ({new_seq.seq_no}) inserted for {feature_name}")
    return new_seq.seq_no


def get_or_create_feat_location(
    session: Session,
    feature_no: int,
    feature_name: str,
    seq_no: int,
    root_seq_no: int,
    start_coord: int,
    stop_coord: int,
    strand: str,
    created_by: str,
) -> None:
    """
    Get existing feature location or create/update one.

    Args:
        session: Database session
        feature_no: Feature number
        feature_name: Feature name (for logging)
        seq_no: Sequence number
        root_seq_no: Root sequence number
        start_coord: Start coordinate
        stop_coord: Stop coordinate
        strand: Strand (W or C)
        created_by: User creating the record
    """
    existing = session.query(FeatLocation).filter(
        and_(
            FeatLocation.feature_no == feature_no,
            FeatLocation.seq_no == seq_no,
            FeatLocation.root_seq_no == root_seq_no,
            FeatLocation.is_loc_current == "Y",
        )
    ).first()

    if existing:
        # Check if location changed
        if (existing.start_coord != start_coord or
                existing.stop_coord != stop_coord or
                existing.strand != strand):
            existing.start_coord = start_coord
            existing.stop_coord = stop_coord
            existing.strand = strand
            session.flush()
            logger.info(f"FEAT_LOCATION updated for {feature_name}")
        else:
            logger.debug(f"FEAT_LOCATION unchanged for {feature_name}")
        return

    # Create new location
    new_location = FeatLocation(
        feature_no=feature_no,
        seq_no=seq_no,
        root_seq_no=root_seq_no,
        coord_version=datetime.now(),
        start_coord=start_coord,
        stop_coord=stop_coord,
        strand=strand,
        is_loc_current="Y",
        created_by=created_by[:12],
    )
    session.add(new_location)
    session.flush()

    logger.info(f"FEAT_LOCATION inserted for {feature_name}")


def load_contigs_from_fasta(
    session: Session,
    fasta_file: Path,
    organism: Organism,
    source: str,
    genome_version_no: int,
    created_by: str,
) -> dict:
    """
    Load contig sequences from FASTA file.

    Args:
        session: Database session
        fasta_file: Path to FASTA file
        organism: Organism object
        source: Source name
        genome_version_no: Genome version number
        created_by: User creating records

    Returns:
        Statistics dictionary
    """
    stats = {
        "contigs_processed": 0,
        "features_created": 0,
        "features_existing": 0,
        "seqs_created": 0,
        "seqs_updated": 0,
        "locations_created": 0,
    }

    for record in SeqIO.parse(fasta_file, "fasta"):
        name = record.id
        sequence = str(record.seq)
        length = len(sequence)

        logger.debug(f"Processing contig: {name} ({length} bp)")

        # Get or create feature
        feature_no, is_new = get_or_create_feature(
            session,
            feature_name=name,
            feature_type="contig",
            organism_no=organism.organism_no,
            source=source,
            created_by=created_by,
        )

        if is_new:
            stats["features_created"] += 1
        else:
            stats["features_existing"] += 1

        # Get or create sequence
        seq_no = get_or_create_seq(
            session,
            feature_no=feature_no,
            feature_name=name,
            sequence=sequence,
            seq_length=length,
            source=source,
            genome_version_no=genome_version_no,
            created_by=created_by,
        )

        # Get or create feature location
        get_or_create_feat_location(
            session,
            feature_no=feature_no,
            feature_name=name,
            seq_no=seq_no,
            root_seq_no=seq_no,
            start_coord=1,
            stop_coord=length,
            strand="W",
            created_by=created_by,
        )

        stats["contigs_processed"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load mitochondrial contig sequences from FASTA file"
    )
    parser.add_argument(
        "fasta_file",
        type=Path,
        help="Input FASTA file with contig sequences",
    )
    parser.add_argument(
        "--organism",
        "-o",
        required=True,
        help="Organism abbreviation (e.g., A_nidulans)",
    )
    parser.add_argument(
        "--source",
        default="CGD",
        help="Source name (default: CGD)",
    )
    parser.add_argument(
        "--genome-version-no",
        type=int,
        required=True,
        help="Genome version number",
    )
    parser.add_argument(
        "--created-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user for created_by field",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse file but don't modify database",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input file
    if not args.fasta_file.exists():
        logger.error(f"Input file not found: {args.fasta_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.fasta_file}")
    logger.info(f"Organism: {args.organism}")
    logger.info(f"Source: {args.source}")
    logger.info(f"Genome version no: {args.genome_version_no}")

    if args.dry_run:
        logger.info("DRY RUN - parsing file only")
        for record in SeqIO.parse(args.fasta_file, "fasta"):
            logger.info(f"  Contig: {record.id} ({len(record.seq)} bp)")
        return

    try:
        with SessionLocal() as session:
            # Get organism
            organism = get_organism(session, args.organism)
            logger.info(f"Found organism: {organism.organism_name}")

            # Load contigs
            stats = load_contigs_from_fasta(
                session,
                args.fasta_file,
                organism,
                args.source,
                args.genome_version_no,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Contigs processed: {stats['contigs_processed']}")
            logger.info(f"  Features created: {stats['features_created']}")
            logger.info(f"  Features existing: {stats['features_existing']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading contigs: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
