#!/usr/bin/env python3
"""
Extract chromosome sequences from database.

This script retrieves chromosome/contig sequences from the database
and outputs them in FASTA format.

Original Perl: getDBchrSeqs.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, Organism, Seq

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
    """Get organism by abbreviation."""
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == organism_abbrev
    ).first()

    if not organism:
        raise ValueError(f"Organism not found: {organism_abbrev}")

    return organism


def get_chromosome_sequences(
    session: Session,
    organism: Organism = None,
    feature_types: list[str] = None,
    feature_names: list[str] = None,
) -> list[dict]:
    """
    Get chromosome/contig sequences from database.

    Args:
        session: Database session
        organism: Optional organism filter
        feature_types: Feature types to include (default: chromosome, contig)
        feature_names: Specific feature names to retrieve

    Returns:
        List of dicts with feature_name, sequence, length
    """
    if feature_types is None:
        feature_types = ['chromosome', 'contig']

    # Build query
    query = session.query(Feature).filter(
        Feature.feature_type.in_(feature_types)
    )

    if organism:
        query = query.filter(Feature.organism_no == organism.organism_no)

    if feature_names:
        query = query.filter(Feature.feature_name.in_(feature_names))

    features = query.all()

    results = []
    for feat in features:
        # Get current genomic sequence
        seq_record = session.query(Seq).filter(
            and_(
                Seq.feature_no == feat.feature_no,
                Seq.seq_type == 'genomic',
                Seq.is_seq_current == 'Y',
            )
        ).first()

        if seq_record and seq_record.residues:
            results.append({
                'feature_name': feat.feature_name,
                'feature_type': feat.feature_type,
                'organism': organism.organism_abbrev if organism else None,
                'sequence': seq_record.residues,
                'length': len(seq_record.residues),
            })

    return results


def write_fasta(
    sequences: list[dict],
    output_file: Path = None,
    line_width: int = 60,
) -> None:
    """
    Write sequences to FASTA format.

    Args:
        sequences: List of sequence dicts
        output_file: Output file (stdout if None)
        line_width: Line width for sequence wrapping
    """
    out_handle = open(output_file, 'w') if output_file else sys.stdout

    try:
        for seq_info in sequences:
            # Write header
            header = f">{seq_info['feature_name']}"
            if seq_info.get('organism'):
                header += f" organism={seq_info['organism']}"
            header += f" length={seq_info['length']}"
            out_handle.write(header + "\n")

            # Write sequence with line wrapping
            seq = seq_info['sequence']
            for i in range(0, len(seq), line_width):
                out_handle.write(seq[i:i+line_width] + "\n")

    finally:
        if output_file:
            out_handle.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract chromosome sequences from database"
    )
    parser.add_argument(
        "--organism",
        help="Organism abbreviation",
    )
    parser.add_argument(
        "--feature-types",
        nargs="+",
        default=['chromosome', 'contig'],
        help="Feature types to include (default: chromosome contig)",
    )
    parser.add_argument(
        "--features",
        nargs="+",
        help="Specific feature names to retrieve",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output FASTA file (default: stdout)",
    )
    parser.add_argument(
        "--line-width",
        type=int,
        default=60,
        help="Line width for sequence wrapping (default: 60)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    try:
        with SessionLocal() as session:
            # Get organism if specified
            organism = None
            if args.organism:
                organism = get_organism(session, args.organism)
                logger.info(f"Filtering by organism: {organism.organism_name}")

            # Get sequences
            sequences = get_chromosome_sequences(
                session,
                organism=organism,
                feature_types=args.feature_types,
                feature_names=args.features,
            )

            logger.info(f"Found {len(sequences)} sequences")

            if not sequences:
                logger.warning("No sequences found")
                return

            # Write output
            write_fasta(sequences, args.output, args.line_width)

            if args.output:
                logger.info(f"Written to {args.output}")

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
