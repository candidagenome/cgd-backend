#!/usr/bin/env python3
"""
Find internal stop codons in ORF sequences.

This script checks ORF sequences for internal stop codons (stop codons
before the end of the coding sequence), which may indicate annotation
errors or pseudogenes.

Original Perl: findInternalStopCodons.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from Bio import SeqIO
from Bio.Seq import Seq
from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, FeatLocation, Organism, Seq as SeqModel

load_dotenv()

logger = logging.getLogger(__name__)

# Genetic code table (standard yeast/fungal)
GENETIC_CODE = 12  # Alternative Yeast Nuclear


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


def translate_sequence(dna_seq: str, genetic_code: int = GENETIC_CODE) -> str:
    """
    Translate DNA sequence to protein.

    Args:
        dna_seq: DNA sequence
        genetic_code: NCBI genetic code table number

    Returns:
        Protein sequence
    """
    seq = Seq(dna_seq)
    return str(seq.translate(table=genetic_code))


def check_internal_stops(protein_seq: str) -> list[int]:
    """
    Check for internal stop codons in protein sequence.

    Args:
        protein_seq: Protein sequence

    Returns:
        List of positions with internal stop codons
    """
    # Remove terminal stop if present
    if protein_seq.endswith('*'):
        protein_seq = protein_seq[:-1]

    # Find all stop positions
    stops = []
    for i, aa in enumerate(protein_seq):
        if aa == '*':
            stops.append(i + 1)  # 1-based position

    return stops


def find_internal_stops_in_db(
    session: Session,
    organism: Organism,
    feature_type: str = 'ORF',
) -> list[dict]:
    """
    Find internal stop codons in database ORF sequences.

    Args:
        session: Database session
        organism: Organism object
        feature_type: Feature type to check

    Returns:
        List of features with internal stops
    """
    results = []

    # Get all features of specified type for organism
    features = session.query(Feature).filter(
        and_(
            Feature.organism_no == organism.organism_no,
            Feature.feature_type == feature_type,
        )
    ).all()

    logger.info(f"Checking {len(features)} {feature_type} features")

    for feat in features:
        # Get current genomic sequence
        seq_record = session.query(SeqModel).filter(
            and_(
                SeqModel.feature_no == feat.feature_no,
                SeqModel.seq_type == 'genomic',
                SeqModel.is_seq_current == 'Y',
            )
        ).first()

        if not seq_record or not seq_record.residues:
            continue

        dna_seq = seq_record.residues

        # Translate
        try:
            protein_seq = translate_sequence(dna_seq)
        except Exception as e:
            logger.warning(f"Translation error for {feat.feature_name}: {e}")
            continue

        # Check for internal stops
        internal_stops = check_internal_stops(protein_seq)

        if internal_stops:
            # Get location info
            location = session.query(FeatLocation).filter(
                and_(
                    FeatLocation.feature_no == feat.feature_no,
                    FeatLocation.is_loc_current == 'Y',
                )
            ).first()

            results.append({
                'feature_name': feat.feature_name,
                'feature_no': feat.feature_no,
                'chromosome': None,  # Would need to resolve from location
                'start': location.start_coord if location else None,
                'stop': location.stop_coord if location else None,
                'strand': location.strand if location else None,
                'stop_positions': internal_stops,
                'protein_length': len(protein_seq),
                'dna_length': len(dna_seq),
            })

    return results


def find_internal_stops_in_file(input_file: Path, file_format: str = 'fasta') -> list[dict]:
    """
    Find internal stop codons in sequences from file.

    Args:
        input_file: Path to input file
        file_format: File format (fasta, genbank, embl)

    Returns:
        List of sequences with internal stops
    """
    results = []

    for record in SeqIO.parse(str(input_file), file_format):
        dna_seq = str(record.seq)

        # Skip if too short
        if len(dna_seq) < 3:
            continue

        # Translate
        try:
            protein_seq = translate_sequence(dna_seq)
        except Exception as e:
            logger.warning(f"Translation error for {record.id}: {e}")
            continue

        # Check for internal stops
        internal_stops = check_internal_stops(protein_seq)

        if internal_stops:
            results.append({
                'feature_name': record.id,
                'description': record.description,
                'stop_positions': internal_stops,
                'protein_length': len(protein_seq),
                'dna_length': len(dna_seq),
            })

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Find internal stop codons in ORF sequences"
    )
    parser.add_argument(
        "--organism",
        help="Organism abbreviation (for database mode)",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        help="Input sequence file (for file mode)",
    )
    parser.add_argument(
        "--format",
        choices=['fasta', 'genbank', 'embl'],
        default='fasta',
        help="Input file format (default: fasta)",
    )
    parser.add_argument(
        "--feature-type",
        default='ORF',
        help="Feature type to check (default: ORF)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Validate arguments
    if not args.organism and not args.input_file:
        print("Error: Must specify either --organism or --input-file", file=sys.stderr)
        sys.exit(1)

    logger.info(f"Started at {datetime.now()}")

    results = []

    if args.input_file:
        # File mode
        if not args.input_file.exists():
            print(f"Error: File not found: {args.input_file}", file=sys.stderr)
            sys.exit(1)

        logger.info(f"Checking sequences in {args.input_file}")
        results = find_internal_stops_in_file(args.input_file, args.format)

    else:
        # Database mode
        try:
            with SessionLocal() as session:
                organism = get_organism(session, args.organism)
                logger.info(f"Checking features for {organism.organism_name}")
                results = find_internal_stops_in_db(
                    session, organism, args.feature_type
                )
        except Exception as e:
            logger.error(f"Database error: {e}")
            sys.exit(1)

    # Output results
    out_handle = open(args.output, 'w') if args.output else sys.stdout

    try:
        out_handle.write(f"# Features with internal stop codons: {len(results)}\n")
        out_handle.write("# Feature\tStopPositions\tProteinLength\tDNALength\n")

        for item in results:
            stops = ','.join(str(p) for p in item['stop_positions'])
            out_handle.write(
                f"{item['feature_name']}\t{stops}\t"
                f"{item['protein_length']}\t{item['dna_length']}\n"
            )

    finally:
        if args.output:
            out_handle.close()

    logger.info(f"Found {len(results)} sequences with internal stop codons")
    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
