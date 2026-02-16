#!/usr/bin/env python3
"""
Dump chromosome sequences from database.

This script dumps chromosome sequences from the database in FASTA format.
The dumped sequences can be used for sequence checking and validation.

Original Perl: dumpChromosomeSequence.pl (Anand Sethuraman, Nov 2003)
Converted to Python: 2024

Usage:
    python dump_chromosome_sequence.py --chromosome 1 --output chr1.fasta
    python dump_chromosome_sequence.py --all --output-dir sequences/
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")

# Chromosome number to Roman numeral mapping
CHR_TO_ROMAN = {
    1: 'I', 2: 'II', 3: 'III', 4: 'IV', 5: 'V',
    6: 'VI', 7: 'VII', 8: 'VIII', 9: 'IX', 10: 'X',
    11: 'XI', 12: 'XII', 13: 'XIII', 14: 'XIV', 15: 'XV',
    16: 'XVI', 17: 'Mt',
}


def setup_logging(verbose: bool = False, log_file: Path = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def get_chromosome_info(session: Session, chromosome: str) -> dict:
    """
    Get chromosome information from database.

    Args:
        session: Database session
        chromosome: Chromosome identifier (number or name)

    Returns:
        Dict with chromosome info
    """
    result = session.execute(
        text(f"""
            SELECT f.feature_no, f.feature_name, f.start_coord, f.stop_coord
            FROM {DB_SCHEMA}.feature f
            WHERE f.feature_name = :chr_name
            AND f.feature_type = 'chromosome'
        """),
        {"chr_name": str(chromosome)}
    ).fetchone()

    if not result:
        return None

    return {
        'feature_no': result[0],
        'feature_name': result[1],
        'start_coord': result[2],
        'stop_coord': result[3],
    }


def get_chromosome_sequence(session: Session, feature_name: str) -> str:
    """
    Get chromosome sequence from database.

    Args:
        session: Database session
        feature_name: Chromosome feature name

    Returns:
        Sequence string
    """
    result = session.execute(
        text(f"""
            SELECT s.residues
            FROM {DB_SCHEMA}.sequence s
            JOIN {DB_SCHEMA}.feat_seq fs ON s.sequence_no = fs.sequence_no
            JOIN {DB_SCHEMA}.feature f ON fs.feature_no = f.feature_no
            WHERE f.feature_name = :feat_name
            AND s.seq_type = 'genomic'
        """),
        {"feat_name": feature_name}
    ).fetchone()

    if result:
        return result[0]
    return None


def dump_chromosome(
    session: Session,
    chromosome: str,
    output_file: Path,
    format: str = 'fasta',
) -> bool:
    """
    Dump chromosome sequence to file.

    Args:
        session: Database session
        chromosome: Chromosome identifier
        output_file: Output file path
        format: Output format (fasta or gcg)

    Returns:
        True if successful
    """
    # Get chromosome info
    chr_info = get_chromosome_info(session, chromosome)
    if not chr_info:
        logger.error(f"Chromosome {chromosome} not found in database")
        return False

    # Get sequence
    sequence = get_chromosome_sequence(session, chr_info['feature_name'])
    if not sequence:
        logger.error(f"Sequence not found for chromosome {chromosome}")
        return False

    # Create description
    chr_name = chr_info['feature_name']
    start = chr_info['start_coord']
    stop = chr_info['stop_coord']

    if chromosome in CHR_TO_ROMAN:
        roman = CHR_TO_ROMAN[chromosome]
        description = f"Chromosome {roman} from {start} to {stop}"
    else:
        description = f"Chromosome {chr_name} from {start} to {stop}"

    # Create SeqRecord
    record = SeqRecord(
        Seq(sequence),
        id=f"chr{chromosome}",
        description=description,
    )

    # Write output
    with open(output_file, 'w') as f:
        SeqIO.write(record, f, format)

    logger.info(f"Wrote chromosome {chromosome} to {output_file}")
    logger.info(f"  Length: {len(sequence)} bp")

    return True


def dump_all_chromosomes(
    session: Session,
    output_dir: Path,
    format: str = 'fasta',
) -> dict:
    """
    Dump all chromosome sequences.

    Args:
        session: Database session
        output_dir: Output directory
        format: Output format

    Returns:
        Stats dict
    """
    stats = {
        'success': 0,
        'failed': 0,
    }

    output_dir.mkdir(parents=True, exist_ok=True)

    # Get all chromosomes
    chromosomes = list(range(1, 18))  # 1-16 + Mt(17)

    for chrom in chromosomes:
        ext = '.fsa' if format == 'fasta' else '.gcg'
        output_file = output_dir / f"chr{chrom}{ext}"

        if dump_chromosome(session, chrom, output_file, format):
            stats['success'] += 1
        else:
            stats['failed'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump chromosome sequences from database"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--chromosome",
        type=str,
        help="Chromosome to dump (1-17 or 2-micron)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Dump all chromosomes",
    )

    parser.add_argument(
        "--output",
        type=Path,
        help="Output file (for single chromosome)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path('.'),
        help="Output directory (for --all mode)",
    )
    parser.add_argument(
        "--format",
        choices=['fasta', 'gcg'],
        default='fasta',
        help="Output format (default: fasta)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose, args.log_file)

    logger.info(f"Started at {datetime.now()}")

    try:
        with SessionLocal() as session:
            if args.all:
                stats = dump_all_chromosomes(
                    session, args.output_dir, args.format
                )
                logger.info(f"Dumped {stats['success']} chromosomes, "
                           f"{stats['failed']} failed")
            else:
                output_file = args.output
                if not output_file:
                    ext = '.fsa' if args.format == 'fasta' else '.gcg'
                    output_file = Path(f"chr{args.chromosome}{ext}")

                if dump_chromosome(session, args.chromosome,
                                  output_file, args.format):
                    logger.info("Success")
                else:
                    logger.error("Failed")
                    sys.exit(1)

    except Exception as e:
        logger.exception(f"Error: {e}")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
