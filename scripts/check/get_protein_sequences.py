#!/usr/bin/env python3
"""
Get protein sequences for ORFs from database.

This script retrieves protein sequences for ORFs from the database,
either stored directly or translated from genomic sequences.

Original Perl: getDB_Ca19ProtSeq_forList.pl, getDB_Ca20ProtSeq_forList.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Feature, Organism, Seq as SeqModel

load_dotenv()

logger = logging.getLogger(__name__)

# Alternative Yeast Nuclear genetic code
GENETIC_CODE = 12


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


def load_orf_list(input_file: Path) -> list[str]:
    """
    Load ORF names from file.

    Args:
        input_file: File with ORF names

    Returns:
        List of ORF names
    """
    orfs = []
    pattern = re.compile(r'(orf\d+\.\d+\.?\d*)', re.IGNORECASE)

    with open(input_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Just extract the ORF name
            match = pattern.search(line)
            if match:
                orfs.append(match.group(1))
            else:
                # Use first column if no pattern match
                parts = line.split('\t')
                if parts:
                    orfs.append(parts[0].strip())

    return orfs


def get_protein_sequence(
    session: Session,
    feature_name: str,
    genetic_code: int = GENETIC_CODE,
    translate_if_missing: bool = True,
) -> tuple[str | None, str]:
    """
    Get protein sequence for an ORF.

    Args:
        session: Database session
        feature_name: Feature name
        genetic_code: Genetic code for translation
        translate_if_missing: Translate genomic if protein not stored

    Returns:
        Tuple of (sequence, source) where source is 'stored' or 'translated'
    """
    feature = session.query(Feature).filter(
        Feature.feature_name == feature_name
    ).first()

    if not feature:
        return None, 'not_found'

    # Try protein sequence first
    protein_seq = session.query(SeqModel).filter(
        and_(
            SeqModel.feature_no == feature.feature_no,
            SeqModel.seq_type == 'protein',
            SeqModel.is_seq_current == 'Y',
        )
    ).first()

    if protein_seq and protein_seq.residues:
        return protein_seq.residues, 'stored'

    # Try translating genomic
    if translate_if_missing:
        genomic_seq = session.query(SeqModel).filter(
            and_(
                SeqModel.feature_no == feature.feature_no,
                SeqModel.seq_type == 'genomic',
                SeqModel.is_seq_current == 'Y',
            )
        ).first()

        if genomic_seq and genomic_seq.residues:
            try:
                dna = Seq(genomic_seq.residues)
                protein = str(dna.translate(table=genetic_code))
                return protein, 'translated'
            except Exception as e:
                logger.warning(f"Translation error for {feature_name}: {e}")

    return None, 'no_sequence'


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Get protein sequences for ORFs from database"
    )
    parser.add_argument(
        "orf_list",
        type=Path,
        help="File with ORF names",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Output FASTA file",
    )
    parser.add_argument(
        "--suffix",
        default='',
        help="Suffix to add to sequence IDs (e.g., '-A19')",
    )
    parser.add_argument(
        "--genetic-code",
        type=int,
        default=GENETIC_CODE,
        help=f"Genetic code for translation (default: {GENETIC_CODE})",
    )
    parser.add_argument(
        "--stored-only",
        action="store_true",
        help="Only use stored protein sequences, don't translate",
    )
    parser.add_argument(
        "--replace-stop",
        help="Character to replace stop codon (*) with (e.g., '.')",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input
    if not args.orf_list.exists():
        logger.error(f"ORF list file not found: {args.orf_list}")
        sys.exit(1)

    # Load ORF list
    orf_names = load_orf_list(args.orf_list)
    logger.info(f"Loaded {len(orf_names)} ORF names")

    stats = {
        'found': 0,
        'stored': 0,
        'translated': 0,
        'not_found': 0,
        'no_sequence': 0,
    }

    try:
        with SessionLocal() as session:
            records = []

            for orf_name in orf_names:
                seq, source = get_protein_sequence(
                    session,
                    orf_name,
                    args.genetic_code,
                    not args.stored_only,
                )

                if seq:
                    stats['found'] += 1
                    stats[source] += 1

                    # Replace stop codon if requested
                    if args.replace_stop:
                        seq = seq.replace('*', args.replace_stop)

                    record = SeqRecord(
                        Seq(seq),
                        id=f"{orf_name}{args.suffix}",
                        description=f"source={source}",
                    )
                    records.append(record)
                else:
                    stats[source] += 1
                    logger.warning(f"No sequence for {orf_name}: {source}")

            # Write output
            with open(args.output, 'w') as out_handle:
                SeqIO.write(records, out_handle, 'fasta')

            logger.info(f"Written {len(records)} sequences to {args.output}")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Total ORFs: {len(orf_names)}")
            logger.info(f"  Found: {stats['found']}")
            logger.info(f"    Stored protein: {stats['stored']}")
            logger.info(f"    Translated: {stats['translated']}")
            logger.info(f"  Not found: {stats['not_found']}")
            logger.info(f"  No sequence: {stats['no_sequence']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
