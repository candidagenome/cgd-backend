#!/usr/bin/env python3
"""
Get genomic sequences for ORFs from database.

This script retrieves genomic DNA sequences for ORFs from the database
and outputs them in FASTA format.

Original Perl: getDB_Ca19_ORFseqs.pl, getDB_Ca20_ORFseqs.pl
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
from cgd.models.models import Feature, FeatLocation, Organism, Seq as SeqModel

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

            match = pattern.search(line)
            if match:
                orfs.append(match.group(1))

    return orfs


def get_orf_sequence(
    session: Session,
    feature_name: str,
    seq_type: str = 'genomic',
) -> tuple[str | None, str | None]:
    """
    Get sequence for an ORF.

    Args:
        session: Database session
        feature_name: Feature name
        seq_type: Sequence type (genomic, protein)

    Returns:
        Tuple of (sequence, feature_no) or (None, None)
    """
    feature = session.query(Feature).filter(
        Feature.feature_name == feature_name
    ).first()

    if not feature:
        return None, None

    seq_record = session.query(SeqModel).filter(
        and_(
            SeqModel.feature_no == feature.feature_no,
            SeqModel.seq_type == seq_type,
            SeqModel.is_seq_current == 'Y',
        )
    ).first()

    if seq_record and seq_record.residues:
        return seq_record.residues, feature.feature_no

    return None, feature.feature_no


def get_orf_sequences_batch(
    session: Session,
    orf_names: list[str] = None,
    organism: Organism = None,
    feature_type: str = 'ORF',
    seq_type: str = 'genomic',
) -> list[SeqRecord]:
    """
    Get sequences for multiple ORFs.

    Args:
        session: Database session
        orf_names: Optional list of specific ORF names
        organism: Optional organism filter
        feature_type: Feature type to retrieve
        seq_type: Sequence type (genomic, protein)

    Returns:
        List of BioPython SeqRecord objects
    """
    records = []

    if orf_names:
        # Get specific ORFs
        for orf_name in orf_names:
            seq, feature_no = get_orf_sequence(session, orf_name, seq_type)
            if seq:
                record = SeqRecord(
                    Seq(seq),
                    id=orf_name,
                    description=f"feature_no={feature_no}",
                )
                records.append(record)
            else:
                logger.warning(f"No sequence found for {orf_name}")
    else:
        # Get all ORFs matching criteria
        query = session.query(Feature).filter(
            Feature.feature_type == feature_type
        )

        if organism:
            query = query.filter(Feature.organism_no == organism.organism_no)

        features = query.all()

        for feat in features:
            seq_record = session.query(SeqModel).filter(
                and_(
                    SeqModel.feature_no == feat.feature_no,
                    SeqModel.seq_type == seq_type,
                    SeqModel.is_seq_current == 'Y',
                )
            ).first()

            if seq_record and seq_record.residues:
                record = SeqRecord(
                    Seq(seq_record.residues),
                    id=feat.feature_name,
                    description=f"feature_no={feat.feature_no}",
                )
                records.append(record)

    return records


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Get genomic sequences for ORFs from database"
    )
    parser.add_argument(
        "--orf-list",
        type=Path,
        help="File with ORF names to retrieve",
    )
    parser.add_argument(
        "--organism",
        help="Organism abbreviation",
    )
    parser.add_argument(
        "--feature-type",
        default='ORF',
        help="Feature type to retrieve (default: ORF)",
    )
    parser.add_argument(
        "--seq-type",
        choices=['genomic', 'protein'],
        default='genomic',
        help="Sequence type to retrieve (default: genomic)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Output FASTA file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Load ORF list if provided
    orf_names = None
    if args.orf_list:
        if not args.orf_list.exists():
            logger.error(f"ORF list file not found: {args.orf_list}")
            sys.exit(1)
        orf_names = load_orf_list(args.orf_list)
        logger.info(f"Loaded {len(orf_names)} ORF names from list")

    try:
        with SessionLocal() as session:
            # Get organism if specified
            organism = None
            if args.organism:
                organism = get_organism(session, args.organism)
                logger.info(f"Filtering by organism: {organism.organism_name}")

            # Get sequences
            records = get_orf_sequences_batch(
                session,
                orf_names=orf_names,
                organism=organism,
                feature_type=args.feature_type,
                seq_type=args.seq_type,
            )

            logger.info(f"Retrieved {len(records)} sequences")

            # Write output
            with open(args.output, 'w') as out_handle:
                SeqIO.write(records, out_handle, 'fasta')

            logger.info(f"Written to {args.output}")

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
