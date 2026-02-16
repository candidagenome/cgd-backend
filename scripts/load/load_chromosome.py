#!/usr/bin/env python3
"""
Load chromosome sequences into the database.

This script loads chromosome sequences from FASTA files into either
the CHROMOSOME or CHROMOSOME_A21 table, depending on the assembly version.

Input: Directory containing chromosome FASTA files
File naming convention: Ca[version]Chr[chr_id].fa or Ca[version]Chr[chr_id].seq

Original Perl: loadChromosome.pl, loadChromosome_A21.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Chromosome, ChromosomeA21

load_dotenv()

logger = logging.getLogger(__name__)


def setup_logging(log_file: Path = None, verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def parse_fasta_file(filepath: Path) -> tuple[str, str]:
    """
    Parse a FASTA file and return the sequence.

    Args:
        filepath: Path to FASTA file

    Returns:
        Tuple of (chromosome_id, sequence)
    """
    try:
        from Bio import SeqIO
    except ImportError:
        logger.error("BioPython is required for this script.")
        logger.error("Install with: pip install biopython")
        sys.exit(1)

    # Extract chromosome ID from filename
    # Pattern: Ca[version]Chr[chr_id].fa or .seq
    filename = filepath.name
    match = re.match(r"Ca\d+Chr(.+)\.(fa|seq)", filename, re.IGNORECASE)
    if not match:
        logger.warning(f"Cannot parse chromosome ID from filename: {filename}")
        return None, None

    chr_id = match.group(1)

    # Read sequence
    record = next(SeqIO.parse(filepath, "fasta"))
    sequence = str(record.seq).upper()

    return chr_id, sequence


def get_or_create_chromosome(
    session: Session,
    chr_id: str,
    sequence: str,
    created_by: str,
    use_a21: bool = False,
) -> dict:
    """
    Get existing chromosome or create new one.

    Args:
        session: Database session
        chr_id: Chromosome identifier
        sequence: Chromosome sequence
        created_by: User creating the record
        use_a21: If True, use ChromosomeA21 table

    Returns:
        Dictionary with operation result
    """
    model_class = ChromosomeA21 if use_a21 else Chromosome
    table_name = "CHROMOSOME_A21" if use_a21 else "CHROMOSOME"

    existing = session.query(model_class).filter(
        model_class.chromosome == chr_id
    ).first()

    if existing:
        # Update existing chromosome
        logger.info(f"Updating chromosome {chr_id} in {table_name}")
        existing.physical_length = len(sequence)
        existing.chr_seq = sequence
        return {"action": "updated", "chromosome": chr_id}
    else:
        # Create new chromosome
        logger.info(f"Creating chromosome {chr_id} in {table_name}")
        new_chr = model_class(
            chromosome=chr_id,
            physical_length=len(sequence),
            chr_seq=sequence,
            created_by=created_by[:12],
        )
        session.add(new_chr)
        return {"action": "created", "chromosome": chr_id}


def load_chromosomes(
    session: Session,
    input_dir: Path,
    created_by: str,
    use_a21: bool = False,
    file_pattern: str = None,
) -> dict:
    """
    Load chromosome sequences from a directory.

    Args:
        session: Database session
        input_dir: Directory containing FASTA files
        created_by: User creating the records
        use_a21: If True, use ChromosomeA21 table
        file_pattern: Optional glob pattern for files

    Returns:
        Dictionary with statistics
    """
    stats = {
        "files_processed": 0,
        "chromosomes_created": 0,
        "chromosomes_updated": 0,
        "errors": [],
    }

    # Default pattern based on assembly version
    if file_pattern is None:
        if use_a21:
            file_pattern = "Ca21Chr*.seq"
        else:
            file_pattern = "Ca20Chr*.fa"

    files = list(input_dir.glob(file_pattern))
    if not files:
        # Try alternative patterns
        alt_patterns = ["Ca*Chr*.fa", "Ca*Chr*.seq", "*.fa", "*.fasta"]
        for pattern in alt_patterns:
            files = list(input_dir.glob(pattern))
            if files:
                break

    if not files:
        logger.warning(f"No FASTA files found in {input_dir}")
        return stats

    logger.info(f"Found {len(files)} chromosome files")

    for filepath in sorted(files):
        try:
            chr_id, sequence = parse_fasta_file(filepath)
            if chr_id is None:
                continue

            stats["files_processed"] += 1

            result = get_or_create_chromosome(
                session, chr_id, sequence, created_by, use_a21
            )

            if result["action"] == "created":
                stats["chromosomes_created"] += 1
            else:
                stats["chromosomes_updated"] += 1

        except Exception as e:
            error_msg = f"Error processing {filepath}: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load chromosome sequences into the database"
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing chromosome FASTA files",
    )
    parser.add_argument(
        "--assembly",
        choices=["20", "21"],
        default="20",
        help="Assembly version (20 or 21, default: 20)",
    )
    parser.add_argument(
        "--file-pattern",
        help="Glob pattern for chromosome files (e.g., 'Ca21Chr*.seq')",
    )
    parser.add_argument(
        "--created-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user name for created_by field",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Path to log file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse files but don't modify database",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    # Validate input directory
    if not args.input_dir.is_dir():
        logger.error(f"Input directory not found: {args.input_dir}")
        sys.exit(1)

    use_a21 = args.assembly == "21"
    table_name = "CHROMOSOME_A21" if use_a21 else "CHROMOSOME"

    logger.info(f"Input directory: {args.input_dir}")
    logger.info(f"Assembly version: {args.assembly}")
    logger.info(f"Target table: {table_name}")
    logger.info(f"Created by: {args.created_by}")

    if args.dry_run:
        logger.info("DRY RUN - scanning files only")
        # Just scan and report
        try:
            from Bio import SeqIO
        except ImportError:
            logger.error("BioPython is required. Install with: pip install biopython")
            sys.exit(1)

        pattern = args.file_pattern or ("Ca21Chr*.seq" if use_a21 else "Ca20Chr*.fa")
        files = list(args.input_dir.glob(pattern))
        if not files:
            for p in ["Ca*Chr*.fa", "Ca*Chr*.seq", "*.fa"]:
                files = list(args.input_dir.glob(p))
                if files:
                    break

        for f in sorted(files):
            chr_id, seq = parse_fasta_file(f)
            if chr_id:
                logger.info(f"  {f.name}: chr {chr_id}, length {len(seq):,}")
        return

    try:
        with SessionLocal() as session:
            stats = load_chromosomes(
                session,
                args.input_dir,
                args.created_by,
                use_a21,
                args.file_pattern,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Files processed: {stats['files_processed']}")
            logger.info(f"  Chromosomes created: {stats['chromosomes_created']}")
            logger.info(f"  Chromosomes updated: {stats['chromosomes_updated']}")
            if stats["errors"]:
                logger.error(f"  Errors: {len(stats['errors'])}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading chromosomes: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
