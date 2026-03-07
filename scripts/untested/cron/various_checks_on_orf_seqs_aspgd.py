#!/usr/bin/env python3
"""
Run various checks on ORF sequences for AspGD strains.

This script reads protein and coding sequence files and checks for:
- ORFs with ambiguous sequence (check coding)
- ORFs without Start codon (check coding)
- ORFs with partial terminal Stop codon (check coding)
- ORFs without terminal Stop codon (check coding)
- ORFs with internal Stop codon(s) (check protein)
- ORFs with multiple terminal Stop codons (check protein)

Based on variousChecksOnOrfSeqs_AspGD.pl.

Usage:
    python various_checks_on_orf_seqs_aspgd.py <strain_abbrev>
    python various_checks_on_orf_seqs_aspgd.py A_nidulans_FGSC_A4

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Directory for log files
    DATA_DIR: Directory for data files
"""

import argparse
import gzip
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Stop codons
STOP_CODONS = {"TAA", "TAG", "TGA"}


def get_orfs_for_strain(session, strain_abbrev: str) -> set[str]:
    """Get all ORF feature names for a strain that have sequence locations."""
    query = text(f"""
        SELECT f.feature_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        WHERE f.feature_type = 'ORF'
        AND o.organism_abbrev = :abbrev
        AND fp.property_value != 'Deleted'
        AND fl.seq_no IS NOT NULL
    """)

    orfs = set()
    for row in session.execute(query, {"abbrev": strain_abbrev}).fetchall():
        orfs.add(row[0])

    return orfs


def read_fasta_sequences(filepath: Path) -> dict[str, str]:
    """
    Read sequences from a FASTA file (supports gzipped files).

    Returns dict mapping sequence ID to sequence.
    """
    sequences = {}

    if str(filepath).endswith(".gz"):
        opener = gzip.open
        mode = "rt"
    else:
        opener = open
        mode = "r"

    if not filepath.exists():
        logger.error(f"File not found: {filepath}")
        return sequences

    current_id = None
    current_seq = []

    with opener(filepath, mode) as f:
        for line in f:
            line = line.strip()
            if line.startswith(">"):
                if current_id:
                    sequences[current_id] = "".join(current_seq)
                current_id = line[1:].split()[0]
                current_seq = []
            else:
                current_seq.append(line)

        if current_id:
            sequences[current_id] = "".join(current_seq)

    return sequences


def check_coding_sequences(
    coding_seqs: dict[str, str],
    orfs: set[str],
) -> tuple[list[str], list[str], list[str], list[str]]:
    """
    Check coding sequences for issues.

    Returns lists of ORFs with:
    - ambiguous sequences
    - missing start codon
    - partial stop codon
    - missing stop codon
    """
    ambiguous = []
    no_start = []
    partial_stop = []
    no_stop = []

    for orf, seq in coding_seqs.items():
        if orf not in orfs:
            continue

        seq = seq.upper()
        length = len(seq)

        # Check for ambiguous bases (anything not ACGT)
        if not re.match(r"^[ACGT]+$", seq):
            ambiguous.append(orf)

        # Check for start codon
        if not seq.startswith("ATG"):
            no_start.append(orf)

        # Check for stop codon
        if length % 3 == 2:
            # Partial codon at end
            if seq.endswith("TG") or seq.endswith("TA"):
                partial_stop.append(orf)
            else:
                no_stop.append(orf)
        elif length % 3 == 1:
            # Single base at end
            if seq.endswith("T"):
                partial_stop.append(orf)
            else:
                no_stop.append(orf)
        else:
            # Complete codon at end
            terminal = seq[-3:]
            if terminal not in STOP_CODONS:
                no_stop.append(orf)

    return ambiguous, no_start, partial_stop, no_stop


def check_protein_sequences(
    protein_seqs: dict[str, str],
    orfs: set[str],
) -> tuple[list[str], list[str]]:
    """
    Check protein sequences for issues.

    Returns lists of ORFs with:
    - internal stop codons
    - multiple terminal stop codons
    """
    internal_stop = []
    multi_stop = []

    for orf, seq in protein_seqs.items():
        if orf not in orfs:
            continue

        # Count terminal stops
        terminal_stops = 0
        while seq.endswith("*"):
            terminal_stops += 1
            seq = seq[:-1]

        if terminal_stops > 1:
            multi_stop.append(orf)

        # Check for internal stops
        if "*" in seq:
            internal_stop.append(orf)

    return internal_stop, multi_stop


def get_sequence_files(strain_abbrev: str) -> tuple[Path, Path]:
    """Get paths to coding and protein sequence files for a strain."""
    # Try to find sequence files in expected locations
    seq_dir = DATA_DIR / "sequences" / strain_abbrev

    # Look for compressed or uncompressed files
    coding_patterns = [
        f"{strain_abbrev}_coding.fasta.gz",
        f"{strain_abbrev}_coding.fasta",
        "coding.fasta.gz",
        "coding.fasta",
    ]

    protein_patterns = [
        f"{strain_abbrev}_protein.fasta.gz",
        f"{strain_abbrev}_protein.fasta",
        "protein.fasta.gz",
        "protein.fasta",
    ]

    coding_file = None
    protein_file = None

    for pattern in coding_patterns:
        path = seq_dir / pattern
        if path.exists():
            coding_file = path
            break

    for pattern in protein_patterns:
        path = seq_dir / pattern
        if path.exists():
            protein_file = path
            break

    return coding_file, protein_file


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run various checks on ORF sequences for AspGD strains"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., A_nidulans_FGSC_A4)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )
    parser.add_argument(
        "--coding-file",
        type=Path,
        help="Path to coding sequence file (overrides default)",
    )
    parser.add_argument(
        "--protein-file",
        type=Path,
        help="Path to protein sequence file (overrides default)",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    strain_abbrev = args.strain_abbrev

    # Set up log file
    log_file = LOG_DIR / f"{strain_abbrev}_OrfCheck.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info(f"Starting ORF checks for {strain_abbrev} at {datetime.now()}")

    # Get sequence files
    if args.coding_file:
        coding_file = args.coding_file
    else:
        coding_file, _ = get_sequence_files(strain_abbrev)

    if args.protein_file:
        protein_file = args.protein_file
    else:
        _, protein_file = get_sequence_files(strain_abbrev)

    if not coding_file or not coding_file.exists():
        logger.error(f"Coding sequence file not found for {strain_abbrev}")
        return 1

    if not protein_file or not protein_file.exists():
        logger.error(f"Protein sequence file not found for {strain_abbrev}")
        return 1

    logger.info(f"Coding file: {coding_file}")
    logger.info(f"Protein file: {protein_file}")

    try:
        with SessionLocal() as session:
            # Get ORFs for strain
            logger.info("Collecting ORFs from database...")
            orfs = get_orfs_for_strain(session, strain_abbrev)
            logger.info(f"Found {len(orfs)} ORFs")

    except Exception as e:
        logger.error(f"Database error: {e}")
        return 1

    # Read sequences
    logger.info("Reading coding sequences...")
    coding_seqs = read_fasta_sequences(coding_file)
    logger.info(f"Read {len(coding_seqs)} coding sequences")

    logger.info("Reading protein sequences...")
    protein_seqs = read_fasta_sequences(protein_file)
    logger.info(f"Read {len(protein_seqs)} protein sequences")

    # Run checks
    logger.info("Running sequence checks...")

    ambiguous, no_start, partial_stop, no_stop = check_coding_sequences(
        coding_seqs, orfs
    )
    internal_stop, multi_stop = check_protein_sequences(protein_seqs, orfs)

    # Report summary
    report = f"""
Report from checks run on {strain_abbrev} ORFs:

   - ORFs with ambiguous sequence            => {len(ambiguous)}
   - ORFs without Start codon                => {len(no_start)}
   - ORFs with partial terminal Stop codon   => {len(partial_stop)}
   - ORFs without terminal Stop codon        => {len(no_stop)}
   - ORFs with internal Stop codon(s)        => {len(internal_stop)}
   - ORFs with multiple terminal Stop codons => {len(multi_stop)}

"""

    print(report)
    logger.info(report)

    # Log details
    log_details = [
        ("ORFs with ambiguous sequence", ambiguous),
        ("ORFs without Start codon", no_start),
        ("ORFs with partial terminal Stop codon", partial_stop),
        ("ORFs without terminal Stop codon", no_stop),
        ("ORFs with internal Stop codon(s)", internal_stop),
        ("ORFs with multiple terminal Stop codons", multi_stop),
    ]

    for title, orf_list in log_details:
        logger.info("####################################")
        logger.info(f"{title}:\n")
        for orf in sorted(orf_list):
            logger.info(orf)
        logger.info("")

    logger.info(f"For ORF identities, see {log_file}")
    logger.info(f"Completed at {datetime.now()}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
