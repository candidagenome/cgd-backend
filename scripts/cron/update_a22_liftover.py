#!/usr/bin/env python3
"""
Update Assembly 22 liftOver chain files.

This script creates liftOver chain files between different assemblies
of C. albicans genome (Assembly 22 haplotypes A/B, Assembly 21, mtDNA).

Based on UpdateA22liftOver.pl.

Usage:
    python update_a22_liftover.py
    python update_a22_liftover.py --debug

Environment Variables:
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
    BIN_DIR: Directory for binary scripts
"""

import argparse
import gzip
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration from environment
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
BIN_DIR = Path(os.getenv("BIN_DIR", "/usr/local/bin"))

# LiftOver directories
LIFTOVER_DIR = DATA_DIR / "liftOver"
FASTA_DIR = LIFTOVER_DIR / "fasta"

# External scripts
BLAT_SCRIPT = BIN_DIR / "liftOver" / "SameSpeciesBlatSetup.sh"
CHAIN_SCRIPT = BIN_DIR / "liftOver" / "SameSpeciesChainNet.sh"
GUNZIP = "/usr/bin/gunzip"

# Strain configuration
STRAIN_ABBREV = "C_albicans_SC5314"

# Assembly tags
A22A_TAG = "Assembly22_hapA"
A22B_TAG = "Assembly22_hapB"
A22MT_TAG = "Assembly22_mtDNA"
A21_TAG = "Assembly21"
A19MT_TAG = "Ca19-mtDNA"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def read_fasta(filepath: Path) -> dict[str, str]:
    """Read sequences from FASTA file (supports gzipped files)."""
    sequences = {}

    if str(filepath).endswith(".gz"):
        opener = gzip.open
        mode = "rt"
    else:
        opener = open
        mode = "r"

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


def write_fasta(filepath: Path, sequences: dict[str, str]) -> None:
    """Write sequences to FASTA file."""
    with open(filepath, "w") as f:
        for seq_id, sequence in sequences.items():
            f.write(f">{seq_id}\n")
            # Write sequence in 60-character lines
            for i in range(0, len(sequence), 60):
                f.write(sequence[i:i + 60] + "\n")


def get_chromosome_from_id(seq_id: str) -> str:
    """Extract chromosome identifier from sequence ID."""
    # Pattern: Ca22chr{chr}_{strain}
    match = re.match(rf"Ca22chr([^_]+)_{STRAIN_ABBREV}$", seq_id)
    if match:
        return match.group(1)
    return ""


def make_chain(assembly1: str, assembly2: str) -> bool:
    """Create chain file between two assemblies using BLAT and chain scripts."""
    output = LIFTOVER_DIR / f"{assembly1}_To_{assembly2}.over.chain.gz"

    logger.info(f"Creating chain file {output}...")

    cmd1 = [str(BLAT_SCRIPT), assembly1, assembly2]
    cmd2 = [str(CHAIN_SCRIPT), assembly1, assembly2]

    # Run BLAT setup
    result = subprocess.run(cmd1, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error running BLAT script: {result.stderr}")
        return False

    # Run chain/net script
    result = subprocess.run(cmd2, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error running chain script: {result.stderr}")
        return False

    # Decompress output if it exists
    if output.exists():
        result = subprocess.run([GUNZIP, str(output)], capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Error decompressing {output}: {result.stderr}")
            return False

    return True


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update Assembly 22 liftOver chain files"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Set up log file
    log_file = LOG_DIR / "update_a22_liftover.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info(f"Starting liftOver update at {datetime.now()}")

    # Create output directories
    FASTA_DIR.mkdir(parents=True, exist_ok=True)

    # Source FASTA files (adjust paths as needed)
    seq_dir = DATA_DIR / "sequences" / STRAIN_ABBREV
    a22_source = seq_dir / f"{STRAIN_ABBREV}_A22_genomic.fasta.gz"
    a21_source = seq_dir / f"{STRAIN_ABBREV}_A21_genomic.fasta.gz"
    a19mt_source = seq_dir / f"{STRAIN_ABBREV}_A19_mito.fasta.gz"

    # Check for alternative locations
    for source in [a22_source, a21_source, a19mt_source]:
        if not source.exists():
            # Try without .gz
            alt_source = source.with_suffix("")
            if alt_source.exists():
                continue
            logger.warning(f"Source file not found: {source}")

    try:
        # Read and split A22 sequences by haplotype
        logger.info("Processing Assembly 22 sequences...")

        if a22_source.exists():
            a22_sequences = read_fasta(a22_source)
        else:
            a22_source_plain = a22_source.with_suffix("")
            if a22_source_plain.exists():
                a22_sequences = read_fasta(a22_source_plain)
            else:
                logger.error(f"A22 source file not found")
                return 1

        a22a_seqs = {}
        a22b_seqs = {}
        a22mt_seqs = {}

        for seq_id, sequence in a22_sequences.items():
            chr_id = get_chromosome_from_id(seq_id)

            if chr_id == "M":
                a22mt_seqs[seq_id] = sequence
            elif chr_id.endswith("A"):
                a22a_seqs[seq_id] = sequence
            elif chr_id.endswith("B"):
                a22b_seqs[seq_id] = sequence
            else:
                logger.warning(f"Unrecognized sequence identifier: {seq_id}")

        # Write haplotype FASTA files
        write_fasta(FASTA_DIR / f"{A22A_TAG}.fasta", a22a_seqs)
        write_fasta(FASTA_DIR / f"{A22B_TAG}.fasta", a22b_seqs)
        write_fasta(FASTA_DIR / f"{A22MT_TAG}.fasta", a22mt_seqs)

        logger.info(f"Wrote {len(a22a_seqs)} A22 haplotype A sequences")
        logger.info(f"Wrote {len(a22b_seqs)} A22 haplotype B sequences")
        logger.info(f"Wrote {len(a22mt_seqs)} A22 mtDNA sequences")

        # Read and write A21 sequences
        logger.info("Processing Assembly 21 sequences...")

        if a21_source.exists():
            a21_sequences = read_fasta(a21_source)
        else:
            a21_source_plain = a21_source.with_suffix("")
            if a21_source_plain.exists():
                a21_sequences = read_fasta(a21_source_plain)
            else:
                a21_sequences = {}
                logger.warning("A21 source file not found")

        if a21_sequences:
            write_fasta(FASTA_DIR / f"{A21_TAG}.fasta", a21_sequences)
            logger.info(f"Wrote {len(a21_sequences)} A21 sequences")

        # Read and write A19 mtDNA sequences
        logger.info("Processing A19 mtDNA sequences...")

        if a19mt_source.exists():
            a19mt_sequences = read_fasta(a19mt_source)
        else:
            a19mt_source_plain = a19mt_source.with_suffix("")
            if a19mt_source_plain.exists():
                a19mt_sequences = read_fasta(a19mt_source_plain)
            else:
                a19mt_sequences = {}
                logger.warning("A19 mtDNA source file not found")

        if a19mt_sequences:
            write_fasta(FASTA_DIR / f"{A19MT_TAG}.fasta", a19mt_sequences)
            logger.info(f"Wrote {len(a19mt_sequences)} A19 mtDNA sequences")

        # Create chain files
        chain_pairs = [
            (A22A_TAG, A22B_TAG),
            (A22B_TAG, A22A_TAG),
            (A21_TAG, A22A_TAG),
            (A22A_TAG, A21_TAG),
            (A22B_TAG, A21_TAG),
            (A21_TAG, A22B_TAG),
            (A22MT_TAG, A19MT_TAG),
            (A19MT_TAG, A22MT_TAG),
        ]

        for asm1, asm2 in chain_pairs:
            if not make_chain(asm1, asm2):
                logger.error(f"Failed to create chain: {asm1} -> {asm2}")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    logger.info(f"LiftOver update completed at {datetime.now()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
