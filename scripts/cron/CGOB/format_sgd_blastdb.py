#!/usr/bin/env python3
"""
Format SGD BLAST databases.

This script creates BLAST databases from SGD sequence files for use in
CGOB ortholog analysis.

Based on formatSGDblastdb.pl.

Usage:
    python format_sgd_blastdb.py
    python format_sgd_blastdb.py --debug

Environment Variables:
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
    BLAST_BIN: Path to BLAST binaries
"""

import argparse
import gzip
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

# Load environment variables
load_dotenv()

# Configuration from environment
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
MAKEBLASTDB = os.getenv("MAKEBLASTDB", "makeblastdb")

# CGOB configuration
CGOB_SEQ_DIR = DATA_DIR / "CGOB" / "sequences"
CGOB_BLAST_DIR = DATA_DIR / "CGOB" / "blastdb"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# SGD strain identifier
SGD_STRAIN = "S_cerevisiae"

# Sequence sets to process
SEQUENCE_SETS = [
    {"name": "protein", "type": "prot", "suffix": "_protein.fasta"},
    {"name": "coding", "type": "nucl", "suffix": "_coding.fasta"},
    {"name": "gene", "type": "nucl", "suffix": "_gene.fasta"},
    {"name": "g1000", "type": "nucl", "suffix": "_g1000.fasta"},
]


def decompress_if_needed(fasta_path: Path) -> Path:
    """Decompress gzipped file if needed, return path to uncompressed file."""
    if str(fasta_path).endswith(".gz"):
        uncompressed = fasta_path.with_suffix("")
        if fasta_path.exists():
            with gzip.open(fasta_path, "rb") as f_in:
                with open(uncompressed, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            return uncompressed
        elif uncompressed.exists():
            return uncompressed
        else:
            raise FileNotFoundError(f"Neither {fasta_path} nor {uncompressed} exists")
    return fasta_path


def create_blast_database(
    fasta_file: Path,
    db_name: Path,
    db_type: str = "prot",
) -> str:
    """
    Create a BLAST database from a FASTA file.

    Returns log message.
    """
    try:
        # Remove existing database files
        for suffix in [".phr", ".pin", ".psq", ".nhr", ".nin", ".nsq", ".pdb", ".pot", ".ptf", ".pto"]:
            db_file = db_name.with_suffix(suffix)
            if db_file.exists():
                db_file.unlink()

        cmd = [
            MAKEBLASTDB,
            "-in", str(fasta_file),
            "-dbtype", db_type,
            "-out", str(db_name),
            "-parse_seqids",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            return f"ERROR: makeblastdb failed for {fasta_file}: {result.stderr}\n"

        return f"Created BLAST database: {db_name} (type: {db_type})\n"

    except Exception as e:
        return f"ERROR creating BLAST database for {fasta_file}: {e}\n"


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Format SGD BLAST databases"
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
    log_file = LOG_DIR / "formatSGD.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_text = f"Program {__file__}: Starting {datetime.now()}\n\n"

    # Create BLAST database directory
    sgd_blast_dir = CGOB_BLAST_DIR / SGD_STRAIN
    sgd_blast_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Creating BLAST databases for SGD sequence files...")

    for seq_set in SEQUENCE_SETS:
        name = seq_set["name"]
        db_type = seq_set["type"]
        suffix = seq_set["suffix"]

        # Find source file
        source_file = CGOB_SEQ_DIR / SGD_STRAIN / f"{SGD_STRAIN}{suffix}"
        if not source_file.exists():
            source_file = source_file.with_suffix(suffix + ".gz")

        if not source_file.exists():
            log_text += f"WARNING: Source file not found: {source_file}\n"
            logger.warning(f"Source file not found: {source_file}")
            continue

        # Decompress if needed
        try:
            fasta_file = decompress_if_needed(source_file)
        except FileNotFoundError as e:
            log_text += f"ERROR: {e}\n"
            logger.error(str(e))
            continue

        # Create BLAST database
        db_name = sgd_blast_dir / f"{SGD_STRAIN}_{name}"

        logger.info(f"Creating {name} BLAST database...")
        result = create_blast_database(fasta_file, db_name, db_type)
        log_text += result

        if "ERROR" in result:
            logger.error(result.strip())
        else:
            logger.info(result.strip())

    log_text += f"\nExiting {__file__}: {datetime.now()}\n\n"

    # Write log file
    with open(log_file, "w") as f:
        f.write(log_text)

    logger.info(f"Log information written to {log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
