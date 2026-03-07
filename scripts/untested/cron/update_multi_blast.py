#!/usr/bin/env python3
"""
Update multi-genome BLAST datasets.

This script is a wrapper to update multi-genome BLAST datasets including
genomic, coding, and protein sequences.

Based on update_multi.pl by Jon Binkley (March 2010).

Usage:
    python update_multi_blast.py
    python update_multi_blast.py --test

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

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration from environment
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
CONF_DIR = Path(os.getenv("CONF_DIR", "/etc/cgd"))
BIN_DIR = Path(os.getenv("BIN_DIR", "/usr/local/bin"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))

# BLAST configuration
BLAST_BIN = Path(os.getenv("BLAST_BIN", "/usr/local/blast/bin"))
MAKEBLASTDB = os.getenv("MAKEBLASTDB", "makeblastdb")

# Translation table for protein sequences
NUCLEAR_TRANSLATION_TABLE = int(os.getenv("NUCLEAR_TRANSLATION_TABLE", "12"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def read_blast_clade_config(
    config_file: Path,
) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """
    Read blast_clade.conf configuration file.

    Returns:
        - db_tags: List of database tags
        - genomic_for_tag: Mapping of tag to genomic file
        - coding_for_tag: Mapping of tag to coding file
    """
    db_tags: list[str] = []
    genomic_for_tag: dict[str, str] = {}
    coding_for_tag: dict[str, str] = {}

    section = ""

    with open(config_file) as f:
        for line in f:
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith("#"):
                section = ""
                continue

            if "TAG_TO_DESC" in line:
                section = "description"
                continue
            elif "TAG_TO_GENOMIC" in line:
                section = "genomic"
                continue
            elif "TAG_TO_CODING" in line:
                section = "coding"
                continue

            if section == "description":
                parts = line.split("\t")
                tag = parts[0].strip()
                if tag:
                    db_tags.append(tag)

            elif section == "genomic":
                parts = line.split("\t")
                if len(parts) >= 2:
                    tag = parts[0].strip()
                    file_path = parts[1].strip()
                    genomic_for_tag[tag] = file_path

            elif section == "coding":
                parts = line.split("\t")
                if len(parts) >= 2:
                    tag = parts[0].strip()
                    file_path = parts[1].strip()
                    coding_for_tag[tag] = file_path

    return db_tags, genomic_for_tag, coding_for_tag


def process_genomic_sequences(
    input_file: Path, output_file: Path, tag: str
) -> bool:
    """Process genomic sequences for a tag."""
    try:
        # Determine if input is gzipped
        if str(input_file).endswith(".gz"):
            opener = gzip.open
        else:
            opener = open

        with opener(input_file, "rt") as f_in, open(output_file, "w") as f_out:
            for line in f_in:
                if line.startswith(">"):
                    # Modify header to include tag
                    header = line[1:].strip()
                    f_out.write(f">{tag}|{header}\n")
                else:
                    f_out.write(line)

        return True

    except Exception as e:
        logger.error(f"Error processing genomic sequences: {e}")
        return False


def process_coding_sequences(
    input_file: Path, output_file: Path, tag: str
) -> bool:
    """Process coding sequences for a tag."""
    try:
        # Determine if input is gzipped
        if str(input_file).endswith(".gz"):
            opener = gzip.open
        else:
            opener = open

        with opener(input_file, "rt") as f_in, open(output_file, "w") as f_out:
            for line in f_in:
                if line.startswith(">"):
                    # Modify header to include tag
                    header = line[1:].strip()
                    f_out.write(f">{tag}|{header}\n")
                else:
                    f_out.write(line)

        return True

    except Exception as e:
        logger.error(f"Error processing coding sequences: {e}")
        return False


def translate_to_protein(
    input_file: Path, output_file: Path, tag: str, trans_table: int
) -> bool:
    """Translate coding sequences to protein."""
    try:
        from Bio import SeqIO
        from Bio.Seq import Seq

        # Determine if input is gzipped
        if str(input_file).endswith(".gz"):
            opener = gzip.open
            mode = "rt"
        else:
            opener = open
            mode = "r"

        with opener(input_file, mode) as f_in, open(output_file, "w") as f_out:
            for record in SeqIO.parse(f_in, "fasta"):
                # Translate sequence
                seq = record.seq
                # Ensure length is multiple of 3
                remainder = len(seq) % 3
                if remainder:
                    seq = seq[:-remainder]

                try:
                    protein = seq.translate(table=trans_table, to_stop=True)
                except Exception:
                    # Try without stop codon handling
                    protein = seq.translate(table=trans_table)

                if len(protein) > 0:
                    header = f"{tag}|{record.id}"
                    f_out.write(f">{header}\n")
                    # Write protein in 60-char lines
                    for i in range(0, len(protein), 60):
                        f_out.write(str(protein[i:i+60]) + "\n")

        return True

    except ImportError:
        logger.warning("BioPython not available, skipping protein translation")
        return False
    except Exception as e:
        logger.error(f"Error translating to protein: {e}")
        return False


def create_blast_database(
    fasta_file: Path, db_name: Path, db_type: str = "nucl"
) -> bool:
    """Create a BLAST database from a FASTA file."""
    try:
        cmd = [
            MAKEBLASTDB,
            "-in", str(fasta_file),
            "-dbtype", db_type,
            "-out", str(db_name),
            "-parse_seqids",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"makeblastdb failed: {result.stderr}")
            return False

        logger.info(f"Created BLAST database: {db_name}")
        return True

    except Exception as e:
        logger.error(f"Error creating BLAST database: {e}")
        return False


def run_dataset_generation(
    dataset_type: str,
    db_tags: list[str],
    genomic_for_tag: dict[str, str],
    coding_for_tag: dict[str, str],
    dataset_dir: Path,
    seq_dir: Path,
) -> str:
    """Generate datasets for a given type (GENOMIC, CODING, or PROTEIN)."""
    log_message = f"\n#########\nGenerating {dataset_type} datasets\n\n"
    log_message += f"Start time: {datetime.now()}\n"

    if dataset_type == "GENOMIC":
        prefix = "genomic_"
        source_files = genomic_for_tag
        db_type = "nucl"
    elif dataset_type == "CODING":
        prefix = "orf_coding_"
        source_files = coding_for_tag
        db_type = "nucl"
    else:  # PROTEIN
        prefix = "orf_trans_all_"
        source_files = coding_for_tag
        db_type = "prot"

    for tag in db_tags:
        input_file = source_files.get(tag)
        if not input_file:
            logger.warning(f"No input file for tag {tag}")
            continue

        input_path = Path(input_file)
        if not input_path.exists():
            logger.warning(f"Input file not found: {input_path}")
            continue

        output_file = seq_dir / f"{prefix}{tag}.fasta"
        blast_db = dataset_dir / f"{prefix}{tag}"

        # Remove existing output files
        if output_file.exists():
            output_file.unlink()

        output_gz = output_file.with_suffix(".fasta.gz")
        if output_gz.exists():
            output_gz.unlink()

        # Process sequences
        success = False
        if dataset_type == "GENOMIC":
            success = process_genomic_sequences(input_path, output_file, tag)
        elif dataset_type == "CODING":
            success = process_coding_sequences(input_path, output_file, tag)
        else:  # PROTEIN
            success = translate_to_protein(
                input_path, output_file, tag, NUCLEAR_TRANSLATION_TABLE
            )

        if not success:
            log_message += f"  Failed to process {tag}\n"
            continue

        # Compress output
        with open(output_file, "rb") as f_in:
            with gzip.open(output_gz, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Create BLAST database from gzipped file
        # Note: makeblastdb can read gzipped files
        if create_blast_database(output_gz, blast_db, db_type):
            log_message += f"  Created {dataset_type} dataset for {tag}\n"
        else:
            log_message += f"  Failed to create BLAST DB for {tag}\n"

        # Clean up uncompressed file
        if output_file.exists():
            output_file.unlink()

    log_message += f"\nEnd time: {datetime.now()}\n"
    return log_message


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update multi-genome BLAST datasets"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode - only log, don't send email",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to blast_clade.conf file",
    )

    args = parser.parse_args()

    # Determine config file path
    config_file = args.config or (CONF_DIR / "blast_clade.conf")

    if not config_file.exists():
        logger.error(f"Config file not found: {config_file}")
        return 1

    # Set up directories
    dataset_dir = DATA_DIR / "blast_datasets"
    seq_dir = dataset_dir / "sequence"
    seq_dir.mkdir(parents=True, exist_ok=True)

    log_file = LOG_DIR / "update_multi.log"

    logger.info("Starting multi-genome BLAST dataset update")

    log_message = "#########\nMulti-Genome BLAST Update\n\n"
    log_message += f"Start time: {datetime.now()}\n"

    try:
        # Read configuration
        db_tags, genomic_for_tag, coding_for_tag = read_blast_clade_config(
            config_file
        )
        logger.info(f"Found {len(db_tags)} database tags in config")

        # Generate datasets
        log_message += run_dataset_generation(
            "GENOMIC", db_tags, genomic_for_tag, coding_for_tag,
            dataset_dir, seq_dir
        )

        log_message += run_dataset_generation(
            "CODING", db_tags, genomic_for_tag, coding_for_tag,
            dataset_dir, seq_dir
        )

        log_message += run_dataset_generation(
            "PROTEIN", db_tags, genomic_for_tag, coding_for_tag,
            dataset_dir, seq_dir
        )

        log_message += "\n#########\nMulti-Genome BLAST update complete\n\n"
        log_message += f"End time: {datetime.now()}\n"

        # Write log file
        with open(log_file, "w") as f:
            f.write(log_message)

        logger.info(f"Log written to {log_file}")
        logger.info("Update complete")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
