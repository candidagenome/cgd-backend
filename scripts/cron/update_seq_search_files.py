#!/usr/bin/env python3
"""
Update sequence search files for PatMatch and BLAST.

This script creates searchable sequence files:
1. Plain-text FASTA files for PatMatch search
2. BLAST-formatted databases using makeblastdb

It processes gzipped weekly sequence download files and:
- Removes descriptions from FASTA headers
- Removes '*' characters terminating protein sequences
- Separates mitochondrial sequences if configured
- Creates seq.count file for PatMatch

Validation checks before copying to final location:
---------------------------------------------------------------------------
- Writes to temp directory first
- Validates minimum sequence count per dataset
- Checks sequence count change < 10% vs existing file
- Validates BLAST database files are created correctly
- Only copies to final location if validation passes
- Sends Slack notifications on success or failure

Based on updateSeqSearchFiles.pl by Jon Binkley (November 2010)

Usage:
    python update_seq_search_files.py --strain C_albicans_SC5314
    python update_seq_search_files.py --all
    python update_seq_search_files.py --strain C_albicans_SC5314 --force

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DOWNLOAD_DIR: Directory for sequence download files
    LOG_DIR: Directory for log files
    BLAST_DB_DIR: Directory for BLAST databases
    FASTA_DIR: Directory for FASTA files
    BLAST_FORMAT_CMD: Path to makeblastdb command
    SLACK_WEBHOOK_URL: Slack webhook URL for notifications
    ENV_STATE: Environment state (dev/prod) for Slack labels
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data" / "download")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))
CGD_DATA_DIR = Path(os.getenv("CGD_DATA_DIR", "/data"))
BLAST_DIR = Path(os.getenv("BLAST_DB_DIR", str(CGD_DATA_DIR / "blast_datasets")))
FASTA_DIR = Path(os.getenv("FASTA_DIR", str(CGD_DATA_DIR / "fasta")))
BLAST_FORMAT_CMD = os.getenv("BLAST_FORMAT_CMD", "makeblastdb")

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_SEQUENCES = {
    "orf_coding": 100,
    "orf_trans_all": 100,
    "orf_genomic": 100,
    "1000_up": 100,
    "other_features_genomic": 10,
    "genomic": 5,  # Some assemblies have as few as 7-8 chromosomes
    "mito": 1,  # Mito files may have just 1 sequence (mtDNA)
}
MAX_SEQUENCE_CHANGE_PERCENT = 10.0

# BLAST database file extensions
BLAST_PROTEIN_EXTENSIONS = ["pdb", "phr", "pin", "pog", "pos", "pot", "psq", "ptf", "pto"]
BLAST_NUCLEOTIDE_EXTENSIONS = ["ndb", "nhr", "nin", "nog", "nos", "not", "nsq", "ntf", "nto"]

# Strain configurations
STRAIN_ABBREVS = [
    "C_albicans_SC5314",
    "C_auris_B8441",
    "C_dubliniensis_CD36",
    "C_glabrata_CBS138",
    "C_parapsilosis_CDC317",
    "C_tropicalis",
]

# Assemblies to process per strain (strains not listed use default/current only)
STRAIN_ASSEMBLIES = {
    "C_albicans_SC5314": ["A22", "A21", "A19"],
}

# Datasets that may legitimately be empty for some organisms.
# For example, C_tropicalis has no tRNA/rRNA/ncRNA/pseudogene annotations, so the
# upstream dump (dump_sequence.py, MIN count 0) produces empty other_features files.
# Skip such empty source files instead of failing makeblastdb ("No sequences added").
OPTIONAL_EMPTY_DATASETS = {
    "other_features_genomic",
    "other_features_genomic_1000",
    "other_features_no_introns",
    "other_features_plus_intergenic",
}

# Datasets to skip for specific assemblies (to match legacy file set)
ASSEMBLY_SKIP_DATASETS = {
    "A19": ["default_coding", "default_genomic", "default_protein", "not_feature"],
    "A21": ["default_coding", "default_genomic", "default_protein"],
}

# Assemblies that need mito FASTA file generation
ASSEMBLY_MITO_FASTA = ["A22", "A21", "A19"]

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def send_slack_message(message: str, is_error: bool = False) -> None:
    """Send a message to Slack webhook."""
    if not SLACK_WEBHOOK_URL:
        return

    emoji = ":x:" if is_error else ":white_check_mark:"
    env_prefix = f"[{ENV_STATE.upper()}] " if ENV_STATE != "prod" else ""

    payload = {"text": f"{emoji} {env_prefix}Seq Search Files: {message}"}

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Slack notification failed: {response.status}")
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")


def count_sequences_in_file(file_path: Path) -> int:
    """Count sequences in a FASTA file (gzipped or plain)."""
    if not file_path.exists():
        return 0

    count = 0
    if file_path.suffix == ".gz":
        with gzip.open(file_path, "rt") as f:
            for line in f:
                if line.startswith(">"):
                    count += 1
    else:
        with open(file_path) as f:
            for line in f:
                if line.startswith(">"):
                    count += 1
    return count


def validate_fasta_file(
    new_file: Path,
    existing_file: Path | None,
    dataset: str,
    strain_abbrev: str,
) -> tuple[bool, str]:
    """
    Validate a generated FASTA file.

    Args:
        dataset: Dataset display name (may include assembly suffix like 'orf_coding_A22')

    Returns:
        Tuple of (success, message)
    """
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_sequences_in_file(new_file)
    # Strip assembly suffix (A## or _current) for MIN_SEQUENCES lookup
    base_dataset = re.sub(r"_(A\d+|current)$", "", dataset)
    min_count = MIN_SEQUENCES.get(base_dataset, 10)

    if new_count < min_count:
        return False, (
            f"Too few sequences for {strain_abbrev} {dataset}: {new_count} "
            f"(minimum: {min_count})"
        )

    if existing_file and existing_file.exists():
        existing_count = count_sequences_in_file(existing_file)
        if existing_count > 0:
            diff = new_count - existing_count
            change_pct = abs(diff) / existing_count * 100
            if change_pct > MAX_SEQUENCE_CHANGE_PERCENT:
                return False, (
                    f"Sequence count changed too much for {strain_abbrev} {dataset}: "
                    f"{existing_count} -> {new_count} ({change_pct:.1f}% change)"
                )
            # Show comparison in success message
            sign = "+" if diff > 0 else ""
            return True, (
                f"{dataset}: {existing_count} -> {new_count} "
                f"({sign}{diff}, {change_pct:.1f}% change)"
            )

    # No existing file to compare against
    return True, f"{dataset}: {new_count} sequences (new file)"


def validate_blast_db(
    temp_db_path: Path,
    existing_db_path: Path | None,
    dataset: str,
    seq_type: str,
    strain_abbrev: str,
) -> tuple[bool, str]:
    """
    Validate a generated BLAST database.

    Checks that all expected database files exist and have reasonable sizes.

    Returns:
        Tuple of (success, message)
    """
    extensions = (
        BLAST_PROTEIN_EXTENSIONS if seq_type == "protein" else BLAST_NUCLEOTIDE_EXTENSIONS
    )

    # Check that at least the core files exist
    core_extensions = ["phr", "pin", "psq"] if seq_type == "protein" else ["nhr", "nin", "nsq"]
    missing_files = []

    for ext in core_extensions:
        db_file = temp_db_path.with_suffix(f".{ext}")
        if not db_file.exists():
            missing_files.append(ext)

    if missing_files:
        return False, (
            f"Missing BLAST database files for {strain_abbrev} {dataset}: "
            f"{', '.join(missing_files)}"
        )

    # Check file sizes are reasonable (not empty)
    for ext in core_extensions:
        db_file = temp_db_path.with_suffix(f".{ext}")
        if db_file.stat().st_size == 0:
            return False, (
                f"Empty BLAST database file for {strain_abbrev} {dataset}: {ext}"
            )

    # Compare with existing database if available
    new_size = sum(
        temp_db_path.with_suffix(f".{ext}").stat().st_size
        for ext in core_extensions
        if temp_db_path.with_suffix(f".{ext}").exists()
    )

    if existing_db_path:
        existing_core = existing_db_path.with_suffix(f".{core_extensions[0]}")
        if existing_core.exists():
            existing_size = sum(
                existing_db_path.with_suffix(f".{ext}").stat().st_size
                for ext in core_extensions
                if existing_db_path.with_suffix(f".{ext}").exists()
            )
            if existing_size > 0:
                diff = new_size - existing_size
                change_pct = abs(diff) / existing_size * 100
                if change_pct > MAX_SEQUENCE_CHANGE_PERCENT * 2:  # Allow more variance for DB size
                    return False, (
                        f"BLAST database size changed too much for {strain_abbrev} {dataset}: "
                        f"{existing_size} -> {new_size} bytes ({change_pct:.1f}% change)"
                    )
                # Show comparison in success message
                sign = "+" if diff > 0 else ""
                new_kb = new_size / 1024
                existing_kb = existing_size / 1024
                return True, (
                    f"{dataset}: {existing_kb:.1f}KB -> {new_kb:.1f}KB "
                    f"({sign}{diff/1024:.1f}KB, {change_pct:.1f}% change)"
                )

    # No existing database to compare against
    new_kb = new_size / 1024
    return True, f"{dataset}: {new_kb:.1f}KB (new database)"


def copy_blast_db(src_path: Path, dst_path: Path, seq_type: str) -> None:
    """Copy all BLAST database files from src to dst."""
    extensions = (
        BLAST_PROTEIN_EXTENSIONS if seq_type == "protein" else BLAST_NUCLEOTIDE_EXTENSIONS
    )

    dst_path.parent.mkdir(parents=True, exist_ok=True)

    for ext in extensions:
        src_file = src_path.with_suffix(f".{ext}")
        if src_file.exists():
            dst_file = dst_path.with_suffix(f".{ext}")
            shutil.copy2(src_file, dst_file)


def archive_old_files(file_path: Path, archive_dir: Path) -> None:
    """Archive old files before replacement."""
    if not file_path.exists():
        return

    archive_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
    archive_path = archive_dir / archive_name
    shutil.copy2(file_path, archive_path)


class SequenceProcessor:
    """Process sequence files for PatMatch and BLAST."""

    def __init__(self, session, strain_abbrev: str, force: bool = False):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.force = force
        self.organism_no = None
        self.mito_features: set[str] = set()
        self.seq_counts: dict[str, int] = {}
        self.log_messages: list[str] = []
        self.errors: list[str] = []
        self.validation_failures: list[str] = []

        # Final output directories
        # FASTA files go in strain subdirectories
        self.fasta_dir = FASTA_DIR / strain_abbrev
        # BLAST databases go directly in base directory (legacy behavior)
        self.blast_dir = BLAST_DIR
        self.download_dir = DOWNLOAD_DIR / "sequence" / strain_abbrev

        # Temp directories for validation before copy
        self.temp_dir = Path(tempfile.mkdtemp(prefix=f"seq_search_{strain_abbrev}_"))
        self.temp_fasta_dir = self.temp_dir / "fasta"
        self.temp_blast_dir = self.temp_dir / "blast"
        self.temp_fasta_dir.mkdir(parents=True, exist_ok=True)
        self.temp_blast_dir.mkdir(parents=True, exist_ok=True)

        # Archive directory for old files
        self.archive_dir = FASTA_DIR / strain_abbrev / "archive"
        self.blast_archive_dir = BLAST_DIR / "archive"

        # Track files to copy after validation
        self.pending_fasta_copies: list[tuple[Path, Path, str]] = []  # (temp, final, dataset)
        self.pending_blast_copies: list[tuple[Path, Path, str, str]] = []  # (temp, final, dataset, seq_type)

    def cleanup_temp(self) -> None:
        """Remove temp directory."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            self.log(f"Cleaned up temp directory: {self.temp_dir}")

    def log(self, message: str) -> None:
        """Log a message to both logger and internal list."""
        logger.info(message)
        self.log_messages.append(message)

    def log_error(self, message: str) -> None:
        """Log an error message."""
        logger.error(message)
        self.errors.append(message)

    def get_organism_no(self) -> int | None:
        """Get organism number from database."""
        query = text(f"""
            SELECT organism_no
            FROM {DB_SCHEMA}.organism
            WHERE organism_abbrev = :strain_abbrev
        """)

        result = self.session.execute(
            query, {"strain_abbrev": self.strain_abbrev}
        ).first()

        if result:
            return result[0]
        return None

    def identify_mito_features(self, mito_feature_names: list[str]) -> None:
        """Identify mitochondrial ORFs for the organism."""
        if not mito_feature_names or not mito_feature_names[0]:
            return

        # Add the mito feature names themselves
        for name in mito_feature_names:
            if name:
                self.mito_features.add(name)

        # Get child ORFs of mito features
        placeholders = ", ".join([f":name{i}" for i in range(len(mito_feature_names))])
        params = {f"name{i}": name for i, name in enumerate(mito_feature_names) if name}
        params["organism_no"] = self.organism_no

        query = text(f"""
            SELECT f.feature_name
            FROM {DB_SCHEMA}.feat_relationship fr
            JOIN {DB_SCHEMA}.feature f ON fr.child_feature_no = f.feature_no
            WHERE f.feature_type = 'ORF'
            AND fr.parent_feature_no IN (
                SELECT feature_no FROM {DB_SCHEMA}.feature
                WHERE feature_name IN ({placeholders})
            )
            AND f.organism_no = :organism_no
        """)

        result = self.session.execute(query, params)
        for row in result:
            self.mito_features.add(row[0])

        self.log(f"Identified {len(self.mito_features)} mitochondrial features")

    def parse_fasta(self, input_file: Path) -> list[dict]:
        """
        Parse a FASTA file (gzipped or plain).

        Returns:
            List of dictionaries with 'id', 'desc', 'seq' keys
        """
        sequences = []
        current_id = None
        current_desc = ""
        current_seq = []

        open_func = gzip.open if str(input_file).endswith(".gz") else open
        mode = "rt" if str(input_file).endswith(".gz") else "r"

        with open_func(input_file, mode) as f:
            for line in f:
                line = line.rstrip("\n\r")
                if line.startswith(">"):
                    # Save previous sequence
                    if current_id is not None:
                        sequences.append({
                            "id": current_id,
                            "desc": current_desc,
                            "seq": "".join(current_seq),
                        })

                    # Parse new header
                    header = line[1:].strip()
                    parts = header.split(None, 1)
                    current_id = parts[0] if parts else ""
                    current_desc = parts[1] if len(parts) > 1 else ""
                    current_seq = []
                else:
                    current_seq.append(line)

        # Save last sequence
        if current_id is not None:
            sequences.append({
                "id": current_id,
                "desc": current_desc,
                "seq": "".join(current_seq),
            })

        return sequences

    def clean_header(self, seq_id: str, desc: str, dataset: str) -> tuple[str, str]:
        """
        Clean FASTA header for PatMatch.

        Args:
            seq_id: Sequence identifier
            desc: Sequence description
            dataset: Dataset name

        Returns:
            Tuple of (cleaned_id, cleaned_desc)
        """
        # For intergenic files, merge description into identifier
        if "not_feature" in dataset.lower():
            combined = f"{seq_id}_{desc.replace(' ', '_')}"
            return combined, ""

        # Keep only coordinate information if present
        match = re.search(r"(COORDS:[^:]+:\d+-\d+[CW])", desc)
        if match:
            return seq_id, match.group(1)

        return seq_id, ""

    def remove_stop_codon(self, sequence: str) -> str:
        """Remove terminal stop codon (*) from protein sequence."""
        while sequence.endswith("*"):
            sequence = sequence[:-1]
        return sequence

    def reformat_fasta(
        self,
        dataset: str,
        input_file: Path,
        output_file: Path,
        seq_type: str = "protein",
    ) -> int:
        """
        Reformat FASTA file for PatMatch searching.

        - Removes descriptions from headers
        - Removes '*' characters from protein sequences
        - Counts sequences

        Writes to temp directory; files will be validated and copied later.

        Args:
            dataset: Dataset name
            input_file: Input gzipped FASTA file
            output_file: Output plain FASTA file (in temp directory)
            seq_type: Sequence type ('protein' or 'dna')

        Returns:
            Number of sequences processed
        """
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        sequences = self.parse_fasta(input_file)
        count = 0

        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            for seq in sequences:
                seq_id, desc = self.clean_header(seq["id"], seq["desc"], dataset)
                sequence = seq["seq"]

                # Remove stop codons from proteins
                if seq_type == "protein":
                    sequence = self.remove_stop_codon(sequence)

                # Write sequence
                if desc:
                    f.write(f">{seq_id} {desc}\n")
                else:
                    f.write(f">{seq_id}\n")
                f.write(f"{sequence}\n")
                count += 1

        self.log(f"Created {output_file} with {count} sequences")

        return count

    def format_blast_db(
        self,
        input_file: Path,
        output_db: Path,
        dataset: str,
        seq_type: str = "protein",
    ) -> bool:
        """
        Format a BLAST database using makeblastdb.

        Writes database files directly to output_db path (in temp directory).
        Files will be validated and copied to final location later.

        Args:
            input_file: Input gzipped FASTA file
            output_db: Output database path (without extension)
            dataset: Dataset name for title
            seq_type: Sequence type ('protein' or 'dna')

        Returns:
            True on success, False on failure
        """
        if not input_file.exists():
            self.log_error(f"Input file not found: {input_file}")
            return False

        output_db.parent.mkdir(parents=True, exist_ok=True)

        # Determine database type
        db_type = "prot" if seq_type == "protein" else "nucl"

        # Build command: decompress if gzipped, then pipe to makeblastdb
        if str(input_file).endswith(".gz"):
            cmd = (
                f"zcat {input_file} | {BLAST_FORMAT_CMD} "
                f"-dbtype {db_type} -input_type fasta "
                f"-parse_seqids -title {dataset} -out {output_db}"
            )
        else:
            cmd = (
                f"cat {input_file} | {BLAST_FORMAT_CMD} "
                f"-dbtype {db_type} -input_type fasta "
                f"-parse_seqids -title {dataset} -out {output_db}"
            )

        self.log(f"Executing: {cmd}")

        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                # If duplicate seq_ids, retry without -parse_seqids
                if "Duplicate seq_ids" in result.stderr:
                    self.log("Duplicate seq_ids detected, retrying without -parse_seqids")
                    if str(input_file).endswith(".gz"):
                        cmd_retry = (
                            f"zcat {input_file} | {BLAST_FORMAT_CMD} "
                            f"-dbtype {db_type} -input_type fasta "
                            f"-title {dataset} -out {output_db}"
                        )
                    else:
                        cmd_retry = (
                            f"cat {input_file} | {BLAST_FORMAT_CMD} "
                            f"-dbtype {db_type} -input_type fasta "
                            f"-title {dataset} -out {output_db}"
                        )
                    self.log(f"Executing: {cmd_retry}")
                    result = subprocess.run(
                        cmd_retry,
                        shell=True,
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode != 0:
                        self.log_error(f"makeblastdb failed: {result.stderr}")
                        return False
                else:
                    self.log_error(f"makeblastdb failed: {result.stderr}")
                    return False

            self.log(f"Created BLAST database: {output_db}")
            return True

        except Exception as e:
            self.log_error(f"Error formatting BLAST database: {e}")
            return False

    def separate_mito_sequences(
        self,
        input_file: Path,
        nuclear_file: Path,
        mito_file: Path,
    ) -> tuple[int, int]:
        """
        Separate mitochondrial sequences from nuclear sequences.

        Args:
            input_file: Input gzipped FASTA file
            nuclear_file: Output file for nuclear sequences (gzipped)
            mito_file: Output file for mito sequences (gzipped)

        Returns:
            Tuple of (nuclear_count, mito_count)
        """
        sequences = self.parse_fasta(input_file)

        nuclear_count = 0
        mito_count = 0

        nuclear_file.parent.mkdir(parents=True, exist_ok=True)
        mito_file.parent.mkdir(parents=True, exist_ok=True)

        with gzip.open(nuclear_file, "wt") as nuc_f, gzip.open(mito_file, "wt") as mito_f:
            for seq in sequences:
                if seq["id"] in self.mito_features:
                    if seq["desc"]:
                        mito_f.write(f">{seq['id']} {seq['desc']}\n")
                    else:
                        mito_f.write(f">{seq['id']}\n")
                    mito_f.write(f"{seq['seq']}\n")
                    mito_count += 1
                else:
                    if seq["desc"]:
                        nuc_f.write(f">{seq['id']} {seq['desc']}\n")
                    else:
                        nuc_f.write(f">{seq['id']}\n")
                    nuc_f.write(f"{seq['seq']}\n")
                    nuclear_count += 1

        return nuclear_count, mito_count

    def create_mito_fasta(self, source_file: Path, output_file: Path) -> int:
        """
        Create a FASTA file containing only mitochondrial sequences.

        Args:
            source_file: Source gzipped FASTA file
            output_file: Output FASTA file for mito sequences

        Returns:
            Number of mito sequences written
        """
        sequences = self.parse_fasta(source_file)
        mito_count = 0

        with open(output_file, "w") as f:
            for seq in sequences:
                if seq["id"] in self.mito_features:
                    # Write sequence on single line (PatMatch format)
                    if seq["desc"]:
                        f.write(f">{seq['id']} {seq['desc']}\n")
                    else:
                        f.write(f">{seq['id']}\n")
                    f.write(f"{seq['seq']}\n")
                    mito_count += 1

        return mito_count

    def process_dataset(
        self,
        dataset: str,
        source_file: Path,
        fasta_file: Path | None,
        blast_db: Path | None,
        seq_type: str,
        assembly: str = "",
    ) -> bool:
        """
        Process a single dataset for PatMatch and/or BLAST.

        Writes to temp directory first, then validates before final copy.

        Args:
            dataset: Dataset name (base name like 'orf_coding')
            source_file: Source gzipped FASTA file
            assembly: Assembly identifier for display (e.g., 'A22', 'A19')
            fasta_file: Final output PatMatch FASTA file (or None)
            blast_db: Final output BLAST database path (or None)
            seq_type: Sequence type ('protein' or 'dna')

        Returns:
            True on success, False on failure
        """
        # Create display name that includes assembly for validation messages
        display_name = f"{dataset}_{assembly}" if assembly else dataset

        self.log(f"Processing dataset: {dataset}")

        if not source_file.exists():
            self.log_error(f"Source file not found: {source_file}")
            return False

        # Some organisms legitimately have no "other features" (tRNA/rRNA/ncRNA/etc.),
        # so the upstream dump produces an empty file. Skip these gracefully instead of
        # failing the whole strain on an empty makeblastdb input ("No sequences added").
        if dataset in OPTIONAL_EMPTY_DATASETS and count_sequences_in_file(source_file) == 0:
            self.log(
                f"Skipping {dataset} for {self.strain_abbrev}: source file is empty "
                f"(organism has no such features): {source_file}"
            )
            return True

        success = True

        # Check if we need to separate mito sequences
        needs_mito_separation = (
            self.mito_features
            and any(kw in dataset for kw in ["genomic", "orf_genomic", "orf_coding"])
        )

        # Process for BLAST - write to temp directory
        if blast_db:
            # Temp path for BLAST database
            temp_blast_db = self.temp_blast_dir / blast_db.name

            if needs_mito_separation:
                # Create temp files for separated sequences
                nuclear_file = self.temp_blast_dir / f"{dataset}_nuclear.fasta.gz"
                mito_file = self.temp_blast_dir / f"{dataset}_mito.fasta.gz"

                nuc_count, mito_count = self.separate_mito_sequences(
                    source_file, nuclear_file, mito_file
                )

                # Format nuclear BLAST database to temp
                if not self.format_blast_db(nuclear_file, temp_blast_db, dataset, seq_type):
                    success = False
                else:
                    # Track for validation and copy (use display_name for validation messages)
                    self.pending_blast_copies.append((temp_blast_db, blast_db, display_name, seq_type))

                # Format mito BLAST database if there are mito sequences
                if mito_count > 0:
                    mito_blast_db = blast_db.parent / f"mito_{blast_db.name}"
                    temp_mito_blast_db = self.temp_blast_dir / f"mito_{blast_db.name}"
                    if not self.format_blast_db(mito_file, temp_mito_blast_db, f"mito_{dataset}", seq_type):
                        success = False
                    else:
                        self.pending_blast_copies.append((temp_mito_blast_db, mito_blast_db, f"mito_{display_name}", seq_type))

                # Cleanup temp fasta files
                if nuclear_file.exists():
                    nuclear_file.unlink()
                if mito_file.exists():
                    mito_file.unlink()
            else:
                if not self.format_blast_db(source_file, temp_blast_db, dataset, seq_type):
                    success = False
                else:
                    # Track for validation and copy (use display_name for validation messages)
                    self.pending_blast_copies.append((temp_blast_db, blast_db, display_name, seq_type))

        # Process for PatMatch - write to temp directory
        if fasta_file:
            temp_fasta_file = self.temp_fasta_dir / fasta_file.name
            try:
                count = self.reformat_fasta(dataset, source_file, temp_fasta_file, seq_type)
                self.seq_counts[display_name] = count
                # Track for validation and copy (use display_name for validation messages)
                self.pending_fasta_copies.append((temp_fasta_file, fasta_file, display_name))

                # Generate mito FASTA for specific assemblies (e.g., A19)
                if (dataset == "genomic" and assembly in ASSEMBLY_MITO_FASTA
                        and self.mito_features):
                    # Use legacy naming: mito_{strain}_{assembly}.fasta (not mito_genomic_...)
                    mito_fasta_name = f"mito_{self.strain_abbrev}_{assembly}.fasta"
                    mito_fasta_file = fasta_file.parent / mito_fasta_name
                    temp_mito_fasta = self.temp_fasta_dir / mito_fasta_name
                    mito_count = self.create_mito_fasta(source_file, temp_mito_fasta)
                    if mito_count > 0:
                        mito_display = f"mito_{assembly}"
                        self.seq_counts[mito_display] = mito_count
                        self.pending_fasta_copies.append((temp_mito_fasta, mito_fasta_file, mito_display))
                        self.log(f"Created mito FASTA with {mito_count} sequences")

            except Exception as e:
                self.log_error(f"Error reformatting {dataset}: {e}")
                success = False

        if success:
            self.log(f"Successfully processed dataset: {dataset}")

        return success

    def validate_all_files(self) -> bool:
        """
        Validate all pending files before copying.

        Returns:
            True if all validations pass (or force mode), False otherwise
        """
        all_valid = True

        # Validate FASTA files
        for temp_file, final_file, dataset in self.pending_fasta_copies:
            valid, message = validate_fasta_file(
                temp_file, final_file, dataset, self.strain_abbrev
            )
            if valid:
                self.log(f"FASTA validation: {message}")
            else:
                self.log_error(f"FASTA validation failed: {message}")
                self.validation_failures.append(message)
                all_valid = False

        # Validate BLAST databases
        for temp_db, final_db, dataset, seq_type in self.pending_blast_copies:
            valid, message = validate_blast_db(
                temp_db, final_db, dataset, seq_type, self.strain_abbrev
            )
            if valid:
                self.log(f"BLAST validation: {message}")
            else:
                self.log_error(f"BLAST validation failed: {message}")
                self.validation_failures.append(message)
                all_valid = False

        return all_valid

    def copy_validated_files(self) -> bool:
        """
        Copy all validated files from temp to final locations.

        Archives old files before replacement.

        Returns:
            True on success, False on failure
        """
        success = True

        # Copy FASTA files
        for temp_file, final_file, dataset in self.pending_fasta_copies:
            try:
                # Archive old file if exists
                if final_file.exists():
                    archive_old_files(final_file, self.archive_dir)

                # Create parent directory and copy
                final_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(temp_file, final_file)
                self.log(f"Copied {temp_file.name} to {final_file}")
            except Exception as e:
                self.log_error(f"Failed to copy {temp_file.name}: {e}")
                success = False

        # Copy BLAST databases
        for temp_db, final_db, dataset, seq_type in self.pending_blast_copies:
            try:
                # Archive old database files if exist
                extensions = (
                    BLAST_PROTEIN_EXTENSIONS if seq_type == "protein"
                    else BLAST_NUCLEOTIDE_EXTENSIONS
                )
                for ext in extensions:
                    old_file = final_db.with_suffix(f".{ext}")
                    if old_file.exists():
                        archive_old_files(old_file, self.blast_archive_dir)

                # Copy all database files
                copy_blast_db(temp_db, final_db, seq_type)
                self.log(f"Copied BLAST database {temp_db.name} to {final_db}")
            except Exception as e:
                self.log_error(f"Failed to copy BLAST database {temp_db.name}: {e}")
                success = False

        return success

    def write_seq_count_file(self, output_file: Path) -> None:
        """Write sequence count file for PatMatch."""
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            for dataset, count in sorted(self.seq_counts.items()):
                f.write(f"{dataset} {count}\n")

        self.log(f"Wrote sequence counts to {output_file}")


def find_source_file(
    download_dir: Path, strain_abbrev: str, file_type: str, assembly: str | None = None
) -> Path | None:
    """
    Find source file in Assembly/current/ or direct current/ directory.

    Source files from dump_sequence.py have versioned names like:
    - C_albicans_SC5314_version_A22-s08-m01-r36_orf_coding.fasta.gz (Assembly format)
    - C_dubliniensis_CD36_version_s01-m04-r07_orf_coding.fasta.gz (direct format)

    For A19, files may have _haploid suffix:
    - C_albicans_SC5314_version_A19-s01-m04-r03_orf_coding_haploid.fasta.gz

    Args:
        download_dir: Base download directory for the strain
        strain_abbrev: Strain abbreviation
        file_type: File type to find (e.g., 'orf_coding', 'chromosomes')
        assembly: Optional specific assembly to look in (e.g., 'A22', 'A19')

    Returns:
        Path to source file or None if not found
    """
    # Pattern to match: {strain}_version_*_{file_type}.fasta.gz
    # Also try with _haploid suffix for A19
    patterns = [
        f"{strain_abbrev}_version_*_{file_type}.fasta.gz",
        f"{strain_abbrev}_version_*_{file_type}_haploid.fasta.gz",
    ]

    if assembly:
        # Look in specific assembly directory
        assembly_num = assembly[1:]  # Extract number from 'A22' -> '22'
        assembly_dir = download_dir / f"Assembly{assembly_num}"
        current_dir = assembly_dir / "current"

        if current_dir.exists():
            for pattern in patterns:
                matches = list(current_dir.glob(pattern))
                # Filter to only match the requested assembly
                matches = [m for m in matches if f"_version_{assembly}-" in m.name]
                if matches:
                    return matches[0]
        return None

    # No specific assembly - try Assembly directories (sorted in reverse to get latest first)
    for assembly_dir in sorted(download_dir.glob("Assembly*"), reverse=True):
        current_dir = assembly_dir / "current"
        if not current_dir.exists():
            continue

        for pattern in patterns:
            matches = list(current_dir.glob(pattern))
            if matches:
                return matches[0]

    # Second, try direct current/ directory (for strains without Assembly prefix)
    current_dir = download_dir / "current"
    if current_dir.exists():
        for pattern in patterns:
            matches = list(current_dir.glob(pattern))
            if matches:
                return matches[0]

    return None


def get_assembly_abbrev(source_file: Path) -> tuple[str, bool]:
    """
    Extract assembly abbreviation from versioned filename.

    E.g., from 'C_albicans_SC5314_version_A22-s08-m01-r36_orf_coding.fasta.gz'
    extracts 'A22'

    For files without assembly prefix (e.g., 'C_dubliniensis_CD36_version_s01-m04-r07_orf_coding.fasta.gz')
    returns empty string (legacy naming doesn't include version suffix).

    Returns:
        Tuple of (assembly_abbrev, is_assembly_format)
        - For Assembly format: ('A22', True)
        - For direct format: ('', False)
    """
    name = source_file.name
    # Pattern 1: ..._version_A22-..._... (assembly format)
    match = re.search(r"_version_(A\d+)-", name)
    if match:
        return match.group(1), True

    # Pattern 2: ..._version_s##-m##-r##_... (direct format without assembly)
    # Legacy naming doesn't include version suffix for these
    match = re.search(r"_version_(s\d+)-", name)
    if match:
        return "", False

    return "", False


def get_dataset_config(
    strain_abbrev: str, download_dir: Path, assembly: str | None = None
) -> list[dict]:
    """
    Get dataset configuration for a strain.

    Finds source files in Assembly/current/ directories and configures
    output filenames with assembly info.

    Args:
        strain_abbrev: Strain abbreviation
        download_dir: Base download directory for the strain
        assembly: Optional specific assembly (e.g., 'A22', 'A19')

    Returns:
        List of dataset configurations
    """
    # Dataset types to process
    dataset_types = [
        {"name": "orf_coding", "source_type": "orf_coding", "seq_type": "dna", "blast": True},
        {"name": "orf_trans_all", "source_type": "orf_trans_all", "seq_type": "protein", "blast": True},
        {"name": "orf_genomic", "source_type": "orf_genomic", "seq_type": "dna", "blast": True},
        {"name": "orf_genomic_1000", "source_type": "orf_genomic_1000", "seq_type": "dna", "blast": False},
        {"name": "other_features_genomic", "source_type": "other_features_genomic", "seq_type": "dna", "blast": True},
        {"name": "other_features_genomic_1000", "source_type": "other_features_genomic_1000", "seq_type": "dna", "blast": False},
        {"name": "other_features_no_introns", "source_type": "other_features_no_introns", "seq_type": "dna", "blast": True},
        {"name": "genomic", "source_type": "chromosomes", "seq_type": "dna", "blast": True},
        {"name": "not_feature", "source_type": "not_feature", "seq_type": "dna", "blast": False},
        {"name": "default_coding", "source_type": "default_coding", "seq_type": "dna", "blast": True},
        {"name": "default_genomic", "source_type": "default_genomic", "seq_type": "dna", "blast": True},
        {"name": "default_protein", "source_type": "default_protein", "seq_type": "protein", "blast": True},
    ]

    datasets = []

    # Get datasets to skip for this assembly
    skip_datasets = ASSEMBLY_SKIP_DATASETS.get(assembly, []) if assembly else []

    for dt in dataset_types:
        # Skip datasets not needed for this assembly
        if dt["name"] in skip_datasets:
            continue

        source_file = find_source_file(download_dir, strain_abbrev, dt["source_type"], assembly)
        if not source_file:
            continue

        file_assembly, is_assembly_format = get_assembly_abbrev(source_file)

        # Output filenames:
        # - Assembly format (A22): {type}_{strain}_{assembly}.fasta
        # - Direct format: {type}_{strain}.fasta (no version suffix, legacy naming)
        if is_assembly_format:
            fasta_name = f"{dt['name']}_{strain_abbrev}_{file_assembly}.fasta"
            blast_name = f"{dt['name']}_{strain_abbrev}_{file_assembly}" if dt["blast"] else None
        else:
            fasta_name = f"{dt['name']}_{strain_abbrev}.fasta"
            blast_name = f"{dt['name']}_{strain_abbrev}" if dt["blast"] else None

        datasets.append({
            "name": dt["name"],
            "source": source_file,
            "fasta": fasta_name,
            "blast": blast_name,
            "type": dt["seq_type"],
            "assembly": file_assembly if file_assembly else "current",
        })

    return datasets


def get_mito_features(session, strain_abbrev: str) -> list[str]:
    """Get mitochondrial feature names for a strain from database."""
    query = text(f"""
        SELECT f.feature_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_abbrev = :strain_abbrev
        AND f.feature_type IN ('chromosome', 'contig')
        AND (UPPER(f.feature_name) LIKE '%MITO%' OR UPPER(f.feature_name) LIKE '%MTDNA%')
    """)

    result = session.execute(query, {"strain_abbrev": strain_abbrev})
    return [row[0] for row in result]


def update_seq_search_files(strain_abbrev: str, force: bool = False) -> tuple[bool, dict]:
    """
    Main function to update sequence search files.

    Files are written to temp directory first, validated, then copied to
    final location if validation passes.

    Args:
        strain_abbrev: Strain abbreviation
        force: If True, skip validation and copy files anyway

    Returns:
        Tuple of (success, stats_dict)
    """
    stats = {
        "strain": strain_abbrev,
        "datasets_processed": 0,
        "datasets_failed": 0,
        "fasta_files": 0,
        "blast_dbs": 0,
        "validation_failures": [],
        "errors": [],
        "copied": False,
    }

    # Set up logging for this strain
    log_file = LOG_DIR / f"{strain_abbrev}_fasta_file_creation.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting sequence search file update for {strain_abbrev}")
    logger.info(f"Start time: {datetime.now()}")
    logger.info(f"Force mode: {force}")

    processor = None

    try:
        with SessionLocal() as session:
            processor = SequenceProcessor(session, strain_abbrev, force=force)

            # Get organism number
            processor.organism_no = processor.get_organism_no()
            if not processor.organism_no:
                logger.error(f"Strain {strain_abbrev} not found in database")
                stats["errors"].append(f"Strain not found in database")
                return False, stats

            logger.info(f"Organism: {strain_abbrev} (organism_no={processor.organism_no})")
            logger.info(f"Temp dir: {processor.temp_dir}")
            logger.info(f"Download dir: {processor.download_dir}")
            logger.info(f"FASTA dir (final): {processor.fasta_dir}")
            logger.info(f"BLAST dir (final): {processor.blast_dir}")

            # Identify mito features
            mito_features = get_mito_features(session, strain_abbrev)
            processor.identify_mito_features(mito_features)

            # Get assemblies to process for this strain
            assemblies = STRAIN_ASSEMBLIES.get(strain_abbrev, [None])
            logger.info(f"Assemblies to process: {assemblies if assemblies != [None] else ['current']}")

            # Get dataset configuration for each assembly
            datasets = []
            for assembly in assemblies:
                assembly_datasets = get_dataset_config(
                    strain_abbrev, processor.download_dir, assembly
                )
                if assembly_datasets:
                    logger.info(f"Found {len(assembly_datasets)} datasets for {assembly or 'current'}")
                    datasets.extend(assembly_datasets)
                else:
                    logger.warning(f"No source files found for assembly {assembly or 'current'}")

            if not datasets:
                logger.error(f"No source files found for {strain_abbrev}")
                stats["errors"].append("No source files found in any Assembly/current/ directory")
                return False, stats

            logger.info(f"Found {len(datasets)} total datasets to process")

            # Process each dataset (writes to temp directory)
            for ds_config in datasets:
                # Source file is already a full Path from get_dataset_config
                source_file = ds_config["source"]

                fasta_file = (
                    processor.fasta_dir / ds_config["fasta"]
                    if ds_config["fasta"]
                    else None
                )
                blast_db = (
                    processor.blast_dir / ds_config["blast"]
                    if ds_config["blast"]
                    else None
                )

                assembly = ds_config.get('assembly', '')
                logger.info(f"Processing {ds_config['name']} ({assembly or 'current'})")

                success = processor.process_dataset(
                    dataset=ds_config["name"],
                    source_file=source_file,
                    fasta_file=fasta_file,
                    blast_db=blast_db,
                    seq_type=ds_config["type"],
                    assembly=assembly,
                )

                if success:
                    stats["datasets_processed"] += 1
                    if fasta_file:
                        stats["fasta_files"] += 1
                    if blast_db:
                        stats["blast_dbs"] += 1
                else:
                    stats["datasets_failed"] += 1

            # If any datasets failed to process, don't proceed to validation
            if stats["datasets_failed"] > 0:
                logger.error(f"Dataset processing failed, skipping validation")
                stats["errors"] = processor.errors
                send_slack_message(
                    f"{strain_abbrev}: Processing failed - {stats['datasets_failed']} datasets failed",
                    is_error=True
                )
                return False, stats

            # Validate all files
            logger.info("Validating generated files...")
            validation_passed = processor.validate_all_files()
            stats["validation_failures"] = processor.validation_failures

            if not validation_passed and not force:
                logger.error("Validation failed, files NOT copied to final location")
                logger.error(f"Validation failures: {processor.validation_failures}")
                stats["errors"] = processor.errors
                send_slack_message(
                    f"{strain_abbrev}: Validation failed - {len(processor.validation_failures)} failures. "
                    f"Use --force to override.",
                    is_error=True
                )
                return False, stats

            if not validation_passed and force:
                logger.warning("Validation failed but --force specified, proceeding with copy")

            # Copy files to final location
            logger.info("Copying validated files to final location...")
            copy_success = processor.copy_validated_files()

            if not copy_success:
                logger.error("Failed to copy some files to final location")
                stats["errors"] = processor.errors
                send_slack_message(
                    f"{strain_abbrev}: Copy failed - see log for details",
                    is_error=True
                )
                return False, stats

            stats["copied"] = True

            # Write sequence count file to final location
            if processor.seq_counts:
                seq_count_file = processor.fasta_dir / "seq.count"
                processor.write_seq_count_file(seq_count_file)

            stats["errors"] = processor.errors
            logger.info(f"Complete: {datetime.now()}")

            # Send success notification
            send_slack_message(
                f"{strain_abbrev}: Updated successfully - "
                f"{stats['fasta_files']} FASTA, {stats['blast_dbs']} BLAST DBs"
            )

            return True, stats

    except Exception as e:
        logger.exception(f"Error updating sequence search files: {e}")
        stats["errors"].append(str(e))
        send_slack_message(
            f"{strain_abbrev}: Error - {str(e)[:100]}",
            is_error=True
        )
        return False, stats

    finally:
        # Clean up temp directory
        if processor:
            processor.cleanup_temp()
        logger.removeHandler(file_handler)
        file_handler.close()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update sequence search files for PatMatch and BLAST"
    )
    parser.add_argument(
        "strain",
        nargs="?",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all strains",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force copy even if validation fails (use with caution)",
    )

    args = parser.parse_args()

    if args.all:
        strains = STRAIN_ABBREVS
    elif args.strain:
        strains = [args.strain]
    else:
        parser.print_help()
        return 1

    all_success = True
    all_stats = []

    for strain in strains:
        print(f"\nProcessing {strain}...")
        success, stats = update_seq_search_files(strain, force=args.force)
        all_stats.append(stats)
        if not success:
            all_success = False

    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    for stats in all_stats:
        status = "OK" if stats.get("copied", False) else "FAILED"
        print(f"\n{stats['strain']}: {status}")
        print(f"  Datasets processed: {stats['datasets_processed']}")
        print(f"  Datasets failed: {stats['datasets_failed']}")
        print(f"  FASTA files created: {stats['fasta_files']}")
        print(f"  BLAST databases created: {stats['blast_dbs']}")
        print(f"  Copied to final location: {'Yes' if stats.get('copied') else 'No'}")
        if stats.get("validation_failures"):
            print(f"  Validation failures: {len(stats['validation_failures'])}")
            for fail in stats["validation_failures"][:3]:
                print(f"    - {fail}")
        if stats["errors"]:
            print(f"  Errors: {len(stats['errors'])}")
            for err in stats["errors"][:3]:
                print(f"    - {err}")

    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
