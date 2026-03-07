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

Usage:
    python update_seq_search_files.py --strain C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
    BLAST_DIR: Directory for BLAST databases
    FASTA_DIR: Directory for FASTA files
    BLAST_FORMAT_CMD: Path to makeblastdb command
    CURATOR_EMAIL: Email for notifications
"""

import argparse
import gzip
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
BLAST_DIR = Path(os.getenv("BLAST_DIR", "/data/blast"))
FASTA_DIR = Path(os.getenv("FASTA_DIR", "/data/fasta"))
BLAST_FORMAT_CMD = os.getenv("BLAST_FORMAT_CMD", "makeblastdb")
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL")
SEQUENCE_FILES_CONFIG = os.getenv("SEQUENCE_FILES_CONFIG", "")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class SequenceProcessor:
    """Process sequence files for PatMatch and BLAST."""

    def __init__(self, session, strain_abbrev: str):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.organism_no = None
        self.mito_features: set[str] = set()
        self.seq_counts: dict[str, int] = {}
        self.log_messages: list[str] = []

        # Directory setup
        self.fasta_dir = FASTA_DIR / strain_abbrev
        self.blast_dir = BLAST_DIR / strain_abbrev
        self.download_dir = DATA_DIR / "download" / strain_abbrev

    def log(self, message: str) -> None:
        """Log a message to both logger and internal list."""
        logger.info(message)
        self.log_messages.append(message)

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
        import re
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

        Args:
            dataset: Dataset name
            input_file: Input gzipped FASTA file
            output_file: Output plain FASTA file
            seq_type: Sequence type ('protein' or 'dna')

        Returns:
            Number of sequences processed
        """
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        sequences = self.parse_fasta(input_file)
        count = 0

        # Write to temp file first
        temp_file = output_file.with_suffix(".temp")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(temp_file, "w") as f:
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

        # Move temp file to final location
        shutil.move(temp_file, output_file)
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

        Args:
            input_file: Input gzipped FASTA file
            output_db: Output database path (without extension)
            dataset: Dataset name for title
            seq_type: Sequence type ('protein' or 'dna')

        Returns:
            True on success, False on failure
        """
        if not input_file.exists():
            self.log(f"ERROR: Input file not found: {input_file}")
            return False

        output_db.parent.mkdir(parents=True, exist_ok=True)

        # Create temp directory for formatting
        temp_dir = output_db.parent / "temp"
        temp_dir.mkdir(exist_ok=True)
        temp_db = temp_dir / output_db.name

        # Determine database type
        db_type = "prot" if seq_type == "protein" else "nucl"

        # Build command: decompress and pipe to makeblastdb
        cmd = (
            f"zcat {input_file} | {BLAST_FORMAT_CMD} "
            f"-dbtype {db_type} -input_type fasta "
            f"-parse_seqids -title {dataset} -out {temp_db}"
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
                self.log(f"ERROR: makeblastdb failed: {result.stderr}")
                return False

            # Move files to final location
            suffixes = (
                ["pdb", "phr", "pin", "pog", "pos", "pot", "psq", "ptf", "pto"]
                if seq_type == "protein"
                else ["ndb", "nhr", "nin", "nog", "nos", "not", "nsq", "ntf", "nto"]
            )

            for suffix in suffixes:
                src = temp_db.with_suffix(f".{suffix}")
                if src.exists():
                    dst = output_db.with_suffix(f".{suffix}")
                    shutil.move(src, dst)
                    self.log(f"Created {dst}")

            return True

        except Exception as e:
            self.log(f"ERROR formatting BLAST database: {e}")
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

    def process_dataset(
        self,
        dataset: str,
        source_file: Path,
        fasta_file: Path | None,
        blast_db: Path | None,
        seq_type: str,
    ) -> None:
        """
        Process a single dataset for PatMatch and/or BLAST.

        Args:
            dataset: Dataset name
            source_file: Source gzipped FASTA file
            fasta_file: Output PatMatch FASTA file (or None)
            blast_db: Output BLAST database path (or None)
            seq_type: Sequence type ('protein' or 'dna')
        """
        self.log(f"Processing dataset: {dataset}")

        if not source_file.exists():
            self.log(f"ERROR: Source file not found: {source_file}")
            return

        # Check if we need to separate mito sequences
        needs_mito_separation = (
            self.mito_features
            and any(kw in dataset for kw in ["genomic", "orf_genomic", "orf_coding"])
        )

        # Process for BLAST
        if blast_db:
            if needs_mito_separation:
                # Create temp files for separated sequences
                temp_dir = blast_db.parent / "temp"
                temp_dir.mkdir(exist_ok=True)

                nuclear_file = temp_dir / f"{dataset}_nuclear.fasta.gz"
                mito_file = temp_dir / f"{dataset}_mito.fasta.gz"

                nuc_count, mito_count = self.separate_mito_sequences(
                    source_file, nuclear_file, mito_file
                )

                # Format nuclear BLAST database
                self.format_blast_db(nuclear_file, blast_db, dataset, seq_type)

                # Format mito BLAST database if there are mito sequences
                if mito_count > 0:
                    mito_blast_db = blast_db.parent / f"mito_{blast_db.name}"
                    self.format_blast_db(mito_file, mito_blast_db, f"mito_{dataset}", seq_type)

                # Cleanup temp files
                if nuclear_file.exists():
                    nuclear_file.unlink()
                if mito_file.exists():
                    mito_file.unlink()
            else:
                self.format_blast_db(source_file, blast_db, dataset, seq_type)

        # Process for PatMatch
        if fasta_file:
            count = self.reformat_fasta(dataset, source_file, fasta_file, seq_type)
            self.seq_counts[dataset] = count

        self.log(f"Successfully processed dataset: {dataset}")

    def write_seq_count_file(self, output_file: Path) -> None:
        """Write sequence count file for PatMatch."""
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            for dataset, count in sorted(self.seq_counts.items()):
                f.write(f"{dataset} {count}\n")

        self.log(f"Wrote sequence counts to {output_file}")


def get_dataset_config(strain_abbrev: str) -> list[dict]:
    """
    Get dataset configuration for a strain.

    This should be loaded from a config file or database in production.
    Returns list of dataset configurations.
    """
    # Default datasets - in production, load from config
    datasets = [
        {
            "name": "orf_coding",
            "source": f"{strain_abbrev}_current_orf_coding.fasta.gz",
            "fasta": f"{strain_abbrev}_orf_coding.fasta",
            "blast": f"{strain_abbrev}_orf_coding",
            "type": "dna",
        },
        {
            "name": "orf_trans",
            "source": f"{strain_abbrev}_current_orf_trans.fasta.gz",
            "fasta": f"{strain_abbrev}_orf_trans.fasta",
            "blast": f"{strain_abbrev}_orf_trans",
            "type": "protein",
        },
        {
            "name": "orf_genomic",
            "source": f"{strain_abbrev}_current_orf_genomic.fasta.gz",
            "fasta": f"{strain_abbrev}_orf_genomic.fasta",
            "blast": f"{strain_abbrev}_orf_genomic",
            "type": "dna",
        },
        {
            "name": "1000_up",
            "source": f"{strain_abbrev}_current_1000_bp_upstream.fasta.gz",
            "fasta": f"{strain_abbrev}_1000_up.fasta",
            "blast": None,
            "type": "dna",
        },
        {
            "name": "not_feature",
            "source": f"{strain_abbrev}_current_intergenic.fasta.gz",
            "fasta": f"{strain_abbrev}_not_feature.fasta",
            "blast": None,
            "type": "dna",
        },
        {
            "name": "genomic",
            "source": f"{strain_abbrev}_current_chromosomes.fasta.gz",
            "fasta": None,
            "blast": f"{strain_abbrev}_genomic",
            "type": "dna",
        },
    ]

    return datasets


def get_mito_features(session, strain_abbrev: str) -> list[str]:
    """Get mitochondrial feature names for a strain from database."""
    # This should be loaded from config or database
    # For now, return empty list - configure as needed
    query = text(f"""
        SELECT f.feature_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_abbrev = :strain_abbrev
        AND f.feature_type = 'chromosome'
        AND UPPER(f.feature_name) LIKE '%MITO%'
    """)

    result = session.execute(query, {"strain_abbrev": strain_abbrev})
    return [row[0] for row in result]


def update_seq_search_files(strain_abbrev: str) -> bool:
    """
    Main function to update sequence search files.

    Args:
        strain_abbrev: Strain abbreviation

    Returns:
        True on success, False on failure
    """
    # Set up logging for this strain
    log_file = LOG_DIR / f"{strain_abbrev}_fasta_file_creation.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting sequence search file update for {strain_abbrev}")
    logger.info(f"Start time: {datetime.now()}")

    try:
        with SessionLocal() as session:
            processor = SequenceProcessor(session, strain_abbrev)

            # Get organism number
            processor.organism_no = processor.get_organism_no()
            if not processor.organism_no:
                logger.error(f"Strain {strain_abbrev} not found in database")
                return False

            # Identify mito features
            mito_features = get_mito_features(session, strain_abbrev)
            processor.identify_mito_features(mito_features)

            # Get dataset configuration
            datasets = get_dataset_config(strain_abbrev)

            # Process each dataset
            for ds_config in datasets:
                source_file = processor.download_dir / ds_config["source"]
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

                processor.process_dataset(
                    dataset=ds_config["name"],
                    source_file=source_file,
                    fasta_file=fasta_file,
                    blast_db=blast_db,
                    seq_type=ds_config["type"],
                )

            # Write sequence count file
            seq_count_file = processor.fasta_dir / "seq.count"
            processor.write_seq_count_file(seq_count_file)

            logger.info(f"Complete: {datetime.now()}")
            return True

    except Exception as e:
        logger.exception(f"Error updating sequence search files: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update sequence search files for PatMatch and BLAST"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )

    args = parser.parse_args()

    success = update_seq_search_files(args.strain)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
