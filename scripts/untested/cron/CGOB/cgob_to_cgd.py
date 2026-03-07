#!/usr/bin/env python3
"""
Map CGOB identifiers to CGD identifiers.

This script creates a key mapping between CGOB (Candida Gene Order Browser)
sequence identifiers and CGD database identifiers. It uses BLAST to match
sequences when identifiers don't match directly.

Based on CGOB2CGD.pl.

Usage:
    python cgob_to_cgd.py
    python cgob_to_cgd.py --debug

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
BLASTP = os.getenv("BLASTP", "blastp")

# CGOB configuration
CGOB_DATA_DIR = DATA_DIR / "CGOB"
CGOB_SEQ_DIR = CGOB_DATA_DIR / "sequences"
CGOB_BLAST_DIR = CGOB_DATA_DIR / "blastdb"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# BLAST match criteria
MIN_IDENTITY = 98.0
MAX_GAPS = 0
MAX_MISMATCH = 4

# Strain prefixes for ID matching
STRAIN_PREFIXES = {
    "orf19": "C_albicans_SC5314",
    "ORF19": "C_albicans_SC5314",
    "CAGL": "C_glabrata_CBS138",
    "CORT": "C_tropicalis_MYA3404",
    "Cd36": "C_dubliniensis_CD36",
    "CD36": "C_dubliniensis_CD36",
}


def get_strain_from_prefix(seq_id: str) -> str | None:
    """Determine strain from sequence ID prefix."""
    for prefix, strain in STRAIN_PREFIXES.items():
        if seq_id.startswith(prefix):
            return strain
    return None


def get_cgd_sequence_ids(session, strain_abbrev: str) -> dict[str, str]:
    """
    Get all sequence IDs for a strain from CGD database.

    Returns dict mapping sequence ID to itself (for quick lookup).
    """
    query = text(f"""
        SELECT feature_name
        FROM {DB_SCHEMA}.feature
        WHERE organism_abbrev = :strain
        AND feature_type IN ('ORF', 'pseudogene', 'allele')
    """)

    ids = {}
    for row in session.execute(query, {"strain": strain_abbrev}).fetchall():
        if row[0]:
            ids[row[0]] = row[0]

    return ids


def normalize_id(seq_id: str) -> str:
    """
    Normalize sequence ID for matching.

    Handles differences in capitalization and underscores.
    """
    normalized = seq_id

    # Handle CORT prefix variations
    if normalized.startswith("CORT0"):
        normalized = normalized.replace("CORT0", "CORT_0", 1)

    # Handle ORF19 prefix variations
    if normalized.startswith("ORF19"):
        normalized = normalized.replace("ORF19", "orf19", 1)

    # Handle CD36 prefix variations
    if normalized.startswith("CD36"):
        normalized = normalized.replace("CD36", "Cd36", 1)

    return normalized


def parse_fasta_ids(fasta_file: Path) -> list[tuple[str, str]]:
    """
    Parse FASTA file and return list of (id, strain) tuples.
    """
    ids = []

    try:
        from Bio import SeqIO

        with open(fasta_file) as f:
            for record in SeqIO.parse(f, "fasta"):
                seq_id = record.id
                strain = get_strain_from_prefix(seq_id)
                if strain:
                    ids.append((seq_id, strain))

    except ImportError:
        # Fallback without BioPython
        with open(fasta_file) as f:
            for line in f:
                if line.startswith(">"):
                    seq_id = line[1:].split()[0]
                    strain = get_strain_from_prefix(seq_id)
                    if strain:
                        ids.append((seq_id, strain))

    return ids


def write_query_sequences(
    fasta_file: Path,
    output_file: Path,
    ids_to_include: set[str],
) -> None:
    """Write subset of sequences to a query file."""
    try:
        from Bio import SeqIO

        with open(fasta_file) as f_in, open(output_file, "w") as f_out:
            for record in SeqIO.parse(f_in, "fasta"):
                if record.id in ids_to_include:
                    SeqIO.write(record, f_out, "fasta")

    except ImportError:
        # Fallback without BioPython
        writing = False
        with open(fasta_file) as f_in, open(output_file, "w") as f_out:
            for line in f_in:
                if line.startswith(">"):
                    seq_id = line[1:].split()[0]
                    writing = seq_id in ids_to_include
                if writing:
                    f_out.write(line)


def run_blast(query_file: Path, database: Path, output_file: Path) -> bool:
    """Run BLASTP and return success status."""
    try:
        cmd = [
            BLASTP,
            "-query", str(query_file),
            "-db", str(database),
            "-outfmt", "6",
            "-out", str(output_file),
            "-evalue", "1e-5",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0

    except Exception as e:
        logger.error(f"BLAST error: {e}")
        return False


def parse_blast_results(
    blast_file: Path,
) -> dict[str, tuple[str, float, int]]:
    """
    Parse BLAST results and return best hits meeting criteria.

    Returns dict mapping query to (hit, identity, mismatches).
    """
    best_hits: dict[str, tuple[str, float, int]] = {}
    best_scores: dict[str, float] = {}

    with open(blast_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 12:
                continue

            query = parts[0]
            hit = parts[1]
            identity = float(parts[2])
            mismatches = int(parts[4])
            gaps = int(parts[5])
            score = float(parts[11])

            # Check criteria
            if identity < MIN_IDENTITY or gaps > MAX_GAPS or mismatches > MAX_MISMATCH:
                continue

            # Perfect match (query == hit with 100% identity)
            if query == hit and identity == 100.0:
                best_hits[query] = (hit, identity, mismatches)
                continue

            # Update best hit if better score
            if query not in best_hits:
                if query not in best_scores or score > best_scores[query]:
                    best_scores[query] = score
                    best_hits[query] = (hit, identity, mismatches)

    return best_hits


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Map CGOB identifiers to CGD identifiers"
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
    log_file = LOG_DIR / "CGOB_2_CGD.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_text = f"Program {__file__}: Starting {datetime.now()}\n\n"

    # Input files
    cgob_proteins = CGOB_DATA_DIR / "cgob_proteins.fasta"
    cglab_orthologs = CGOB_DATA_DIR / "cglab_orthologs.txt"
    key_file = CGOB_DATA_DIR / "cgob_to_cgd_key.txt"

    if not cgob_proteins.exists():
        error_msg = f"CGOB proteins file not found: {cgob_proteins}"
        log_text += f"ERROR: {error_msg}\n"
        logger.error(error_msg)
        with open(log_file, "w") as f:
            f.write(log_text)
        return 1

    try:
        with SessionLocal() as session:
            # Get CGD sequence IDs for each strain
            logger.info("Collecting CGD sequence IDs...")
            cgd_ids_for_strain: dict[str, dict[str, str]] = {}

            for strain in set(STRAIN_PREFIXES.values()):
                cgd_ids_for_strain[strain] = get_cgd_sequence_ids(session, strain)
                logger.info(f"  {strain}: {len(cgd_ids_for_strain[strain])} sequences")

            # Parse CGOB sequences and match by name
            logger.info("Parsing CGOB sequences...")
            cgob_ids = parse_fasta_ids(cgob_proteins)
            logger.info(f"Found {len(cgob_ids)} CGOB sequences")

            same_name: dict[str, str] = {}
            unmatched: dict[str, set[str]] = {}  # strain -> set of unmatched IDs

            for seq_id, strain in cgob_ids:
                normalized = normalize_id(seq_id)

                if strain in cgd_ids_for_strain:
                    if normalized in cgd_ids_for_strain[strain]:
                        same_name[seq_id] = cgd_ids_for_strain[strain][normalized]
                    else:
                        if strain not in unmatched:
                            unmatched[strain] = set()
                        unmatched[strain].add(seq_id)

            logger.info(f"Matched {len(same_name)} sequences by name")

            # Process C. glabrata sequences from YGOB
            if cglab_orthologs.exists():
                logger.info("Collecting C. glabrata sequence IDs from YGOB...")
                with open(cglab_orthologs) as f:
                    for line in f:
                        parts = line.strip().split("\t")
                        for seq_id in parts:
                            strain = get_strain_from_prefix(seq_id)
                            if strain and strain in cgd_ids_for_strain:
                                if seq_id in cgd_ids_for_strain[strain]:
                                    if seq_id not in same_name:
                                        same_name[seq_id] = cgd_ids_for_strain[strain][seq_id]

            # BLAST unmatched sequences
            logger.info("BLASTing unmatched sequences...")
            blast_hits: dict[str, tuple[str, float, int]] = {}

            for strain, ids in unmatched.items():
                if not ids:
                    continue

                logger.info(f"  BLASTing {len(ids)} {strain} sequences...")
                log_text += f"BLASTing {strain}...\n"

                # Write query file
                with tempfile.NamedTemporaryFile(mode="w", suffix=".fasta", delete=False) as tmp:
                    query_file = Path(tmp.name)

                write_query_sequences(cgob_proteins, query_file, ids)

                # Find BLAST database
                blast_db = CGOB_BLAST_DIR / strain / f"{strain}_protein"

                if not blast_db.with_suffix(".phr").exists() and not blast_db.with_suffix(".psq").exists():
                    logger.warning(f"  BLAST database not found for {strain}")
                    log_text += f"WARNING: BLAST database not found for {strain}\n"
                    query_file.unlink()
                    continue

                # Run BLAST
                with tempfile.NamedTemporaryFile(mode="w", suffix=".blast", delete=False) as tmp:
                    blast_output = Path(tmp.name)

                if run_blast(query_file, blast_db, blast_output):
                    hits = parse_blast_results(blast_output)
                    blast_hits.update(hits)
                    logger.info(f"    Found {len(hits)} BLAST matches")

                # Cleanup
                query_file.unlink()
                blast_output.unlink()

                log_text += f"BLAST complete: {datetime.now()}\n\n"

            # Write key file
            logger.info(f"Writing key file to {key_file}")
            with open(key_file, "w") as f:
                # Write name matches
                for cgob_id, cgd_id in sorted(same_name.items()):
                    strain = get_strain_from_prefix(cgob_id) or "unknown"
                    f.write(f"{cgob_id}\t{strain}\t{cgd_id}\tID_MATCH\tID_MATCH\n")

                # Write BLAST matches
                for cgob_id, (cgd_id, identity, mismatches) in sorted(blast_hits.items()):
                    if cgob_id in same_name:
                        continue
                    strain = get_strain_from_prefix(cgob_id) or "unknown"
                    f.write(f"{cgob_id}\t{strain}\t{cgd_id}\t{identity:.2f}\t{mismatches}\n")

            total_matches = len(same_name) + len(blast_hits)
            log_text += f"{key_file} written\n"
            log_text += f"Total matches: {total_matches}\n\n"

            logger.info(f"Total matches: {total_matches}")

    except Exception as e:
        log_text += f"ERROR: {e}\n"
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())

    log_text += f"Exiting {__file__}: {datetime.now()}\n\n"

    # Write log file
    with open(log_file, "w") as f:
        f.write(log_text)

    logger.info(f"Log written to {log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
