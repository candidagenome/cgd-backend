#!/usr/bin/env python3
"""
Generic reciprocal BLAST analysis.

This script performs reciprocal BLAST analysis between two sequence files
to identify best reciprocal hits (orthologs).

Based on genericReciprocal.pl.

Usage:
    python generic_reciprocal.py --first ref.fasta --second comp.fasta
    python generic_reciprocal.py --first ref.fasta --second comp.fasta --evalue 1e-10

Environment Variables:
    BLAST_BIN: Path to BLAST binaries
"""

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from environment
BLAST_BIN = Path(os.getenv("BLAST_BIN", "/usr/local/blast/bin"))
MAKEBLASTDB = os.getenv("MAKEBLASTDB", "makeblastdb")
BLASTP = os.getenv("BLASTP", "blastp")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


def create_blast_database(fasta_file: Path, db_type: str = "prot") -> bool:
    """Create a BLAST database from a FASTA file."""
    try:
        cmd = [
            MAKEBLASTDB,
            "-in", str(fasta_file),
            "-dbtype", db_type,
            "-parse_seqids",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"makeblastdb failed: {result.stderr}")
            return False

        return True

    except Exception as e:
        logger.error(f"Error creating BLAST database: {e}")
        return False


def run_blast(
    database: Path,
    query_file: Path,
    output_file: Path,
    evalue: float = 1e-5,
    num_threads: int = 4,
) -> bool:
    """Run BLASTP and return success status."""
    try:
        cmd = [
            BLASTP,
            "-db", str(database),
            "-query", str(query_file),
            "-outfmt", "6",  # tabular format
            "-out", str(output_file),
            "-num_threads", str(num_threads),
            "-evalue", str(evalue),
            "-max_target_seqs", "10",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"BLAST failed: {result.stderr}")
            return False

        return True

    except Exception as e:
        logger.error(f"Error running BLAST: {e}")
        return False


def parse_blast_results(blast_file: Path) -> dict[str, list[str]]:
    """
    Parse BLAST results and return top hits for each query.

    Returns dict mapping query_id to list of hit_ids (best hits first).
    """
    top_hits: dict[str, list[str]] = {}

    with open(blast_file) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 12:
                continue

            query_id = parts[0]
            hit_id = parts[1]

            if query_id not in top_hits:
                top_hits[query_id] = []

            # Only keep first N hits per query
            if len(top_hits[query_id]) < 10:
                top_hits[query_id].append(hit_id)

    return top_hits


def find_reciprocal_best_hits(
    hits_first_vs_second: dict[str, list[str]],
    hits_second_vs_first: dict[str, list[str]],
) -> list[tuple[str, str]]:
    """
    Find reciprocal best hits.

    Returns list of (first_id, second_id) tuples for reciprocal best hits.
    """
    reciprocal_hits = []

    for first_id, second_hits in hits_first_vs_second.items():
        if not second_hits:
            continue

        # Get the best hit in second
        best_second = second_hits[0]

        # Check if this second sequence has first_id as a hit
        if best_second not in hits_second_vs_first:
            continue

        # Check if first_id is in the top hits for best_second
        for first_hit in hits_second_vs_first[best_second]:
            if first_hit == first_id:
                reciprocal_hits.append((first_id, best_second))
                break

    return reciprocal_hits


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Perform reciprocal BLAST analysis"
    )
    parser.add_argument(
        "--first",
        required=True,
        type=Path,
        help="First (reference) sequence file in FASTA format",
    )
    parser.add_argument(
        "--second",
        required=True,
        type=Path,
        help="Second (comparison) sequence file in FASTA format",
    )
    parser.add_argument(
        "--evalue",
        type=float,
        default=1e-5,
        help="E-value threshold for BLAST (default: 1e-5)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads for BLAST (default: 4)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file (default: stdout)",
    )

    args = parser.parse_args()

    # Validate input files
    if not args.first.exists():
        logger.error(f"Sequence file {args.first} does not exist")
        return 1

    if not args.second.exists():
        logger.error(f"Sequence file {args.second} does not exist")
        return 1

    # Use temp directory for intermediate files
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create BLAST databases
        logger.info(f"Creating BLAST database for {args.first.name}")
        if not create_blast_database(args.first):
            logger.error("Failed to create BLAST database for first file")
            return 1

        logger.info(f"Creating BLAST database for {args.second.name}")
        if not create_blast_database(args.second):
            logger.error("Failed to create BLAST database for second file")
            return 1

        # Run BLAST: first vs second
        blast_first_vs_second = tmpdir / "first_vs_second.blast"
        logger.info(f"BLASTing {args.first.name} vs {args.second.name}")
        if not run_blast(
            args.second, args.first, blast_first_vs_second,
            args.evalue, args.threads
        ):
            logger.error("BLAST first vs second failed")
            return 1
        logger.info("BLAST complete")

        # Run BLAST: second vs first
        blast_second_vs_first = tmpdir / "second_vs_first.blast"
        logger.info(f"BLASTing {args.second.name} vs {args.first.name}")
        if not run_blast(
            args.first, args.second, blast_second_vs_first,
            args.evalue, args.threads
        ):
            logger.error("BLAST second vs first failed")
            return 1
        logger.info("BLAST complete")

        # Parse BLAST results
        hits_first_vs_second = parse_blast_results(blast_first_vs_second)
        hits_second_vs_first = parse_blast_results(blast_second_vs_first)

        logger.info(f"Found {len(hits_first_vs_second)} queries with hits (first vs second)")
        logger.info(f"Found {len(hits_second_vs_first)} queries with hits (second vs first)")

        # Find reciprocal best hits
        reciprocal_hits = find_reciprocal_best_hits(
            hits_first_vs_second, hits_second_vs_first
        )

        logger.info(f"Found {len(reciprocal_hits)} reciprocal best hits")

        # Output results
        if args.output:
            out_file = open(args.output, "w")
        else:
            out_file = sys.stdout

        try:
            for first_id, second_id in sorted(reciprocal_hits):
                out_file.write(f"{first_id}\t{second_id}\n")
        finally:
            if args.output:
                out_file.close()

        return 0


if __name__ == "__main__":
    sys.exit(main())
