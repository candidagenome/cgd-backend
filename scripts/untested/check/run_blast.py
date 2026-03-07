#!/usr/bin/env python3
"""
Run BLAST searches and parse results.

This script runs BLAST searches (blastn, blastp, blastx, tblastn, tblastx)
against a database and generates summary reports.

Original Perl: runBlastOnMissingOrfs.pl
Converted to Python: 2024
"""

import argparse
import subprocess
import sys
from pathlib import Path

from Bio import SearchIO, SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord


def run_blast(
    query_file: Path,
    database: Path,
    program: str = 'blastn',
    output_file: Path = None,
    evalue: float = 10.0,
    max_target_seqs: int = 5,
    num_threads: int = 1,
    outfmt: int = 5,  # XML format
) -> Path:
    """
    Run BLAST search.

    Args:
        query_file: Query sequences in FASTA format
        database: BLAST database path
        program: BLAST program (blastn, blastp, etc.)
        output_file: Output file path
        evalue: E-value threshold
        max_target_seqs: Maximum number of target sequences
        num_threads: Number of threads
        outfmt: Output format (5=XML, 6=tabular)

    Returns:
        Path to output file
    """
    if output_file is None:
        output_file = query_file.with_suffix('.blast.xml')

    cmd = [
        program,
        '-query', str(query_file),
        '-db', str(database),
        '-out', str(output_file),
        '-evalue', str(evalue),
        '-max_target_seqs', str(max_target_seqs),
        '-num_threads', str(num_threads),
        '-outfmt', str(outfmt),
    ]

    print(f"Running: {' '.join(cmd)}", file=sys.stderr)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"BLAST error: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"BLAST failed with return code {result.returncode}")

    return output_file


def parse_blast_results(
    blast_file: Path,
    format: str = 'blast-xml',
    max_hits: int = None,
) -> list[dict]:
    """
    Parse BLAST results.

    Args:
        blast_file: BLAST output file
        format: File format (blast-xml, blast-tab, blast-text)
        max_hits: Maximum hits per query to return

    Returns:
        List of result dicts
    """
    results = []

    for query_result in SearchIO.parse(str(blast_file), format):
        query_name = query_result.id
        query_len = query_result.seq_len

        hit_count = 0
        for hit in query_result:
            if max_hits and hit_count >= max_hits:
                break

            for hsp in hit:
                results.append({
                    'query_name': query_name,
                    'query_length': query_len,
                    'hit_name': hit.id,
                    'hit_description': hit.description,
                    'hit_length': hit.seq_len,
                    'score': hsp.bitscore,
                    'evalue': hsp.evalue,
                    'identity': hsp.ident_num,
                    'identity_pct': (hsp.ident_num / hsp.aln_span * 100) if hsp.aln_span else 0,
                    'alignment_length': hsp.aln_span,
                    'query_start': hsp.query_start,
                    'query_end': hsp.query_end,
                    'hit_start': hsp.hit_start,
                    'hit_end': hsp.hit_end,
                })
                hit_count += 1
                break  # Only first HSP per hit

    return results


def write_summary(
    results: list[dict],
    output_file: Path = None,
) -> None:
    """
    Write BLAST summary report.

    Args:
        results: List of BLAST result dicts
        output_file: Output file (stdout if None)
    """
    out_handle = open(output_file, 'w') if output_file else sys.stdout

    try:
        # Header
        out_handle.write(
            "Query\tQuery_Len\tHit\tHit_Len\tScore\tE-value\t"
            "Identity\tIdentity%\tAlign_Len\tQ_Start\tQ_End\tH_Start\tH_End\n"
        )

        # Data rows
        for r in results:
            out_handle.write(
                f"{r['query_name']}\t{r['query_length']}\t"
                f"{r['hit_name']}\t{r['hit_length']}\t"
                f"{r['score']:.1f}\t{r['evalue']:.2e}\t"
                f"{r['identity']}\t{r['identity_pct']:.1f}\t"
                f"{r['alignment_length']}\t"
                f"{r['query_start']}\t{r['query_end']}\t"
                f"{r['hit_start']}\t{r['hit_end']}\n"
            )

    finally:
        if output_file:
            out_handle.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run BLAST searches and parse results"
    )
    parser.add_argument(
        "query",
        type=Path,
        help="Query sequences in FASTA format",
    )
    parser.add_argument(
        "database",
        type=Path,
        help="BLAST database path",
    )
    parser.add_argument(
        "--program",
        choices=['blastn', 'blastp', 'blastx', 'tblastn', 'tblastx'],
        default='blastn',
        help="BLAST program (default: blastn)",
    )
    parser.add_argument(
        "--evalue",
        type=float,
        default=10.0,
        help="E-value threshold (default: 10.0)",
    )
    parser.add_argument(
        "--max-hits",
        type=int,
        default=5,
        help="Maximum hits per query (default: 5)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Number of threads (default: 1)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        help="Output summary file (default: stdout)",
    )
    parser.add_argument(
        "--blast-output",
        type=Path,
        help="BLAST raw output file (default: query.blast.xml)",
    )
    parser.add_argument(
        "--parse-only",
        type=Path,
        help="Parse existing BLAST output instead of running BLAST",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Either run BLAST or parse existing output
    if args.parse_only:
        if not args.parse_only.exists():
            print(f"Error: BLAST output file not found: {args.parse_only}", file=sys.stderr)
            sys.exit(1)
        blast_output = args.parse_only
    else:
        # Validate inputs
        if not args.query.exists():
            print(f"Error: Query file not found: {args.query}", file=sys.stderr)
            sys.exit(1)

        # Run BLAST
        try:
            blast_output = run_blast(
                args.query,
                args.database,
                args.program,
                args.blast_output,
                args.evalue,
                args.max_hits,
                args.threads,
            )
            if args.verbose:
                print(f"BLAST output written to: {blast_output}", file=sys.stderr)
        except Exception as e:
            print(f"Error running BLAST: {e}", file=sys.stderr)
            sys.exit(1)

    # Parse results
    try:
        results = parse_blast_results(blast_output, 'blast-xml', args.max_hits)
        if args.verbose:
            print(f"Parsed {len(results)} BLAST hits", file=sys.stderr)
    except Exception as e:
        print(f"Error parsing BLAST output: {e}", file=sys.stderr)
        sys.exit(1)

    # Write summary
    write_summary(results, args.output)

    if args.verbose:
        # Summary stats
        queries_with_hits = len(set(r['query_name'] for r in results))
        print(f"Queries with hits: {queries_with_hits}", file=sys.stderr)


if __name__ == "__main__":
    main()
