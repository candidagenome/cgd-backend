#!/usr/bin/env python3
"""
BLAST proteins against PDB database.

This script performs BLASTP searches against the PDB database and
generates output for database loading.

Part 2 of 3-part PDB pipeline:
    1. download_pdb_seq.py
    2. blast_pdb.py
    3. load_pdb.py

Original Perl: blastPDB.pl
Converted to Python: 2024
"""

import argparse
import gzip
import logging
import subprocess
import sys
import tempfile
from pathlib import Path

from Bio import SearchIO, SeqIO

logger = logging.getLogger(__name__)

# BLAST options
BLAST_OPTS = {
    'num_alignments': 1000,
    'num_descriptions': 0,
    'evalue': 0.01,
    'num_threads': 2,
}


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def run_blastp(
    query_file: Path,
    database: Path,
    output_file: Path,
    evalue: float = 0.01,
    num_threads: int = 2,
) -> None:
    """
    Run BLASTP search.

    Args:
        query_file: Query FASTA file
        database: BLAST database path
        output_file: Output XML file
        evalue: E-value cutoff
        num_threads: Number of threads
    """
    cmd = [
        'blastp',
        '-query', str(query_file),
        '-db', str(database),
        '-out', str(output_file),
        '-outfmt', '5',  # XML format
        '-evalue', str(evalue),
        '-num_threads', str(num_threads),
        '-num_alignments', str(BLAST_OPTS['num_alignments']),
    ]

    logger.info(f"Running BLASTP: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"BLASTP failed: {result.stderr}")
        raise RuntimeError("BLASTP failed")


def parse_blast_results(
    blast_xml: Path,
    output_file: Path,
) -> int:
    """
    Parse BLAST XML results and write tab-delimited output.

    Output format (tab-delimited):
        ORF, ORF_length, PDB, PDB_length, Description, P-Value,
        ORF_aln_start, ORF_aln_end, PDB_aln_start, PDB_aln_end,
        pct_similar, pct_identical, pct_aligned,
        TaxID, query_seq, align_symbol, hit_seq

    Args:
        blast_xml: BLAST XML output file
        output_file: Tab-delimited output file

    Returns:
        Number of hits processed
    """
    count = 0

    with open(output_file, 'w') as f_out:
        for query_result in SearchIO.parse(str(blast_xml), 'blast-xml'):
            query_name = query_result.id
            query_len = query_result.seq_len

            for hit in query_result:
                hit_name = hit.id.replace('lcl|', '')
                hit_len = hit.seq_len

                # Parse description for taxid
                taxid = 'NA'
                desc = hit.description or ''
                if 'TaxID:' in desc:
                    parts = desc.split('TaxID:')
                    if len(parts) > 1:
                        tax_part = parts[1].strip().split()[0]
                        taxid = tax_part

                for hsp in hit.hsps:
                    # Calculate percentages
                    pct_similar = 100 * hsp.pos_num / hsp.aln_span if hsp.aln_span else 0
                    pct_identical = 100 * hsp.ident_num / hsp.aln_span if hsp.aln_span else 0
                    pct_aligned = 100 * hsp.query_span / query_len if query_len else 0

                    # Get alignment strings
                    query_seq = str(hsp.query.seq)
                    hit_seq = str(hsp.hit.seq)

                    # Build alignment symbol string
                    align_symbol = ''
                    if hasattr(hsp, 'aln_annotation') and 'similarity' in hsp.aln_annotation:
                        align_symbol = hsp.aln_annotation['similarity']
                    else:
                        # Construct from sequences
                        for q, h in zip(query_seq, hit_seq):
                            if q == h:
                                align_symbol += q
                            elif q == '-' or h == '-':
                                align_symbol += ' '
                            else:
                                align_symbol += '+'

                    # Get p-value or e-value
                    pval = hsp.evalue

                    # Write output line
                    f_out.write('\t'.join([
                        query_name,
                        str(query_len),
                        hit_name,
                        str(hit_len),
                        desc,
                        f"{pval:.4e}",
                        str(hsp.query_start + 1),  # 1-based
                        str(hsp.query_end),
                        str(hsp.hit_start + 1),  # 1-based
                        str(hsp.hit_end),
                        f"{pct_similar:.0f}",
                        f"{pct_identical:.0f}",
                        f"{pct_aligned:.0f}",
                        taxid,
                        query_seq,
                        align_symbol,
                        hit_seq,
                    ]) + '\n')

                    count += 1
                    break  # Only first HSP per hit

    return count


def blast_sequences(
    query_file: Path,
    database: Path,
    output_file: Path,
    evalue: float = 0.01,
    num_threads: int = 2,
) -> int:
    """
    BLAST all sequences and parse results.

    Args:
        query_file: Query FASTA file (can be gzipped)
        database: BLAST database path
        output_file: Tab-delimited output file
        evalue: E-value cutoff
        num_threads: Number of threads

    Returns:
        Number of hits
    """
    # Handle gzipped input
    if str(query_file).endswith('.gz'):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fasta', delete=False) as tmp:
            tmp_query = Path(tmp.name)

        with gzip.open(query_file, 'rt') as f_in, open(tmp_query, 'w') as f_out:
            f_out.write(f_in.read())
    else:
        tmp_query = query_file

    # Run BLAST
    with tempfile.NamedTemporaryFile(suffix='.xml', delete=False) as tmp:
        blast_xml = Path(tmp.name)

    try:
        run_blastp(tmp_query, database, blast_xml, evalue, num_threads)
        hits = parse_blast_results(blast_xml, output_file)
    finally:
        blast_xml.unlink(missing_ok=True)
        if str(query_file).endswith('.gz'):
            tmp_query.unlink(missing_ok=True)

    return hits


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="BLAST proteins against PDB database"
    )
    parser.add_argument(
        "organism",
        help="Organism abbreviation (for output naming)",
    )
    parser.add_argument(
        "queries",
        type=Path,
        nargs='?',
        help="Query protein FASTA file (optional, uses default if not provided)",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("data/blast_datasets/pdb.fasta"),
        help="PDB BLAST database (default: data/blast_datasets/pdb.fasta)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/pdb"),
        help="Output directory (default: data/pdb)",
    )
    parser.add_argument(
        "--evalue",
        type=float,
        default=0.01,
        help="E-value cutoff (default: 0.01)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=2,
        help="Number of threads (default: 2)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Validate inputs
    if args.queries and not args.queries.exists():
        logger.error(f"Query file not found: {args.queries}")
        sys.exit(1)

    # Check database
    db_check = Path(str(args.database) + '.psq')
    if not db_check.exists():
        logger.error(f"BLAST database not found: {args.database}")
        logger.error("Run download_pdb_seq.py first to create the database")
        sys.exit(1)

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Output file
    output_file = args.output_dir / f"{args.organism}_pdb.out"

    # Run BLAST
    if args.queries:
        query_file = args.queries
    else:
        # Default query file location
        query_file = Path(f"data/sequences/{args.organism}_proteins.fasta")
        if not query_file.exists():
            query_file = Path(f"data/sequences/{args.organism}_proteins.fasta.gz")

        if not query_file.exists():
            logger.error(f"No query file found. Please provide query file as argument.")
            sys.exit(1)

    logger.info(f"Query file: {query_file}")
    logger.info(f"Database: {args.database}")
    logger.info(f"Output: {output_file}")

    hits = blast_sequences(
        query_file,
        args.database,
        output_file,
        args.evalue,
        args.threads,
    )

    logger.info(f"Processed {hits} BLAST hits")
    logger.info(f"Output written to {output_file}")


if __name__ == "__main__":
    main()
