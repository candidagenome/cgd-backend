#!/usr/bin/env python3
"""
Map proteins to UniProt (SwissProt/TrEMBL).

This script maps organism proteins to UniProt entries using:
1. Exact sequence matches
2. BLAST alignment for remaining sequences

Generates mapping files for database loading and Alliance submission.

Original Perl: MapUniProt.pl
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
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

load_dotenv()

logger = logging.getLogger(__name__)

# BLAST thresholds
E_VALUE = 1e-5
LENGTH_CUT = 95  # % length similarity
ALIGN_CUT = 95   # % alignment coverage
IDENT_CUT = 95   # % identity
GAP_CUT = 3      # % gaps


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def load_sequences(fasta_file: Path) -> dict[str, dict]:
    """
    Load sequences from FASTA file.

    Args:
        fasta_file: FASTA file (can be gzipped)

    Returns:
        Dict mapping ID to sequence info
    """
    sequences = {}

    open_func = gzip.open if str(fasta_file).endswith('.gz') else open
    mode = 'rt' if str(fasta_file).endswith('.gz') else 'r'

    with open_func(fasta_file, mode) as f:
        for record in SeqIO.parse(f, 'fasta'):
            seq = str(record.seq).rstrip('*')
            sequences[record.id] = {
                'id': record.id,
                'seq': seq,
                'length': len(seq),
                'description': record.description,
            }

    return sequences


def parse_uniprot_sequences(
    fasta_file: Path,
    target_strain: str = None,
) -> tuple[dict, dict, dict]:
    """
    Parse UniProt sequences and extract strain info.

    Args:
        fasta_file: UniProt FASTA file
        target_strain: Target strain to prioritize

    Returns:
        Tuple of (seq_to_id, id_to_strain, unique_seqs)
    """
    seq_to_id = {}
    id_to_strain = {}
    unique_seqs = {}

    open_func = gzip.open if str(fasta_file).endswith('.gz') else open
    mode = 'rt' if str(fasta_file).endswith('.gz') else 'r'

    with open_func(fasta_file, mode) as f:
        for record in SeqIO.parse(f, 'fasta'):
            # Parse UniProt ID
            up_id = record.id
            if '|' in up_id:
                parts = up_id.split('|')
                if len(parts) >= 2:
                    up_id = parts[1]

            seq = str(record.seq).rstrip('*')

            # Extract strain from description
            strain = 'other'
            desc = record.description
            if '(strain ' in desc.lower():
                import re
                match = re.search(r'\(strain ([^)]+)\)', desc, re.IGNORECASE)
                if match:
                    strain_list = match.group(1).split('/')
                    for s in strain_list:
                        s = s.strip()
                        if target_strain and s == target_strain:
                            strain = target_strain
                            break
                    if strain == 'other' and strain_list:
                        strain = 'NA'

            id_to_strain[up_id] = strain

            # Map sequence to ID (prioritize target strain)
            if seq not in seq_to_id:
                seq_to_id[seq] = up_id
                unique_seqs[up_id] = seq
            elif strain == target_strain:
                # Replace with target strain entry
                old_id = seq_to_id[seq]
                if id_to_strain.get(old_id) != target_strain:
                    seq_to_id[seq] = up_id
                    unique_seqs[up_id] = seq

    return seq_to_id, id_to_strain, unique_seqs


def write_fasta(sequences: dict, output_file: Path) -> None:
    """Write sequences to FASTA file."""
    with open(output_file, 'w') as f:
        for seq_id, seq in sequences.items():
            f.write(f">{seq_id}\n{seq}\n")


def run_blast(
    query_file: Path,
    database: Path,
    output_file: Path,
    evalue: float = E_VALUE,
    threads: int = 2,
) -> None:
    """Run BLASTP search."""
    cmd = [
        'blastp',
        '-query', str(query_file),
        '-db', str(database),
        '-out', str(output_file),
        '-outfmt', '5',  # XML
        '-evalue', str(evalue),
        '-num_threads', str(threads),
        '-threshold', '20',
        '-comp_based_stats', '0',
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"BLAST failed: {result.stderr}")


def format_blast_db(fasta_file: Path, db_name: Path) -> None:
    """Format BLAST database."""
    cmd = [
        'makeblastdb',
        '-in', str(fasta_file),
        '-dbtype', 'prot',
        '-out', str(db_name),
        '-parse_seqids',
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"makeblastdb failed: {result.stderr}")


def parse_blast_and_filter(
    blast_xml: Path,
    query_seqs: dict,
    target_seqs: dict,
    id_to_strain: dict,
) -> dict:
    """
    Parse BLAST results and filter by thresholds.

    Returns:
        Dict mapping query ID to best UniProt hit
    """
    matches = {}
    top_scores = {}

    for query_result in SearchIO.parse(str(blast_xml), 'blast-xml'):
        query_id = query_result.id.replace('lcl|', '')
        query_len = query_result.seq_len

        for hit in query_result:
            hit_id = hit.id.replace('lcl|', '')
            hit_len = hit.seq_len

            # Length filter
            len_ratio = 100 - (100 * abs(query_len - hit_len) / query_len)
            if len_ratio < LENGTH_CUT:
                continue

            for hsp in hit.hsps:
                # Calculate metrics
                q_aln = 100 * hsp.query_span / query_len
                h_aln = 100 * hsp.hit_span / hit_len
                gaps = 100 * hsp.gap_num / hsp.aln_span if hsp.aln_span else 0
                ident = 100 * hsp.ident_num / hsp.aln_span if hsp.aln_span else 0

                # Apply filters
                if q_aln < ALIGN_CUT:
                    continue
                if h_aln < ALIGN_CUT:
                    continue
                if gaps > GAP_CUT:
                    continue
                if ident < IDENT_CUT:
                    continue

                score = hsp.bitscore

                # Keep best hit
                if query_id not in top_scores or score > top_scores[query_id]:
                    matches[query_id] = {
                        'hit_id': hit_id,
                        'score': score,
                        'evalue': hsp.evalue,
                        'identity': ident,
                        'q_aln': q_aln,
                        'h_aln': h_aln,
                        'gaps': gaps,
                        'strain': id_to_strain.get(hit_id, 'other'),
                    }
                    top_scores[query_id] = score

                break  # Only first HSP

    return matches


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Map proteins to UniProt (SwissProt/TrEMBL)"
    )
    parser.add_argument(
        "--organism",
        required=True,
        help="Organism abbreviation",
    )
    parser.add_argument(
        "--queries",
        type=Path,
        required=True,
        help="Query protein FASTA file",
    )
    parser.add_argument(
        "--swissprot",
        type=Path,
        required=True,
        help="SwissProt FASTA file",
    )
    parser.add_argument(
        "--trembl",
        type=Path,
        required=True,
        help="TrEMBL FASTA file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/UniProt"),
        help="Output directory",
    )
    parser.add_argument(
        "--target-strain",
        help="Target strain name in UniProt (e.g., 'SC5314')",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=2,
        help="BLAST threads (default: 2)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load query sequences
    logger.info(f"Loading query sequences from {args.queries}")
    query_seqs = load_sequences(args.queries)
    logger.info(f"Loaded {len(query_seqs)} query sequences")

    # Load UniProt sequences
    logger.info(f"Loading SwissProt sequences from {args.swissprot}")
    sp_seq_to_id, sp_id_to_strain, sp_unique = parse_uniprot_sequences(
        args.swissprot, args.target_strain
    )
    logger.info(f"Loaded {len(sp_unique)} unique SwissProt sequences")

    logger.info(f"Loading TrEMBL sequences from {args.trembl}")
    tr_seq_to_id, tr_id_to_strain, tr_unique = parse_uniprot_sequences(
        args.trembl, args.target_strain
    )
    logger.info(f"Loaded {len(tr_unique)} unique TrEMBL sequences")

    # Find exact matches
    logger.info("Finding exact sequence matches")
    sp_matches = {}
    tr_matches = {}
    unmatched = {}

    for query_id, query_info in query_seqs.items():
        seq = query_info['seq']

        if seq in sp_seq_to_id:
            up_id = sp_seq_to_id[seq]
            if sp_id_to_strain.get(up_id) != 'other':
                sp_matches[query_id] = up_id

        if seq in tr_seq_to_id and query_id not in sp_matches:
            up_id = tr_seq_to_id[seq]
            if tr_id_to_strain.get(up_id) != 'other':
                tr_matches[query_id] = up_id

        if query_id not in sp_matches and query_id not in tr_matches:
            unmatched[query_id] = query_info

    logger.info(f"Exact matches: SwissProt={len(sp_matches)}, TrEMBL={len(tr_matches)}")
    logger.info(f"Remaining for BLAST: {len(unmatched)}")

    # BLAST unmatched sequences
    if unmatched:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Write unmatched queries
            query_file = tmpdir / 'queries.fasta'
            write_fasta({q: unmatched[q]['seq'] for q in unmatched}, query_file)

            # Write and format UniProt databases
            for name, unique_seqs, id_to_strain, matches_dict in [
                ('sp', sp_unique, sp_id_to_strain, sp_matches),
                ('tr', tr_unique, tr_id_to_strain, tr_matches),
            ]:
                db_fasta = tmpdir / f'{name}.fasta'
                write_fasta(unique_seqs, db_fasta)
                format_blast_db(db_fasta, tmpdir / name)

                # Run BLAST
                blast_out = tmpdir / f'{name}.xml'
                logger.info(f"Running BLAST against {name.upper()}")
                run_blast(query_file, tmpdir / name, blast_out, threads=args.threads)

                # Parse results
                blast_matches = parse_blast_and_filter(
                    blast_out, unmatched, unique_seqs, id_to_strain
                )

                for query_id, match in blast_matches.items():
                    if query_id not in sp_matches and query_id not in tr_matches:
                        matches_dict[query_id] = match['hit_id']

    # Write output files
    summary_file = args.output_dir / f"{args.organism}_UniProt_summary.tab"
    gp2protein_file = args.output_dir / f"{args.organism}_gp2protein.tab"
    sp_load_file = args.output_dir / f"{args.organism}_SwissProt_load.tab"
    tr_load_file = args.output_dir / f"{args.organism}_TrEMBL_load.tab"

    with open(summary_file, 'w') as f_sum, \
         open(gp2protein_file, 'w') as f_gp, \
         open(sp_load_file, 'w') as f_sp, \
         open(tr_load_file, 'w') as f_tr:

        f_sum.write("Query\tUniProt_Hit\tDatabase\tStrain\n")
        f_sp.write("Query Name\tMatch\n")
        f_tr.write("Query Name\tMatch\n")

        for query_id in sorted(query_seqs.keys()):
            if query_id in sp_matches:
                up_id = sp_matches[query_id]
                strain = sp_id_to_strain.get(up_id, 'NA')
                f_sum.write(f"{query_id}\t{up_id}\tSwissProt\t{strain}\n")
                f_sp.write(f"{query_id}\t{up_id}\n")
                f_gp.write(f"CGD:{query_id}\tUniProtKB:{up_id}\n")
            elif query_id in tr_matches:
                up_id = tr_matches[query_id]
                strain = tr_id_to_strain.get(up_id, 'NA')
                f_sum.write(f"{query_id}\t{up_id}\tTrEMBL\t{strain}\n")
                f_tr.write(f"{query_id}\t{up_id}\n")
                f_gp.write(f"CGD:{query_id}\tUniProtKB:{up_id}\n")
            else:
                f_sum.write(f"{query_id}\tNA\tNA\tNA\n")

    logger.info(f"Summary written to {summary_file}")
    logger.info(f"GP2Protein written to {gp2protein_file}")
    logger.info("Complete")


if __name__ == "__main__":
    main()
