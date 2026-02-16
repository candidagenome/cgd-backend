#!/usr/bin/env python3
"""
Download PDB sequences from NCBI.

This script downloads the pdbaa file from NCBI, adds taxonomy information,
and creates a formatted BLAST database.

Part 1 of 3-part PDB pipeline:
    1. download_pdb_seq.py
    2. blast_pdb.py
    3. load_pdb.py

Original Perl: downloadPDBseq.pl
Converted to Python: 2024
"""

import argparse
import gzip
import logging
import re
import subprocess
import sys
from pathlib import Path
from urllib.request import urlretrieve

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord

logger = logging.getLogger(__name__)

# NCBI source URLs
PDB_SOURCE_URL = 'https://ftp.ncbi.nlm.nih.gov/blast/db/FASTA/pdbaa.gz'
TAX_SOURCE_URL = 'https://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/prot.accession2taxid.gz'


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def download_file(url: str, output_path: Path) -> None:
    """
    Download file from URL.

    Args:
        url: Source URL
        output_path: Local destination path
    """
    logger.info(f"Downloading {url}")
    urlretrieve(url, output_path)
    logger.info(f"Saved to {output_path}")


def collect_pdb_accessions(pdb_file: Path) -> dict[str, str]:
    """
    Collect all accessions from PDB FASTA file.

    Args:
        pdb_file: Path to pdbaa.gz file

    Returns:
        Dict mapping accession to 'NA' (placeholder for taxid)
    """
    accessions = {}

    open_func = gzip.open if str(pdb_file).endswith('.gz') else open
    mode = 'rt' if str(pdb_file).endswith('.gz') else 'r'

    with open_func(pdb_file, mode) as f:
        for record in SeqIO.parse(f, 'fasta'):
            # Extract accessions from header
            defline = f"{record.id} {record.description}"

            # Look for various ID patterns
            for match in re.finditer(r'\|([A-Z0-9_]+)\|', defline):
                acc = match.group(1)
                accessions[acc] = 'NA'

    logger.info(f"Collected {len(accessions)} accessions from PDB file")
    return accessions


def get_taxonomy_ids(
    tax_file: Path,
    accessions: dict[str, str],
) -> dict[str, str]:
    """
    Get taxonomy IDs for accessions.

    Args:
        tax_file: Path to accession2taxid file
        accessions: Dict to update with taxids

    Returns:
        Updated accessions dict
    """
    logger.info("Reading taxonomy information")

    open_func = gzip.open if str(tax_file).endswith('.gz') else open
    mode = 'rt' if str(tax_file).endswith('.gz') else 'r'

    found = 0
    with open_func(tax_file, mode) as f:
        # Skip header
        next(f, None)

        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                acc = parts[1]
                taxid = parts[2]

                if acc in accessions:
                    accessions[acc] = taxid
                    found += 1

    logger.info(f"Found taxonomy IDs for {found} accessions")
    return accessions


def reformat_pdb(
    pdb_file: Path,
    output_file: Path,
    tax_for_acc: dict[str, str],
) -> int:
    """
    Reformat PDB FASTA with taxonomy information.

    Args:
        pdb_file: Input pdbaa.gz file
        output_file: Output reformatted FASTA file
        tax_for_acc: Dict mapping accession to taxid

    Returns:
        Number of sequences written
    """
    logger.info("Reformatting PDB sequences with taxonomy info")

    seen = set()
    count = 0

    open_func = gzip.open if str(pdb_file).endswith('.gz') else open
    mode = 'rt' if str(pdb_file).endswith('.gz') else 'r'

    out_open = gzip.open if str(output_file).endswith('.gz') else open
    out_mode = 'wt' if str(output_file).endswith('.gz') else 'w'

    with open_func(pdb_file, mode) as f_in, out_open(output_file, out_mode) as f_out:
        for record in SeqIO.parse(f_in, 'fasta'):
            defline = f"{record.id} {record.description}"

            # Parse PDB entries from defline
            # Format: >pdb|XXXX|chain description
            for match in re.finditer(
                r'(?:gi\|(\d+)\|)?pdb\|(\w{4})\|(\w*)\s*(?:Chain (\w*))?[,\s]*(.*)$',
                defline
            ):
                gi = match.group(1) or ''
                pdb_id = match.group(2)
                chain = match.group(3) or match.group(4) or ''
                desc = match.group(5) or ''

                # Build unique ID
                seq_id = pdb_id
                if chain:
                    seq_id = f"{pdb_id}_{chain}"

                if seq_id in seen:
                    continue
                seen.add(seq_id)

                # Get taxid
                taxid = 'NA'
                if gi and gi in tax_for_acc:
                    taxid = tax_for_acc[gi]

                # Build new description
                new_desc = f"TaxID: {taxid} {desc.strip()}"

                # Write reformatted record
                new_record = SeqRecord(
                    record.seq,
                    id=seq_id,
                    description=new_desc,
                )
                SeqIO.write(new_record, f_out, 'fasta')
                count += 1

    logger.info(f"Wrote {count} reformatted sequences")
    return count


def format_blast_db(fasta_file: Path, db_name: Path) -> None:
    """
    Format BLAST database from FASTA file.

    Args:
        fasta_file: Input FASTA file
        db_name: Output database name
    """
    logger.info(f"Creating BLAST database: {db_name}")

    cmd = [
        'makeblastdb',
        '-in', str(fasta_file),
        '-dbtype', 'prot',
        '-out', str(db_name),
        '-title', db_name.stem,
        '-parse_seqids',
    ]

    # Handle gzipped input
    if str(fasta_file).endswith('.gz'):
        # Need to decompress first or use a pipe
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.fasta', delete=False) as tmp:
            tmp_path = Path(tmp.name)

        with gzip.open(fasta_file, 'rt') as f_in, open(tmp_path, 'w') as f_out:
            f_out.write(f_in.read())

        cmd[2] = str(tmp_path)

        result = subprocess.run(cmd, capture_output=True, text=True)
        tmp_path.unlink()
    else:
        result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"makeblastdb failed: {result.stderr}")
        raise RuntimeError("Failed to create BLAST database")

    logger.info("BLAST database created successfully")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download PDB sequences from NCBI and create BLAST database"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/pdb"),
        help="Output directory (default: data/pdb)",
    )
    parser.add_argument(
        "--blast-dir",
        type=Path,
        default=Path("data/blast_datasets"),
        help="BLAST database directory (default: data/blast_datasets)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download, use existing files",
    )
    parser.add_argument(
        "--skip-taxonomy",
        action="store_true",
        help="Skip taxonomy lookup",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Create directories
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.blast_dir.mkdir(parents=True, exist_ok=True)

    # File paths
    pdb_download = args.output_dir / 'pdbaa.gz'
    tax_download = args.output_dir / 'prot.accession2taxid.gz'
    pdb_modified = args.output_dir / 'pdb.fasta.gz'
    blast_db = args.blast_dir / 'pdb.fasta'

    # Download files
    if not args.skip_download:
        download_file(PDB_SOURCE_URL, pdb_download)

        if not args.skip_taxonomy:
            download_file(TAX_SOURCE_URL, tax_download)

    # Collect accessions and get taxonomy
    tax_for_acc = {}
    if not args.skip_taxonomy and tax_download.exists():
        tax_for_acc = collect_pdb_accessions(pdb_download)
        tax_for_acc = get_taxonomy_ids(tax_download, tax_for_acc)

    # Reformat PDB file
    reformat_pdb(pdb_download, pdb_modified, tax_for_acc)

    # Create BLAST database
    format_blast_db(pdb_modified, blast_db)

    logger.info("PDB download and formatting complete")


if __name__ == "__main__":
    main()
