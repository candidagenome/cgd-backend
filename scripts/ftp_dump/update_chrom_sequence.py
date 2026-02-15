#!/usr/bin/env python3
"""
Update chromosome sequence files.

This script updates GCG and FASTA sequence files for a given chromosome.
It retrieves the sequence from the database and writes it in the appropriate
formats, then archives the old files.

Based on updateChromSequence.pl.

Usage:
    python update_chrom_sequence.py <chrnum>
    python update_chrom_sequence.py 3
    python update_chrom_sequence.py 2-micron
    python update_chrom_sequence.py --help

Arguments:
    chrnum: Chromosome number (1-17) or '2-micron'

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    FTP_DIR: FTP directory for output files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)
    ORGANISM_NAME: Organism name (e.g., Candida albicans)
    STRAIN_NAME: Strain name (e.g., SC5314)

Output Files:
    data_download/sequence/genomic_sequence/chromosomes/gcg/chrXX.gcg
    data_download/sequence/NCBI_genome_source/chrXX.fsa
"""

import argparse
import gzip
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
ORGANISM_NAME = os.getenv("ORGANISM_NAME", "Candida albicans")
STRAIN_NAME = os.getenv("STRAIN_NAME", "SC5314")

# Sequence type
SEQ_TYPE = "genomic"
IS_SEQ_CURRENT = "Y"
DBXREF_SOURCE = "NCBI"
DBXREF_TYPE = "RefSeq Accession"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_chromosome_number_to_roman(session) -> dict[int, str]:
    """Get mapping of chromosome numbers to Roman numerals."""
    query = text(f"""
        SELECT feature_name, dbxref_id
        FROM {DB_SCHEMA}.feature
        WHERE feature_type = 'chromosome'
    """)

    # Standard mapping
    num_to_roman = {
        1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
        6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
        11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV",
        16: "XVI", 17: "Mito"
    }

    return num_to_roman


def retrieve_sequence(session, chrnum: str) -> tuple[str, int, int]:
    """
    Retrieve chromosome sequence from database.

    Returns tuple of (sequence, feature_no, seq_no).
    """
    # Get feature info
    feat_query = text(f"""
        SELECT f.feature_no, fl.stop_coord
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        WHERE f.feature_name = :chrnum
        AND f.feature_type = 'chromosome'
    """)

    result = session.execute(feat_query, {"chrnum": chrnum}).fetchone()
    if not result:
        raise ValueError(f"Chromosome {chrnum} not found in database")

    feature_no, stop_coord = result

    # Get sequence
    seq_query = text(f"""
        SELECT s.seq_no, s.residues
        FROM {DB_SCHEMA}.seq s
        WHERE s.feature_no = :feature_no
        AND s.seq_type = :seq_type
        AND s.is_seq_current = :is_current
    """)

    result = session.execute(
        seq_query,
        {"feature_no": feature_no, "seq_type": SEQ_TYPE, "is_current": IS_SEQ_CURRENT}
    ).fetchone()

    if not result:
        raise ValueError(f"No {SEQ_TYPE} sequence found for chromosome {chrnum}")

    seq_no, residues = result

    return residues, feature_no, seq_no


def get_ncbi_accession(session, feature_no: int) -> str | None:
    """Get NCBI RefSeq accession for a feature."""
    query = text(f"""
        SELECT d.dbxref_id
        FROM {DB_SCHEMA}.dbxref d
        JOIN {DB_SCHEMA}.dbxref_feat df ON d.dbxref_no = df.dbxref_no
        WHERE d.source = :source
        AND d.dbxref_type = :dbxref_type
        AND df.feature_no = :feature_no
    """)

    result = session.execute(
        query,
        {"source": DBXREF_SOURCE, "dbxref_type": DBXREF_TYPE, "feature_no": feature_no}
    ).fetchone()

    return result[0] if result else None


def get_reference_info(session, feature_no: int) -> dict | None:
    """Get reference information for chromosome."""
    query = text(f"""
        SELECT r.dbxref_id, r.pubmed, r.citation
        FROM {DB_SCHEMA}.reference r
        JOIN {DB_SCHEMA}.reflink rl ON r.reference_no = rl.reference_no
        WHERE rl.tab_name = 'FEATURE'
        AND rl.col_name = 'FEATURE_NO'
        AND rl.primary_key = :feature_no
    """)

    result = session.execute(query, {"feature_no": feature_no}).fetchone()

    if not result:
        return None

    return {
        "dbxref_id": result[0],
        "pubmed": result[1],
        "citation": result[2],
    }


def archive_sequence(chrfile: Path, seq_no: int = None) -> None:
    """Archive the sequence file."""
    if not chrfile.exists():
        return

    now = datetime.now()
    date_str = now.strftime("%Y%m%d")

    # Create archive path
    archive_dir = chrfile.parent / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Archive filename with date
    suffix = chrfile.suffix
    archive_file = archive_dir / f"{chrfile.stem}.{date_str}{suffix}"

    try:
        shutil.copy(str(chrfile), str(archive_file))

        # Gzip the archive
        with open(archive_file, "rb") as f_in:
            with gzip.open(f"{archive_file}.gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        archive_file.unlink()
        logger.info(f"Archived {chrfile} to {archive_file}.gz")

    except Exception as e:
        logger.error(f"Error archiving {chrfile}: {e}")


def write_gcg_file(
    sequence: str,
    chrnum: str,
    chrname: str,
    output_file: Path
) -> None:
    """Write sequence in GCG format."""
    # Determine chromosome identifier
    if chrnum == "17":
        chr_id = "chrMt"
    elif "micron" in chrnum.lower():
        chr_id = "2micron"
    else:
        chr_id = f"chr{int(chrnum):02d}"

    # Create description
    description = f"Chromosome {chrname} Sequence\n\n{chr_id}.gcg"

    # Create SeqRecord
    record = SeqRecord(
        Seq(sequence),
        id=chr_id,
        description=description,
    )

    # Write in GCG format
    # Note: BioPython doesn't have native GCG support, so we write a simplified version
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file = output_file.with_suffix(".gcg.new")

    with open(temp_file, "w") as fh:
        fh.write(f"!!NA_SEQUENCE 1.0\n")
        fh.write(f"{description}\n")
        fh.write(f"\n")
        fh.write(f"{chr_id}  Length: {len(sequence)}  Type: N  Check: 0\n")
        fh.write(f"\n")

        # Write sequence in 50-char lines with position numbers
        pos = 1
        for i in range(0, len(sequence), 50):
            chunk = sequence[i:i+50]
            # Format into groups of 10
            formatted = " ".join([chunk[j:j+10] for j in range(0, len(chunk), 10)])
            fh.write(f"{pos:8d}  {formatted}\n")
            pos += len(chunk)

    # Move to final location
    shutil.move(str(temp_file), str(output_file))
    output_file.chmod(0o444)

    logger.info(f"Created {output_file}")


def write_fasta_file(
    sequence: str,
    chrnum: str,
    chrname: str,
    accession: str | None,
    output_file: Path
) -> None:
    """Write sequence in FASTA format."""
    # Build identifier
    if accession:
        fasta_id = f"ref|{accession}|"
    else:
        fasta_id = f"{PROJECT_ACRONYM}|chr{chrnum}|"

    # Build description based on chromosome type
    if chrnum == "17":
        chr_str = "chrmt"
        desc = f"[org={ORGANISM_NAME}] [strain={STRAIN_NAME}] [moltype=genomic] [location=mitochondrion] [top=circular]"
    elif "micron" in chrnum.lower():
        chr_str = "2micron"
        desc = f"{ORGANISM_NAME} 2 micron circle plasmid, complete sequence"
    else:
        chr_str = f"chr{int(chrnum):02d}"
        desc = f"[org={ORGANISM_NAME}] [strain={STRAIN_NAME}] [moltype=genomic] [chromosome={chrname}]"

    full_id = f"{fasta_id} {desc}"

    # Create SeqRecord
    record = SeqRecord(
        Seq(sequence),
        id=full_id,
        description="",
    )

    # Write in FASTA format
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file = output_file.with_suffix(".fsa.new")

    with open(temp_file, "w") as fh:
        SeqIO.write(record, fh, "fasta")

    # Move to final location
    shutil.move(str(temp_file), str(output_file))
    output_file.chmod(0o444)

    logger.info(f"Created {output_file}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update chromosome sequence files"
    )
    parser.add_argument(
        "chrnum",
        help="Chromosome number (1-17) or '2-micron'",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    chrnum = args.chrnum

    # Output directories
    ftp_seq_dir = FTP_DIR / "data_download" / "sequence"
    gcg_dir = ftp_seq_dir / "genomic_sequence" / "chromosomes" / "gcg"
    fasta_dir = ftp_seq_dir / "NCBI_genome_source"

    try:
        with SessionLocal() as session:
            # Get chromosome number to Roman numeral mapping
            num_to_roman = get_chromosome_number_to_roman(session)

            # Get chrname (Roman numeral)
            if chrnum.isdigit():
                chrname = num_to_roman.get(int(chrnum), chrnum)
            else:
                chrname = chrnum

            logger.info(f"Processing chromosome {chrnum} ({chrname})")

            # Retrieve sequence
            logger.info("Retrieving sequence from database...")
            sequence, feature_no, seq_no = retrieve_sequence(session, chrnum)
            logger.info(f"Retrieved sequence of length {len(sequence)}")

            # Get NCBI accession
            accession = get_ncbi_accession(session, feature_no)
            if accession:
                logger.info(f"NCBI accession: {accession}")

            # Determine output file names
            if chrnum == "17":
                gcg_file = gcg_dir / "chrMt.gcg"
                fasta_file = fasta_dir / "chrmt.fsa"
            elif "micron" in chrnum.lower():
                gcg_file = gcg_dir / "2micron.gcg"
                fasta_file = fasta_dir / "2micron.fsa"
            else:
                chr_num_padded = f"{int(chrnum):02d}"
                gcg_file = gcg_dir / f"chr{chr_num_padded}.gcg"
                fasta_file = fasta_dir / f"chr{chr_num_padded}.fsa"

            # Write GCG format
            logger.info("Writing GCG format...")
            write_gcg_file(sequence, chrnum, chrname, gcg_file)
            archive_sequence(gcg_file)

            # Write FASTA format
            logger.info("Writing FASTA format...")
            write_fasta_file(sequence, chrnum, chrname, accession, fasta_file)
            archive_sequence(fasta_file, seq_no)

            logger.info("Chromosome sequence update complete")

    except ValueError as e:
        logger.error(str(e))
        return 1
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
