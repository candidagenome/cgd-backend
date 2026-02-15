#!/usr/bin/env python3
"""
Create UTR sequence files.

This script generates FASTA format UTR (untranslated region) sequence files
for 3' and 5' UTRs of specified length.

Based on create_utr_seq_files.pl.

Usage:
    python create_utr_seq_files.py 500
    python create_utr_seq_files.py 1000
    python create_utr_seq_files.py --help

Arguments:
    utr_length: Length of UTR sequences to generate (e.g., 500, 1000, 2000)

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Directory for log files
    DATA_DIR: Directory for data files
"""

import argparse
import gzip
import logging
import os
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
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output directory
UTR_DIR = DATA_DIR / "data_download" / "sequence" / "genomic_sequence" / "utr"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_chromosome_lengths(session) -> dict[int, int]:
    """Get lengths of all chromosomes."""
    query = text(f"""
        SELECT f.chromosome_no, f.stop_coord
        FROM {DB_SCHEMA}.feature f
        WHERE f.feature_type = 'chromosome'
    """)

    lengths = {}
    for row in session.execute(query).fetchall():
        lengths[row[0]] = row[1]

    return lengths


def get_chromosome_number_to_roman(session) -> dict[int, str]:
    """Get mapping of chromosome numbers to Roman numerals."""
    query = text(f"""
        SELECT chromosome_no, chr_roman
        FROM {DB_SCHEMA}.chromosome
    """)

    mapping = {}
    for row in session.execute(query).fetchall():
        mapping[row[0]] = row[1]

    return mapping


def get_feature_qualifiers(session) -> dict[str, str]:
    """Get feature qualifiers (Verified, Uncharacterized, etc.) for all features."""
    query = text(f"""
        SELECT UPPER(f.feature_name), fp.property_value
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
        WHERE fp.property_type = 'Feature Qualifier'
    """)

    qualifiers = {}
    for row in session.execute(query).fetchall():
        qualifiers[row[0]] = row[1]

    return qualifiers


def get_features_for_utr(session) -> list[tuple]:
    """
    Get all features for UTR sequence generation.

    Returns list of tuples with:
    (feature_no, feature_name, gene_name, chrnum, start, stop, strand,
     feature_type, dbxref_id, headline)
    """
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, fl.chromosome_no,
               fl.start_coord, fl.stop_coord, fl.strand, f.feature_type,
               f.dbxref_id, f.headline
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.web_metadata wm ON f.feature_type = wm.col_value
        WHERE wm.application_name = 'Ftp ORF Sequence'
        AND wm.tab_name = 'FEATURE'
        AND wm.col_name = 'FEATURE_TYPE'
        ORDER BY fl.chromosome_no, fl.start_coord
    """)

    return session.execute(query).fetchall()


def get_chromosome_sequence(session, chrnum: int, start: int, end: int, rev: bool = False) -> str | None:
    """
    Get sequence from a chromosome region.

    Args:
        session: Database session
        chrnum: Chromosome number
        start: Start coordinate
        end: End coordinate
        rev: Whether to return reverse complement

    Returns:
        Sequence string or None if not found
    """
    query = text(f"""
        SELECT SUBSTR(s.residues, :start, :length)
        FROM {DB_SCHEMA}.sequence s
        JOIN {DB_SCHEMA}.feature f ON s.seq_no = f.seq_no
        WHERE f.chromosome_no = :chrnum
        AND f.feature_type = 'chromosome'
    """)

    length = end - start + 1
    result = session.execute(
        query,
        {"chrnum": chrnum, "start": start, "length": length}
    ).fetchone()

    if not result or not result[0]:
        return None

    sequence = result[0]

    if rev:
        # Return reverse complement
        seq_obj = Seq(sequence)
        sequence = str(seq_obj.reverse_complement())

    return sequence


def format_feature_type(types: str) -> str:
    """Format feature type string, removing duplicates and prioritizing non-ORF types."""
    types = types.strip("|")

    if "|" not in types:
        return types

    type_list = types.split("|")
    seen = set()
    new_types = []

    for t in type_list:
        if t in seen:
            continue
        seen.add(t)

        if "ORF" in t.upper():
            new_types.append(t)
        else:
            new_types.insert(0, t)

    return " ".join(new_types)


def create_utr3_sequences(
    session,
    features: list[tuple],
    chr_lengths: dict[int, int],
    num2rom: dict[int, str],
    qualifiers: dict[str, str],
    utr_length: int,
    output_file: Path,
) -> int:
    """
    Create 3' UTR sequences.

    Returns count of sequences written.
    """
    count = 0
    records = []

    for row in features:
        (feat_no, feat_nm, gene_nm, chrnum, start, stop, strand,
         feat_type, dbxref_id, headline) = row

        # Check qualifier
        qualifier = qualifiers.get(feat_nm.upper(), "")
        if "merged" in qualifier.lower() or "deleted" in qualifier.lower():
            continue

        if qualifier:
            feat_type = f"{qualifier}|{feat_type}"

        chr_length = chr_lengths.get(chrnum, 0)
        dbxref_str = f"{PROJECT_ACRONYM}ID:{dbxref_id}"

        # Calculate 3' UTR coordinates
        rev = False
        reverse_str = ""

        if strand == "C":
            if start < stop:
                start, stop = stop, start
            utr_stop = stop - 1
            utr_start = stop - utr_length
            reverse_str = " (revcom)"
            rev = True
        else:
            utr_start = stop + 1
            utr_stop = stop + utr_length

        # Boundary checks
        if utr_start <= 0:
            utr_start = 1
        if utr_stop > chr_length:
            utr_stop = chr_length

        calc_length = utr_stop - utr_start + 1
        if calc_length < 1:
            continue

        # Build description
        chr_roman = num2rom.get(chrnum, str(chrnum))
        desc = f"{dbxref_str} 3' untranslated region, Chr {chr_roman} {utr_start} - {utr_stop}"
        if reverse_str:
            desc += reverse_str
        desc += f", {calc_length} bp"
        if feat_type:
            desc += f", {format_feature_type(feat_type)}"
        if headline:
            desc += f', "{headline}"'

        # Get sequence
        sequence = get_chromosome_sequence(session, chrnum, utr_start, utr_stop, rev)
        if not sequence:
            continue

        # Create record
        record = SeqRecord(
            Seq(sequence),
            id=feat_nm,
            description=desc,
        )
        records.append(record)
        count += 1

    # Write to gzipped file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_file, "wt") as f:
        SeqIO.write(records, f, "fasta")

    return count


def create_utr5_sequences(
    session,
    features: list[tuple],
    chr_lengths: dict[int, int],
    num2rom: dict[int, str],
    qualifiers: dict[str, str],
    utr_length: int,
    output_file: Path,
) -> int:
    """
    Create 5' UTR sequences.

    Returns count of sequences written.
    """
    count = 0
    records = []

    for row in features:
        (feat_no, feat_nm, gene_nm, chrnum, start, stop, strand,
         feat_type, dbxref_id, headline) = row

        # Check qualifier
        qualifier = qualifiers.get(feat_nm.upper(), "")
        if "merged" in qualifier.lower() or "deleted" in qualifier.lower():
            continue

        if qualifier:
            feat_type = f"{qualifier}|{feat_type}"

        chr_length = chr_lengths.get(chrnum, 0)
        dbxref_str = f"{PROJECT_ACRONYM}ID:{dbxref_id}"

        # Calculate 5' UTR coordinates
        rev = False
        reverse_str = ""

        if strand == "C":
            if start < stop:
                start, stop = stop, start
            utr_start = start + 1
            utr_stop = start + utr_length
            reverse_str = " (revcom)"
            rev = True
        else:
            utr_start = start - utr_length
            utr_stop = start - 1

        # Boundary checks
        if utr_start <= 0:
            utr_start = 1
        if utr_stop > chr_length:
            utr_stop = chr_length

        calc_length = utr_stop - utr_start + 1
        if calc_length < 1:
            continue

        # Build description
        chr_roman = num2rom.get(chrnum, str(chrnum))
        desc = f"{dbxref_str} 5' untranslated region, Chr {chr_roman} {utr_start} - {utr_stop}"
        if reverse_str:
            desc += reverse_str
        desc += f", {calc_length} bp"
        if feat_type:
            desc += f", {format_feature_type(feat_type)}"
        if headline:
            desc += f', "{headline}"'

        # Get sequence
        sequence = get_chromosome_sequence(session, chrnum, utr_start, utr_stop, rev)
        if not sequence:
            continue

        # Create record
        record = SeqRecord(
            Seq(sequence),
            id=feat_nm,
            description=desc,
        )
        records.append(record)
        count += 1

    # Write to gzipped file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_file, "wt") as f:
        SeqIO.write(records, f, "fasta")

    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create UTR sequence files"
    )
    parser.add_argument(
        "utr_length",
        type=int,
        help="Length of UTR sequences to generate (e.g., 500, 1000, 2000)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    utr_length = args.utr_length

    # Set up log file
    log_file = LOG_DIR / "utr_seq_dump.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info("*" * 42)
    logger.info(f"Start execution: {datetime.now()}")

    logger.info(f"Destination directory: {UTR_DIR}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Creating 3' and 5' {utr_length} UTR sequence files...")

    try:
        with SessionLocal() as session:
            # Get chromosome data
            chr_lengths = get_chromosome_lengths(session)
            num2rom = get_chromosome_number_to_roman(session)
            qualifiers = get_feature_qualifiers(session)

            # Get features
            features = get_features_for_utr(session)
            logger.info(f"Found {len(features)} features")

            # Create output directory
            UTR_DIR.mkdir(parents=True, exist_ok=True)

            # Create 3' UTR file
            utr3_file = UTR_DIR / f"utr3_sc_{utr_length}.fasta.gz"
            utr3_count = create_utr3_sequences(
                session, features, chr_lengths, num2rom, qualifiers,
                utr_length, utr3_file
            )
            logger.info(f"Total {utr_length} 3' UTR sequences: {utr3_count}")

            # Create 5' UTR file
            utr5_file = UTR_DIR / f"utr5_sc_{utr_length}.fasta.gz"
            utr5_count = create_utr5_sequences(
                session, features, chr_lengths, num2rom, qualifiers,
                utr_length, utr5_file
            )
            logger.info(f"Total {utr_length} 5' UTR sequences: {utr5_count}")

            logger.info(f"Done creating 3' and 5' {utr_length} UTR sequence files")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    logger.info(f"Stop execution: {datetime.now()}")
    logger.info("*" * 42)

    return 0


if __name__ == "__main__":
    sys.exit(main())
