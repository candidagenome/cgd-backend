#!/usr/bin/env python3
"""
Create intergenic (NOT feature) sequence file.

This script generates FASTA format sequences for all intergenic regions
that are greater than 1 nucleotide in length and not contained within
features whose immediate parent is 'chromosome'.

Based on createNOTFile.pl.

Usage:
    python create_intergenic_file.py
    python create_intergenic_file.py --debug
    python create_intergenic_file.py --help

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Directory for log files
    DATA_DIR: Directory for data files
    FTP_DIR: FTP directory for output files
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
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))

# Output directories
DATA_DUMP_DIR = DATA_DIR / "data_download" / "sequence" / "genomic_sequence"
FTP_INTERGENIC_DIR = FTP_DIR / "data_download" / "sequence" / "genomic_sequence" / "intergenic"

# Chromosome number to letter mapping
NUM_TO_LETTER = {
    1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "F",
    7: "G", 8: "H", 9: "I", 10: "J", 11: "K", 12: "L",
    13: "M", 14: "N", 15: "O", 16: "P", 17: "Q",
}

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


def get_features_for_intergenic(
    session,
    chrnum: int,
    start: int,
    stop: int,
) -> list[tuple]:
    """
    Get features in a chromosomal region for intergenic calculation.

    Returns list of tuples with:
    (feature_name, gene_name, start_coord, stop_coord, strand,
     feature_type, dbxref_id, headline)
    """
    query = text(f"""
        SELECT f.feature_name, f.gene_name, fl.start_coord, fl.stop_coord,
               fl.strand, f.feature_type, f.dbxref_id, f.headline
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.web_metadata wm ON f.feature_type = wm.col_value
        WHERE wm.application_name = 'Ftp Intergenic Sequence'
        AND wm.tab_name = 'FEATURE'
        AND wm.col_name = 'FEATURE_TYPE'
        AND fl.chromosome_no = :chrnum
        AND fl.start_coord >= :start
        AND fl.stop_coord <= :stop
        ORDER BY fl.start_coord
    """)

    return session.execute(
        query,
        {"chrnum": chrnum, "start": start, "stop": stop}
    ).fetchall()


def get_chromosome_sequence(
    session,
    chrnum: int,
    start: int,
    end: int,
) -> str | None:
    """
    Get sequence from a chromosome region.

    Args:
        session: Database session
        chrnum: Chromosome number
        start: Start coordinate
        end: End coordinate

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

    return result[0]


def create_intergenic_sequences(
    session,
    chrnum: int,
    chr_length: int,
) -> tuple[list[SeqRecord], int, int]:
    """
    Create intergenic sequences for a chromosome.

    Returns tuple of (records, intergenic_count, feature_count).
    """
    records = []
    intergenic_count = 0
    feature_count = 0

    # Get all features for this chromosome
    features = get_features_for_intergenic(session, chrnum, 1, chr_length)

    # Build a map of start -> (feature_name, stop) using the longest stop for each start
    start_to_info: dict[int, tuple[str, int]] = {}

    for row in features:
        feat_nm, _, start_coord, stop_coord, *_ = row
        feature_count += 1

        # Normalize coordinates
        if start_coord > stop_coord:
            start_coord, stop_coord = stop_coord, start_coord

        # Keep the longest feature at each start position
        if start_coord in start_to_info:
            old_nm, old_stop = start_to_info[start_coord]
            if stop_coord > old_stop:
                start_to_info[start_coord] = (feat_nm, stop_coord)
        else:
            start_to_info[start_coord] = (feat_nm, stop_coord)

    # Sort by start coordinate
    sorted_starts = sorted(start_to_info.keys())

    if len(sorted_starts) < 2:
        return records, intergenic_count, feature_count

    # Track the current "held" feature (the one we're finding gaps after)
    h_feat_nm, h_end = start_to_info[sorted_starts[0]]

    for i in range(1, len(sorted_starts)):
        beg = sorted_starts[i]
        feat_nm, end = start_to_info[beg]

        # Calculate intergenic region
        i_start = h_end + 1
        i_stop = beg - 1
        intergenic_length = i_stop - i_start + 1

        if intergenic_length >= 1:
            # Get sequence
            sequence = get_chromosome_sequence(session, chrnum, i_start, i_stop)

            if sequence:
                # Build ID and description
                chr_letter = NUM_TO_LETTER.get(chrnum, str(chrnum))
                chr_display = "Mito" if chrnum == 17 else chrnum

                seq_id = f"{chr_letter}:{i_start}-{i_stop},"
                desc = f"Chr {chr_display} from {i_start}-{i_stop}, between {h_feat_nm} and {feat_nm}"

                record = SeqRecord(
                    Seq(sequence),
                    id=seq_id,
                    description=desc,
                )
                records.append(record)
                intergenic_count += 1

        # Update held feature
        h_feat_nm = feat_nm
        h_end = end

    return records, intergenic_count, feature_count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create intergenic (NOT feature) sequence file"
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
    log_file = LOG_DIR / "ftp_intergenic_sequence_dump.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info("*" * 42)
    logger.info(f"Start execution: {datetime.now()}")

    logger.info(f"Destination directory: {FTP_INTERGENIC_DIR}")
    logger.info(f"Log file: {log_file}")
    logger.info("Recreating intergenic sequences...")

    try:
        with SessionLocal() as session:
            # Get chromosome lengths
            chr_lengths = get_chromosome_lengths(session)

            # Create output directories
            intergenic_dir = DATA_DUMP_DIR / "intergenic"
            intergenic_dir.mkdir(parents=True, exist_ok=True)
            FTP_INTERGENIC_DIR.mkdir(parents=True, exist_ok=True)

            # Collect all intergenic sequences
            all_records = []
            total_intergenic = 0
            total_features = 0

            logger.info("Summary of Intergenic Sequences recreated:")

            for chrnum in sorted(chr_lengths.keys()):
                chr_length = chr_lengths[chrnum]

                records, intergenic_count, feature_count = create_intergenic_sequences(
                    session, chrnum, chr_length
                )

                all_records.extend(records)
                total_intergenic += intergenic_count
                total_features += feature_count

                logger.info(f"Chr {chrnum}\t=>\t{intergenic_count}")

            logger.info(f"Total number of intergenic sequences: {total_intergenic}")
            logger.info(f"Total number of features: {total_features}")

            # Write FASTA file
            out_file = intergenic_dir / "NotFeature.fasta"
            with open(out_file, "w") as f:
                SeqIO.write(all_records, f, "fasta")

            logger.info(f"Wrote {len(all_records)} sequences to {out_file}")

            # Gzip and copy to FTP
            gz_file = FTP_INTERGENIC_DIR / "NotFeature.fasta.gz"
            with open(out_file, "rb") as f_in:
                with gzip.open(gz_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            logger.info(f"Compressed to {gz_file}")

            # Archive with date stamp
            now = datetime.now()
            date_stamp = now.strftime("%Y%m%d")

            archive_dir = FTP_INTERGENIC_DIR / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)

            archive_file = archive_dir / f"NotFeature.{date_stamp}.fasta.gz"
            shutil.copy(str(gz_file), str(archive_file))
            logger.info(f"Archived to {archive_file}")

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
