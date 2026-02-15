#!/usr/bin/env python3
"""
Export sequence similarity data for FTP.

This script generates FTP files for sequence similarity data including:
- BLAST best hits
- UniProt PSI-BLAST results
- PDB homologs
- InterProScan domain data

Based on exportSeqSimilarityData.pl.

Usage:
    python export_seq_similarity_data.py pdb
    python export_seq_similarity_data.py domain
    python export_seq_similarity_data.py besthits
    python export_seq_similarity_data.py uniprot
    python export_seq_similarity_data.py --help

Arguments:
    source: Data source type (pdb, domain, besthits, uniprot)

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
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

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))

# Method constants
BEST_HITS_METHOD = "BLASTP"
UNIPROT_METHOD = "PSI-BLAST"
UNIPROT_SOURCE = "UniProt"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def convert_score(score: float | None) -> str:
    """Convert BLAST score to string format."""
    if score is None:
        return ""

    # Scientific notation for small values
    if score < 0.0001:
        return f"{score:.2e}"

    return f"{score:.4f}"


def copy_file_to_archive(ftp_dir: Path, file_name: str) -> None:
    """Copy a file to the archive directory with date stamp."""
    data_file = ftp_dir / file_name
    if not data_file.exists():
        logger.warning(f"File not found for archiving: {data_file}")
        return

    # Get current date
    now = datetime.now()
    date_stamp = now.strftime("%Y%m")

    archive_dir = ftp_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Build archive filename
    parts = file_name.rsplit(".", 1)
    if len(parts) == 2:
        archive_name = f"{parts[0]}.{date_stamp}.{parts[1]}"
    else:
        archive_name = f"{file_name}.{date_stamp}"

    archive_file = archive_dir / archive_name

    shutil.copy(str(data_file), str(archive_file))
    logger.info(f"Archived to {archive_file}")


def copy_domain_file_to_ftp(data_dir: Path, ftp_dir: Path) -> int:
    """Copy domain data file to FTP site."""
    domain_data_dir = data_dir / "domain"
    domain_file = "domains.tab"

    orig_file = domain_data_dir / domain_file
    if not orig_file.exists():
        logger.error(f"Domain file not found: {orig_file}")
        return 1

    ftp_domain_dir = ftp_dir / "sequence_similarity" / "domains"
    ftp_domain_dir.mkdir(parents=True, exist_ok=True)

    ftp_file = ftp_domain_dir / domain_file

    shutil.copy(str(orig_file), str(ftp_file))
    logger.info(f"Copied {orig_file} to {ftp_file}")

    copy_file_to_archive(ftp_domain_dir, domain_file)

    return 0


def create_pdb_homolog_file(session, ftp_dir: Path) -> int:
    """Create PDB homologs data file."""
    query = text(f"""
        SELECT PS1.sequence_name, PA.query_align_start_coord,
               PA.query_align_stop_coord, PA.target_align_start_coord,
               PA.target_align_stop_coord, PA.pct_aligned,
               PA.score, PS2.sequence_name, PS2.taxon_id, T.tax_term
        FROM {DB_SCHEMA}.pdb_sequence PS1
        JOIN {DB_SCHEMA}.pdb_alignment PA ON PS1.pdb_sequence_no = PA.query_seq_no
        JOIN {DB_SCHEMA}.pdb_sequence PS2 ON PA.target_seq_no = PS2.pdb_sequence_no
        LEFT JOIN {DB_SCHEMA}.taxonomy T ON PS2.taxon_id = T.taxon_id
        ORDER BY 1, 7
    """)

    results = session.execute(query).fetchall()

    ftp_pdb_dir = ftp_dir / "sequence_similarity" / "pdb_homologs"
    ftp_pdb_dir.mkdir(parents=True, exist_ok=True)

    pdb_file = "pdb_homologs.tab"
    out_file = ftp_pdb_dir / pdb_file

    # Write data
    with open(out_file, "w") as f:
        for row in results:
            (seq_name, query_start, query_stop, target_start, target_stop,
             pct_aligned, score, target_name, taxon_id, tax_term) = row

            score_str = convert_score(score)

            fields = [
                seq_name or "",
                str(query_start) if query_start else "",
                str(query_stop) if query_stop else "",
                str(target_start) if target_start else "",
                str(target_stop) if target_stop else "",
                str(pct_aligned) if pct_aligned else "",
                score_str,
                target_name or "",
                str(taxon_id) if taxon_id else "",
                tax_term or "",
            ]
            f.write("\t".join(fields) + "\n")

    logger.info(f"Wrote {len(results)} PDB homolog records to {out_file}")

    # Gzip the file
    gz_file = out_file.with_suffix(".tab.gz")
    with open(out_file, "rb") as f_in:
        with gzip.open(gz_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    out_file.unlink()
    logger.info(f"Compressed to {gz_file}")

    copy_file_to_archive(ftp_pdb_dir, f"{pdb_file}.gz")

    return 0


def create_file_from_homolog_table(session, source: str, ftp_dir: Path) -> int:
    """Create BLAST hits or UniProt PSI-BLAST data file."""
    if source == "besthits":
        where_clause = ""
        where_values = {"method": BEST_HITS_METHOD}
        ftp_subdir = "best_hits"
        file_name = "best_hits.tab"
        compress = False
    elif source == "uniprot":
        where_clause = "AND H.source = :source"
        where_values = {"method": UNIPROT_METHOD, "source": UNIPROT_SOURCE}
        ftp_subdir = "psi_blast"
        file_name = "psi_blast.tab"
        compress = True
    else:
        logger.error(f"Unknown source: {source}")
        return 1

    query = text(f"""
        SELECT F.feature_name, BA.query_start_coord,
               BA.query_stop_coord, BA.target_start_coord,
               BA.target_stop_coord, BA.pct_aligned, BA.score,
               BH.identifier, BH.taxon_id, T.tax_term
        FROM {DB_SCHEMA}.feature F
        JOIN {DB_SCHEMA}.blast_alignment BA ON F.feature_no = BA.query_no
        JOIN {DB_SCHEMA}.blast_hit BH ON BA.target_no = BH.blast_hit_no
        LEFT JOIN {DB_SCHEMA}.taxonomy T ON BH.taxon_id = T.taxon_id
        WHERE BA.method = :method
        {where_clause}
        ORDER BY 1, 7
    """)

    ftp_output_dir = ftp_dir / "sequence_similarity" / ftp_subdir
    ftp_output_dir.mkdir(parents=True, exist_ok=True)

    out_file = ftp_output_dir / file_name

    # Write data directly to file (can be large)
    count = 0
    with open(out_file, "w") as f:
        for row in session.execute(query, where_values):
            (feat_name, query_start, query_stop, target_start, target_stop,
             pct_aligned, score, identifier, taxon_id, tax_term) = row

            score_str = convert_score(score)

            fields = [
                feat_name or "",
                str(query_start) if query_start else "",
                str(query_stop) if query_stop else "",
                str(target_start) if target_start else "",
                str(target_stop) if target_stop else "",
                str(pct_aligned) if pct_aligned else "",
                score_str,
                identifier or "",
                str(taxon_id) if taxon_id else "",
                tax_term or "",
            ]
            f.write("\t".join(fields) + "\n")
            count += 1

    logger.info(f"Wrote {count} {source} records to {out_file}")

    if compress:
        # Gzip the file
        gz_file = out_file.with_suffix(".tab.gz")
        with open(out_file, "rb") as f_in:
            with gzip.open(gz_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        out_file.unlink()
        logger.info(f"Compressed to {gz_file}")
        copy_file_to_archive(ftp_output_dir, f"{file_name}.gz")
    else:
        copy_file_to_archive(ftp_output_dir, file_name)

    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export sequence similarity data for FTP"
    )
    parser.add_argument(
        "source",
        choices=["pdb", "domain", "besthits", "uniprot"],
        help="Data source type",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    source = args.source.lower()

    logger.info(f"Exporting {source} data...")

    # Handle domain file copy (no database needed)
    if source == "domain":
        return copy_domain_file_to_ftp(DATA_DIR, FTP_DIR)

    try:
        with SessionLocal() as session:
            if source == "pdb":
                return create_pdb_homolog_file(session, FTP_DIR)
            else:
                return create_file_from_homolog_table(session, source, FTP_DIR)

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
