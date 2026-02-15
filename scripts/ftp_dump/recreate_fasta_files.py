#!/usr/bin/env python3
"""
Recreate FASTA sequence files from chromosome sequences.

This script generates FASTA format sequence files for ORFs, RNAs, and
other features using coordinates from the database and chromosome sequences.

Based on recreateFASTAFilesFromChr.pl.

Usage:
    python recreate_fasta_files.py ORF
    python recreate_fasta_files.py RNA
    python recreate_fasta_files.py Other
    python recreate_fasta_files.py --help

Arguments:
    seq_type: Sequence data type (ORF, RNA, or Other)

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    LOG_DIR: Directory for log files
    DATA_DIR: Directory for data files
    FTP_DIR: FTP directory for output files
    PROJECT_ACRONYM: Project acronym (e.g., CGD)

Output Files:
    ORF:
        - orf_dna/orf_genomic.fasta.gz
        - orf_dna/orf_genomic_all.fasta.gz
        - orf_dna/orf_genomic_dubious.fasta.gz
        - orf_dna/orf_genomic_1000.fasta.gz
        - orf_dna/orf_coding.fasta.gz
        - orf_protein/orf_trans.fasta.gz

    RNA:
        - rna/rna_genomic.fasta.gz
        - rna/rna_genomic_1000.fasta.gz
        - rna/rna_coding.fasta.gz

    Other:
        - other_features/other_features_genomic.fasta.gz
        - other_features/other_features_genomic_1000.fasta.gz
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
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output directories
DATA_DUMP_DIR = DATA_DIR / "data_download" / "sequence" / "genomic_sequence"
FTP_SEQ_DIR = FTP_DIR / "data_download" / "sequence" / "genomic_sequence"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


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


def get_subfeature_info(session) -> dict[str, str]:
    """Get subfeature info (exons, introns, CDS) for all features."""
    query = text(f"""
        SELECT UPPER(f.feature_name),
               sf.subfeature_type || '|' || sf.start_coord || '|' ||
               sf.stop_coord || '|' || sf.strand
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.subfeature sf ON f.feature_no = sf.feature_no
        ORDER BY f.feature_name, sf.start_coord
    """)

    # Build pipe-delimited string for each feature
    subfeat_info: dict[str, list[str]] = {}
    for row in session.execute(query).fetchall():
        feat_nm, info = row
        if feat_nm not in subfeat_info:
            subfeat_info[feat_nm] = []
        subfeat_info[feat_nm].append(info)

    return {k: "|".join(v) for k, v in subfeat_info.items()}


def get_reserved_gene_names(session) -> set[int]:
    """Get feature numbers with reserved gene names."""
    query = text(f"""
        SELECT feature_no
        FROM {DB_SCHEMA}.gene_reservation
        WHERE date_standardized IS NULL
    """)

    return {row[0] for row in session.execute(query).fetchall()}


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


def get_chromosome_length(session, chrnum: int) -> int:
    """Get length of a chromosome."""
    query = text(f"""
        SELECT stop_coord
        FROM {DB_SCHEMA}.feature
        WHERE chromosome_no = :chrnum
        AND feature_type = 'chromosome'
    """)

    result = session.execute(query, {"chrnum": chrnum}).fetchone()
    return result[0] if result else 0


def get_features_for_ftp_dump(session, app_name: str) -> list[tuple]:
    """
    Get features for FTP sequence dump.

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
        WHERE wm.application_name = :app_name
        AND wm.tab_name = 'FEATURE'
        AND wm.col_name = 'FEATURE_TYPE'
        ORDER BY fl.chromosome_no, fl.start_coord
    """)

    return session.execute(query, {"app_name": app_name}).fetchall()


def get_sequence(session, feature_name: str, seq_type: str) -> str | None:
    """Get sequence for a feature (genomic, coding, or protein)."""
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.sequence s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        WHERE f.feature_name = :name
        AND s.seq_type = :seq_type
        AND s.is_seq_current = 'Y'
    """)

    result = session.execute(
        query,
        {"name": feature_name, "seq_type": seq_type}
    ).fetchone()

    return result[0] if result else None


def get_chromosome_sequence_region(
    session,
    chrnum: int,
    start: int,
    end: int,
    reverse: bool = False,
) -> str | None:
    """Get sequence region from chromosome."""
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

    if reverse:
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


def build_description(
    gene_nm: str,
    feat_nm: str,
    dbxref_id: str,
    chrnum: int,
    coord_str: str,
    strand: str,
    feat_type: str | None,
    headline: str | None,
    num2rom: dict[int, str],
    extra: str = "",
) -> str:
    """Build FASTA description line."""
    # Display name
    name = gene_nm or feat_nm

    # Build dbxref
    dbxref_str = f"{PROJECT_ACRONYM}ID:{dbxref_id}"

    # Chromosome or plasmid
    if str(chrnum).lower() == "micron":
        chr_str = "2-micron plasmid"
    else:
        chr_str = f"Chr {num2rom.get(chrnum, str(chrnum))}"

    desc = f"{name} {dbxref_str}, {chr_str} from {coord_str}"

    # Reverse complement
    if strand == "C":
        desc += ", reverse complement"

    # Extra info (e.g., intron removed)
    if extra:
        desc += extra

    # Feature type
    if feat_type:
        desc += f", {format_feature_type(feat_type)}"

    # Headline
    if headline:
        desc += f', "{headline}"'

    return desc


def write_fasta_file(records: list[SeqRecord], output_file: Path) -> None:
    """Write FASTA records to gzipped file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(output_file, "wt") as f:
        SeqIO.write(records, f, "fasta")


def archive_file(ftp_dir: Path, filename: str) -> None:
    """Copy file to archive directory with date stamp."""
    src_file = ftp_dir / filename

    if not src_file.exists():
        return

    now = datetime.now()
    date_stamp = now.strftime("%Y%m%d")

    archive_dir = ftp_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Build archive filename
    parts = filename.rsplit(".", 2)
    if len(parts) >= 2:
        base = parts[0]
        archive_name = f"{base}.{date_stamp}.fasta.gz"
    else:
        archive_name = f"{filename}.{date_stamp}"

    archive_file = archive_dir / archive_name
    shutil.copy(str(src_file), str(archive_file))


def create_orf_files(
    session,
    qualifiers: dict[str, str],
    subfeat_info: dict[str, str],
    reserved_genes: set[int],
    num2rom: dict[int, str],
) -> dict[str, int]:
    """Create ORF sequence files (genomic, coding, protein)."""
    counts = {
        "genomic": 0, "genomic_dubious": 0, "genomic_all": 0,
        "genomic_1k": 0, "genomic_1k_dubious": 0, "genomic_1k_all": 0,
        "coding": 0, "coding_dubious": 0, "coding_all": 0,
        "protein": 0, "protein_dubious": 0, "protein_all": 0,
    }

    # Record collections
    records = {
        "genomic": [], "genomic_dubious": [], "genomic_all": [],
        "genomic_1k": [], "genomic_1k_dubious": [], "genomic_1k_all": [],
        "coding": [], "coding_dubious": [], "coding_all": [],
        "protein": [], "protein_dubious": [], "protein_all": [],
    }

    features = get_features_for_ftp_dump(session, "Ftp ORF Sequence")
    logger.info(f"Processing {len(features)} ORF features...")

    for row in features:
        (feat_no, feat_nm, gene_nm, chrnum, start, stop, strand,
         feat_type, dbxref_id, headline) = row

        # Check qualifier
        qualifier = qualifiers.get(feat_nm.upper(), "")
        if "merged" in qualifier.lower() or "deleted" in qualifier.lower():
            continue

        # Add qualifier to feature type
        if qualifier:
            feat_type = f"{qualifier}|{feat_type}"

        # Use feature name if gene name is reserved
        if not gene_nm or feat_no in reserved_genes:
            gene_nm = feat_nm

        # Determine category
        is_dubious = "dubious" in qualifier.lower()
        is_pseudogene = "pseudogene" in feat_type.lower() if feat_type else False

        # Build coordinate string
        if strand == "C" and start < stop:
            coord_str = f"{stop}-{start}"
        else:
            coord_str = f"{start}-{stop}"

        # Get genomic sequence
        genomic_seq = get_sequence(session, feat_nm, "genomic")
        if genomic_seq:
            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum, coord_str, strand,
                feat_type, headline, num2rom
            )
            record = SeqRecord(Seq(genomic_seq), id=feat_nm, description=desc)

            records["genomic_all"].append(record)
            counts["genomic_all"] += 1

            if not is_pseudogene:
                if is_dubious:
                    records["genomic_dubious"].append(record)
                    counts["genomic_dubious"] += 1
                else:
                    records["genomic"].append(record)
                    counts["genomic"] += 1

        # Get genomic +1000bp sequence
        h_start, h_stop = (start, stop) if start <= stop else (stop, start)
        h_start = max(1, h_start - 1001)
        chr_length = get_chromosome_length(session, chrnum)
        h_stop = min(chr_length, h_stop + 1000)

        genomic_1k_seq = get_chromosome_sequence_region(
            session, chrnum, h_start, h_stop, reverse=(strand == "C")
        )
        if genomic_1k_seq:
            if strand == "C":
                g1k_coord = f"{h_stop}-{h_start}"
            else:
                g1k_coord = f"{h_start}-{h_stop}"

            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum, g1k_coord, strand,
                feat_type, headline, num2rom
            )
            record = SeqRecord(Seq(genomic_1k_seq), id=feat_nm, description=desc)

            records["genomic_1k_all"].append(record)
            counts["genomic_1k_all"] += 1

            if not is_pseudogene:
                if is_dubious:
                    records["genomic_1k_dubious"].append(record)
                    counts["genomic_1k_dubious"] += 1
                else:
                    records["genomic_1k"].append(record)
                    counts["genomic_1k"] += 1

        # Get coding sequence
        coding_seq = get_sequence(session, feat_nm, "coding")
        if coding_seq:
            # Build coordinate string from subfeatures
            sf_info = subfeat_info.get(feat_nm.upper(), "")
            extra = ""
            coords = []

            if sf_info:
                parts = sf_info.split("|")
                for i in range(0, len(parts), 4):
                    if i + 2 >= len(parts):
                        break
                    sf_type = parts[i]
                    sf_start = parts[i + 1]
                    sf_stop = parts[i + 2]

                    if sf_type.upper() in ("CDS", "EXON"):
                        if strand == "C":
                            coords.append(f"{sf_stop}-{sf_start}")
                        else:
                            coords.append(f"{sf_start}-{sf_stop}")
                    elif "intron" in sf_type.lower():
                        extra = ", intron sequence removed"
                    elif "frameshift" in sf_type.lower():
                        extra = ", one base removed to allow translational frameshift"

            if strand == "C":
                coords.reverse()

            coding_coord = ",".join(coords) if coords else coord_str

            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum, coding_coord, strand,
                feat_type, headline, num2rom, extra
            )
            record = SeqRecord(Seq(coding_seq), id=feat_nm, description=desc)

            records["coding_all"].append(record)
            counts["coding_all"] += 1

            if not is_pseudogene:
                if is_dubious:
                    records["coding_dubious"].append(record)
                    counts["coding_dubious"] += 1
                else:
                    records["coding"].append(record)
                    counts["coding"] += 1

        # Get protein sequence
        protein_seq = get_sequence(session, feat_nm, "protein")
        if protein_seq:
            # Use same coordinate format as coding
            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum,
                coding_coord if coding_seq else coord_str, strand,
                feat_type, headline, num2rom
            )
            record = SeqRecord(Seq(protein_seq), id=feat_nm, description=desc)

            records["protein_all"].append(record)
            counts["protein_all"] += 1

            if not is_pseudogene:
                if is_dubious:
                    records["protein_dubious"].append(record)
                    counts["protein_dubious"] += 1
                else:
                    records["protein"].append(record)
                    counts["protein"] += 1

    # Write files
    orf_dna_dir = FTP_SEQ_DIR / "orf_dna"
    orf_protein_dir = FTP_SEQ_DIR / "orf_protein"

    file_mapping = [
        ("genomic", orf_dna_dir, "orf_genomic.fasta.gz"),
        ("genomic_dubious", orf_dna_dir, "orf_genomic_dubious.fasta.gz"),
        ("genomic_all", orf_dna_dir, "orf_genomic_all.fasta.gz"),
        ("genomic_1k", orf_dna_dir, "orf_genomic_1000.fasta.gz"),
        ("genomic_1k_dubious", orf_dna_dir, "orf_genomic_1000_dubious.fasta.gz"),
        ("genomic_1k_all", orf_dna_dir, "orf_genomic_1000_all.fasta.gz"),
        ("coding", orf_dna_dir, "orf_coding.fasta.gz"),
        ("coding_dubious", orf_dna_dir, "orf_coding_dubious.fasta.gz"),
        ("coding_all", orf_dna_dir, "orf_coding_all.fasta.gz"),
        ("protein", orf_protein_dir, "orf_trans.fasta.gz"),
        ("protein_dubious", orf_protein_dir, "orf_trans_dubious.fasta.gz"),
        ("protein_all", orf_protein_dir, "orf_trans_all.fasta.gz"),
    ]

    for key, ftp_dir, filename in file_mapping:
        if records[key]:
            output_file = ftp_dir / filename
            write_fasta_file(records[key], output_file)
            archive_file(ftp_dir, filename)
            logger.info(f"Wrote {len(records[key])} sequences to {output_file}")

    return counts


def create_rna_files(
    session,
    qualifiers: dict[str, str],
    reserved_genes: set[int],
    num2rom: dict[int, str],
) -> dict[str, int]:
    """Create RNA sequence files."""
    counts = {"genomic": 0, "genomic_1k": 0, "coding": 0}
    records = {"genomic": [], "genomic_1k": [], "coding": []}

    features = get_features_for_ftp_dump(session, "Ftp RNA Sequence")
    logger.info(f"Processing {len(features)} RNA features...")

    for row in features:
        (feat_no, feat_nm, gene_nm, chrnum, start, stop, strand,
         feat_type, dbxref_id, headline) = row

        # Check qualifier
        qualifier = qualifiers.get(feat_nm.upper(), "")
        if "merged" in qualifier.lower() or "deleted" in qualifier.lower():
            continue

        if qualifier:
            feat_type = f"{qualifier}|{feat_type}"

        if not gene_nm or feat_no in reserved_genes:
            gene_nm = feat_nm

        # Coordinate string
        if strand == "C" and start < stop:
            coord_str = f"{stop}-{start}"
        else:
            coord_str = f"{start}-{stop}"

        # Genomic sequence
        genomic_seq = get_sequence(session, feat_nm, "genomic")
        if genomic_seq:
            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum, coord_str, strand,
                feat_type, headline, num2rom
            )
            record = SeqRecord(Seq(genomic_seq), id=feat_nm, description=desc)
            records["genomic"].append(record)
            counts["genomic"] += 1

        # Genomic +1000bp
        h_start, h_stop = (start, stop) if start <= stop else (stop, start)
        h_start = max(1, h_start - 1001)
        chr_length = get_chromosome_length(session, chrnum)
        h_stop = min(chr_length, h_stop + 1000)

        genomic_1k_seq = get_chromosome_sequence_region(
            session, chrnum, h_start, h_stop, reverse=(strand == "C")
        )
        if genomic_1k_seq:
            g1k_coord = f"{h_stop}-{h_start}" if strand == "C" else f"{h_start}-{h_stop}"
            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum, g1k_coord, strand,
                feat_type, headline, num2rom
            )
            record = SeqRecord(Seq(genomic_1k_seq), id=feat_nm, description=desc)
            records["genomic_1k"].append(record)
            counts["genomic_1k"] += 1

        # Coding sequence
        coding_seq = get_sequence(session, feat_nm, "coding")
        if coding_seq:
            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum, coord_str, strand,
                feat_type, headline, num2rom
            )
            record = SeqRecord(Seq(coding_seq), id=feat_nm, description=desc)
            records["coding"].append(record)
            counts["coding"] += 1

    # Write files
    rna_dir = FTP_SEQ_DIR / "rna"

    for key, filename in [
        ("genomic", "rna_genomic.fasta.gz"),
        ("genomic_1k", "rna_genomic_1000.fasta.gz"),
        ("coding", "rna_coding.fasta.gz"),
    ]:
        if records[key]:
            output_file = rna_dir / filename
            write_fasta_file(records[key], output_file)
            archive_file(rna_dir, filename)
            logger.info(f"Wrote {len(records[key])} sequences to {output_file}")

    return counts


def create_other_feature_files(
    session,
    qualifiers: dict[str, str],
    reserved_genes: set[int],
    num2rom: dict[int, str],
) -> dict[str, int]:
    """Create other feature sequence files."""
    counts = {"genomic": 0, "genomic_1k": 0}
    records = {"genomic": [], "genomic_1k": []}

    features = get_features_for_ftp_dump(session, "Ftp Other Sequence")
    logger.info(f"Processing {len(features)} other features...")

    for row in features:
        (feat_no, feat_nm, gene_nm, chrnum, start, stop, strand,
         feat_type, dbxref_id, headline) = row

        # Check qualifier
        qualifier = qualifiers.get(feat_nm.upper(), "")
        if "merged" in qualifier.lower() or "deleted" in qualifier.lower():
            continue

        if qualifier:
            feat_type = f"{qualifier}|{feat_type}"

        if not gene_nm or feat_no in reserved_genes:
            gene_nm = feat_nm

        # Coordinate string
        if strand == "C" and start < stop:
            coord_str = f"{stop}-{start}"
        else:
            coord_str = f"{start}-{stop}"

        # Genomic sequence
        genomic_seq = get_sequence(session, feat_nm, "genomic")
        if genomic_seq:
            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum, coord_str, strand,
                feat_type, headline, num2rom
            )
            record = SeqRecord(Seq(genomic_seq), id=feat_nm, description=desc)
            records["genomic"].append(record)
            counts["genomic"] += 1

        # Genomic +1000bp
        h_start, h_stop = (start, stop) if start <= stop else (stop, start)
        h_start = max(1, h_start - 1001)
        chr_length = get_chromosome_length(session, chrnum)
        h_stop = min(chr_length, h_stop + 1000)

        genomic_1k_seq = get_chromosome_sequence_region(
            session, chrnum, h_start, h_stop, reverse=(strand == "C")
        )
        if genomic_1k_seq:
            g1k_coord = f"{h_stop}-{h_start}" if strand == "C" else f"{h_start}-{h_stop}"
            desc = build_description(
                gene_nm, feat_nm, dbxref_id, chrnum, g1k_coord, strand,
                feat_type, headline, num2rom
            )
            record = SeqRecord(Seq(genomic_1k_seq), id=feat_nm, description=desc)
            records["genomic_1k"].append(record)
            counts["genomic_1k"] += 1

    # Write files
    other_dir = FTP_SEQ_DIR / "other_features"

    for key, filename in [
        ("genomic", "other_features_genomic.fasta.gz"),
        ("genomic_1k", "other_features_genomic_1000.fasta.gz"),
    ]:
        if records[key]:
            output_file = other_dir / filename
            write_fasta_file(records[key], output_file)
            archive_file(other_dir, filename)
            logger.info(f"Wrote {len(records[key])} sequences to {output_file}")

    return counts


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Recreate FASTA sequence files from chromosome sequences"
    )
    parser.add_argument(
        "seq_type",
        choices=["ORF", "RNA", "Other"],
        help="Sequence data type",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    seq_type = args.seq_type

    # Set up log file
    log_file = LOG_DIR / "ftp_sequence_dump.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(file_handler)

    logger.info("*" * 42)
    logger.info(f"Start execution: {datetime.now()}")
    logger.info(f"Destination directory: {FTP_SEQ_DIR}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Recreating FASTA files for {seq_type}...")

    try:
        with SessionLocal() as session:
            # Load reference data
            qualifiers = get_feature_qualifiers(session)
            subfeat_info = get_subfeature_info(session)
            reserved_genes = get_reserved_gene_names(session)
            num2rom = get_chromosome_number_to_roman(session)

            if seq_type == "ORF":
                counts = create_orf_files(
                    session, qualifiers, subfeat_info, reserved_genes, num2rom
                )
                logger.info(f"\nSummary of ORF Sequences recreated:")
                logger.info(f"ORF Genomic\t=> {counts['genomic']}")
                logger.info(f"ORF Genomic1K\t=> {counts['genomic_1k']}")
                logger.info(f"ORF Coding\t=> {counts['coding']}")
                logger.info(f"ORF Protein\t=> {counts['protein']}")
                logger.info(f"ORF Genomic Dubious\t=> {counts['genomic_dubious']}")
                logger.info(f"ORF Genomic1K Dubious\t=> {counts['genomic_1k_dubious']}")
                logger.info(f"ORF Coding Dubious\t=> {counts['coding_dubious']}")
                logger.info(f"ORF Protein Dubious\t=> {counts['protein_dubious']}")
                logger.info(f"ORF Genomic All\t=> {counts['genomic_all']}")
                logger.info(f"ORF Genomic1K All\t=> {counts['genomic_1k_all']}")
                logger.info(f"ORF Coding All\t=> {counts['coding_all']}")
                logger.info(f"ORF Protein All\t=> {counts['protein_all']}")

            elif seq_type == "RNA":
                counts = create_rna_files(
                    session, qualifiers, reserved_genes, num2rom
                )
                logger.info(f"\nSummary of RNA Sequences recreated:")
                logger.info(f"RNA Genomic\t=> {counts['genomic']}")
                logger.info(f"RNA Genomic1K\t=> {counts['genomic_1k']}")
                logger.info(f"RNA Coding\t=> {counts['coding']}")

            elif seq_type == "Other":
                counts = create_other_feature_files(
                    session, qualifiers, reserved_genes, num2rom
                )
                logger.info(f"\nSummary of Other Features Sequences recreated:")
                logger.info(f"Other Features Genomic\t=> {counts['genomic']}")
                logger.info(f"Other Features Genomic1K\t=> {counts['genomic_1k']}")

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
