#!/usr/bin/env python3
"""
Dump sequence files for download.

This script generates FASTA sequence files from the database for various
sequence types including:
- Chromosome sequences
- ORF genomic sequences (with optional flanking regions)
- ORF coding sequences (introns removed)
- ORF protein translations
- Other feature sequences

Usage:
    python dump_sequence.py --strain C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
    TMP_DIR: Temporary directory
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
    CURATOR_EMAIL: Email for notifications
"""

import argparse
import gzip
import hashlib
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/tmp"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
CURATOR_EMAIL = os.getenv("CURATOR_EMAIL")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Genetic code for translation (standard code)
CODON_TABLE = {
    "TTT": "F", "TTC": "F", "TTA": "L", "TTG": "L",
    "TCT": "S", "TCC": "S", "TCA": "S", "TCG": "S",
    "TAT": "Y", "TAC": "Y", "TAA": "*", "TAG": "*",
    "TGT": "C", "TGC": "C", "TGA": "*", "TGG": "W",
    "CTT": "L", "CTC": "L", "CTA": "L", "CTG": "L",
    "CCT": "P", "CCC": "P", "CCA": "P", "CCG": "P",
    "CAT": "H", "CAC": "H", "CAA": "Q", "CAG": "Q",
    "CGT": "R", "CGC": "R", "CGA": "R", "CGG": "R",
    "ATT": "I", "ATC": "I", "ATA": "I", "ATG": "M",
    "ACT": "T", "ACC": "T", "ACA": "T", "ACG": "T",
    "AAT": "N", "AAC": "N", "AAA": "K", "AAG": "K",
    "AGT": "S", "AGC": "S", "AGA": "R", "AGG": "R",
    "GTT": "V", "GTC": "V", "GTA": "V", "GTG": "V",
    "GCT": "A", "GCC": "A", "GCA": "A", "GCG": "A",
    "GAT": "D", "GAC": "D", "GAA": "E", "GAG": "E",
    "GGT": "G", "GGC": "G", "GGA": "G", "GGG": "G",
}

# CTG clade alternative code (CTG = Ser instead of Leu)
CTG_CODON_TABLE = CODON_TABLE.copy()
CTG_CODON_TABLE["CTG"] = "S"


def translate_sequence(dna_seq: str, use_ctg_code: bool = False) -> str:
    """Translate DNA sequence to protein."""
    codon_table = CTG_CODON_TABLE if use_ctg_code else CODON_TABLE
    protein = []
    for i in range(0, len(dna_seq) - 2, 3):
        codon = dna_seq[i:i + 3].upper()
        aa = codon_table.get(codon, "X")
        if aa == "*":
            break
        protein.append(aa)
    return "".join(protein)


def reverse_complement(seq: str) -> str:
    """Get reverse complement of DNA sequence."""
    complement = {"A": "T", "T": "A", "G": "C", "C": "G",
                  "a": "t", "t": "a", "g": "c", "c": "g",
                  "N": "N", "n": "n"}
    return "".join(complement.get(base, "N") for base in reversed(seq))


class SequenceDumper:
    """Dump various sequence files from database."""

    def __init__(self, session, strain_abbrev: str, seq_source: str):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.seq_source = seq_source
        self.organism_no = None
        self.use_ctg_code = False  # Set based on organism

        # Check if CTG clade organism
        if "albicans" in strain_abbrev.lower() or "dubliniensis" in strain_abbrev.lower():
            self.use_ctg_code = True

    def get_organism_info(self) -> dict | None:
        """Get organism information from database."""
        query = text(f"""
            SELECT organism_no, organism_name, taxon_id
            FROM {DB_SCHEMA}.organism
            WHERE organism_abbrev = :strain_abbrev
        """)
        result = self.session.execute(
            query, {"strain_abbrev": self.strain_abbrev}
        ).first()

        if result:
            self.organism_no = result[0]
            return {
                "organism_no": result[0],
                "organism_name": result[1],
                "taxon_id": result[2],
            }
        return None

    def get_chromosomes(self) -> list[dict]:
        """Get chromosome/contig information."""
        query = text(f"""
            SELECT f.feature_no, f.feature_name,
                   fl.stop_coord as length
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
            WHERE f.organism_no = :organism_no
            AND f.feature_type IN ('chromosome', 'contig', 'supercontig')
            AND fl.seq_source = :seq_source
            AND fl.is_loc_current = 'Y'
            ORDER BY f.feature_name
        """)

        result = self.session.execute(query, {
            "organism_no": self.organism_no,
            "seq_source": self.seq_source,
        })

        return [
            {"feature_no": row[0], "feature_name": row[1], "length": row[2]}
            for row in result
        ]

    def get_chromosome_sequence(self, feature_name: str) -> str:
        """Get sequence for a chromosome."""
        query = text(f"""
            SELECT s.residues
            FROM {DB_SCHEMA}.sequence s
            JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
            WHERE f.feature_name = :feature_name
            AND f.organism_no = :organism_no
        """)

        result = self.session.execute(query, {
            "feature_name": feature_name,
            "organism_no": self.organism_no,
        }).first()

        return result[0] if result else ""

    def get_features(self, coding_only: bool = True) -> list[dict]:
        """Get features for sequence dumping."""
        feature_filter = "AND f.is_coding = 'Y'" if coding_only else "AND f.is_coding = 'N'"

        query = text(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name, f.feature_type,
                   f.feature_qualifier, f.headline, f.dbxref_id, f.is_coding,
                   fl.start_coord, fl.stop_coord, fl.strand,
                   fl.root_sequence_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
            WHERE f.organism_no = :organism_no
            AND fl.seq_source = :seq_source
            AND fl.is_loc_current = 'Y'
            AND f.feature_type NOT IN ('chromosome', 'contig', 'supercontig')
            {feature_filter}
            ORDER BY fl.root_sequence_name, fl.start_coord
        """)

        result = self.session.execute(query, {
            "organism_no": self.organism_no,
            "seq_source": self.seq_source,
        })

        features = []
        for row in result:
            qualifier = row[4] or ""
            # Skip deleted features
            if "Deleted" in qualifier:
                continue

            features.append({
                "feature_no": row[0],
                "feature_name": row[1],
                "gene_name": row[2],
                "feature_type": row[3],
                "feature_qualifier": qualifier,
                "headline": row[5],
                "dbxref_id": row[6],
                "is_coding": row[7] == "Y",
                "start_coord": row[8],
                "stop_coord": row[9],
                "strand": row[10],
                "root_sequence_name": row[11],
            })

        return features

    def get_feature_sequence(
        self,
        feature_name: str,
        upstream: int = 0,
        downstream: int = 0,
        coding_only: bool = False,
    ) -> tuple[str, str]:
        """
        Get sequence for a feature.

        Args:
            feature_name: Feature name
            upstream: Bases upstream to include
            downstream: Bases downstream to include
            coding_only: If True, return only coding sequence (no introns)

        Returns:
            Tuple of (sequence, location_info)
        """
        # Get feature location
        query = text(f"""
            SELECT fl.start_coord, fl.stop_coord, fl.strand,
                   fl.root_sequence_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
            WHERE f.feature_name = :feature_name
            AND f.organism_no = :organism_no
            AND fl.seq_source = :seq_source
            AND fl.is_loc_current = 'Y'
        """)

        loc_result = self.session.execute(query, {
            "feature_name": feature_name,
            "organism_no": self.organism_no,
            "seq_source": self.seq_source,
        }).first()

        if not loc_result:
            return "", ""

        start, stop, strand, root_name = loc_result

        if coding_only:
            # Get CDS coordinates
            return self._get_coding_sequence(feature_name, strand)

        # Get chromosome sequence
        chr_seq = self.get_chromosome_sequence(root_name)
        if not chr_seq:
            return "", ""

        # Adjust for strand and flanking
        if strand == "W":
            seq_start = max(1, start - upstream) - 1
            seq_end = min(len(chr_seq), stop + downstream)
            sequence = chr_seq[seq_start:seq_end]
        else:
            seq_start = max(1, start - downstream) - 1
            seq_end = min(len(chr_seq), stop + upstream)
            sequence = reverse_complement(chr_seq[seq_start:seq_end])

        loc_info = f"COORDS:{root_name}:{start}-{stop}{strand}"
        if upstream or downstream:
            loc_info += f" with {upstream} bases upstream and {downstream} bases downstream"

        return sequence, loc_info

    def _get_coding_sequence(self, feature_name: str, strand: str) -> tuple[str, str]:
        """Get coding sequence (CDS) for a feature."""
        query = text(f"""
            SELECT sf.start_coord, sf.stop_coord
            FROM {DB_SCHEMA}.subfeature sf
            JOIN {DB_SCHEMA}.feature f ON sf.feature_no = f.feature_no
            WHERE f.feature_name = :feature_name
            AND f.organism_no = :organism_no
            AND sf.seq_source = :seq_source
            AND sf.feature_type = 'CDS'
            ORDER BY sf.start_coord
        """)

        result = self.session.execute(query, {
            "feature_name": feature_name,
            "organism_no": self.organism_no,
            "seq_source": self.seq_source,
        })

        cds_coords = [(row[0], row[1]) for row in result]
        if not cds_coords:
            return "", ""

        # Get chromosome sequence
        root_query = text(f"""
            SELECT fl.root_sequence_name
            FROM {DB_SCHEMA}.feat_location fl
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            WHERE f.feature_name = :feature_name
            AND f.organism_no = :organism_no
            AND fl.seq_source = :seq_source
        """)
        root_result = self.session.execute(root_query, {
            "feature_name": feature_name,
            "organism_no": self.organism_no,
            "seq_source": self.seq_source,
        }).first()

        if not root_result:
            return "", ""

        chr_seq = self.get_chromosome_sequence(root_result[0])
        if not chr_seq:
            return "", ""

        # Concatenate CDS segments
        cds_seq = ""
        for cds_start, cds_stop in cds_coords:
            cds_seq += chr_seq[cds_start - 1:cds_stop]

        if strand == "C":
            cds_seq = reverse_complement(cds_seq)

        loc_info = f"CDS:{len(cds_coords)} exons"
        return cds_seq, loc_info

    def dump_chromosomes(self, output_file: Path) -> int:
        """Dump chromosome sequences to FASTA file."""
        chromosomes = self.get_chromosomes()
        count = 0

        with open(output_file, "w") as f:
            for chrom in chromosomes:
                sequence = self.get_chromosome_sequence(chrom["feature_name"])
                if sequence:
                    f.write(f">{chrom['feature_name']}\n")
                    # Write sequence in 60-char lines
                    for i in range(0, len(sequence), 60):
                        f.write(sequence[i:i + 60] + "\n")
                    count += 1

        logger.info(f"Wrote {count} chromosome sequences to {output_file}")
        return count

    def dump_feature_sequences(
        self,
        output_file: Path,
        coding_features: bool = True,
        upstream: int = 0,
        downstream: int = 0,
        coding_only: bool = False,
        translate: bool = False,
    ) -> int:
        """
        Dump feature sequences to FASTA file.

        Args:
            output_file: Output file path
            coding_features: If True, dump coding features (ORFs)
            upstream: Bases upstream to include
            downstream: Bases downstream to include
            coding_only: If True, exclude introns
            translate: If True, translate to protein

        Returns:
            Number of sequences written
        """
        features = self.get_features(coding_only=coding_features)
        count = 0

        with open(output_file, "w") as f:
            for feat in features:
                sequence, loc_info = self.get_feature_sequence(
                    feat["feature_name"],
                    upstream=upstream,
                    downstream=downstream,
                    coding_only=coding_only,
                )

                if not sequence:
                    continue

                if translate:
                    sequence = translate_sequence(sequence, self.use_ctg_code)

                # Build header
                name = feat["gene_name"] or feat["feature_name"]
                header = f">{feat['feature_name']} {name}"

                if feat["dbxref_id"]:
                    header += f" {PROJECT_ACRONYM}ID:{feat['dbxref_id']}"

                if loc_info:
                    header += f" {loc_info}"

                # ORF classification
                qualifier = feat["feature_qualifier"]
                for cls in ["Verified", "Uncharacterized", "Dubious"]:
                    if cls in qualifier:
                        header += f" {cls} ORF"
                        break

                if feat["headline"]:
                    header += f"; {feat['headline']}"

                f.write(header + "\n")
                # Write sequence in 60-char lines
                for i in range(0, len(sequence), 60):
                    f.write(sequence[i:i + 60] + "\n")
                count += 1

        logger.info(f"Wrote {count} sequences to {output_file}")
        return count


def calculate_checksum(filepath: Path) -> str:
    """Calculate checksum of file."""
    hasher = hashlib.md5()

    open_func = gzip.open if str(filepath).endswith(".gz") else open
    mode = "rb"

    with open_func(filepath, mode) as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def dump_sequences(strain_abbrev: str, seq_source: str | None = None) -> bool:
    """
    Main function to dump all sequence files.

    Args:
        strain_abbrev: Strain abbreviation
        seq_source: Sequence source (optional, uses default if not provided)

    Returns:
        True on success, False on failure
    """
    # Set up logging
    log_file = LOG_DIR / f"sequence_dump_{strain_abbrev}.log"
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting sequence dump for {strain_abbrev}")
    logger.info(f"Start time: {datetime.now()}")

    try:
        with SessionLocal() as session:
            # Get default seq_source if not provided
            if not seq_source:
                query = text(f"""
                    SELECT DISTINCT fl.seq_source
                    FROM {DB_SCHEMA}.feat_location fl
                    JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
                    JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
                    WHERE o.organism_abbrev = :strain_abbrev
                    AND fl.is_loc_current = 'Y'
                """)
                result = session.execute(
                    query, {"strain_abbrev": strain_abbrev}
                ).first()
                if result:
                    seq_source = result[0]
                else:
                    logger.error(f"Could not determine seq_source for {strain_abbrev}")
                    return False

            logger.info(f"Using seq_source: {seq_source}")

            dumper = SequenceDumper(session, strain_abbrev, seq_source)

            # Get organism info
            org_info = dumper.get_organism_info()
            if not org_info:
                logger.error(f"Strain {strain_abbrev} not found in database")
                return False

            # Output directory
            output_dir = DATA_DIR / "download" / strain_abbrev / "current"
            output_dir.mkdir(parents=True, exist_ok=True)

            archive_dir = DATA_DIR / "download" / strain_abbrev / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)

            # Define files to dump
            dump_configs = [
                {
                    "filename": f"{strain_abbrev}_current_chromosomes.fasta",
                    "type": "chromosomes",
                },
                {
                    "filename": f"{strain_abbrev}_current_orf_genomic.fasta",
                    "type": "features",
                    "coding_features": True,
                },
                {
                    "filename": f"{strain_abbrev}_current_orf_genomic_1000.fasta",
                    "type": "features",
                    "coding_features": True,
                    "upstream": 1000,
                    "downstream": 1000,
                },
                {
                    "filename": f"{strain_abbrev}_current_orf_coding.fasta",
                    "type": "features",
                    "coding_features": True,
                    "coding_only": True,
                },
                {
                    "filename": f"{strain_abbrev}_current_orf_trans.fasta",
                    "type": "features",
                    "coding_features": True,
                    "coding_only": True,
                    "translate": True,
                },
                {
                    "filename": f"{strain_abbrev}_current_other_features_genomic.fasta",
                    "type": "features",
                    "coding_features": False,
                },
            ]

            for config in dump_configs:
                filename = config["filename"]
                output_file = output_dir / filename
                tmp_file = TMP_DIR / f"{filename}.tmp"

                logger.info(f"Dumping {filename}...")

                if config["type"] == "chromosomes":
                    dumper.dump_chromosomes(tmp_file)
                else:
                    dumper.dump_feature_sequences(
                        tmp_file,
                        coding_features=config.get("coding_features", True),
                        upstream=config.get("upstream", 0),
                        downstream=config.get("downstream", 0),
                        coding_only=config.get("coding_only", False),
                        translate=config.get("translate", False),
                    )

                # Check if content changed
                gz_output = output_file.with_suffix(".fasta.gz")
                if gz_output.exists():
                    # Decompress old file for comparison
                    old_checksum = calculate_checksum(gz_output)

                    # Compress tmp file and get checksum
                    subprocess.run(["gzip", "-f", str(tmp_file)], check=True)
                    new_checksum = calculate_checksum(tmp_file.with_suffix(".fasta.gz"))

                    if old_checksum == new_checksum:
                        logger.info(f"No change in {filename}, skipping")
                        tmp_file.with_suffix(".fasta.gz").unlink()
                        continue

                    # Archive old file
                    archive_path = archive_dir / gz_output.name
                    shutil.move(gz_output, archive_path)
                    logger.info(f"Archived: {gz_output} -> {archive_path}")

                    # Move new file
                    shutil.move(tmp_file.with_suffix(".fasta.gz"), gz_output)
                else:
                    # Compress and move
                    subprocess.run(["gzip", "-f", str(tmp_file)], check=True)
                    shutil.move(tmp_file.with_suffix(".fasta.gz"), gz_output)

                logger.info(f"Created: {gz_output}")

            logger.info(f"Sequence dump complete: {datetime.now()}")
            return True

    except Exception as e:
        logger.exception(f"Error dumping sequences: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump sequence files for download"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--assembly",
        default=None,
        help="Assembly/sequence source name (optional)",
    )

    args = parser.parse_args()

    success = dump_sequences(args.strain, args.assembly)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
