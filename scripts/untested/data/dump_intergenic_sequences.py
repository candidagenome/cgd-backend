#!/usr/bin/env python3
"""
Dump intergenic (NOT feature) sequences to FASTA and GFF files.

This script generates sequences for all intergenic regions (regions not
contained within any feature that has chromosome as a direct parent).
Also generates a GFF file with intergenic region annotations.

Based on createNOTFile.pl by Anand Sethuraman (June 2005)

Usage:
    python dump_intergenic_sequences.py --strain C_albicans_SC5314 --output /path/to/output

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (e.g., CGD)
"""

import argparse
import gzip
import logging
import os
import sys
from pathlib import Path
from urllib.parse import quote

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class IntergenicSequenceDumper:
    """Dump intergenic sequences to FASTA and GFF files."""

    def __init__(self, session, strain_abbrev: str, seq_source: str | None = None):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.seq_source = seq_source or self._get_default_seq_source()

        # Counters
        self.total_features = 0
        self.intergenic_count_by_chr: dict[str, int] = {}
        self.total_intergenic = 0

    def _get_default_seq_source(self) -> str:
        """Get default sequence source for strain."""
        query = text(f"""
            SELECT DISTINCT fl.seq_source
            FROM {DB_SCHEMA}.feat_location fl
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_abbrev = :strain
            AND fl.is_loc_current = 'Y'
        """)

        result = self.session.execute(query, {"strain": self.strain_abbrev}).first()
        return result[0] if result else "GenBank"

    def get_chromosome_lengths(self) -> dict[str, int]:
        """Get all chromosome lengths."""
        query = text(f"""
            SELECT s.seq_name, s.seq_length
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
            JOIN {DB_SCHEMA}.organism o ON gv.organism_no = o.organism_no
            WHERE o.organism_abbrev = :strain
            AND s.source = :seq_source
            AND s.is_seq_current = 'Y'
            AND gv.is_ver_current = 'Y'
        """)

        result = self.session.execute(query, {
            "strain": self.strain_abbrev,
            "seq_source": self.seq_source,
        })

        return {row[0]: row[1] for row in result}

    def get_chromosome_names(self) -> list[str]:
        """Get sorted list of chromosome names."""
        query = text(f"""
            SELECT DISTINCT s.seq_name
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
            JOIN {DB_SCHEMA}.organism o ON gv.organism_no = o.organism_no
            WHERE o.organism_abbrev = :strain
            AND s.source = :seq_source
            AND s.is_seq_current = 'Y'
            AND gv.is_ver_current = 'Y'
            ORDER BY s.seq_name
        """)

        result = self.session.execute(query, {
            "strain": self.strain_abbrev,
            "seq_source": self.seq_source,
        })

        return [row[0] for row in result]

    def get_features_for_region(
        self, chromosome: str, start: int, stop: int
    ) -> list[tuple[str, int, int]]:
        """
        Get features for a chromosomal region.

        Returns list of (feature_name, start_coord, stop_coord) tuples.
        """
        # This query mimics the 'Ftp Intergenic Sequence' application
        query = text(f"""
            SELECT f.feature_name, fl.coord_start, fl.coord_end
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
            WHERE o.organism_abbrev = :strain
            AND s.seq_name = :chromosome
            AND fl.seq_source = :seq_source
            AND fl.is_loc_current = 'Y'
            AND fl.coord_start <= :stop
            AND fl.coord_end >= :start
            ORDER BY fl.coord_start
        """)

        result = self.session.execute(query, {
            "strain": self.strain_abbrev,
            "chromosome": chromosome,
            "seq_source": self.seq_source,
            "start": start,
            "stop": stop,
        })

        features = []
        for row in result:
            feat_name, coord_start, coord_end = row
            # Normalize coordinates
            if coord_start > coord_end:
                coord_start, coord_end = coord_end, coord_start
            features.append((feat_name, coord_start, coord_end))

        return features

    def get_sequence(self, chromosome: str, start: int, end: int) -> str | None:
        """Get chromosome sequence for a region."""
        query = text(f"""
            SELECT SUBSTR(s.residues, :start, :length)
            FROM {DB_SCHEMA}.seq s
            JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
            JOIN {DB_SCHEMA}.organism o ON gv.organism_no = o.organism_no
            WHERE o.organism_abbrev = :strain
            AND s.seq_name = :chromosome
            AND s.source = :seq_source
            AND s.is_seq_current = 'Y'
            AND gv.is_ver_current = 'Y'
        """)

        result = self.session.execute(query, {
            "strain": self.strain_abbrev,
            "chromosome": chromosome,
            "seq_source": self.seq_source,
            "start": start,
            "length": end - start + 1,
        }).first()

        return result[0] if result else None

    def find_intergenic_regions(
        self, chromosome: str, chr_length: int
    ) -> list[tuple[int, int, str, str]]:
        """
        Find all intergenic regions for a chromosome.

        Returns list of (start, end, left_feature, right_feature) tuples.
        """
        features = self.get_features_for_region(chromosome, 1, chr_length)

        # Build non-overlapping feature spans
        spans: dict[int, tuple[str, int]] = {}  # start -> (feature_name, end)

        for feat_name, start, end in features:
            self.total_features += 1

            if start in spans:
                old_name, old_end = spans[start]
                if old_end <= end:
                    spans[start] = (feat_name, end)
            else:
                spans[start] = (feat_name, end)

        # Find intergenic regions
        intergenic: list[tuple[int, int, str, str]] = []

        sorted_starts = sorted(spans.keys())

        if not sorted_starts:
            return intergenic

        # Check region before first feature
        first_start = sorted_starts[0]
        if first_start > 1:
            first_name, _ = spans[first_start]
            intergenic.append((1, first_start - 1, f"start of {chromosome}", first_name))

        # Find regions between features
        prev_name, prev_end = spans[sorted_starts[0]]

        for start in sorted_starts[1:]:
            curr_name, curr_end = spans[start]

            if start > prev_end + 1:
                intergenic.append((prev_end + 1, start - 1, prev_name, curr_name))

            # Update prev if this feature extends further
            if curr_end > prev_end:
                prev_name = curr_name
                prev_end = curr_end

        # Check region after last feature
        if prev_end < chr_length:
            intergenic.append((prev_end + 1, chr_length, prev_name, f"end of {chromosome}"))

        return intergenic

    def dump_sequences(
        self, fasta_file: Path, gff_file: Path
    ) -> tuple[int, int]:
        """
        Dump intergenic sequences to FASTA and GFF files.

        Args:
            fasta_file: Output FASTA file path
            gff_file: Output GFF file path

        Returns:
            Tuple of (sequence count, feature count)
        """
        chr_lengths = self.get_chromosome_lengths()
        chr_names = self.get_chromosome_names()

        logger.info(f"Processing {len(chr_names)} chromosomes")

        fasta_file.parent.mkdir(parents=True, exist_ok=True)
        gff_file.parent.mkdir(parents=True, exist_ok=True)

        records: list[SeqRecord] = []
        gff_lines: list[str] = [
            "##gff-version\t3",
            f"# Intergenic regions for {self.strain_abbrev}",
            f"# Sequence source: {self.seq_source}",
        ]

        for chromosome in chr_names:
            chr_length = chr_lengths.get(chromosome, 0)
            if not chr_length:
                continue

            self.intergenic_count_by_chr[chromosome] = 0

            intergenic_regions = self.find_intergenic_regions(chromosome, chr_length)

            for start, end, left_feat, right_feat in intergenic_regions:
                length = end - start + 1
                if length < 1:
                    continue

                # Get sequence
                sequence = self.get_sequence(chromosome, start, end)
                if not sequence or "sequence not found" in sequence.lower():
                    continue

                # Create FASTA record
                seq_id = f"{chromosome}:{start}-{end}"

                if left_feat == right_feat:
                    description = f"between {left_feat}"
                else:
                    description = f"between {left_feat} and {right_feat}"

                record = SeqRecord(
                    Seq(sequence),
                    id=seq_id,
                    description=description,
                )
                records.append(record)

                # Calculate GC content
                sequence_clean = sequence.upper().replace(" ", "").replace("\n", "")
                gc_count = sequence_clean.count("G") + sequence_clean.count("C")
                at_count = sequence_clean.count("A") + sequence_clean.count("T")
                gc_percent = (gc_count / len(sequence_clean)) * 100 if sequence_clean else 0
                at_percent = (at_count / len(sequence_clean)) * 100 if sequence_clean else 0

                # Add GFF line
                gff_attrs = (
                    f"ID={quote(seq_id)};"
                    f"Note={quote(description)};"
                    f"Length={length};"
                    f"GCcontent={gc_percent:.3f};"
                    f"ATcontent={at_percent:.3f}"
                )
                gff_lines.append(
                    f"{chromosome}\t{PROJECT_ACRONYM}\tintergenic_region\t{start}\t{end}\t.\t+\t.\t{gff_attrs}"
                )

                self.intergenic_count_by_chr[chromosome] += 1
                self.total_intergenic += 1

            logger.info(
                f"Chr {chromosome}\t=>\t{self.intergenic_count_by_chr[chromosome]}"
            )

        # Write FASTA file
        with open(fasta_file, "w") as f:
            SeqIO.write(records, f, "fasta")

        # Write GFF file
        with open(gff_file, "w") as f:
            f.write("\n".join(gff_lines) + "\n")

        logger.info(f"Total intergenic regions: {self.total_intergenic}")
        logger.info(f"Total features examined: {self.total_features}")

        return self.total_intergenic, self.total_features


def dump_intergenic_sequences(
    strain_abbrev: str,
    output_dir: Path,
    seq_source: str | None = None,
    compress: bool = True,
) -> bool:
    """
    Main function to dump intergenic sequences.

    Args:
        strain_abbrev: Strain abbreviation
        output_dir: Output directory
        seq_source: Sequence source (optional)
        compress: Whether to gzip output files

    Returns:
        True on success, False on failure
    """
    # Set up logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{strain_abbrev}_intergenic_seq_dump.log"

    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info("*" * 40)
    logger.info(f"Starting intergenic sequence dump for {strain_abbrev}")

    fasta_file = output_dir / "not_feature.fasta"
    gff_file = output_dir / "intergenic.gff"

    try:
        with SessionLocal() as session:
            dumper = IntergenicSequenceDumper(session, strain_abbrev, seq_source)
            intergenic_count, feature_count = dumper.dump_sequences(fasta_file, gff_file)

            if compress:
                # Compress files
                import shutil

                for file_path in [fasta_file, gff_file]:
                    if file_path.exists():
                        with open(file_path, "rb") as f_in:
                            with gzip.open(f"{file_path}.gz", "wb") as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        file_path.unlink()
                        logger.info(f"Compressed {file_path}")

            logger.info(f"Intergenic sequences dumped to {output_dir}")
            return intergenic_count > 0

    except Exception as e:
        logger.exception(f"Error dumping intergenic sequences: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump intergenic (NOT feature) sequences"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output directory",
    )
    parser.add_argument(
        "--seq-source",
        help="Sequence source (default: auto-detect)",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Don't compress output files",
    )

    args = parser.parse_args()

    success = dump_intergenic_sequences(
        args.strain,
        args.output,
        seq_source=args.seq_source,
        compress=not args.no_compress,
    )
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
