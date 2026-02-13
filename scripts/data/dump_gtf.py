#!/usr/bin/env python3
"""
Dump GTF (Gene Transfer Format) file for features.

This script generates a GTF file containing gene structure information
including exons, CDS, start codons, and stop codons for ORFs.

Based on dumpGTF.pl

Usage:
    python dump_gtf.py --strain C_albicans_SC5314

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

from Bio import SeqIO
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
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class GTFDumper:
    """Dump GTF format files."""

    def __init__(self, session, strain_abbrev: str):
        self.session = session
        self.strain_abbrev = strain_abbrev
        self.seq_source = None

    def get_seq_source(self) -> str | None:
        """Get sequence source for strain."""
        query = text(f"""
            SELECT DISTINCT fl.seq_source
            FROM {DB_SCHEMA}.feat_location fl
            JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_abbrev = :strain_abbrev
            AND fl.is_loc_current = 'Y'
        """)

        result = self.session.execute(
            query, {"strain_abbrev": self.strain_abbrev}
        ).first()

        if result:
            self.seq_source = result[0]
            return self.seq_source
        return None

    def get_feature_info(self, feature_name: str) -> dict | None:
        """Get feature information from database."""
        query = text(f"""
            SELECT f.feature_no, f.feature_name, f.feature_type,
                   fl.strand, fl.coord_start, fl.coord_end,
                   s.seq_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
                AND fl.is_loc_current = 'Y'
            JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
                AND s.is_seq_current = 'Y'
                AND s.source = :seq_source
            JOIN {DB_SCHEMA}.genome_version gv ON s.genome_version_no = gv.genome_version_no
                AND gv.is_ver_current = 'Y'
            WHERE f.feature_name = :feature_name
        """)

        result = self.session.execute(
            query, {"seq_source": self.seq_source, "feature_name": feature_name}
        ).first()

        if result:
            return {
                "feature_no": result[0],
                "feature_name": result[1],
                "feature_type": result[2],
                "strand": result[3],
                "coord_start": result[4],
                "coord_end": result[5],
                "chromosome": result[6],
            }
        return None

    def get_subfeatures(self, feature_no: int) -> list[dict]:
        """Get subfeatures (exons, CDS) for a feature."""
        query = text(f"""
            SELECT sf.subfeature_type, sfl.coord_start, sfl.coord_end,
                   sf.subfeature_no, sf.subfeature_name
            FROM {DB_SCHEMA}.subfeature sf
            JOIN {DB_SCHEMA}.subfeat_location sfl ON sf.subfeature_no = sfl.subfeature_no
            WHERE sf.feature_no = :feature_no
            ORDER BY sfl.coord_start
        """)

        result = self.session.execute(query, {"feature_no": feature_no})

        subfeatures = []
        for row in result:
            subfeatures.append({
                "type": row[0],
                "start": row[1],
                "end": row[2],
                "subfeature_no": row[3],
                "name": row[4],
            })

        return subfeatures

    def get_feature_qualifier(self, feature_no: int) -> str | None:
        """Get feature qualifier (e.g., Deleted, Dubious)."""
        query = text(f"""
            SELECT property_value
            FROM {DB_SCHEMA}.feat_property
            WHERE feature_no = :feature_no
            AND property_type = 'feature_qualifier'
        """)

        result = self.session.execute(query, {"feature_no": feature_no}).first()
        return result[0] if result else None

    def dump_gtf(self, coding_fasta: Path, output_file: Path) -> int:
        """
        Dump GTF file using coding sequence FASTA.

        Args:
            coding_fasta: Path to coding sequence FASTA file
            output_file: Path to output GTF file

        Returns:
            Number of features written
        """
        if not self.get_seq_source():
            logger.error(f"No sequence source found for {self.strain_abbrev}")
            return 0

        output_file.parent.mkdir(parents=True, exist_ok=True)

        count = 0

        # Open input FASTA (may be gzipped)
        if str(coding_fasta).endswith(".gz"):
            fasta_fh = gzip.open(coding_fasta, "rt")
        else:
            fasta_fh = open(coding_fasta, "r")

        with open(output_file, "w") as out_fh:
            for record in SeqIO.parse(fasta_fh, "fasta"):
                orf = record.id

                feat_info = self.get_feature_info(orf)
                if not feat_info:
                    continue

                # Only process ORFs
                if feat_info["feature_type"] != "ORF":
                    continue

                # Skip deleted features
                qualifier = self.get_feature_qualifier(feat_info["feature_no"])
                if qualifier and "Deleted" in qualifier:
                    continue

                chromosome = feat_info["chromosome"]
                strand = "-" if feat_info["strand"] == "C" else "+"

                # Check for start and stop codons
                seq = str(record.seq).upper()
                has_start = seq[:3] == "ATG"
                has_stop = seq[-3:] in ("TAA", "TAG", "TGA")

                attribute = f'gene_id "{orf}"; transcript_id "{orf}";'

                # Get CDS subfeatures
                subfeatures = self.get_subfeatures(feat_info["feature_no"])
                cds_features = [sf for sf in subfeatures if sf["type"] == "CDS"]

                if not cds_features:
                    # Single exon gene - use main feature coordinates
                    cds_features = [{
                        "start": feat_info["coord_start"],
                        "end": feat_info["coord_end"],
                    }]

                # Sort by position
                cds_features.sort(key=lambda x: min(x["start"], x["end"]))

                # Process exons
                exon_coords = []
                for sf in cds_features:
                    start, end = sf["start"], sf["end"]
                    if start > end:
                        start, end = end, start
                    exon_coords.append((start, end))

                # Calculate start/stop codon positions
                start_codon_coords = []
                stop_codon_coords = []
                cds_coords = list(exon_coords)

                if strand == "+":
                    if has_start:
                        first_len = exon_coords[0][1] - exon_coords[0][0] + 1
                        if first_len >= 3:
                            start_codon_coords.append(
                                (exon_coords[0][0], exon_coords[0][0] + 2)
                            )
                        else:
                            start_codon_coords.append(
                                (exon_coords[0][0], exon_coords[0][1])
                            )
                            if len(exon_coords) > 1:
                                start_codon_coords.append(
                                    (exon_coords[1][0],
                                     exon_coords[1][0] + (2 - first_len))
                                )

                    if has_stop:
                        last_idx = len(exon_coords) - 1
                        last_len = exon_coords[last_idx][1] - exon_coords[last_idx][0] + 1
                        if last_len >= 3:
                            stop_codon_coords.append(
                                (exon_coords[last_idx][1] - 2, exon_coords[last_idx][1])
                            )
                            cds_coords[last_idx] = (
                                cds_coords[last_idx][0],
                                cds_coords[last_idx][1] - 3
                            )
                        else:
                            # Stop codon spans exons
                            if last_idx > 0:
                                stop_codon_coords.append(
                                    (exon_coords[last_idx - 1][1] - (2 - last_len),
                                     exon_coords[last_idx - 1][1])
                                )
                            stop_codon_coords.append(
                                (exon_coords[last_idx][0], exon_coords[last_idx][1])
                            )
                            cds_coords[last_idx] = (0, 0)
                else:
                    # Minus strand
                    if has_start:
                        last_idx = len(exon_coords) - 1
                        last_len = exon_coords[last_idx][1] - exon_coords[last_idx][0] + 1
                        if last_len >= 3:
                            start_codon_coords.append(
                                (exon_coords[last_idx][1] - 2, exon_coords[last_idx][1])
                            )
                        else:
                            start_codon_coords.append(
                                (exon_coords[last_idx][0], exon_coords[last_idx][1])
                            )
                            if last_idx > 0:
                                start_codon_coords.append(
                                    (exon_coords[last_idx - 1][1] - (2 - last_len),
                                     exon_coords[last_idx - 1][1])
                                )

                    if has_stop:
                        first_len = exon_coords[0][1] - exon_coords[0][0] + 1
                        if first_len >= 3:
                            stop_codon_coords.append(
                                (exon_coords[0][0], exon_coords[0][0] + 2)
                            )
                            cds_coords[0] = (cds_coords[0][0] + 3, cds_coords[0][1])
                        else:
                            stop_codon_coords.append(
                                (exon_coords[0][0], exon_coords[0][1])
                            )
                            if len(exon_coords) > 1:
                                stop_codon_coords.append(
                                    (exon_coords[1][0],
                                     exon_coords[1][0] + (2 - first_len))
                                )
                            cds_coords[0] = (0, 0)

                # Write GTF entries
                total_length = 0
                for i, (start, end) in enumerate(exon_coords):
                    if strand == "-":
                        i = len(exon_coords) - 1 - i

                    frame = total_length % 3
                    exon_num = i + 1
                    attr = f'{attribute} exon_number "{exon_num}";'

                    # Exon
                    out_fh.write(
                        f"{chromosome}\t{PROJECT_ACRONYM}\texon\t{start}\t{end}\t.\t{strand}\t{frame}\t{attr}\n"
                    )

                    # CDS (if valid)
                    cds_start, cds_end = cds_coords[i if strand == "+" else len(exon_coords) - 1 - i]
                    if cds_start > 0 and cds_end > 0:
                        out_fh.write(
                            f"{chromosome}\t{PROJECT_ACRONYM}\tCDS\t{cds_start}\t{cds_end}\t.\t{strand}\t{frame}\t{attr}\n"
                        )

                    total_length += end - start + 1

                # Start codons
                for sc_start, sc_end in start_codon_coords:
                    out_fh.write(
                        f"{chromosome}\t{PROJECT_ACRONYM}\tstart_codon\t{sc_start}\t{sc_end}\t.\t{strand}\t0\t{attribute}\n"
                    )

                # Stop codons
                for st_start, st_end in stop_codon_coords:
                    out_fh.write(
                        f"{chromosome}\t{PROJECT_ACRONYM}\tstop_codon\t{st_start}\t{st_end}\t.\t{strand}\t0\t{attribute}\n"
                    )

                count += 1

        fasta_fh.close()

        logger.info(f"Wrote {count} features to {output_file}")
        return count


def dump_gtf(strain_abbrev: str, coding_fasta: Path, output_file: Path) -> bool:
    """
    Main function to dump GTF file.

    Args:
        strain_abbrev: Strain abbreviation
        coding_fasta: Path to coding sequence FASTA
        output_file: Path to output GTF file

    Returns:
        True on success, False on failure
    """
    logger.info(f"Generating GTF file for {strain_abbrev}")

    try:
        with SessionLocal() as session:
            dumper = GTFDumper(session, strain_abbrev)
            count = dumper.dump_gtf(coding_fasta, output_file)
            return count > 0

    except Exception as e:
        logger.exception(f"Error generating GTF: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GTF file for features"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--fasta",
        required=True,
        type=Path,
        help="Path to coding sequence FASTA file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output GTF file path (default: stdout)",
    )

    args = parser.parse_args()

    output_file = args.output
    if not output_file:
        output_file = Path(f"{args.strain}.gtf")

    success = dump_gtf(args.strain, args.fasta, output_file)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
