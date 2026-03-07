#!/usr/bin/env python3
"""
Create intergenic region (NOT feature) files.

This script generates sequences of all intergenic regions (regions not
contained within features) that are at least 1 nucleotide in length.
It produces both FASTA sequence files and GFF annotation files.

Based on createNOTFile.pl by Anand Sethuraman (Jun 2005).

Usage:
    python create_not_file.py <strain_abbrev>
    python create_not_file.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for download files
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
PROJECT_URL = os.getenv("PROJECT_URL", "http://www.candidagenome.org")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class IntergenicRegion:
    """An intergenic region."""
    chromosome: str
    start: int
    end: int
    left_feature: str | None
    right_feature: str | None
    sequence: str


def get_strain_config(session, strain_abbrev: str) -> dict | None:
    """Get strain configuration from database."""
    query = text(f"""
        SELECT o.organism_no, o.organism_abbrev, o.organism_name
        FROM {DB_SCHEMA}.organism o
        WHERE o.organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    if not result:
        return None

    # Get seq_source
    seq_query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_abbrev = :strain_abbrev
        FETCH FIRST 1 ROW ONLY
    """)
    seq_result = session.execute(seq_query, {"strain_abbrev": strain_abbrev}).fetchone()

    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "organism_name": result[2],
        "seq_source": seq_result[0] if seq_result else None,
    }


def get_chromosomes(session, strain_abbrev: str, seq_source: str) -> dict[str, int]:
    """Get all chromosomes/contigs with their lengths."""
    query = text(f"""
        SELECT f.feature_name, LENGTH(s.residues)
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.seq s ON f.feature_no = s.feature_no
        WHERE f.organism_abbrev = :strain_abbrev
        AND f.feature_type IN ('chromosome', 'contig')
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
        ORDER BY f.feature_name
    """)

    chromosomes = {}
    for row in session.execute(
        query, {"strain_abbrev": strain_abbrev, "seq_source": seq_source}
    ).fetchall():
        chromosomes[row[0]] = row[1]

    return chromosomes


def get_chromosome_sequence(session, chr_name: str, seq_source: str) -> str | None:
    """Get the full sequence for a chromosome."""
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        WHERE f.feature_name = :chr_name
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
    """)
    result = session.execute(
        query, {"chr_name": chr_name, "seq_source": seq_source}
    ).fetchone()
    return result[0] if result else None


def get_features_on_chromosome(
    session, chr_name: str, strain_abbrev: str, seq_source: str
) -> list[dict]:
    """
    Get all features on a chromosome that have the chromosome as direct parent.

    These are the features that define the gene regions, excluding intergenic areas.
    """
    query = text(f"""
        SELECT f1.feature_name, fl.min_coord, fl.max_coord
        FROM {DB_SCHEMA}.feature f1
        JOIN {DB_SCHEMA}.feat_relationship fr ON f1.feature_no = fr.child_feature_no
        JOIN {DB_SCHEMA}.feature f2 ON fr.parent_feature_no = f2.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON f1.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        WHERE f2.feature_name = :chr_name
        AND fr.relationship_type = 'part of'
        AND fr.rank = 1
        AND fl.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
        ORDER BY fl.min_coord
    """)

    features = []
    for row in session.execute(
        query, {"chr_name": chr_name, "seq_source": seq_source}
    ).fetchall():
        start = row[1]
        end = row[2]
        if start > end:
            start, end = end, start
        features.append({
            "feature_name": row[0],
            "start": start,
            "end": end,
        })

    return features


def find_intergenic_regions(
    chr_name: str,
    chr_length: int,
    features: list[dict],
    chr_sequence: str
) -> list[IntergenicRegion]:
    """
    Find all intergenic regions on a chromosome.

    An intergenic region is any gap between features (or between chromosome
    ends and features) that is at least 1 nucleotide long.
    """
    regions = []

    if not features:
        # No features on this chromosome - entire chromosome is intergenic
        if chr_length >= 1:
            regions.append(IntergenicRegion(
                chromosome=chr_name,
                start=1,
                end=chr_length,
                left_feature=None,
                right_feature=None,
                sequence=chr_sequence,
            ))
        return regions

    # Merge overlapping features to get consolidated gene regions
    merged = []
    for feat in sorted(features, key=lambda x: x["start"]):
        if not merged:
            merged.append({
                "feature_name": feat["feature_name"],
                "start": feat["start"],
                "end": feat["end"],
            })
        else:
            last = merged[-1]
            if feat["start"] <= last["end"] + 1:
                # Overlapping or adjacent - extend
                if feat["end"] > last["end"]:
                    last["end"] = feat["end"]
                    # Keep the original feature name
            else:
                merged.append({
                    "feature_name": feat["feature_name"],
                    "start": feat["start"],
                    "end": feat["end"],
                })

    # Find gaps between merged features
    # Region before first feature
    first_feat = merged[0]
    if first_feat["start"] > 1:
        start = 1
        end = first_feat["start"] - 1
        seq = chr_sequence[start - 1:end] if chr_sequence else ""
        regions.append(IntergenicRegion(
            chromosome=chr_name,
            start=start,
            end=end,
            left_feature=None,
            right_feature=first_feat["feature_name"],
            sequence=seq,
        ))

    # Regions between features
    for i in range(len(merged) - 1):
        curr_feat = merged[i]
        next_feat = merged[i + 1]

        gap_start = curr_feat["end"] + 1
        gap_end = next_feat["start"] - 1

        if gap_end >= gap_start:
            seq = chr_sequence[gap_start - 1:gap_end] if chr_sequence else ""
            regions.append(IntergenicRegion(
                chromosome=chr_name,
                start=gap_start,
                end=gap_end,
                left_feature=curr_feat["feature_name"],
                right_feature=next_feat["feature_name"],
                sequence=seq,
            ))

    # Region after last feature
    last_feat = merged[-1]
    if last_feat["end"] < chr_length:
        start = last_feat["end"] + 1
        end = chr_length
        seq = chr_sequence[start - 1:end] if chr_sequence else ""
        regions.append(IntergenicRegion(
            chromosome=chr_name,
            start=start,
            end=end,
            left_feature=last_feat["feature_name"],
            right_feature=None,
            sequence=seq,
        ))

    return regions


def format_intergenic_description(region: IntergenicRegion) -> str:
    """Format a description for an intergenic region."""
    if region.left_feature and region.right_feature:
        return f"between {region.left_feature} and {region.right_feature}"
    elif region.left_feature:
        return f"between {region.left_feature} and end of {region.chromosome}"
    elif region.right_feature:
        return f"between start of {region.chromosome} and {region.right_feature}"
    else:
        return f"entire {region.chromosome}"


def write_fasta_file(regions: list[IntergenicRegion], output_file: Path, compress: bool = True):
    """Write intergenic regions to a FASTA file."""
    opener = gzip.open if compress else open
    mode = "wt" if compress else "w"

    with opener(output_file, mode) as f:
        for region in regions:
            # Format ID: chr:start-end
            seq_id = f"{region.chromosome}:{region.start}-{region.end}"
            desc = format_intergenic_description(region)

            f.write(f">{seq_id} {desc}\n")

            # Write sequence in 60-character lines
            seq = region.sequence
            for i in range(0, len(seq), 60):
                f.write(seq[i:i + 60] + "\n")


def write_gff_file(
    regions: list[IntergenicRegion],
    output_file: Path,
    strain_abbrev: str,
    seq_source: str
):
    """Write intergenic regions to a GFF3 file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(output_file, "w") as f:
        f.write("##gff-version\t3\n")
        f.write(f"# Generated by {PROJECT_ACRONYM} on {timestamp}\n")
        f.write(f"# Strain: {strain_abbrev}\n")
        f.write(f"# Seq source: {seq_source}\n")
        f.write("#\n")

        for region in regions:
            # Calculate sequence statistics
            seq = region.sequence
            length = len(seq)

            if length > 0:
                gc_count = seq.upper().count("G") + seq.upper().count("C")
                at_count = seq.upper().count("A") + seq.upper().count("T")
                gc_percent = (gc_count / length) * 100
                at_percent = (at_count / length) * 100
            else:
                gc_percent = 0
                at_percent = 0

            # Format attributes
            region_id = f"{region.chromosome}:{region.start}-{region.end}"
            desc = format_intergenic_description(region)

            attributes = [
                f"ID={quote(region_id)}",
                f"Note={quote(desc)}",
                f"Length={length}",
                f"GCcontent={gc_percent:.3f}",
                f"ATcontent={at_percent:.3f}",
            ]

            # Write GFF line
            # seqid  source  type  start  end  score  strand  phase  attributes
            f.write(f"{region.chromosome}\t{PROJECT_ACRONYM}\tintergenic_region\t"
                    f"{region.start}\t{region.end}\t.\t+\t.\t{';'.join(attributes)}\n")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create intergenic region (NOT feature) files"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for files",
    )
    parser.add_argument(
        "--gff-dir",
        type=Path,
        default=None,
        help="Output directory for GFF files (default: output-dir)",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Don't gzip the FASTA output",
    )

    args = parser.parse_args()
    strain_abbrev = args.strain_abbrev

    logger.info(f"Creating intergenic region files for {strain_abbrev}")

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, strain_abbrev)
            if not config:
                logger.error(f"Strain not found: {strain_abbrev}")
                return 1

            seq_source = config["seq_source"]
            if not seq_source:
                logger.error(f"No seq_source found for {strain_abbrev}")
                return 1

            logger.info(f"Seq source: {seq_source}")

            # Get chromosomes
            chromosomes = get_chromosomes(session, strain_abbrev, seq_source)
            logger.info(f"Found {len(chromosomes)} chromosomes/contigs")

            # Find all intergenic regions
            all_regions: list[IntergenicRegion] = []

            for chr_name, chr_length in chromosomes.items():
                logger.info(f"Processing {chr_name} (length: {chr_length})")

                # Get chromosome sequence
                chr_sequence = get_chromosome_sequence(session, chr_name, seq_source)
                if not chr_sequence:
                    logger.warning(f"No sequence found for {chr_name}")
                    continue

                # Get features on this chromosome
                features = get_features_on_chromosome(
                    session, chr_name, strain_abbrev, seq_source
                )
                logger.info(f"  Found {len(features)} features")

                # Find intergenic regions
                regions = find_intergenic_regions(
                    chr_name, chr_length, features, chr_sequence
                )
                logger.info(f"  Found {len(regions)} intergenic regions")

                all_regions.extend(regions)

            logger.info(f"Total intergenic regions: {len(all_regions)}")

            # Determine output directories
            if args.output_dir:
                seq_output_dir = args.output_dir
            else:
                seq_output_dir = (
                    HTML_ROOT_DIR / "download" / "sequence" /
                    strain_abbrev / "current"
                )

            gff_output_dir = args.gff_dir or (HTML_ROOT_DIR / "download" / "gff")

            seq_output_dir.mkdir(parents=True, exist_ok=True)
            gff_output_dir.mkdir(parents=True, exist_ok=True)

            # Write FASTA file
            fasta_suffix = ".fasta.gz" if not args.no_compress else ".fasta"
            fasta_file = seq_output_dir / f"not_feature{fasta_suffix}"
            write_fasta_file(all_regions, fasta_file, compress=not args.no_compress)
            logger.info(f"FASTA written to {fasta_file}")

            # Write GFF file
            gff_file = gff_output_dir / f"{strain_abbrev}_intergenic.gff"
            write_gff_file(all_regions, gff_file, strain_abbrev, seq_source)
            logger.info(f"GFF written to {gff_file}")

            # Summary statistics
            total_length = sum(len(r.sequence) for r in all_regions)
            avg_length = total_length / len(all_regions) if all_regions else 0
            logger.info(f"Total intergenic sequence: {total_length:,} bp")
            logger.info(f"Average region length: {avg_length:.1f} bp")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
