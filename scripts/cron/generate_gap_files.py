#!/usr/bin/env python3
"""
Generate files listing genes with introns and adjustments (gaps).

This script creates three files:
1. GenesWithIntrons.tab - One line per gene with intron information
2. ORFsWithAdjustments.tab - One line per ORF with adjustment information
3. AllGaps.tab - One line per gap (intron or adjustment)

Based on generateGapFiles.pl by Prachi Shah (May 2007).

Usage:
    python generate_gap_files.py <strain_abbrev>
    python generate_gap_files.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for download files
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class GapInfo:
    """Information about a gap (intron or adjustment)."""
    feature_name: str
    gene_name: str | None
    chromosome: str
    start: int
    end: int
    strand: str
    gap_type: str
    gap_length: int


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


def get_features_with_gaps(
    session, gap_type: str, seq_source: str
) -> list[dict]:
    """Get all features that have a specific type of gap (intron or adjustment)."""
    query = text(f"""
        SELECT DISTINCT f1.feature_no, f1.feature_name, f1.gene_name
        FROM {DB_SCHEMA}.feature f1
        JOIN {DB_SCHEMA}.feat_relationship fr ON f1.feature_no = fr.parent_feature_no
        JOIN {DB_SCHEMA}.feature f2 ON fr.child_feature_no = f2.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON f2.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        JOIN {DB_SCHEMA}.feature f3 ON s.feature_no = f3.feature_no
        WHERE f2.feature_type = :gap_type
        AND s.is_seq_current = 'Y'
        AND fl.is_loc_current = 'Y'
        AND (s.source = :seq_source OR f3.feature_name LIKE '%mtDNA')
        ORDER BY f1.feature_name
    """)

    features = []
    for row in session.execute(
        query, {"gap_type": gap_type, "seq_source": seq_source}
    ).fetchall():
        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2],
        })

    return features


def get_feature_location(session, feature_no: int, seq_source: str) -> dict | None:
    """Get location information for a feature."""
    query = text(f"""
        SELECT fl.min_coord, fl.max_coord, fl.strand, f2.feature_name
        FROM {DB_SCHEMA}.feat_location fl
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        JOIN {DB_SCHEMA}.feature f2 ON s.feature_no = f2.feature_no
        WHERE fl.feature_no = :feature_no
        AND fl.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND (s.source = :seq_source OR f2.feature_name LIKE '%mtDNA')
    """)
    result = session.execute(
        query, {"feature_no": feature_no, "seq_source": seq_source}
    ).fetchone()

    if not result:
        return None

    return {
        "start": result[0],
        "end": result[1],
        "strand": result[2],
        "chromosome": result[3],
    }


def get_subfeatures(session, feature_no: int) -> list[dict]:
    """Get subfeatures for a feature."""
    query = text(f"""
        SELECT f2.feature_type, sf.relative_coord_start, sf.relative_coord_end
        FROM {DB_SCHEMA}.feat_relationship fr
        JOIN {DB_SCHEMA}.feature f2 ON fr.child_feature_no = f2.feature_no
        JOIN {DB_SCHEMA}.subfeature sf ON f2.feature_no = sf.feature_no
        WHERE fr.parent_feature_no = :feature_no
        ORDER BY sf.relative_coord_start
    """)

    subfeatures = []
    for row in session.execute(query, {"feature_no": feature_no}).fetchall():
        subfeatures.append({
            "type": row[0],
            "start": row[1],
            "end": row[2],
        })

    return subfeatures


def get_feature_subfeature_details(
    session, feature_no: int, seq_source: str
) -> tuple[list[str], list[GapInfo]]:
    """
    Get detailed subfeature information for a feature.

    Returns:
        Tuple of (exon_segments, gap_info_list)
    """
    location = get_feature_location(session, feature_no, seq_source)
    if not location:
        return [], []

    # Get feature info
    feat_query = text(f"""
        SELECT f.feature_name, f.gene_name
        FROM {DB_SCHEMA}.feature f
        WHERE f.feature_no = :feature_no
    """)
    feat_result = session.execute(feat_query, {"feature_no": feature_no}).fetchone()
    if not feat_result:
        return [], []

    feature_name = feat_result[0]
    gene_name = feat_result[1]
    chromosome = location["chromosome"]
    strand = location["strand"]

    # Get subfeatures
    subfeatures = get_subfeatures(session, feature_no)

    exon_segments = []
    gaps = []

    for sf in subfeatures:
        sf_type = sf["type"]
        sf_start = sf["start"]
        sf_end = sf["end"]

        if sf_type in ("CDS", "noncoding_exon"):
            exon_segments.append(f"{sf_start}-{sf_end}")
        elif sf_type in ("intron", "adjustment"):
            # Calculate gap length based on strand
            if strand == "C":
                gap_length = sf_start - sf_end + 1
            else:
                gap_length = sf_end - sf_start + 1

            gaps.append(GapInfo(
                feature_name=feature_name,
                gene_name=gene_name,
                chromosome=chromosome,
                start=sf_start,
                end=sf_end,
                strand=strand,
                gap_type=sf_type,
                gap_length=abs(gap_length),
            ))

    return exon_segments, gaps


def write_introns_file(
    session, seq_source: str, output_file: Path
):
    """Write file listing genes with introns."""
    features = get_features_with_gaps(session, "intron", seq_source)
    logger.info(f"Found {len(features)} features with introns")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(output_file, "w") as f:
        f.write(f"# This file was created at {PROJECT_ACRONYM} on: {timestamp}\n\n")
        f.write("Gene name\tLocus name\tCoordinates\tGap length(s)\tType of gap(s)\n")

        for feat in features:
            exons, gaps = get_feature_subfeature_details(
                session, feat["feature_no"], seq_source
            )

            if not gaps:
                continue

            # Filter for introns only
            introns = [g for g in gaps if g.gap_type == "intron"]
            if not introns:
                continue

            chromosome = introns[0].chromosome
            strand = introns[0].strand

            coords = f"{chromosome}:{','.join(exons)}{strand}"
            lengths = ",".join(str(g.gap_length) for g in introns)
            types = ",".join(g.gap_type for g in introns)

            f.write(f"{feat['feature_name']}\t{feat['gene_name'] or ''}\t"
                    f"{coords}\t{lengths}\t{types}\n")


def write_adjustments_file(
    session, seq_source: str, output_file: Path
):
    """Write file listing ORFs with adjustments."""
    features = get_features_with_gaps(session, "adjustment", seq_source)
    logger.info(f"Found {len(features)} features with adjustments")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(output_file, "w") as f:
        f.write(f"# This file was created at {PROJECT_ACRONYM} on: {timestamp}\n\n")
        f.write("ORF name\tLocus name\tCoordinates\tGap length(s)\tType of gap(s)\n")

        for feat in features:
            exons, gaps = get_feature_subfeature_details(
                session, feat["feature_no"], seq_source
            )

            if not gaps:
                continue

            # Filter for adjustments only
            adjustments = [g for g in gaps if g.gap_type == "adjustment"]
            if not adjustments:
                continue

            chromosome = adjustments[0].chromosome
            strand = adjustments[0].strand

            coords = f"{chromosome}:{','.join(exons)}{strand}"
            lengths = ",".join(str(g.gap_length) for g in adjustments)
            types = ",".join(g.gap_type for g in adjustments)

            f.write(f"{feat['feature_name']}\t{feat['gene_name'] or ''}\t"
                    f"{coords}\t{lengths}\t{types}\n")


def write_all_gaps_file(
    session, seq_source: str, output_file: Path
):
    """Write file listing all gaps (introns and adjustments)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get features with introns
    intron_features = get_features_with_gaps(session, "intron", seq_source)

    # Get features with adjustments
    adj_features = get_features_with_gaps(session, "adjustment", seq_source)

    # Combine and deduplicate
    all_feature_nos = set()
    all_features = []

    for feat in intron_features + adj_features:
        if feat["feature_no"] not in all_feature_nos:
            all_feature_nos.add(feat["feature_no"])
            all_features.append(feat)

    logger.info(f"Found {len(all_features)} features with gaps")

    with open(output_file, "w") as f:
        f.write(f"# This file was created at {PROJECT_ACRONYM} on: {timestamp}\n\n")
        f.write("ORF name\tLocus name\tGap coordinates\tGap length\tType of gap\n")

        for feat in all_features:
            _, gaps = get_feature_subfeature_details(
                session, feat["feature_no"], seq_source
            )

            for gap in gaps:
                gap_coords = f"{gap.chromosome}:{gap.start}-{gap.end}{gap.strand}"
                f.write(f"{gap.feature_name}\t{gap.gene_name or ''}\t"
                        f"{gap_coords}\t{gap.gap_length}\t{gap.gap_type}\n")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate files listing genes with introns and adjustments"
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
        "--no-introns",
        action="store_true",
        help="Don't write introns file",
    )
    parser.add_argument(
        "--no-adjustments",
        action="store_true",
        help="Don't write adjustments file",
    )
    parser.add_argument(
        "--no-all-gaps",
        action="store_true",
        help="Don't write all gaps file",
    )

    args = parser.parse_args()
    strain_abbrev = args.strain_abbrev

    logger.info(f"Generating gap files for {strain_abbrev}")

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

            # Determine output directory
            output_dir = args.output_dir or (HTML_ROOT_DIR / "download" / "CurrentNotes")
            output_dir.mkdir(parents=True, exist_ok=True)

            # Write introns file
            if not args.no_introns:
                introns_file = output_dir / "GenesWithIntrons.tab"
                write_introns_file(session, seq_source, introns_file)
                logger.info(f"Introns file written to {introns_file}")

            # Write adjustments file
            if not args.no_adjustments:
                adjs_file = output_dir / "ORFsWithAdjustments.tab"
                write_adjustments_file(session, seq_source, adjs_file)
                logger.info(f"Adjustments file written to {adjs_file}")

            # Write all gaps file
            if not args.no_all_gaps:
                all_gaps_file = output_dir / "AllGaps.tab"
                write_all_gaps_file(session, seq_source, all_gaps_file)
                logger.info(f"All gaps file written to {all_gaps_file}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
