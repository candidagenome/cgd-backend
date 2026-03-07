#!/usr/bin/env python3
"""
Dump GFF file for C. albicans Assembly 20.

This script creates a GFF3 file for Candida albicans Assembly 20,
including chromosome features, contig mappings, and gene features.

Based on dumpGFF_Assem20.pl.

Usage:
    python dump_gff_assem20.py
    python dump_gff_assem20.py --debug

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    HTML_ROOT_DIR: HTML root directory
"""

import argparse
import logging
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

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
HTML_ROOT = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output paths
GBROWSE_DIR = HTML_ROOT / "gbrowse" / "databases" / "candida_20"
DOWNLOAD_DIR = HTML_ROOT / "download" / "gff"

# Assembly configuration
SEQ_SOURCE = "C. albicans SC5314 Assembly 20"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_root_sequences(session, seq_source: str) -> list[tuple[str, int]]:
    """Get root sequences (chromosomes) for an assembly."""
    query = text(f"""
        SELECT f.feature_name, f.stop_coord
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.sequence s ON f.feature_no = s.root_feature_no
        WHERE s.seq_source = :seq_source
        AND f.feature_type = 'chromosome'
        ORDER BY f.feature_name
    """)

    results = []
    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        results.append((row[0], row[1]))

    return results


def get_features_for_assembly(session, seq_source: str) -> list[dict]:
    """Get all features with locations in the given assembly."""
    query = text(f"""
        SELECT f.feature_name, f.feature_type, f.gene_name, f.headline,
               fl.start_coord, fl.stop_coord, fl.strand,
               s.root_feature_name, fp.property_value as feature_qualifier
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.sequence s ON fl.seq_no = s.seq_no
        LEFT JOIN {DB_SCHEMA}.feat_property fp ON f.feature_no = fp.feature_no
            AND fp.property_type = 'Feature Qualifier'
        WHERE s.seq_source = :seq_source
        AND f.feature_type NOT IN ('chromosome', 'contig')
        ORDER BY s.root_feature_name, fl.start_coord
    """)

    features = []
    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        features.append({
            "feature_name": row[0],
            "feature_type": row[1],
            "gene_name": row[2],
            "headline": row[3],
            "start_coord": row[4],
            "stop_coord": row[5],
            "strand": row[6],
            "root_sequence": row[7],
            "feature_qualifier": row[8],
        })

    return features


def get_feature_aliases(session, feature_name: str) -> list[str]:
    """Get aliases for a feature."""
    query = text(f"""
        SELECT a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON a.alias_no = fa.alias_no
        JOIN {DB_SCHEMA}.feature f ON fa.feature_no = f.feature_no
        WHERE f.feature_name = :name
    """)

    aliases = []
    for row in session.execute(query, {"name": feature_name}).fetchall():
        if row[0]:
            aliases.append(row[0])

    return aliases


def get_subfeatures(session, feature_name: str, seq_source: str) -> list[dict]:
    """Get subfeatures (CDS, introns, etc.) for a feature."""
    query = text(f"""
        SELECT sf.subfeature_type, sf.start_coord, sf.stop_coord
        FROM {DB_SCHEMA}.subfeature sf
        JOIN {DB_SCHEMA}.feature f ON sf.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.sequence s ON sf.seq_no = s.seq_no
        WHERE f.feature_name = :name
        AND s.seq_source = :seq_source
        ORDER BY sf.start_coord
    """)

    subfeatures = []
    for row in session.execute(
        query, {"name": feature_name, "seq_source": seq_source}
    ).fetchall():
        subfeatures.append({
            "type": row[0],
            "start": row[1],
            "stop": row[2],
        })

    return subfeatures


def parse_embl_contigs(data_dir: Path) -> dict[str, list[dict]]:
    """
    Parse EMBL files to get contig to chromosome mappings.

    Returns dict mapping contig name to list of mapping info.
    """
    contig_mapping: dict[str, list[dict]] = {}

    embl_dir = data_dir / "Ca20_EMBL_as_released"
    if not embl_dir.exists():
        logger.warning(f"EMBL directory not found: {embl_dir}")
        return contig_mapping

    try:
        from Bio import SeqIO

        for embl_file in sorted(embl_dir.glob("*.embl.out")):
            # Extract chromosome from filename (Ca20Chr{chr}.*)
            match = re.search(r"Ca20Chr(.+?)\.", embl_file.name)
            if not match:
                continue

            chr_id = match.group(1)

            for record in SeqIO.parse(embl_file, "embl"):
                for feature in record.features:
                    if feature.type != "misc_feature":
                        continue

                    start = int(feature.location.start) + 1
                    end = int(feature.location.end)
                    strand = "+" if feature.location.strand == 1 else "-"

                    notes = feature.qualifiers.get("note", [])
                    note = " ".join(notes)

                    # Check for contig name
                    contig_match = re.match(r"^(Ca19-\d+)", note)
                    if contig_match:
                        contig_name = contig_match.group(1).replace("Ca", "Contig")
                        if contig_name not in contig_mapping:
                            contig_mapping[contig_name] = []
                        contig_mapping[contig_name].append({
                            "chr": chr_id,
                            "start": start,
                            "end": end,
                            "strand": strand,
                            "note": note,
                        })

    except ImportError:
        logger.warning("BioPython not available, skipping EMBL parsing")

    return contig_mapping


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GFF file for C. albicans Assembly 20"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Create output directories
    GBROWSE_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Output files
    out_file = GBROWSE_DIR / "candida_20.gff"
    tmp_file = out_file.with_suffix(".gff.tmp")

    try:
        with SessionLocal() as session:
            # Get chromosomes
            logger.info("Getting chromosomes...")
            chromosomes = get_root_sequences(session, SEQ_SOURCE)
            logger.info(f"Found {len(chromosomes)} chromosomes")

            # Get features
            logger.info("Getting features...")
            features = get_features_for_assembly(session, SEQ_SOURCE)
            logger.info(f"Found {len(features)} features")

            # Parse contig mappings from EMBL files
            logger.info("Parsing contig mappings...")
            contig_mapping = parse_embl_contigs(DATA_DIR)
            logger.info(f"Found {len(contig_mapping)} contig mappings")

            # Write GFF file
            with open(tmp_file, "w") as f:
                # Header
                f.write("##gff-version\t3\n")
                f.write(f"# date\t{datetime.now()}\n")

                # Chromosomes
                for chr_name, length in chromosomes:
                    f.write(
                        f"{chr_name}\t{PROJECT_ACRONYM}\tchromosome\t1\t{length}\t"
                        f".\t.\t.\tID={chr_name}\n"
                    )

                # Contig mappings
                for contig_name, mappings in sorted(contig_mapping.items()):
                    for i, mapping in enumerate(mappings):
                        chr_name = f"Ca20chr{mapping['chr']}"
                        segment_id = f"{contig_name}_{i + 1}" if len(mappings) > 1 else contig_name

                        note = quote(
                            "Mappings of Assembly19 contigs to Assembly 20 - provided by BRI"
                        )
                        attrs = f"ID={segment_id};Name={contig_name};Note={note}"

                        f.write(
                            f"{chr_name}\tContig19\tAssem19Contig-BRI\t"
                            f"{mapping['start']}\t{mapping['end']}\t.\t"
                            f"{mapping['strand']}\t.\t{attrs}\n"
                        )

                # Features
                for feat in features:
                    # Skip deleted features (except those deleted from A21)
                    qualifier = feat["feature_qualifier"] or ""
                    if "Deleted" in qualifier and "Deleted from Assembly 21" not in qualifier:
                        continue

                    name = feat["feature_name"]
                    root_seq = feat["root_sequence"]
                    feat_type = feat["feature_type"].replace(" ", "_")

                    # Alleles treated as ORF
                    if feat_type == "allele":
                        feat_type = "ORF"

                    # Handle coordinates
                    start = feat["start_coord"]
                    end = feat["stop_coord"]
                    if start > end:
                        start, end = end, start

                    strand = "-" if feat["strand"] == "C" else "+"

                    # Build attributes
                    attrs = [f"ID={name};Name={name}"]

                    if feat["gene_name"]:
                        attrs.append(f"Gene={quote(feat['gene_name'])}")

                    if feat["headline"]:
                        attrs.append(f"Note={quote(feat['headline'])}")

                    # Get aliases
                    aliases = get_feature_aliases(session, name)
                    if aliases:
                        alias_str = ",".join(quote(a) for a in aliases)
                        attrs.append(f"Alias={alias_str}")

                    f.write(
                        f"{root_seq}\t{PROJECT_ACRONYM}\t{feat_type}\t"
                        f"{start}\t{end}\t.\t{strand}\t.\t"
                        f"{';'.join(attrs)}\n"
                    )

                    # Subfeatures
                    subfeatures = get_subfeatures(session, name, SEQ_SOURCE)
                    for sf in subfeatures:
                        sf_start, sf_end = sf["start"], sf["stop"]
                        if strand == "-":
                            sf_start, sf_end = sf_end, sf_start

                        f.write(
                            f"{root_seq}\t{PROJECT_ACRONYM}\t{sf['type']}\t"
                            f"{sf_start}\t{sf_end}\t.\t{strand}\t.\t"
                            f"Parent={name}\n"
                        )

            # Move temp file to final location
            shutil.move(str(tmp_file), str(out_file))
            logger.info(f"Created {out_file}")

            # Copy to download directory
            download_file = DOWNLOAD_DIR / "candida_20.gff"
            shutil.copy(str(out_file), str(download_file))
            logger.info(f"Copied to {download_file}")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())

        # Clean up temp file
        if tmp_file.exists():
            tmp_file.unlink()

        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
