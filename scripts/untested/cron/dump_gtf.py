#!/usr/bin/env python3
"""
Dump gene annotation data in GTF (Gene Transfer Format) format.

This script exports gene annotations (ORFs with their exon/CDS structures)
to standard GTF format for use with bioinformatics tools like TopHat,
Cufflinks, etc.

GTF Format:
- Tab-separated: seqname, source, feature, start, end, score, strand, frame, attributes
- Features include: exon, CDS, start_codon, stop_codon
- Attributes: gene_id, transcript_id, exon_number

Based on dumpGTF.pl by CGD team.

Usage:
    python dump_gtf.py <strain_abbrev>
    python dump_gtf.py C_albicans_SC5314 > genes.gtf
    python dump_gtf.py C_albicans_SC5314 --output genes.gtf

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    DATA_DIR: Base data directory
    LOG_DIR: Directory for log files
"""

import argparse
import gzip
import logging
import os
import sys
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
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# Configure logging to stderr so stdout can be used for GTF output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


def get_strain_config(session, strain_abbrev: str) -> dict | None:
    """Get strain configuration from database."""
    query = text(f"""
        SELECT o.organism_no, o.organism_abbrev, o.taxon_id
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
        "taxon_id": result[2],
        "seq_source": seq_result[0] if seq_result else None,
    }


def get_features(session, seq_source: str) -> list[dict]:
    """Get ORF features with their location information."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.feature_qualifier,
               fl.strand, s.seq_name as chr_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl
            ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s
            ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        WHERE f.feature_type = 'ORF'
        ORDER BY s.seq_name, fl.min_coord
    """)

    features = []
    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        feature_qualifier = row[3] or ""
        if "Deleted" in feature_qualifier:
            continue

        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2] or row[1],
            "strand": "-" if row[4] == "C" else "+",
            "chr_name": row[5],
        })

    return features


def get_subfeatures(session, feature_no: int) -> list[dict]:
    """Get CDS subfeatures (exons) for a feature."""
    query = text(f"""
        SELECT sf.subfeature_type, sf.relative_coord_start, sf.relative_coord_end
        FROM {DB_SCHEMA}.subfeature sf
        WHERE sf.feature_no = :feature_no
        AND sf.subfeature_type = 'CDS'
        ORDER BY sf.relative_coord_start
    """)

    subfeatures = []
    for row in session.execute(query, {"feature_no": feature_no}).fetchall():
        start = row[1]
        end = row[2]
        # Ensure start < end
        if start > end:
            start, end = end, start
        subfeatures.append({
            "type": row[0],
            "start": start,
            "end": end,
        })

    return subfeatures


def get_coding_sequence(session, feature_name: str, seq_source: str) -> str | None:
    """Get the coding sequence for a feature to determine start/stop codons."""
    # This is a simplified version - in production, you might read from FASTA files
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE f.feature_name = :feature_name
        AND fl.is_loc_current = 'Y'
    """)
    result = session.execute(
        query, {"feature_name": feature_name}
    ).fetchone()

    return result[0] if result else None


def dump_gtf(
    session,
    strain_abbrev: str,
    output_file=None,
) -> int:
    """
    Dump GTF format gene annotations.

    Args:
        session: Database session
        strain_abbrev: Strain abbreviation
        output_file: Output file handle (defaults to stdout)

    Returns:
        Number of features written
    """
    if output_file is None:
        output_file = sys.stdout

    # Get strain config
    config = get_strain_config(session, strain_abbrev)
    if not config:
        logger.error(f"Strain {strain_abbrev} not found in database")
        return 0

    if not config["seq_source"]:
        logger.error(f"No seq_source found for {strain_abbrev}")
        return 0

    seq_source = config["seq_source"]
    source = PROJECT_ACRONYM

    logger.info(f"Dumping GTF for {strain_abbrev} (seq_source: {seq_source})")

    # Get features
    features = get_features(session, seq_source)
    logger.info(f"Found {len(features)} ORF features")

    count = 0

    for feat in features:
        feature_name = feat["feature_name"]
        chr_name = feat["chr_name"]
        strand = feat["strand"]

        # Get subfeatures (exons/CDS)
        subfeatures = get_subfeatures(session, feat["feature_no"])

        if not subfeatures:
            continue

        # Sort by position
        subfeatures = sorted(subfeatures, key=lambda x: x["start"])

        # Build attribute string
        attr_base = f'gene_id "{feature_name}"; transcript_id "{feature_name}";'

        # For strand -, process exons in reverse order for exon numbering
        if strand == "-":
            exon_order = list(reversed(range(len(subfeatures))))
        else:
            exon_order = list(range(len(subfeatures)))

        total_length = 0

        for exon_num, exon_idx in enumerate(exon_order, 1):
            sf = subfeatures[exon_idx]

            attr = f'{attr_base} exon_number "{exon_num}";'
            frame = total_length % 3

            # Print exon feature
            output_file.write(
                f"{chr_name}\t{source}\texon\t{sf['start']}\t{sf['end']}\t.\t"
                f"{strand}\t{frame}\t{attr}\n"
            )

            # Print CDS feature
            output_file.write(
                f"{chr_name}\t{source}\tCDS\t{sf['start']}\t{sf['end']}\t.\t"
                f"{strand}\t{frame}\t{attr}\n"
            )

            # Track total length for frame calculation
            total_length += (sf["end"] - sf["start"] + 1)

        count += 1

    logger.info(f"Wrote {count} features to GTF")
    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump gene annotation data in GTF format"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--gzip", "-z",
        action="store_true",
        help="Gzip the output file",
    )

    args = parser.parse_args()

    try:
        with SessionLocal() as session:
            if args.output:
                if args.gzip:
                    with gzip.open(args.output, "wt") as f:
                        count = dump_gtf(session, args.strain_abbrev, f)
                else:
                    with open(args.output, "w") as f:
                        count = dump_gtf(session, args.strain_abbrev, f)
                logger.info(f"Output written to {args.output}")
            else:
                count = dump_gtf(session, args.strain_abbrev)

            if count == 0:
                return 1

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
