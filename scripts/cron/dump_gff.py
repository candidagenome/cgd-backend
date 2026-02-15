#!/usr/bin/env python3
"""
Dump gene annotation data in GFF3 (General Feature Format version 3) format.

This script exports gene annotations (features with their subfeatures/exons)
to standard GFF3 format for use with genome browsers and bioinformatics tools.

GFF3 Format:
- Tab-separated: seqname, source, feature, start, end, score, strand, phase, attributes
- Features include: chromosome, ORF, gene, CDS, etc.
- Attributes: ID, Name, Gene, Note, Alias, orf_classification

Based on dumpGFF_generic.pl by CGD team.

Usage:
    python dump_gff.py <strain_abbrev> [seq_source]
    python dump_gff.py C_albicans_SC5314 > genes.gff
    python dump_gff.py C_albicans_SC5314 --output genes.gff

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

# Configure logging to stderr so stdout can be used for GFF output
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

    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "taxon_id": result[2],
    }


def get_seq_source(session, strain_abbrev: str) -> str | None:
    """Get sequence source for a strain."""
    query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_abbrev = :strain_abbrev
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_root_sequences(session, seq_source: str, strain_abbrev: str) -> list[dict]:
    """Get root sequences (chromosomes/contigs) for an assembly."""
    query = text(f"""
        SELECT f.feature_name, f.feature_type, fl.max_coord
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.seq s ON f.feature_no = s.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        WHERE s.source = :seq_source
        AND s.is_seq_current = 'Y'
        AND f.feature_type IN ('chromosome', 'contig')
        ORDER BY f.feature_name
    """)

    roots = []
    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        roots.append({
            "name": row[0],
            "type": row[1],
            "length": row[2],
        })

    return roots


def get_features(session, strain_abbrev: str, seq_source: str) -> list[dict]:
    """Get features for a strain that have current locations."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.feature_type,
               f.feature_qualifier, f.dbxref_id, f.headline,
               fl.min_coord, fl.max_coord, fl.strand,
               s.seq_name as root_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        WHERE f.organism_abbrev = :strain_abbrev
        ORDER BY s.seq_name, fl.min_coord
    """)

    features = []
    for row in session.execute(
        query, {"strain_abbrev": strain_abbrev, "seq_source": seq_source}
    ).fetchall():
        feature_qualifier = row[4] or ""

        # Skip deleted features (unless it's a specific assembly exception)
        if "Deleted" in feature_qualifier:
            continue

        features.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "gene_name": row[2],
            "feature_type": row[3],
            "feature_qualifier": feature_qualifier,
            "dbxref_id": row[5],
            "headline": row[6],
            "min_coord": row[7],
            "max_coord": row[8],
            "strand": row[9],
            "root_name": row[10],
        })

    return features


def get_feature_aliases(session, feature_no: int) -> list[str]:
    """Get aliases for a feature."""
    query = text(f"""
        SELECT a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON a.alias_no = fa.alias_no
        WHERE fa.feature_no = :feature_no
    """)
    return [row[0] for row in session.execute(query, {"feature_no": feature_no}).fetchall()]


def get_subfeatures(session, feature_no: int) -> list[dict]:
    """Get subfeatures (CDS, exons) for a feature."""
    query = text(f"""
        SELECT sf.subfeature_type, sf.relative_coord_start, sf.relative_coord_end
        FROM {DB_SCHEMA}.subfeature sf
        WHERE sf.feature_no = :feature_no
        ORDER BY sf.relative_coord_start
    """)

    subfeatures = []
    for row in session.execute(query, {"feature_no": feature_no}).fetchall():
        start = row[1]
        end = row[2]
        # Ensure start <= end
        if start > end:
            start, end = end, start
        subfeatures.append({
            "type": row[0],
            "start": start,
            "end": end,
        })

    return subfeatures


def escape_gff_value(value: str) -> str:
    """URL-encode special characters in GFF attribute values."""
    # GFF3 spec requires URL encoding for certain characters
    return quote(str(value), safe="")


def format_gff_attributes(attrs: dict) -> str:
    """Format attributes dict as GFF3 attribute string."""
    parts = []
    for key, value in attrs.items():
        if value is not None:
            if isinstance(value, list):
                value = ",".join(escape_gff_value(v) for v in value)
            else:
                value = escape_gff_value(str(value))
            parts.append(f"{key}={value}")
    return ";".join(parts)


def dump_gff(
    session,
    strain_abbrev: str,
    seq_source: str | None = None,
    output_file=None,
) -> int:
    """
    Dump GFF3 format gene annotations.

    Args:
        session: Database session
        strain_abbrev: Strain abbreviation
        seq_source: Sequence source (optional, auto-detected if not provided)
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

    # Get or determine seq_source
    if not seq_source:
        seq_source = get_seq_source(session, strain_abbrev)
        if not seq_source:
            logger.error(f"No seq_source found for {strain_abbrev}")
            return 0

    logger.info(f"Dumping GFF for {strain_abbrev} (seq_source: {seq_source})")

    source = PROJECT_ACRONYM

    # Write GFF header
    output_file.write("##gff-version\t3\n")

    # Write root sequences (chromosomes/contigs)
    roots = get_root_sequences(session, seq_source, strain_abbrev)
    logger.info(f"Found {len(roots)} root sequences")

    for root in roots:
        attrs = format_gff_attributes({
            "ID": root["name"],
            "Name": root["name"],
        })
        output_file.write(
            f"{root['name']}\t{source}\t{root['type']}\t1\t{root['length']}\t.\t.\t.\t{attrs}\n"
        )

    # Get features
    features = get_features(session, strain_abbrev, seq_source)
    logger.info(f"Found {len(features)} features")

    count = 0

    for feat in features:
        feature_name = feat["feature_name"]
        root_name = feat["root_name"]
        feature_type = feat["feature_type"].replace(" ", "_")

        # Get coordinates
        start = feat["min_coord"]
        end = feat["max_coord"]
        if start > end:
            start, end = end, start

        strand = "-" if feat["strand"] == "C" else "+"

        # Build attributes
        attrs = {"ID": feature_name, "Name": feature_name}

        if feat["gene_name"]:
            attrs["Gene"] = feat["gene_name"]

        if feat["headline"]:
            # Strip HTML tags from headline
            headline = feat["headline"]
            import re
            headline = re.sub(r"<[^<>]+>", "", headline)
            attrs["Note"] = headline

        # Get ORF classification from qualifier
        if feat["feature_qualifier"]:
            import re
            match = re.search(r"(Verified|Uncharacterized|Dubious)", feat["feature_qualifier"])
            if match:
                attrs["orf_classification"] = match.group(1)

        # Get aliases
        aliases = get_feature_aliases(session, feat["feature_no"])
        if aliases:
            attrs["Alias"] = aliases

        attr_str = format_gff_attributes(attrs)
        output_file.write(
            f"{root_name}\t{source}\t{feature_type}\t{start}\t{end}\t.\t{strand}\t.\t{attr_str}\n"
        )

        # Get and write subfeatures
        subfeatures = get_subfeatures(session, feat["feature_no"])
        for sf in subfeatures:
            sf_attrs = {"Parent": feature_name}

            # Add orf_classification to CDS subfeatures
            if feature_type == "ORF" and sf["type"] == "CDS":
                if "orf_classification" in attrs:
                    sf_attrs["orf_classification"] = attrs["orf_classification"]

            sf_attrs["parent_feature_type"] = feature_type

            sf_start = sf["start"]
            sf_end = sf["end"]
            if strand == "-":
                sf_start, sf_end = sf_end, sf_start

            sf_attr_str = format_gff_attributes(sf_attrs)
            output_file.write(
                f"{root_name}\t{source}\t{sf['type']}\t{sf_start}\t{sf_end}\t.\t{strand}\t.\t{sf_attr_str}\n"
            )

        count += 1

    logger.info(f"Wrote {count} features to GFF")
    return count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump gene annotation data in GFF3 format"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "seq_source",
        nargs="?",
        default=None,
        help="Sequence source (optional, auto-detected if not provided)",
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
                        count = dump_gff(session, args.strain_abbrev, args.seq_source, f)
                else:
                    with open(args.output, "w") as f:
                        count = dump_gff(session, args.strain_abbrev, args.seq_source, f)
                logger.info(f"Output written to {args.output}")
            else:
                count = dump_gff(session, args.strain_abbrev, args.seq_source)

            if count == 0:
                return 1

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
