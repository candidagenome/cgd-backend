#!/usr/bin/env python3
"""
Generate GFF3 format files.

This script generates GFF3 format files to represent sequence features.
It supports multiple output types: ORFMAP, CloneGFF, and Regulatory.

Based on generateGFF3.pl.

Usage:
    python generate_gff3.py ORFMAP output.gff
    python generate_gff3.py CloneGFF clonedata.gff
    python generate_gff3.py Regulatory regulatory.gff
    python generate_gff3.py --help

Arguments:
    app_name: Application name (ORFMAP, CloneGFF, or Regulatory)
    filename: Output filename (without path)

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    FTP_DIR: FTP directory for output files
    PROJECT_ACRONYM: Project acronym (e.g., CGD, SGD)
    ORGANISM_NAME: Organism name
    STRAIN_NAME: Strain name

Output Files:
    GFF3 format files in data_download/chromosomal_feature/
"""

import argparse
import html
import logging
import os
import re
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
FTP_DIR = Path(os.getenv("FTP_DIR", "/var/ftp/cgd"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
ORGANISM_NAME = os.getenv("ORGANISM_NAME", "Candida albicans")
STRAIN_NAME = os.getenv("STRAIN_NAME", "SC5314")
HTML_ROOT_URL = os.getenv("HTML_ROOT_URL", "http://www.candidagenome.org")
FTP_ROOT_URL = os.getenv("FTP_ROOT_URL", "ftp://ftp.candidagenome.org/")
HELP_EMAIL = os.getenv("HELP_EMAIL", "candida-curator@lists.stanford.edu")

# Output directory
GFF_DIR = FTP_DIR / "data_download" / "chromosomal_feature"

# Sequence length per line
SEQ_LEN = 80

# Constants
IGNORE = "."
WATSON = "W"
CRICK = "C"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Feature type to SO term mapping
TYPE_TO_SO = {
    "ORF": "gene",
    "telomeric_repeat": "repeat_family",
    "X_element_combinatorial_repeats": "repeat_family",
    "X_element_core_sequence": "repeat_family",
    "Y'_element": "repeat_family",
    "ARS consensus sequence": "nucleotide_match",
    "CDEI": "region",
    "CDEII": "region",
    "CDEIII": "region",
    "plus_1_translational_frameshift": "region",
    "noncoding_exon": "ncRNA",
    "external_transcribed_spacer_region": "nc_primary_transcript",
    "internal_transcribed_spacer_region": "nc_primary_transcript",
    "long_terminal_repeat": "repeat_region",
    "retrotransposon": "transposable_element",
    "non_transcribed_region": "region",
}

# SO term to attribute mapping
SO_TO_ATTRIBUTE = {
    "noncoding_exon": "0000198",
    "plus_1_translational_frameshift": "1001263",
    "external_transcribed_spacer_region": "0000640",
    "internal_transcribed_spacer_region": "0000639",
    "long_terminal_repeat": "0000286",
    "retrotransposon": "0000180",
    "non_transcribed_region": "0000183",
}

# Feature types to exclude
TYPE_TO_EXCLUDE = {
    "Deleted",
    "Merged",
    "not in systematic sequence of S288C",
    "not physically mapped",
}

# Qualifier values to exclude
QUALIFIER_TO_EXCLUDE = {"Deleted", "Merged"}


def delete_html_tag(text_str: str | None) -> str:
    """Remove HTML tags from text."""
    if not text_str:
        return ""
    clean = re.sub(r"<[^>]+>", "", text_str)
    return html.unescape(clean)


def delete_unwanted_char(text_str: str | None) -> str:
    """Remove unwanted characters from text."""
    if not text_str:
        return ""
    # Remove control characters
    clean = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", text_str)
    return clean.strip()


def url_escape(text_str: str, tag: str) -> str:
    """URL-encode special characters in GFF3 attribute values."""
    # Characters to keep unescaped
    safe_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

    if tag in ("Ontology_term", "Alias", "dbxref"):
        safe_chars += ",:"

    return quote(text_str, safe=safe_chars)


def get_chromosome_symbol_map(session) -> dict[str, str]:
    """Get mapping from chromosome number to Roman numeral."""
    # Standard Roman numeral mapping
    num_to_roman = {
        "1": "I", "2": "II", "3": "III", "4": "IV", "5": "V",
        "6": "VI", "7": "VII", "8": "VIII", "9": "IX", "10": "X",
        "11": "XI", "12": "XII", "13": "XIII", "14": "XIV", "15": "XV",
        "16": "XVI", "17": "Mito",
    }
    return num_to_roman


def get_seqid(chr_num: str, chr_symbol_map: dict[str, str]) -> str:
    """Get sequence ID for GFF3 from chromosome number."""
    if chr_num in chr_symbol_map:
        return f"chr{chr_symbol_map[chr_num]}"
    return chr_num


def get_feature_qualifiers(session) -> dict[str, str]:
    """Get feature qualifiers for all features."""
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


def get_chromosome_lengths(session) -> dict[str, int]:
    """Get chromosome lengths."""
    query = text(f"""
        SELECT f.feature_name, fl.stop_coord
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        WHERE f.feature_type = 'chromosome'
    """)

    lengths = {}
    for row in session.execute(query).fetchall():
        lengths[row[0]] = row[1]

    return lengths


def get_feature_aliases(session) -> dict[str, list[str]]:
    """Get aliases for all features."""
    query = text(f"""
        SELECT f.feature_name, a.alias_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_alias fa ON f.feature_no = fa.feature_no
        JOIN {DB_SCHEMA}.alias a ON fa.alias_no = a.alias_no
    """)

    aliases: dict[str, list[str]] = {}
    for row in session.execute(query).fetchall():
        feat_name, alias = row
        if feat_name not in aliases:
            aliases[feat_name] = []
        aliases[feat_name].append(alias)

    return aliases


def get_feature_goids(session) -> dict[str, list[str]]:
    """Get GO IDs for all features."""
    query = text(f"""
        SELECT DISTINCT f.feature_name, g.goid
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.go_annotation ga ON f.feature_no = ga.feature_no
        JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
    """)

    goids: dict[str, list[str]] = {}
    for row in session.execute(query).fetchall():
        feat_name, goid = row
        if feat_name not in goids:
            goids[feat_name] = []
        goid_fmt = f"GO:{goid:07d}" if goid else None
        if goid_fmt and goid_fmt not in goids[feat_name]:
            goids[feat_name].append(goid_fmt)

    return goids


def get_subfeature_info(session) -> dict[str, dict[str, list[tuple]]]:
    """
    Get subfeature coordinates for all features.

    Returns dict of feature_name -> subfeature_type -> list of (start, stop, strand).
    """
    query = text(f"""
        SELECT UPPER(p.feature_name), sf.feature_type as sf_type,
               fl.start_coord, fl.stop_coord, fl.strand
        FROM {DB_SCHEMA}.feature p
        JOIN {DB_SCHEMA}.feat_relationship fr ON p.feature_no = fr.parent_feature_no
        JOIN {DB_SCHEMA}.feature sf ON fr.child_feature_no = sf.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON sf.feature_no = fl.feature_no
        ORDER BY p.feature_name, fl.start_coord
    """)

    subfeatures: dict[str, dict[str, list[tuple]]] = {}
    for row in session.execute(query).fetchall():
        feat_name, sf_type, start, stop, strand = row

        if not start or not stop:
            continue

        # Ensure start < stop for GFF3
        if start > stop:
            start, stop = stop, start

        if feat_name not in subfeatures:
            subfeatures[feat_name] = {}
        if sf_type not in subfeatures[feat_name]:
            subfeatures[feat_name][sf_type] = []

        subfeatures[feat_name][sf_type].append((start, stop, strand))

    return subfeatures


def get_all_features(session, app_name: str) -> list[dict]:
    """Get all features for GFF3 output."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name,
               p.feature_name as chromosome,
               fl.start_coord, fl.stop_coord, fl.strand,
               f.feature_type, f.dbxref_id, f.headline,
               COALESCE(s.source, :default_source) as source
        FROM {DB_SCHEMA}.feature p
        JOIN {DB_SCHEMA}.feat_relationship fr ON p.feature_no = fr.parent_feature_no
        JOIN {DB_SCHEMA}.feature f ON fr.child_feature_no = f.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        LEFT JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        WHERE p.feature_type = 'chromosome'
        AND fr.rank = 1
        ORDER BY p.feature_name, fl.start_coord, fl.stop_coord
    """)

    features = []
    seen = set()

    for row in session.execute(query, {"default_source": PROJECT_ACRONYM}).fetchall():
        (feat_no, feat_name, gene_name, chromosome, start, stop,
         strand, feat_type, dbxref_id, headline, source) = row

        # Skip duplicates
        if feat_name in seen:
            continue
        seen.add(feat_name)

        # Ensure start < stop
        if start and stop and start > stop:
            start, stop = stop, start

        # Convert strand
        if strand == WATSON:
            strand_gff = "+"
        elif strand == CRICK:
            strand_gff = "-"
        else:
            strand_gff = IGNORE

        features.append({
            "feature_no": feat_no,
            "feature_name": feat_name,
            "gene_name": gene_name,
            "chromosome": chromosome,
            "start": start,
            "stop": stop,
            "strand": strand_gff,
            "feature_type": feat_type,
            "dbxref_id": dbxref_id,
            "headline": headline,
            "source": source or PROJECT_ACRONYM,
        })

    return features


def get_chromosome_sequences(session) -> dict[str, str]:
    """Get chromosome sequences for FASTA section."""
    query = text(f"""
        SELECT f.feature_name, s.residues
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.seq s ON f.feature_no = s.feature_no
        WHERE f.feature_type = 'chromosome'
        AND s.seq_type = 'genomic'
        AND s.is_seq_current = 'Y'
    """)

    sequences = {}
    for row in session.execute(query).fetchall():
        chr_name, seq = row
        if seq:
            sequences[chr_name] = seq

    return sequences


def calculate_phase(cds_coords: list[tuple], strand: str) -> dict[int, int]:
    """Calculate phase for CDS exons."""
    # Sort by start coord (ascending for +, descending for -)
    if strand == "+":
        sorted_coords = sorted(cds_coords, key=lambda x: x[0])
    else:
        sorted_coords = sorted(cds_coords, key=lambda x: x[0], reverse=True)

    phases = {}
    length = 0

    for start, stop, _ in sorted_coords:
        phase = length % 3
        # Phase is bases to skip to reach first complete codon
        if phase != 0:
            phase = 3 - phase
        phases[start] = phase
        length += abs(stop - start) + 1

    return phases


def generate_header(app_name: str, filename: str) -> str:
    """Generate GFF3 file header."""
    now = datetime.now()

    header_lines = [
        "##gff-version\t3" if app_name == "ORFMAP" else "##gff-version 3",
        f"#date {now.strftime('%c')}",
        "#",
        f"# {ORGANISM_NAME} {STRAIN_NAME} genome",
        "#",
    ]

    if app_name != "ORFMAP":
        header_lines.append(
            f"# {app_name} Features from the 16 nuclear chromosomes labeled chrI to chrXVI,"
        )
    else:
        header_lines.append(
            "# Features from the 16 nuclear chromosomes labeled chrI to chrXVI,"
        )

    header_lines.extend([
        "# plus the mitochondrial genome labeled chrMito",
        "#",
        f"# Created by {PROJECT_ACRONYM} ({HTML_ROOT_URL})",
        "#",
        "# Weekly updates of this file are available via Anonymous FTP from:",
        f"# {FTP_ROOT_URL}data_download/chromosomal_feature/{filename}",
        "#",
        f"# Please send comments and suggestions to {HELP_EMAIL}",
        "#",
        f"# {PROJECT_ACRONYM} is funded as a National Human Genome Research Institute Biomedical Informatics Resource from",
        f"# the U. S. National Institutes of Health to Stanford University.",
        "#",
    ])

    return "\n".join(header_lines) + "\n"


def write_gff3_file(
    output_file: Path,
    app_name: str,
    features: list[dict],
    qualifiers: dict[str, str],
    chr_lengths: dict[str, int],
    aliases: dict[str, list[str]],
    goids: dict[str, list[str]],
    subfeatures: dict[str, dict[str, list[tuple]]],
    chr_symbol_map: dict[str, str],
    chr_sequences: dict[str, str],
    log_file: Path,
) -> None:
    """Write GFF3 output file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as gff, open(log_file, "a") as log:
        log.write("*" * 50 + "\n")
        log.write(f"{datetime.now()}\n")

        # Write header
        gff.write(generate_header(app_name, output_file.name))

        previous_chr = ""

        for feat in features:
            feat_name = feat["feature_name"]

            # Check if should skip
            feat_types = feat["feature_type"].split(":") if feat["feature_type"] else []
            should_skip = any(ft in TYPE_TO_EXCLUDE for ft in feat_types)

            qualifier = qualifiers.get(feat_name.upper() if feat_name else "", "")
            if any(q in qualifier for q in QUALIFIER_TO_EXCLUDE):
                should_skip = True

            if should_skip:
                continue

            chromosome = feat["chromosome"]
            seqid = get_seqid(chromosome, chr_symbol_map)

            # Write chromosome line if new chromosome
            if chromosome != previous_chr and chromosome in chr_lengths:
                chr_type = "chromosome"
                if chromosome == "2-micron":
                    chr_type = "region"

                chr_line = [
                    seqid,
                    PROJECT_ACRONYM,
                    chr_type,
                    "1",
                    str(chr_lengths[chromosome]),
                    IGNORE,
                    IGNORE,
                    IGNORE,
                    f"ID={seqid}",
                ]
                gff.write("\t".join(chr_line) + "\n")

            # Determine SO type
            so_type = TYPE_TO_SO.get(feat["feature_type"], feat["feature_type"])
            so_attr = SO_TO_ATTRIBUTE.get(feat["feature_type"])

            # Build attributes
            attributes = []

            # ID and Name
            if feat_name:
                attributes.append(f"ID={feat_name}")
                attributes.append(f"Name={feat_name}")
            else:
                log.write(f"No ID/feature name\n")

            # gene (locus name)
            if feat["gene_name"]:
                attributes.append(f"gene={feat['gene_name']}")

            # Alias
            feat_aliases = aliases.get(feat_name, [])
            if feat["gene_name"]:
                all_aliases = [feat["gene_name"]] + feat_aliases
            else:
                all_aliases = feat_aliases

            if all_aliases:
                alias_str = ",".join(all_aliases)
                attributes.append(f"Alias={alias_str}")

            # Ontology_term (GO IDs and SO type)
            ont_terms = goids.get(feat_name, [])
            if so_attr:
                ont_terms = ont_terms + [f"SO:{so_attr}"]
            if ont_terms:
                attributes.append(f"Ontology_term={','.join(ont_terms)}")

            # Note (description/headline)
            if feat["headline"]:
                desc = delete_html_tag(feat["headline"])
                desc = delete_unwanted_char(desc)
                if desc:
                    attributes.append(f"Note={url_escape(desc, 'Note')}")

            # dbxref
            if feat["dbxref_id"]:
                attributes.append(f"dbxref={PROJECT_ACRONYM}:{feat['dbxref_id']}")
            else:
                log.write(f"No dbxref for {feat_name}\n")

            # ORF classification
            if qualifier:
                orf_class = qualifier.replace("|", ":")
                attributes.append(f"orf_classification={orf_class}")

            # Build feature line
            columns = [
                seqid,
                feat["source"],
                so_type,
                str(feat["start"]) if feat["start"] else "",
                str(feat["stop"]) if feat["stop"] else "",
                IGNORE,
                feat["strand"],
                IGNORE,
                ";".join(attributes),
            ]

            # Validate required columns
            if not seqid:
                log.write(f"No seqid for {feat_name}\n")
            if not so_type:
                log.write(f"No type for {feat_name}\n")
            if not feat["start"]:
                log.write(f"No start coord for {feat_name}\n")
            if not feat["stop"]:
                log.write(f"No stop coord for {feat_name}\n")
            if not feat["strand"] or feat["strand"] == IGNORE:
                log.write(f"No strand for {feat_name}\n")

            gff.write("\t".join(columns) + "\n")

            # Write subfeatures
            feat_key = feat_name.upper() if feat_name else ""
            if feat_key in subfeatures:
                for sf_type, coords in sorted(subfeatures[feat_key].items()):
                    sf_so_type = TYPE_TO_SO.get(sf_type, sf_type)
                    sf_so_attr = SO_TO_ATTRIBUTE.get(sf_type)

                    # Calculate phase for CDS
                    phases = {}
                    if sf_type == "CDS":
                        phases = calculate_phase(coords, feat["strand"])

                    for start, stop, strand in coords:
                        # Convert strand
                        if strand == WATSON:
                            sf_strand = "+"
                        elif strand == CRICK:
                            sf_strand = "-"
                        else:
                            sf_strand = IGNORE

                        # Phase
                        phase = phases.get(start, IGNORE)
                        if phase != IGNORE:
                            phase = str(phase)

                        # Subfeature attributes (Parent instead of ID)
                        sf_attributes = [a for a in attributes]
                        sf_attributes[0] = f"Parent={feat_name}"

                        # Add SO term
                        if sf_so_attr:
                            for i, attr in enumerate(sf_attributes):
                                if attr.startswith("Ontology_term="):
                                    sf_attributes[i] = f"{attr},SO:{sf_so_attr}"
                                    break
                            else:
                                sf_attributes.append(f"Ontology_term=SO:{sf_so_attr}")

                        sf_columns = [
                            seqid,
                            feat["source"],
                            sf_so_type,
                            str(start),
                            str(stop),
                            IGNORE,
                            sf_strand,
                            str(phase) if phase != IGNORE else IGNORE,
                            ";".join(sf_attributes),
                        ]
                        gff.write("\t".join(sf_columns) + "\n")

            previous_chr = chromosome

        # Write FASTA section for ORFMAP
        if app_name == "ORFMAP" and chr_sequences:
            gff.write("###\n")
            gff.write("##FASTA\n")

            for chr_name in sorted(chr_sequences.keys()):
                seqid = get_seqid(chr_name, chr_symbol_map)
                seq = chr_sequences[chr_name]

                gff.write(f">{seqid}\n")

                # Write sequence in chunks
                for i in range(0, len(seq), SEQ_LEN):
                    gff.write(seq[i:i+SEQ_LEN] + "\n")

    logger.info(f"Created {output_file}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate GFF3 format files"
    )
    parser.add_argument(
        "app_name",
        choices=["ORFMAP", "CloneGFF", "Regulatory"],
        help="Application name (ORFMAP, CloneGFF, or Regulatory)",
    )
    parser.add_argument(
        "filename",
        help="Output filename (without path)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Validate filename
    if "/" in args.filename:
        logger.error("Filename must not contain path. Provide filename only.")
        return 1

    output_file = GFF_DIR / args.filename
    log_file = LOG_DIR / f"{args.filename}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        with SessionLocal() as session:
            logger.info("Loading chromosome symbol map...")
            chr_symbol_map = get_chromosome_symbol_map(session)

            logger.info("Loading feature qualifiers...")
            qualifiers = get_feature_qualifiers(session)

            logger.info("Loading chromosome lengths...")
            chr_lengths = get_chromosome_lengths(session)

            logger.info("Loading feature aliases...")
            aliases = get_feature_aliases(session)

            logger.info("Loading feature GO IDs...")
            goids = get_feature_goids(session)

            logger.info("Loading subfeature info...")
            subfeatures = get_subfeature_info(session)

            logger.info(f"Loading features for {args.app_name}...")
            features = get_all_features(session, args.app_name)
            logger.info(f"Found {len(features)} features")

            # Load chromosome sequences for ORFMAP
            chr_sequences = {}
            if args.app_name == "ORFMAP":
                logger.info("Loading chromosome sequences...")
                chr_sequences = get_chromosome_sequences(session)

            logger.info("Writing GFF3 file...")
            write_gff3_file(
                output_file,
                args.app_name,
                features,
                qualifiers,
                chr_lengths,
                aliases,
                goids,
                subfeatures,
                chr_symbol_map,
                chr_sequences,
                log_file,
            )

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
