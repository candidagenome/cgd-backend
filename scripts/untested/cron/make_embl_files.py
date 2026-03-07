#!/usr/bin/env python3
"""
Generate EMBL format files for chromosome/contig sequences.

This script creates EMBL format files containing chromosome sequences with
ORF feature annotations. Each chromosome/contig gets its own EMBL file
with gene, mRNA, and CDS features for all ORFs.

Based on makeEmblFiles.pl by Prachi Shah (Jun 2011).

Usage:
    python make_embl_files.py <strain_abbrev>
    python make_embl_files.py C_albicans_SC5314

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    HTML_ROOT_DIR: Root directory for download files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
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

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
HTML_ROOT_DIR = Path(os.getenv("HTML_ROOT_DIR", "/var/www/html"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))

# Translation tables (genetic code)
DEFAULT_NUCLEAR_TRANS_TABLE = 12  # Alternative yeast nuclear code
DEFAULT_MITO_TRANS_TABLE = 3     # Yeast mitochondrial code

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_strain_config(session, strain_abbrev: str) -> dict | None:
    """Get strain configuration from database."""
    query = text(f"""
        SELECT o.organism_no, o.organism_abbrev, o.organism_name, o.common_name
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
        "common_name": result[3],
        "seq_source": seq_result[0] if seq_result else None,
    }


def get_chromosomes(session, seq_source: str) -> dict[str, str]:
    """Get chromosomes/contigs for a sequence source."""
    query = text(f"""
        SELECT f.feature_name, f.feature_type
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.seq s ON (f.feature_no = s.feature_no
            AND s.source = :seq_source AND s.is_seq_current = 'Y')
        WHERE f.feature_type IN ('chromosome', 'contig')
    """)

    chromosomes = {}
    for row in session.execute(query, {"seq_source": seq_source}).fetchall():
        chromosomes[row[0]] = row[1]

    return chromosomes


def get_chromosome_sequence(session, chr_name: str) -> str | None:
    """Get the sequence for a chromosome."""
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        WHERE f.feature_name = :chr_name
        AND s.is_seq_current = 'Y'
    """)
    result = session.execute(query, {"chr_name": chr_name}).fetchone()
    return result[0] if result else None


def get_chromosome_orfs(session, chr_name: str, seq_source: str) -> list[dict]:
    """Get ORF features for a chromosome."""
    query = text(f"""
        SELECT f1.feature_no, f1.feature_name, f1.dbxref_id, f1.gene_name, f1.headline
        FROM {DB_SCHEMA}.feature f1
        JOIN {DB_SCHEMA}.feat_relationship fr ON (f1.feature_no = fr.child_feature_no
            AND fr.rank = 1 AND fr.relationship_type = 'part of')
        JOIN {DB_SCHEMA}.feature f2 ON (fr.parent_feature_no = f2.feature_no
            AND f2.feature_name = :chr_name)
        WHERE f1.feature_type = 'ORF'
        AND f1.feature_no NOT IN (
            SELECT feature_no
            FROM {DB_SCHEMA}.feat_property
            WHERE property_value LIKE 'Deleted%')
        ORDER BY f1.feature_name
    """)

    orfs = []
    for row in session.execute(query, {"chr_name": chr_name}).fetchall():
        orfs.append({
            "feature_no": row[0],
            "feature_name": row[1],
            "dbxref_id": row[2],
            "gene_name": row[3],
            "headline": row[4],
        })

    return orfs


def get_feature_location(session, feature_no: int, seq_source: str) -> dict | None:
    """Get location information for a feature."""
    query = text(f"""
        SELECT fl.min_coord, fl.max_coord, fl.strand
        FROM {DB_SCHEMA}.feat_location fl
        JOIN {DB_SCHEMA}.seq s ON fl.root_seq_no = s.seq_no
        WHERE fl.feature_no = :feature_no
        AND fl.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
    """)
    result = session.execute(
        query, {"feature_no": feature_no, "seq_source": seq_source}
    ).fetchone()

    if not result:
        return None

    return {
        "min_coord": result[0],
        "max_coord": result[1],
        "strand": result[2],
    }


def get_feature_subfeatures(session, feature_no: int) -> list[dict]:
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
        if start > end:
            start, end = end, start
        subfeatures.append({
            "type": row[0],
            "start": start,
            "end": end,
        })

    return subfeatures


def get_feature_aliases(session, feature_no: int) -> list[str]:
    """Get aliases for a feature."""
    query = text(f"""
        SELECT a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON a.alias_no = fa.alias_no
        WHERE fa.feature_no = :feature_no
    """)
    return [row[0] for row in session.execute(query, {"feature_no": feature_no}).fetchall()]


def get_feature_go_annotations(session, feature_no: int) -> list[str]:
    """Get GO annotations for a feature."""
    query = text(f"""
        SELECT DISTINCT g.goid
        FROM {DB_SCHEMA}.go_annotation ga
        JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
        WHERE ga.feature_no = :feature_no
    """)
    return [f"GO:{str(row[0]).zfill(7)}" for row in session.execute(query, {"feature_no": feature_no}).fetchall()]


def format_embl_location(start: int, end: int, strand: str, is_complement: bool = False) -> str:
    """Format a location string for EMBL format."""
    if strand == "C" or strand == "-":
        return f"complement({start}..{end})"
    return f"{start}..{end}"


def format_embl_join(locations: list[tuple[int, int]], strand: str) -> str:
    """Format a join location string for EMBL format."""
    loc_strs = [f"{s}..{e}" for s, e in locations]
    joined = f"join({','.join(loc_strs)})"
    if strand == "C" or strand == "-":
        return f"complement({joined})"
    return joined


def write_embl_file(
    session,
    chr_name: str,
    chr_type: str,
    seq_source: str,
    output_file: Path,
    strain_abbrev: str,
    trans_table: int = DEFAULT_NUCLEAR_TRANS_TABLE,
) -> int:
    """
    Write EMBL format file for a chromosome.

    Returns number of features written.
    """
    # Get chromosome sequence
    sequence = get_chromosome_sequence(session, chr_name)
    if not sequence:
        logger.warning(f"No sequence found for {chr_name}")
        return 0

    # Get ORFs
    orfs = get_chromosome_orfs(session, chr_name, seq_source)

    with open(output_file, "w") as f:
        # EMBL header
        f.write(f"ID   {chr_name}; SV 1; linear; genomic DNA; STD; FUN; {len(sequence)} BP.\n")
        f.write("XX\n")
        f.write(f"AC   {chr_type}:{PROJECT_ACRONYM}:{chr_name}:1:{len(sequence)};\n")
        f.write("XX\n")
        f.write(f"DE   {strain_abbrev} {chr_name} ({len(sequence)} nucleotides)\n")
        f.write("XX\n")
        f.write(f"OS   {strain_abbrev.replace('_', ' ')}\n")
        f.write("XX\n")
        f.write(f"CC   Generated by {PROJECT_ACRONYM} on {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write("XX\n")
        f.write("FH   Key             Location/Qualifiers\n")
        f.write("FH\n")

        # Source feature
        f.write(f"FT   source          1..{len(sequence)}\n")
        f.write(f'FT                   /organism="{strain_abbrev.replace("_", " ")}"\n')
        f.write(f'FT                   /mol_type="genomic DNA"\n')
        f.write("FT\n")

        # ORF features
        feat_count = 0
        for orf in orfs:
            location = get_feature_location(session, orf["feature_no"], seq_source)
            if not location:
                continue

            subfeatures = get_feature_subfeatures(session, orf["feature_no"])
            aliases = get_feature_aliases(session, orf["feature_no"])
            go_terms = get_feature_go_annotations(session, orf["feature_no"])

            strand = location["strand"]
            min_coord = location["min_coord"]
            max_coord = location["max_coord"]

            # Gene feature
            loc_str = format_embl_location(min_coord, max_coord, strand)
            f.write(f"FT   gene            {loc_str}\n")
            f.write(f'FT                   /locus_tag="{orf["feature_name"]}"\n')
            if orf["gene_name"]:
                f.write(f'FT                   /gene="{orf["gene_name"]}"\n')
            f.write(f'FT                   /db_xref="{PROJECT_ACRONYM}:{orf["dbxref_id"]}"\n')

            # mRNA feature
            if subfeatures:
                exon_locs = [(sf["start"], sf["end"]) for sf in subfeatures]
                mrna_loc = format_embl_join(exon_locs, strand)
            else:
                mrna_loc = loc_str

            f.write(f"FT   mRNA            {mrna_loc}\n")
            f.write(f'FT                   /locus_tag="{orf["feature_name"]}"\n')
            if orf["gene_name"]:
                f.write(f'FT                   /gene="{orf["gene_name"]}"\n')

            # CDS feature
            f.write(f"FT   CDS             {mrna_loc}\n")
            f.write(f'FT                   /locus_tag="{orf["feature_name"]}"\n')
            if orf["gene_name"]:
                f.write(f'FT                   /gene="{orf["gene_name"]}"\n')
            f.write(f'FT                   /db_xref="{PROJECT_ACRONYM}:{orf["dbxref_id"]}"\n')
            f.write(f'FT                   /transl_table={trans_table}\n')

            if orf["headline"]:
                # Clean headline for EMBL format
                headline = orf["headline"].replace('"', "'")[:200]
                f.write(f'FT                   /note="{headline}"\n')

            if aliases:
                alias_str = ", ".join(aliases[:5])  # Limit aliases
                f.write(f'FT                   /gene_synonym="{alias_str}"\n')

            for go_term in go_terms[:10]:  # Limit GO terms
                f.write(f'FT                   /db_xref="{go_term}"\n')

            f.write("FT\n")
            feat_count += 1

        # Sequence
        f.write("XX\n")
        f.write(f"SQ   Sequence {len(sequence)} BP;\n")

        # Format sequence (60 bp per line, with position counter)
        seq_lower = sequence.lower()
        pos = 0
        while pos < len(seq_lower):
            line_seq = seq_lower[pos:pos+60]
            # Split into 10-bp blocks
            blocks = [line_seq[i:i+10] for i in range(0, len(line_seq), 10)]
            f.write(f"     {' '.join(blocks):<66} {min(pos+60, len(seq_lower)):>9}\n")
            pos += 60

        f.write("//\n")

    return feat_count


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate EMBL format files for chromosome sequences"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: auto-generated)",
    )
    parser.add_argument(
        "--trans-table",
        type=int,
        default=DEFAULT_NUCLEAR_TRANS_TABLE,
        help=f"Translation table (default: {DEFAULT_NUCLEAR_TRANS_TABLE})",
    )

    args = parser.parse_args()

    strain_abbrev = args.strain_abbrev

    # Set up logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"make_embl_files_{strain_abbrev}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Generating EMBL files for {strain_abbrev}")

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
            if args.output_dir:
                output_dir = args.output_dir
            else:
                output_dir = (
                    HTML_ROOT_DIR / "download" / "sequence" /
                    strain_abbrev / "current" / "EMBL_format"
                )

            output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Output directory: {output_dir}")

            # Get chromosomes
            chromosomes = get_chromosomes(session, seq_source)
            logger.info(f"Found {len(chromosomes)} chromosomes/contigs")

            total_features = 0
            for chr_name, chr_type in chromosomes.items():
                output_file = output_dir / f"{chr_name}.embl"
                logger.info(f"Writing {output_file}")

                count = write_embl_file(
                    session,
                    chr_name,
                    chr_type,
                    seq_source,
                    output_file,
                    strain_abbrev,
                    args.trans_table,
                )

                logger.info(f"  {count} features written")
                total_features += count

            logger.info(f"Total: {total_features} features across {len(chromosomes)} files")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()


if __name__ == "__main__":
    sys.exit(main())
