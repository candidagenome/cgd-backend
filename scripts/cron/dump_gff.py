#!/usr/bin/env python3
from __future__ import annotations

"""
Dump gene annotation data in GFF3 (General Feature Format version 3) format.

This script exports gene annotations (features with their subfeatures/exons)
to standard GFF3 format for use with genome browsers and bioinformatics tools.

Validation checks before copying to final location:
---------------------------------------------------------------------------
- Writes to temp directory first
- Validates minimum feature count (100+ per strain)
- Checks feature count change < 10% vs existing file
- Only copies to final location if validation passes
- Sends Slack notifications on success or failure

GFF3 Format:
- Tab-separated: seqname, source, feature, start, end, score, strand, phase, attributes
- Features include: chromosome, gene, mRNA, exon, CDS, tRNA, etc.
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
    DOWNLOAD_DIR: Directory for output files
    LOG_DIR: Directory for log files
    SLACK_WEBHOOK_URL: Slack webhook URL for notifications
    ENV_STATE: Environment state (dev/prod) for Slack labels
"""

import argparse
import gzip
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directories to path
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_FEATURES = 100  # Minimum features expected per strain
MAX_FEATURE_CHANGE_PERCENT = 10.0  # Maximum allowed change from previous file

# Configure logging to stderr so stdout can be used for GFF output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


def send_slack_message(message: str, is_error: bool = False) -> None:
    """Send a message to Slack webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping notification")
        return

    emoji = ":x:" if is_error else ":white_check_mark:"
    env_prefix = f"[{ENV_STATE.upper()}] " if ENV_STATE != "prod" else ""

    payload = {
        "text": f"{emoji} {env_prefix}GFF Dump: {message}"
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Slack notification failed: {response.status}")
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")


def count_features_in_file(file_path: Path) -> int:
    """Count non-header feature lines in a GFF file."""
    if not file_path.exists():
        return 0

    count = 0
    with open(file_path) as f:
        for line in f:
            if not line.startswith("#") and line.strip():
                parts = line.split("\t")
                # Count only gene lines (top-level features)
                if len(parts) >= 3 and parts[2] == "gene":
                    count += 1
    return count


def validate_output_file(
    new_file: Path,
    existing_file: Path | None = None,
    strain_abbrev: str = "",
) -> tuple[bool, str]:
    """
    Validate the generated GFF file.

    Returns:
        Tuple of (is_valid, message)
    """
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_features_in_file(new_file)

    if new_count < MIN_FEATURES:
        return False, (
            f"Too few features for {strain_abbrev}: {new_count} "
            f"(minimum: {MIN_FEATURES})"
        )

    if existing_file and existing_file.exists():
        existing_count = count_features_in_file(existing_file)
        if existing_count > 0:
            change_pct = abs(new_count - existing_count) / existing_count * 100
            if change_pct > MAX_FEATURE_CHANGE_PERCENT:
                return False, (
                    f"Feature count changed too much for {strain_abbrev}: "
                    f"{existing_count} -> {new_count} ({change_pct:.1f}% "
                    f"change, max: {MAX_FEATURE_CHANGE_PERCENT}%)"
                )

    return True, f"Validation passed for {strain_abbrev}: {new_count} features"


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


def get_seq_source(session, organism_no: int) -> str | None:
    """Get sequence source for a strain (returns latest/highest numbered assembly)."""
    query = text(f"""
        SELECT DISTINCT s.source
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feat_location fl ON s.seq_no = fl.root_seq_no
        JOIN {DB_SCHEMA}.feature f ON fl.feature_no = f.feature_no
        WHERE s.is_seq_current = 'Y'
        AND f.organism_no = :organism_no
        ORDER BY s.source DESC
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(query, {"organism_no": organism_no}).fetchone()
    return result[0] if result else None


def get_genome_version(session, seq_source: str) -> str:
    """Get full genome version string from database."""
    query = text(f"""
        SELECT gv.genome_version
        FROM {DB_SCHEMA}.genome_version gv
        JOIN {DB_SCHEMA}.seq s ON s.genome_version_no = gv.genome_version_no
        WHERE s.is_seq_current = 'Y'
        AND s.source = :seq_source
        AND gv.is_ver_current = 'Y'
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(query, {"seq_source": seq_source}).fetchone()
    return result[0] if result else ""


def get_root_sequences(session, seq_source: str) -> list[dict]:
    """Get root sequences (chromosomes/contigs) for an assembly."""
    query = text(f"""
        SELECT f.feature_name, f.feature_type, fl.stop_coord
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


def get_features(session, organism_no: int, seq_source: str) -> list[dict]:
    """Get features for a strain that have current locations."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.feature_type,
               fp.property_value as feature_qualifier, f.dbxref_id, f.headline,
               fl.start_coord, fl.stop_coord, fl.strand,
               root_feat.feature_name as root_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        JOIN {DB_SCHEMA}.feature root_feat ON s.feature_no = root_feat.feature_no
        LEFT JOIN {DB_SCHEMA}.feat_property fp ON (f.feature_no = fp.feature_no AND fp.property_type = 'feature_qualifier')
        WHERE f.organism_no = :organism_no
        AND f.feature_type NOT IN ('chromosome', 'contig',
            'CDS', 'intron', 'noncoding_exon', 'adjustment', 'gap',
            'three_prime_UTR', 'five_prime_UTR',
            'three_prime_UTR_intron', 'five_prime_UTR_intron')
        ORDER BY root_feat.feature_name, fl.start_coord
    """)

    features = []
    for row in session.execute(
        query, {"organism_no": organism_no, "seq_source": seq_source}
    ).fetchall():
        feature_qualifier = row[4] or ""

        # Skip deleted features
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
            "start_coord": row[7],
            "stop_coord": row[8],
            "strand": row[9],
            "root_name": row[10],
        })

    return features


def get_all_feature_aliases(session, organism_no: int) -> dict[int, list[str]]:
    """Get all aliases for features of an organism (batched for performance)."""
    query = text(f"""
        SELECT fa.feature_no, a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON a.alias_no = fa.alias_no
        JOIN {DB_SCHEMA}.feature f ON fa.feature_no = f.feature_no
        WHERE f.organism_no = :organism_no
        ORDER BY fa.feature_no, a.alias_name
    """)

    alias_map: dict[int, list[str]] = defaultdict(list)
    for row in session.execute(query, {"organism_no": organism_no}).fetchall():
        alias_map[row[0]].append(row[1])

    return alias_map


def get_all_subfeatures(session, organism_no: int, seq_source: str) -> dict[int, list[dict]]:
    """Get all subfeatures for features of an organism (batched for performance)."""
    query = text(f"""
        SELECT fr.parent_feature_no, f.feature_type, fl.start_coord, fl.stop_coord,
               f.feature_no, f.feature_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_relationship fr ON fr.child_feature_no = f.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.seq_no = s.seq_no AND s.is_seq_current = 'Y')
        JOIN {DB_SCHEMA}.genome_version gv ON (s.genome_version_no = gv.genome_version_no AND gv.is_ver_current = 'Y')
        JOIN {DB_SCHEMA}.feature parent_f ON fr.parent_feature_no = parent_f.feature_no
        WHERE parent_f.organism_no = :organism_no
        AND fr.rank = 2
        AND s.source = :seq_source
        ORDER BY fr.parent_feature_no, f.feature_type, fl.start_coord
    """)

    subfeature_map: dict[int, list[dict]] = defaultdict(list)
    for row in session.execute(query, {"organism_no": organism_no, "seq_source": seq_source}).fetchall():
        subfeature_map[row[0]].append({
            "type": row[1],
            "start": row[2],
            "end": row[3],
            "feature_no": row[4],
            "feature_name": row[5],
        })

    return subfeature_map


def get_all_allele_parent_types(session, organism_no: int) -> dict[int, str]:
    """Get parent feature types for all allele features of an organism."""
    query = text(f"""
        SELECT fr.child_feature_no, f.feature_type
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_relationship fr ON f.feature_no = fr.parent_feature_no
        JOIN {DB_SCHEMA}.feature child_f ON fr.child_feature_no = child_f.feature_no
        WHERE child_f.organism_no = :organism_no
        AND child_f.feature_type = 'allele'
        AND fr.relationship_type = 'allele'
        AND fr.rank = 3
    """)

    parent_type_map = {}
    for row in session.execute(query, {"organism_no": organism_no}).fetchall():
        parent_type_map[row[0]] = row[1]

    return parent_type_map


def escape_gff_value(value: str) -> str:
    """URL-encode special characters in GFF attribute values."""
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


def get_note_prefix(gene_name: str | None, aliases: list[str]) -> str:
    """Get the note prefix (e.g., orf19.xxxx) from gene name or aliases."""
    # First try gene_name
    if gene_name:
        return gene_name

    # Look for orf19 alias
    for alias in aliases:
        if alias.startswith("orf19.") and not alias.startswith("orf19.1"):
            return alias

    # Look for any orf19 alias
    for alias in aliases:
        if alias.startswith("orf19."):
            return alias

    return ""


def dump_gff(
    session,
    organism_no: int,
    strain_abbrev: str,
    seq_source: str | None = None,
    output_file=None,
) -> int:
    """
    Dump GFF3 format gene annotations.

    Args:
        session: Database session
        organism_no: Organism number
        strain_abbrev: Strain abbreviation
        seq_source: Sequence source (optional, auto-detected if not provided)
        output_file: Output file handle (defaults to stdout)

    Returns:
        Number of features written
    """
    if output_file is None:
        output_file = sys.stdout

    # Get or determine seq_source
    if not seq_source:
        seq_source = get_seq_source(session, organism_no)
        if not seq_source:
            logger.error(f"No seq_source found for {strain_abbrev}")
            return 0

    logger.info(f"Dumping GFF for {strain_abbrev} (seq_source: {seq_source})")

    source = PROJECT_ACRONYM

    # Get full genome version from database
    genome_version = get_genome_version(session, seq_source)
    if not genome_version:
        # Fallback to extracting from seq_source
        genome_version = seq_source.split()[-1] if seq_source else ""
        if genome_version.isdigit():
            genome_version = f"A{genome_version}"

    # Get organism name
    org_query = text(f"""
        SELECT organism_name FROM {DB_SCHEMA}.organism WHERE organism_no = :organism_no
    """)
    org_result = session.execute(org_query, {"organism_no": organism_no}).fetchone()
    organism_name = org_result[0] if org_result else strain_abbrev

    # Write GFF header (space before 3, not tab, to match old format)
    output_file.write("##gff-version 3\n")
    output_file.write("# File name: \n")
    output_file.write(f"# Organism: {organism_name}\n")
    output_file.write(f"# Genome version: {genome_version}\n")
    output_file.write(f"# Date created: {datetime.now().strftime('%a %b %d %H:%M:%S %Y')}\n")
    output_file.write("# Created by: The Candida Genome Database (http://www.candidagenome.org/)\n")
    output_file.write("# Contact Email: candida-curator AT lists DOT stanford DOT edu\n")
    output_file.write("# Funding: NIDCR at US NIH, grant number 1-R01-DE015873-01\n")
    output_file.write("#\n")

    # Write root sequences (chromosomes/contigs)
    roots = get_root_sequences(session, seq_source)
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
    features = get_features(session, organism_no, seq_source)
    logger.info(f"Found {len(features)} features")

    # Batch fetch all aliases, subfeatures, and allele parent types
    logger.info("Fetching aliases...")
    alias_map = get_all_feature_aliases(session, organism_no)
    logger.info("Fetching subfeatures...")
    subfeature_map = get_all_subfeatures(session, organism_no, seq_source)
    logger.info("Fetching allele parent types...")
    allele_parent_map = get_all_allele_parent_types(session, organism_no)

    count = 0

    for feat in features:
        feature_name = feat["feature_name"]
        root_name = feat["root_name"]
        original_feature_type = feat["feature_type"].replace(" ", "_")

        # Handle allele features - look up parent feature type
        if original_feature_type == "allele":
            parent_type = allele_parent_map.get(feat["feature_no"])
            original_feature_type = parent_type if parent_type else "ORF"

        # Get coordinates
        start = feat["start_coord"]
        end = feat["stop_coord"]
        if start > end:
            start, end = end, start

        strand = "-" if feat["strand"] == "C" else "+"

        # Get aliases
        aliases = alias_map.get(feat["feature_no"], [])

        # Get ORF classification from qualifier
        orf_classification = None
        if feat["feature_qualifier"]:
            match = re.search(r"(Verified|Uncharacterized|Dubious)", feat["feature_qualifier"])
            if match:
                orf_classification = match.group(1)

        # Build note with prefix
        note_prefix = get_note_prefix(feat["gene_name"], aliases)
        headline = feat["headline"] or ""
        headline = re.sub(r"<[^<>]+>", "", headline)  # Strip HTML tags

        if note_prefix and headline:
            note = f"({note_prefix}) {headline}"
        elif headline:
            note = headline
        else:
            note = None

        # Build gene attributes
        gene_attrs = {"ID": feature_name, "Name": feature_name}
        if feat["gene_name"]:
            gene_attrs["Gene"] = feat["gene_name"]
        if note:
            gene_attrs["Note"] = note
        if orf_classification:
            gene_attrs["orf_classification"] = orf_classification
        if aliases:
            gene_attrs["Alias"] = aliases

        # Write gene line
        gene_attr_str = format_gff_attributes(gene_attrs)
        output_file.write(
            f"{root_name}\t{source}\tgene\t{start}\t{end}\t.\t{strand}\t.\t{gene_attr_str}\n"
        )

        # Determine transcript type based on feature type
        if original_feature_type == "ORF":
            transcript_type = "mRNA"
        else:
            transcript_type = original_feature_type

        # Write mRNA/tRNA/etc line with -T suffix
        transcript_id = f"{feature_name}-T"
        transcript_attrs = {
            "ID": transcript_id,
            "Parent": feature_name,
            "Name": feature_name,
        }
        if feat["gene_name"]:
            transcript_attrs["Gene"] = feat["gene_name"]
        if note:
            transcript_attrs["Note"] = note
        if orf_classification:
            transcript_attrs["orf_classification"] = orf_classification
        if aliases:
            transcript_attrs["Alias"] = aliases

        transcript_attr_str = format_gff_attributes(transcript_attrs)
        output_file.write(
            f"{root_name}\t{source}\t{transcript_type}\t{start}\t{end}\t.\t{strand}\t.\t{transcript_attr_str}\n"
        )

        # Get subfeatures and write exon/CDS lines
        subfeatures = subfeature_map.get(feat["feature_no"], [])

        # Group CDS subfeatures for exon numbering
        cds_subfeatures = [sf for sf in subfeatures if sf["type"] == "CDS"]

        # Sort by start coordinate
        cds_subfeatures.sort(key=lambda x: x["start"])

        # Write exon lines for ORFs
        if original_feature_type == "ORF":
            for i, sf in enumerate(cds_subfeatures, 1):
                exon_id = f"{feature_name}-T-E{i}"
                exon_attrs = {"ID": exon_id, "Parent": transcript_id}

                sf_start = sf["start"]
                sf_end = sf["end"]
                if sf_start > sf_end:
                    sf_start, sf_end = sf_end, sf_start

                exon_attr_str = format_gff_attributes(exon_attrs)
                output_file.write(
                    f"{root_name}\t{source}\texon\t{sf_start}\t{sf_end}\t.\t{strand}\t.\t{exon_attr_str}\n"
                )

        # Write CDS lines for ORFs
        if original_feature_type == "ORF":
            for sf in cds_subfeatures:
                cds_id = f"{feature_name}-P"
                cds_attrs = {
                    "ID": cds_id,
                    "Parent": transcript_id,
                }
                if orf_classification:
                    cds_attrs["orf_classification"] = orf_classification
                cds_attrs["parent_feature_type"] = "ORF"

                sf_start = sf["start"]
                sf_end = sf["end"]
                if sf_start > sf_end:
                    sf_start, sf_end = sf_end, sf_start

                cds_attr_str = format_gff_attributes(cds_attrs)
                # Use phase 0 for CDS
                output_file.write(
                    f"{root_name}\t{source}\tCDS\t{sf_start}\t{sf_end}\t.\t{strand}\t0\t{cds_attr_str}\n"
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

    # Create temp directory for safe generation
    temp_dir = Path(tempfile.mkdtemp(prefix="gff_"))

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, args.strain_abbrev)
            if not config:
                error_msg = f"Strain {args.strain_abbrev} not found in database"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return 1

            if args.output:
                # Write to temp file first
                temp_file = temp_dir / args.output.name

                if args.gzip:
                    with gzip.open(temp_file, "wt") as f:
                        count = dump_gff(session, config["organism_no"], args.strain_abbrev, args.seq_source, f)
                else:
                    with open(temp_file, "w") as f:
                        count = dump_gff(session, config["organism_no"], args.strain_abbrev, args.seq_source, f)

                if count == 0:
                    error_msg = f"No features found for {args.strain_abbrev}"
                    logger.error(error_msg)
                    send_slack_message(error_msg, is_error=True)
                    return 1

                # Validate the generated file
                is_valid, validation_msg = validate_output_file(
                    temp_file,
                    args.output if args.output.exists() else None,
                    args.strain_abbrev,
                )

                if not is_valid:
                    error_msg = f"Validation failed: {validation_msg}"
                    logger.error(error_msg)
                    send_slack_message(error_msg, is_error=True)
                    return 1

                # Copy validated file to final location
                shutil.copy(str(temp_file), str(args.output))
                logger.info(f"Output written to {args.output}")

                # Print summary to stdout for Slack
                print(f"*{args.strain_abbrev}*: {count} features exported to {args.output.name}")

                # Send success notification
                send_slack_message(
                    f"Successfully exported {args.strain_abbrev} with {count} features"
                )
            else:
                count = dump_gff(session, config["organism_no"], args.strain_abbrev, args.seq_source)
                print(f"*{args.strain_abbrev}*: {count} features exported", file=sys.stderr)

                if count == 0:
                    return 1

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        send_slack_message(f"Error dumping GFF for {args.strain_abbrev}: {e}", is_error=True)
        return 1

    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
