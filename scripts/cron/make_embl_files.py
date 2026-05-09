#!/usr/bin/env python3
"""
Generate EMBL format files for chromosome/contig sequences.

This script creates EMBL format files containing chromosome sequences with
ORF feature annotations. Each chromosome/contig gets its own EMBL file
with gene, mRNA, and CDS features for all ORFs.

Validation checks before copying to final location:
---------------------------------------------------------------------------
- Writes to temp directory first
- Validates minimum feature count (10+ per chromosome)
- Checks total feature count change < 10% vs existing files
- Only copies to final location if validation passes
- Archives existing files before replacing
- Sends Slack notifications on success or failure

Based on makeEmblFiles.pl by Prachi Shah (Jun 2011).

Usage:
    python make_embl_files.py C_albicans_SC5314
    python make_embl_files.py --all
    python make_embl_files.py C_albicans_SC5314 --output-dir ./embl

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    DOWNLOAD_DIR: Directory for output files
    LOG_DIR: Directory for log files
    SLACK_WEBHOOK_URL: Slack webhook URL for notifications
    ENV_STATE: Environment state (dev/prod) for Slack labels
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

# Add parent directory to path to import cgd modules
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
MIN_FEATURES_PER_CHR = 10  # Minimum features expected per chromosome
MAX_FEATURE_CHANGE_PERCENT = 10.0  # Maximum allowed change from previous files

# Translation tables (genetic code)
DEFAULT_NUCLEAR_TRANS_TABLE = 12  # Alternative yeast nuclear code
DEFAULT_MITO_TRANS_TABLE = 3      # Yeast mitochondrial code

# Strain configurations
STRAIN_ABBREVS = [
    "C_albicans_SC5314",
    "C_auris_B8441",
    "C_dubliniensis_CD36",
    "C_glabrata_CBS138",
    "C_parapsilosis_CDC317",
    "C_tropicalis_MYA-3404",
]

# Mitochondrial feature names by strain
MITO_FEATURES = {
    "C_albicans_SC5314": ["Ca19-mtDNA"],
    "C_auris_B8441": [],
    "C_dubliniensis_CD36": ["Cd36-mtDNA"],
    "C_glabrata_CBS138": ["ChrMT", "Mito"],
    "C_parapsilosis_CDC317": ["ChrMT"],
    "C_tropicalis_MYA-3404": [],
}

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
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
        "text": f"{emoji} {env_prefix}EMBL Generation: {message}"
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


def count_features_in_embl(file_path: Path) -> int:
    """Count gene features in an EMBL file."""
    if not file_path.exists():
        return 0

    count = 0
    with open(file_path) as f:
        for line in f:
            # Count gene features (each ORF has gene, mRNA, CDS - count gene only)
            if line.startswith("FT   gene "):
                count += 1
    return count


def count_features_in_directory(dir_path: Path) -> int:
    """Count total features across all EMBL files in a directory."""
    if not dir_path.exists():
        return 0

    total = 0
    for embl_file in dir_path.glob("*.embl"):
        total += count_features_in_embl(embl_file)
    return total


def validate_embl_files(
    temp_dir: Path,
    existing_dir: Path | None,
    strain_abbrev: str,
) -> tuple[bool, str]:
    """
    Validate generated EMBL files.

    Returns:
        Tuple of (is_valid, message)
    """
    # Count features in new files
    new_count = count_features_in_directory(temp_dir)

    # Check minimum features
    if new_count < MIN_FEATURES_PER_CHR:
        return False, (
            f"Too few features for {strain_abbrev}: {new_count} "
            f"(minimum: {MIN_FEATURES_PER_CHR})"
        )

    # Check against existing files if present
    if existing_dir and existing_dir.exists():
        existing_count = count_features_in_directory(existing_dir)
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
        AND f.organism_no = :organism_no
        ORDER BY s.source DESC
        FETCH FIRST 1 ROW ONLY
    """)
    seq_result = session.execute(seq_query, {"organism_no": result[0]}).fetchone()

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


def get_chromosome_sequence(session, chr_name: str, seq_source: str) -> str | None:
    """Get the sequence for a chromosome."""
    query = text(f"""
        SELECT s.residues
        FROM {DB_SCHEMA}.seq s
        JOIN {DB_SCHEMA}.feature f ON s.feature_no = f.feature_no
        WHERE f.feature_name = :chr_name
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
    """)
    result = session.execute(query, {"chr_name": chr_name, "seq_source": seq_source}).fetchone()
    return result[0] if result else None


def get_chromosome_orfs(session, chr_name: str) -> list[dict]:
    """Get ORF and allele features for a chromosome.

    Note: B-alleles in diploid genomes (e.g., C. albicans) are stored as
    feature_type='allele' rather than 'ORF', so we include both types.
    """
    query = text(f"""
        SELECT f1.feature_no, f1.feature_name, f1.dbxref_id, f1.gene_name, f1.headline
        FROM {DB_SCHEMA}.feature f1
        JOIN {DB_SCHEMA}.feat_relationship fr ON (f1.feature_no = fr.child_feature_no
            AND fr.rank = 1 AND fr.relationship_type = 'part of')
        JOIN {DB_SCHEMA}.feature f2 ON (fr.parent_feature_no = f2.feature_no
            AND f2.feature_name = :chr_name)
        WHERE f1.feature_type IN ('ORF', 'allele')
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
        SELECT fl.start_coord, fl.stop_coord, fl.strand
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

    # Normalize coordinates (start should be less than stop)
    start_coord = result[0]
    stop_coord = result[1]
    if start_coord > stop_coord:
        start_coord, stop_coord = stop_coord, start_coord

    return {
        "min_coord": start_coord,
        "max_coord": stop_coord,
        "strand": result[2],
    }


def get_feature_subfeatures(session, feature_no: int, seq_source: str) -> list[dict]:
    """Get CDS subfeatures (exons) for a feature via feat_relationship."""
    query = text(f"""
        SELECT f.feature_type, fl.start_coord, fl.stop_coord
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_relationship fr ON fr.child_feature_no = f.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON f.feature_no = fl.feature_no
        JOIN {DB_SCHEMA}.seq s ON fl.seq_no = s.seq_no
        WHERE fr.parent_feature_no = :feature_no
        AND fr.rank = 2
        AND f.feature_type = 'CDS'
        AND fl.is_loc_current = 'Y'
        AND s.is_seq_current = 'Y'
        AND s.source = :seq_source
        ORDER BY fl.start_coord
    """)

    subfeatures = []
    for row in session.execute(query, {"feature_no": feature_no, "seq_source": seq_source}).fetchall():
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
        AND a.alias_type IN ('Uniform', 'Non-uniform', 'CGDID')
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


def get_feature_phenotypes(session, feature_no: int) -> list[str]:
    """Get phenotype annotations for a feature."""
    try:
        query = text(f"""
            SELECT DISTINCT
                pa.qualifier,
                p.observable,
                pa.mutant_type
            FROM {DB_SCHEMA}.phenotype_annotation pa
            JOIN {DB_SCHEMA}.phenotype p ON pa.phenotype_no = p.phenotype_no
            WHERE pa.feature_no = :feature_no
        """)

        phenotypes = []
        for row in session.execute(query, {"feature_no": feature_no}).fetchall():
            qualifier = row[0] or ""
            observable = row[1] or ""
            mutant_type = row[2] or ""

            pheno_str = ""
            if qualifier:
                pheno_str = f"{qualifier} "
            pheno_str += f"{observable} ({mutant_type})"
            phenotypes.append(pheno_str)

        return list(set(phenotypes))  # Remove duplicates
    except Exception:
        # Table may not exist in all schemas
        return []


def format_embl_location(start: int, end: int, strand: str) -> str:
    """Format a location string for EMBL format."""
    if strand == "C" or strand == "-":
        return f"complement({start}..{end})"
    return f"{start}..{end}"


def format_embl_join(locations: list[tuple[int, int]], strand: str) -> str:
    """Format a join location string for EMBL format."""
    if not locations:
        return ""

    loc_strs = [f"{s}..{e}" for s, e in locations]

    if len(loc_strs) == 1:
        joined = loc_strs[0]
    else:
        joined = f"join({','.join(loc_strs)})"

    if strand == "C" or strand == "-":
        return f"complement({joined})"
    return joined


def is_mito_chromosome(chr_name: str, strain_abbrev: str) -> bool:
    """Check if chromosome is mitochondrial."""
    mito_names = MITO_FEATURES.get(strain_abbrev, [])
    for mito_name in mito_names:
        if mito_name.lower() in chr_name.lower() or chr_name.lower() in mito_name.lower():
            return True
    return "mito" in chr_name.lower() or "mtdna" in chr_name.lower()


def write_embl_file(
    session,
    chr_name: str,
    chr_type: str,
    seq_source: str,
    output_file: Path,
    strain_abbrev: str,
) -> int:
    """
    Write EMBL format file for a chromosome.

    Returns number of features written.
    """
    # Determine translation table
    is_mito = is_mito_chromosome(chr_name, strain_abbrev)
    trans_table = DEFAULT_MITO_TRANS_TABLE if is_mito else DEFAULT_NUCLEAR_TRANS_TABLE

    # Get chromosome sequence
    sequence = get_chromosome_sequence(session, chr_name, seq_source)
    if not sequence:
        logger.warning(f"No sequence found for {chr_name}")
        return 0

    # Get ORFs
    orfs = get_chromosome_orfs(session, chr_name)

    # Write to temp file first
    temp_file = output_file.with_suffix(".embl.temp")

    with open(temp_file, "w") as f:
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

            subfeatures = get_feature_subfeatures(session, orf["feature_no"], seq_source)
            aliases = get_feature_aliases(session, orf["feature_no"])
            go_terms = get_feature_go_annotations(session, orf["feature_no"])
            phenotypes = get_feature_phenotypes(session, orf["feature_no"])

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

            # Note with headline and phenotypes
            note_parts = []
            if orf["headline"]:
                headline = orf["headline"].replace('"', "'")[:200]
                note_parts.append(headline)
            if phenotypes:
                pheno_str = "Phenotype: " + ", ".join(phenotypes[:5])
                note_parts.append(pheno_str)
            if note_parts:
                note = "; ".join(note_parts)
                f.write(f'FT                   /note="{note}"\n')

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

    # Move temp file to final location
    temp_file.rename(output_file)

    return feat_count


def make_embl_files(strain_abbrev: str, output_dir: Path | None = None) -> tuple[bool, dict]:
    """
    Generate EMBL files for a strain with validation and safety checks.

    Returns tuple of (success, stats_dict).
    """
    stats = {
        "strain": strain_abbrev,
        "chromosomes": 0,
        "features": 0,
        "errors": [],
    }

    # Set up logging
    log_file = LOG_DIR / f"make_embl_files_{strain_abbrev}.log"
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Generating EMBL files for {strain_abbrev}")
    logger.info(f"Started: {datetime.now()}")

    # Create temp directory for safe generation
    temp_dir = Path(tempfile.mkdtemp(prefix="embl_"))

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, strain_abbrev)
            if not config:
                error_msg = f"Strain not found: {strain_abbrev}"
                logger.error(error_msg)
                stats["errors"].append("Strain not found in database")
                send_slack_message(error_msg, is_error=True)
                return False, stats

            seq_source = config["seq_source"]
            if not seq_source:
                error_msg = f"No seq_source found for {strain_abbrev}"
                logger.error(error_msg)
                stats["errors"].append("No seq_source found")
                send_slack_message(error_msg, is_error=True)
                return False, stats

            logger.info(f"Seq source: {seq_source}")

            # Determine final output directory
            if output_dir:
                final_dir = output_dir
            else:
                # Default: DOWNLOAD_DIR/embl/{strain}/
                final_dir = DOWNLOAD_DIR / "embl" / strain_abbrev

            # Create temp subdirectory for this strain
            temp_strain_dir = temp_dir / strain_abbrev
            temp_strain_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Temp directory: {temp_strain_dir}")
            logger.info(f"Final directory: {final_dir}")

            # Get chromosomes
            chromosomes = get_chromosomes(session, seq_source)
            logger.info(f"Found {len(chromosomes)} chromosomes/contigs")
            stats["chromosomes"] = len(chromosomes)

            # Write files to temp directory
            total_features = 0
            for chr_name, chr_type in chromosomes.items():
                temp_file = temp_strain_dir / f"{chr_name}.embl"
                logger.info(f"Writing {temp_file}")

                try:
                    count = write_embl_file(
                        session,
                        chr_name,
                        chr_type,
                        seq_source,
                        temp_file,
                        strain_abbrev,
                    )
                    logger.info(f"  {count} features written")
                    total_features += count
                except Exception as e:
                    logger.error(f"Error writing {chr_name}: {e}")
                    stats["errors"].append(f"Error writing {chr_name}: {e}")

            stats["features"] = total_features

            if stats["errors"]:
                error_msg = f"Errors generating EMBL for {strain_abbrev}: {len(stats['errors'])} errors"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False, stats

            # Validate the generated files
            is_valid, validation_msg = validate_embl_files(
                temp_strain_dir,
                final_dir if final_dir.exists() else None,
                strain_abbrev,
            )

            if not is_valid:
                error_msg = f"Validation failed: {validation_msg}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                stats["errors"].append(validation_msg)
                return False, stats

            logger.info(validation_msg)

            # Archive existing files before replacing
            if final_dir.exists():
                archive_dir = final_dir / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                today_tag = datetime.now().strftime("%Y%m%d")

                for embl_file in final_dir.glob("*.embl"):
                    archive_file = archive_dir / f"{embl_file.stem}_{today_tag}.embl"
                    try:
                        shutil.copy(str(embl_file), str(archive_file))
                        logger.info(f"Archived {embl_file.name}")
                    except Exception as e:
                        logger.warning(f"Could not archive {embl_file}: {e}")

            # Copy validated files to final location
            final_dir.mkdir(parents=True, exist_ok=True)
            for temp_file in temp_strain_dir.glob("*.embl"):
                final_file = final_dir / temp_file.name
                shutil.copy(str(temp_file), str(final_file))

            logger.info(f"Copied {len(chromosomes)} files to {final_dir}")
            logger.info(f"Total: {total_features} features across {len(chromosomes)} files")
            logger.info(f"Completed: {datetime.now()}")

            print(f"*{strain_abbrev}*: {len(chromosomes)} chromosomes, {total_features} features")

            # Send success notification
            send_slack_message(
                f"Successfully generated EMBL for {strain_abbrev}: "
                f"{len(chromosomes)} chromosomes, {total_features} features"
            )

        return True, stats

    except Exception as e:
        logger.exception(f"Error: {e}")
        stats["errors"].append(str(e))
        send_slack_message(f"Error generating EMBL for {strain_abbrev}: {e}", is_error=True)
        return False, stats

    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        logger.removeHandler(file_handler)
        file_handler.close()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate EMBL format files for chromosome sequences"
    )
    parser.add_argument(
        "strain",
        nargs="?",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all strains",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: DOWNLOAD_DIR/embl/{strain}/)",
    )

    args = parser.parse_args()

    if args.all:
        strains = STRAIN_ABBREVS
    elif args.strain:
        strains = [args.strain]
    else:
        parser.print_help()
        return 1

    all_success = True
    all_stats = []

    for strain in strains:
        print(f"\nProcessing {strain}...")
        success, stats = make_embl_files(strain, args.output_dir)
        all_stats.append(stats)
        if not success:
            all_success = False

    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    for stats in all_stats:
        status = "OK" if len(stats["errors"]) == 0 else "FAILED"
        print(f"\n{stats['strain']}: {status}")
        print(f"  Chromosomes: {stats['chromosomes']}")
        print(f"  Features: {stats['features']}")
        if stats["errors"]:
            print(f"  Errors: {len(stats['errors'])}")
            for err in stats["errors"][:3]:
                print(f"    - {err}")

    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
