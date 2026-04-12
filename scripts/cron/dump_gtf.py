#!/usr/bin/env python3
from __future__ import annotations

"""
Dump gene annotation data in GTF (Gene Transfer Format) format.

This script exports gene annotations (ORFs with their CDS structures)
to standard GTF format for use with bioinformatics tools like TopHat,
Cufflinks, etc.

Validation checks before copying to final location:
---------------------------------------------------------------------------
- Writes to temp directory first
- Validates minimum feature count (100+ per strain)
- Checks feature count change < 10% vs existing file
- Only copies to final location if validation passes
- Sends Slack notifications on success or failure

GTF Format:
- Tab-separated: seqname, source, feature, start, end, score, strand, frame, attributes
- Features include: start_codon, CDS, stop_codon
- Attributes: gene_id, transcript_id

Based on dumpGTF.pl by CGD team.

Usage:
    python dump_gtf.py <strain_abbrev>
    python dump_gtf.py C_albicans_SC5314
    python dump_gtf.py --all

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
import json
import logging
import os
import shutil
import sys
import tempfile
import urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path

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

# Configure logging to stderr so stdout can be used for GTF output
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
        "text": f"{emoji} {env_prefix}GTF Dump: {message}"
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
    """Count feature lines in a GTF file."""
    if not file_path.exists():
        return 0

    count = 0
    with open(file_path) as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                count += 1
    return count


def validate_output_file(
    new_file: Path,
    existing_file: Path | None = None,
    strain_abbrev: str = "",
) -> tuple[bool, str]:
    """
    Validate the generated GTF file.

    Returns:
        Tuple of (is_valid, message)
    """
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_features_in_file(new_file)

    # Check minimum features
    if new_count < MIN_FEATURES:
        return False, (
            f"Too few features for {strain_abbrev}: {new_count} "
            f"(minimum: {MIN_FEATURES})"
        )

    # Check against existing file if present
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

    organism_no = result[0]

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
    seq_result = session.execute(seq_query, {"organism_no": organism_no}).fetchone()

    return {
        "organism_no": result[0],
        "organism_abbrev": result[1],
        "taxon_id": result[2],
        "seq_source": seq_result[0] if seq_result else None,
    }


def get_genome_version(session, seq_source: str) -> str | None:
    """Get current genome version for the sequence source."""
    query = text(f"""
        SELECT gv.genome_version
        FROM {DB_SCHEMA}.genome_version gv
        WHERE gv.is_ver_current = 'Y'
        AND gv.genome_version_no IN (
            SELECT DISTINCT s.genome_version_no
            FROM {DB_SCHEMA}.seq s
            WHERE s.is_seq_current = 'Y'
            AND s.source = :seq_source
        )
    """)

    result = session.execute(query, {"seq_source": seq_source}).fetchone()
    return result[0] if result else None


def get_features(session, organism_no: int, seq_source: str) -> list[dict]:
    """Get ORF features with their location information."""
    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name,
               fp.property_value as feature_qualifier,
               fl.strand, root_feat.feature_name as chr_name
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl
            ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s
            ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        JOIN {DB_SCHEMA}.feature root_feat ON s.feature_no = root_feat.feature_no
        LEFT JOIN {DB_SCHEMA}.feat_property fp
            ON (f.feature_no = fp.feature_no AND fp.property_type = 'feature_qualifier')
        WHERE f.organism_no = :organism_no
        AND f.feature_type = 'ORF'
        ORDER BY root_feat.feature_name, fl.start_coord
    """)

    features = []
    for row in session.execute(query, {"organism_no": organism_no, "seq_source": seq_source}).fetchall():
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


def get_all_subfeatures(session, organism_no: int, seq_source: str) -> dict[int, list[dict]]:
    """Get all CDS subfeatures for features of an organism (batched for performance)."""
    query = text(f"""
        SELECT fr.parent_feature_no, fl.start_coord, fl.stop_coord
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_relationship fr ON fr.child_feature_no = f.feature_no
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.seq_no = s.seq_no AND s.is_seq_current = 'Y')
        JOIN {DB_SCHEMA}.feature parent_f ON fr.parent_feature_no = parent_f.feature_no
        WHERE parent_f.organism_no = :organism_no
        AND fr.rank = 2
        AND f.feature_type = 'CDS'
        AND s.source = :seq_source
        ORDER BY fr.parent_feature_no, fl.start_coord
    """)

    subfeature_map: dict[int, list[dict]] = defaultdict(list)
    for row in session.execute(query, {"organism_no": organism_no, "seq_source": seq_source}).fetchall():
        start = row[1]
        end = row[2]
        if start > end:
            start, end = end, start
        subfeature_map[row[0]].append({
            "start": start,
            "end": end,
        })

    return subfeature_map


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
    organism_no = config["organism_no"]
    source = PROJECT_ACRONYM

    logger.info(f"Dumping GTF for {strain_abbrev} (seq_source: {seq_source})")

    # Get features
    features = get_features(session, organism_no, seq_source)
    logger.info(f"Found {len(features)} ORF features")

    # Batch fetch all subfeatures
    logger.info("Fetching subfeatures...")
    subfeature_map = get_all_subfeatures(session, organism_no, seq_source)

    count = 0

    for feat in features:
        feature_name = feat["feature_name"]
        chr_name = feat["chr_name"]
        strand = feat["strand"]
        transcript_id = f"{feature_name}-T"

        # Get subfeatures (CDS segments)
        subfeatures = subfeature_map.get(feat["feature_no"], [])

        if not subfeatures:
            continue

        # Sort by position
        subfeatures = sorted(subfeatures, key=lambda x: x["start"])

        # Build attribute string
        attr = f'gene_id "{feature_name}"; transcript_id "{transcript_id}";'

        # For each CDS segment, output start_codon, CDS, stop_codon
        for i, sf in enumerate(subfeatures):
            cds_start = sf["start"]
            cds_end = sf["end"]

            if strand == "+":
                # First segment has start codon
                if i == 0:
                    # Start codon (first 3 bases)
                    output_file.write(
                        f"{chr_name}\t{source}\tstart_codon\t{cds_start}\t{cds_start + 2}\t.\t"
                        f"{strand}\t0\t{attr}\n"
                    )

                # Last segment has stop codon
                if i == len(subfeatures) - 1:
                    # CDS without stop codon
                    output_file.write(
                        f"{chr_name}\t{source}\tCDS\t{cds_start}\t{cds_end - 3}\t.\t"
                        f"{strand}\t0\t{attr}\n"
                    )
                    # Stop codon (last 3 bases)
                    output_file.write(
                        f"{chr_name}\t{source}\tstop_codon\t{cds_end - 2}\t{cds_end}\t.\t"
                        f"{strand}\t.\t{attr}\n"
                    )
                else:
                    # Full CDS for non-last segments
                    output_file.write(
                        f"{chr_name}\t{source}\tCDS\t{cds_start}\t{cds_end}\t.\t"
                        f"{strand}\t0\t{attr}\n"
                    )

            else:  # strand == "-"
                # First segment (lowest coordinates) has stop codon
                if i == 0:
                    # Stop codon (first 3 bases on - strand)
                    output_file.write(
                        f"{chr_name}\t{source}\tstop_codon\t{cds_start}\t{cds_start + 2}\t.\t"
                        f"{strand}\t.\t{attr}\n"
                    )
                    # CDS without stop codon
                    output_file.write(
                        f"{chr_name}\t{source}\tCDS\t{cds_start + 3}\t{cds_end}\t.\t"
                        f"{strand}\t0\t{attr}\n"
                    )
                else:
                    # Full CDS for non-first segments
                    output_file.write(
                        f"{chr_name}\t{source}\tCDS\t{cds_start}\t{cds_end}\t.\t"
                        f"{strand}\t0\t{attr}\n"
                    )

                # Last segment (highest coordinates) has start codon
                if i == len(subfeatures) - 1:
                    # Start codon (last 3 bases on - strand)
                    output_file.write(
                        f"{chr_name}\t{source}\tstart_codon\t{cds_end - 2}\t{cds_end}\t.\t"
                        f"{strand}\t0\t{attr}\n"
                    )

        count += 1

    logger.info(f"Wrote {count} features to GTF")
    return count


def get_output_path(strain_abbrev: str, genome_version: str) -> Path:
    """Get the output path for the GTF file, matching GFF directory structure."""
    # GTF files go in same directory as GFF files
    gff_dir = DOWNLOAD_DIR / "gff" / strain_abbrev

    # Determine assembly directory (e.g., Assembly22 for A22-xxx)
    if genome_version.startswith("A"):
        assembly_num = genome_version.split("-")[0][1:]  # Extract number from "A22"
        assembly_dir = gff_dir / f"Assembly{assembly_num}"
    else:
        # For non-A versions, use root strain directory
        assembly_dir = gff_dir

    assembly_dir.mkdir(parents=True, exist_ok=True)

    # Filename: <strain>_version_<genome_version>_features.gtf
    filename = f"{strain_abbrev}_version_{genome_version}_features.gtf"

    return assembly_dir / filename


def generate_gtf(strain_abbrev: str) -> bool:
    """
    Generate GTF file for a strain with validation and safety checks.

    Returns:
        True on success, False on failure
    """
    # Create temp directory for safe generation
    temp_dir = Path(tempfile.mkdtemp(prefix="gtf_"))

    try:
        with SessionLocal() as session:
            # Get strain config
            config = get_strain_config(session, strain_abbrev)
            if not config:
                error_msg = f"Strain {strain_abbrev} not found in database"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            if not config["seq_source"]:
                error_msg = f"No seq_source found for {strain_abbrev}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            # Get genome version
            genome_version = get_genome_version(session, config["seq_source"])
            if not genome_version:
                genome_version = "unknown"

            # Determine output path
            final_file = get_output_path(strain_abbrev, genome_version)
            temp_file = temp_dir / final_file.name

            logger.info(f"Generating GTF for {strain_abbrev} -> {final_file}")

            # Generate GTF to temp file
            with open(temp_file, "w") as f:
                count = dump_gtf(session, strain_abbrev, f)

            if count == 0:
                error_msg = f"No features found for {strain_abbrev}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            # Validate the generated file
            is_valid, validation_msg = validate_output_file(
                temp_file,
                final_file if final_file.exists() else None,
                strain_abbrev,
            )

            if not is_valid:
                error_msg = f"Validation failed: {validation_msg}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            logger.info(validation_msg)

            # Archive existing file before replacing
            if final_file.exists():
                archive_dir = final_file.parent / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                today_tag = datetime.now().strftime("%Y%m%d")
                archive_file = archive_dir / f"{final_file.stem}_{today_tag}.gtf"
                try:
                    shutil.copy(str(final_file), str(archive_file))
                    logger.info(f"Archived {final_file} to {archive_file}")
                except Exception as e:
                    logger.warning(f"Could not archive {final_file}: {e}")

            # Copy validated file to final location
            shutil.copy(str(temp_file), str(final_file))
            logger.info(f"Output written to {final_file}")

            # Print summary for wrapper script
            print(f"*{strain_abbrev}*: {count} features exported to {final_file.name}")

            # Send success notification
            send_slack_message(
                f"Successfully generated {final_file.name} with {count} features"
            )

            return True

    except Exception as e:
        logger.exception(f"Error generating GTF for {strain_abbrev}: {e}")
        send_slack_message(
            f"Error generating GTF for {strain_abbrev}: {e}",
            is_error=True
        )
        return False

    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump gene annotation data in GTF format"
    )
    parser.add_argument(
        "strain_abbrev",
        nargs="?",
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate GTF for all strains",
    )

    args = parser.parse_args()

    if args.all:
        strains = [
            "C_albicans_SC5314",
            "C_dubliniensis_CD36",
            "C_glabrata_CBS138",
            "C_parapsilosis_CDC317",
            "C_auris_B8441",
        ]
    elif args.strain_abbrev:
        strains = [args.strain_abbrev]
    else:
        parser.print_help()
        return 1

    success = True
    for strain in strains:
        if not generate_gtf(strain):
            success = False

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
