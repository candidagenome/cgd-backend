#!/usr/bin/env python3
from __future__ import annotations

"""
Dump GO annotations to GAF (Gene Association File) format.

This script generates gene_association.cgd file in GAF 2.0 format
for submission to the GO Consortium.

Validation checks before copying to final location:
---------------------------------------------------------------------------
- Writes to temp directory first
- Validates minimum annotation count (1,000+ absolute floor)
- Checks annotation count change < 10% vs existing file
- Only copies to final location if validation passes
- Sends Slack notifications on success or failure

Based on dumpAnnotation.pl by Shuai Weng (Sept 2000)
Updated for MULTI by Jon Binkley (Feb 2011)

Usage:
    python dump_annotation.py
    python dump_annotation.py --output-dir /path/to/output

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DOWNLOAD_DIR: Directory for output files
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

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data")))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Output directories
GO_DIR = DOWNLOAD_DIR / "go"
ARCHIVE_DIR = GO_DIR / "archive"

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_ANNOTATIONS = 1000  # Absolute minimum annotations expected
MAX_ANNOTATION_CHANGE_PERCENT = 10.0  # Maximum allowed change from previous file

# Strain configurations with taxon IDs
STRAIN_CONFIGS = {
    "C_albicans_SC5314": {
        "seq_source": "C. albicans SC5314 Assembly 22",
        "taxon_id": "237561",
    },
    "C_auris_B8441": {
        "seq_source": "C. auris B8441",
        "taxon_id": "498019",
    },
    "C_dubliniensis_CD36": {
        "seq_source": "C. dubliniensis CD36",
        "taxon_id": "573826",
    },
    "C_glabrata_CBS138": {
        "seq_source": "C. glabrata CBS138",
        "taxon_id": "284593",
    },
    "C_parapsilosis_CDC317": {
        "seq_source": "C. parapsilosis CDC317",
        "taxon_id": "578454",
    },
    "C_tropicalis_MYA-3404": {
        "seq_source": "C. tropicalis MYA-3404",
        "taxon_id": "294747",
    },
}

# Configure logging
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
        "text": f"{emoji} {env_prefix}GO Annotation Dump: {message}"
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


def count_annotations_in_file(file_path: Path) -> int:
    """Count non-header lines (annotations) in a GAF file."""
    if not file_path.exists():
        return 0

    count = 0
    with open(file_path) as f:
        for line in f:
            if not line.startswith("!"):
                count += 1
    return count


def validate_output_file(
    new_file: Path,
    existing_file: Path | None = None,
) -> tuple[bool, str]:
    """
    Validate the generated GAF file.

    Returns:
        Tuple of (is_valid, message)
    """
    # Check file exists and has content
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_annotations_in_file(new_file)

    # Check minimum annotations
    if new_count < MIN_ANNOTATIONS:
        return False, (
            f"Too few annotations: {new_count} (minimum: {MIN_ANNOTATIONS})"
        )

    # Check against existing file if present
    if existing_file and existing_file.exists():
        existing_count = count_annotations_in_file(existing_file)
        if existing_count > 0:
            change_pct = abs(new_count - existing_count) / existing_count * 100
            if change_pct > MAX_ANNOTATION_CHANGE_PERCENT:
                return False, (
                    f"Annotation count changed too much: {existing_count} -> "
                    f"{new_count} ({change_pct:.1f}% change, max: "
                    f"{MAX_ANNOTATION_CHANGE_PERCENT}%)"
                )

    return True, f"Validation passed: {new_count} annotations"


def zero_pad_goid(goid: str | int) -> str:
    """Pad GOID with leading zeros to 7 digits."""
    goid_str = str(goid)
    return goid_str.zfill(7)


def get_organism_info(session, strain_abbrev: str) -> dict | None:
    """Get organism_no for a strain."""
    query = text(f"""
        SELECT organism_no, taxon_id
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :strain_abbrev
    """)
    result = session.execute(query, {"strain_abbrev": strain_abbrev}).fetchone()
    if result:
        return {"organism_no": result[0], "taxon_id": result[1]}
    return None


def get_features(session, seq_source: str) -> dict:
    """
    Get features for GAF output.

    Returns dict mapping feature_no to feature info.
    """
    # Get feature types from web_metadata
    type_query = text(f"""
        SELECT col_value
        FROM {DB_SCHEMA}.web_metadata
        WHERE tab_name = 'FEATURE'
        AND col_name = 'FEATURE_TYPE'
        AND application_name = 'Chromosomal Feature Search'
    """)
    type_result = session.execute(type_query)
    feature_types = [row[0] for row in type_result]

    if not feature_types:
        # Fallback to common types
        feature_types = ['ORF', 'ncRNA', 'rRNA', 'snRNA', 'snoRNA', 'tRNA', 'pseudogene']

    # Build type list for SQL
    type_list = ", ".join(f"'{t}'" for t in feature_types)

    query = text(f"""
        SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location fl ON (f.feature_no = fl.feature_no AND fl.is_loc_current = 'Y')
        JOIN {DB_SCHEMA}.seq s ON (fl.root_seq_no = s.seq_no AND s.is_seq_current = 'Y' AND s.source = :seq_source)
        JOIN {DB_SCHEMA}.genome_version gv ON (s.genome_version_no = gv.genome_version_no AND gv.is_ver_current = 'Y')
        WHERE f.feature_type IN ({type_list})
        AND f.feature_no NOT IN (
            SELECT DISTINCT fp.feature_no
            FROM {DB_SCHEMA}.feat_property fp
            WHERE fp.property_type = 'feature_qualifier'
            AND (fp.property_value LIKE 'Deleted%' OR fp.property_value = 'Dubious')
        )
    """)

    result = session.execute(query, {"seq_source": seq_source})
    features = {}

    for row in result:
        feature_no, feature_name, gene_name, dbxref_id = row
        features[feature_no] = {
            "feature_name": feature_name,
            "gene_name": gene_name or feature_name,
            "dbxref_id": dbxref_id,
        }

    return features


def get_aliases(session, feature_no: int) -> list[str]:
    """Get aliases for a feature."""
    query = text(f"""
        SELECT a.alias_name
        FROM {DB_SCHEMA}.alias a
        JOIN {DB_SCHEMA}.feat_alias fa ON (a.alias_no = fa.alias_no AND fa.feature_no = :feature_no)
    """)
    result = session.execute(query, {"feature_no": feature_no})
    return [row[0] for row in result if row[0]]


def get_go_info(session) -> tuple[dict, dict]:
    """
    Get GO term info.

    Returns:
        - go_no_to_info: dict mapping go_no to {goid, aspect}
        - goid_to_aspect: dict mapping goid to aspect
    """
    query = text(f"""
        SELECT go_no, goid, go_aspect
        FROM {DB_SCHEMA}.go
    """)
    result = session.execute(query)

    go_no_to_info = {}
    goid_to_aspect = {}

    for row in result:
        go_no, goid, go_aspect = row
        go_no_to_info[go_no] = {"goid": str(goid), "aspect": go_aspect}
        goid_to_aspect[str(goid)] = go_aspect

    return go_no_to_info, goid_to_aspect


def get_go_annotations(session, feature_nos: set) -> dict:
    """
    Get GO annotations for features.

    Returns dict mapping feature_no to list of annotations.
    """
    if not feature_nos:
        return {}

    # Get GO refs with reference info
    query = text(f"""
        SELECT gr.go_ref_no, r.dbxref_id, r.pubmed, gr.go_annotation_no,
               TO_CHAR(gr.date_created, 'YYYYMMDD') as date_created
        FROM {DB_SCHEMA}.go_ref gr
        JOIN {DB_SCHEMA}.reference r ON gr.reference_no = r.reference_no
    """)

    go_refs = {}
    result = session.execute(query)
    for row in result:
        go_ref_no, ref_dbxref_id, pubmed, go_annotation_no, date_created = row
        if go_annotation_no not in go_refs:
            go_refs[go_annotation_no] = []
        go_refs[go_annotation_no].append({
            "go_ref_no": go_ref_no,
            "ref_dbxref_id": ref_dbxref_id,
            "pubmed": pubmed,
            "date_created": date_created or datetime.now().strftime("%Y%m%d"),
        })

    # Get GO annotations
    annot_query = text(f"""
        SELECT go_annotation_no, go_no, feature_no, go_evidence, source
        FROM {DB_SCHEMA}.go_annotation
    """)

    annotations = {}
    annot_result = session.execute(annot_query)

    for row in annot_result:
        go_annotation_no, go_no, feature_no, go_evidence, source = row
        if feature_no not in feature_nos:
            continue

        if feature_no not in annotations:
            annotations[feature_no] = []

        # Get refs for this annotation
        refs = go_refs.get(go_annotation_no, [])
        for ref in refs:
            annotations[feature_no].append({
                "go_no": go_no,
                "go_evidence": go_evidence,
                "source": source,
                "go_ref_no": ref["go_ref_no"],
                "ref_dbxref_id": ref["ref_dbxref_id"],
                "pubmed": ref["pubmed"],
                "date_created": ref["date_created"],
            })

    return annotations


def get_go_qualifiers(session) -> dict:
    """Get NOT qualifiers for go_refs."""
    query = text(f"""
        SELECT go_ref_no, qualifier
        FROM {DB_SCHEMA}.go_qualifier
        WHERE qualifier = 'NOT'
    """)
    result = session.execute(query)
    return {row[0]: row[1] for row in result}


def get_supporting_evidence(session) -> dict:
    """Get supporting evidence (with/from) for go_refs."""
    # Load DB code mapping
    db_code_file = PROJECT_ROOT / "data" / "GO_DB_code_mapping"
    db_code_map = {}

    if db_code_file.exists():
        with open(db_code_file) as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    go_code, db_code = parts[0], parts[1]
                    db_code_map[db_code.upper()] = go_code

    query = text(f"""
        SELECT gr.go_ref_no, d.source, d.dbxref_type, d.dbxref_id
        FROM {DB_SCHEMA}.go_ref gr
        JOIN {DB_SCHEMA}.goref_dbxref gd ON gd.go_ref_no = gr.go_ref_no
        JOIN {DB_SCHEMA}.dbxref d ON gd.dbxref_no = d.dbxref_no
        ORDER BY gr.go_ref_no, d.source
    """)

    result = session.execute(query)
    supports = {}

    for row in result:
        go_ref_no, source, dbxref_type, dbxref_id = row

        # Map source to GO code
        key = f"{source.upper()}:{dbxref_type.upper()}" if dbxref_type else source.upper()
        go_code = db_code_map.get(key, db_code_map.get(source.upper(), source))

        # Pad GOID if needed
        if go_code == "GO":
            dbxref_id = zero_pad_goid(dbxref_id)

        evidence = f"{go_code}:{dbxref_id}"

        if go_ref_no in supports:
            supports[go_ref_no] += f"|{evidence}"
        else:
            supports[go_ref_no] = evidence

    return supports


def generate_gaf_header(strains: list[str], date_str: str) -> str:
    """Generate GAF 2.0 header."""
    lines = []
    lines.append("!gaf-version: 2.0")
    lines.append(f"!Project_name: {PROJECT_ACRONYM}")
    lines.append(f"!Date: {date_str}")
    lines.append(f"!URL: http://www.candidagenome.org")
    lines.append(f"!Contact: cgd-submission@lists.stanford.edu")
    lines.append(f"!Strains: {', '.join(strains)}")
    return "\n".join(lines) + "\n"


def dump_annotation(output_dir: Path | None = None) -> bool:
    """
    Generate GAF file with GO annotations.

    Args:
        output_dir: Output directory (default: DOWNLOAD_DIR/go)

    Returns:
        True on success, False on failure
    """
    if output_dir is None:
        output_dir = GO_DIR

    output_dir.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    gaf_filename = f"gene_association.{PROJECT_ACRONYM.lower()}"
    final_file = output_dir / gaf_filename
    today = datetime.now().strftime("%Y-%m-%d")
    today_tag = datetime.now().strftime("%Y%m%d")

    # Create temp directory for safe generation
    temp_dir = Path(tempfile.mkdtemp(prefix="go_annotation_"))
    temp_file = temp_dir / gaf_filename

    try:
        with SessionLocal() as session:
            # Get GO info
            logger.info("Loading GO term info...")
            go_no_to_info, goid_to_aspect = get_go_info(session)

            # Get qualifiers and supporting evidence
            logger.info("Loading qualifiers and supporting evidence...")
            qualifiers = get_go_qualifiers(session)
            supports = get_supporting_evidence(session)

            # Collect all features and annotations
            all_features = {}  # feature_no -> info
            all_aliases = {}   # feature_no -> alias string
            feature_to_strain = {}  # feature_no -> (strain_abbrev, taxon_id)

            for strain_abbrev, config in STRAIN_CONFIGS.items():
                seq_source = config["seq_source"]
                taxon_id = config["taxon_id"]

                logger.info(f"Loading features for {strain_abbrev}...")
                features = get_features(session, seq_source)

                for feat_no, feat_info in features.items():
                    all_features[feat_no] = feat_info
                    feature_to_strain[feat_no] = (strain_abbrev, taxon_id)

                    # Get aliases
                    aliases = get_aliases(session, feat_no)
                    alias_str = feat_info["feature_name"]
                    if aliases:
                        alias_str += "|" + "|".join(aliases)
                    all_aliases[feat_no] = alias_str

            logger.info(f"Loaded {len(all_features)} features")

            # Get annotations
            logger.info("Loading GO annotations...")
            annotations = get_go_annotations(session, set(all_features.keys()))

            # Write GAF file to temp location
            annotation_count = 0
            with open(temp_file, "w") as f:
                # Write header
                f.write(generate_gaf_header(list(STRAIN_CONFIGS.keys()), today))

                # Write annotations
                seen = set()  # Track unique annotations

                for feat_no, feat_info in sorted(all_features.items()):
                    if feat_no not in annotations:
                        continue

                    strain_abbrev, taxon_id = feature_to_strain[feat_no]
                    dbxref_id = feat_info["dbxref_id"]
                    gene_name = feat_info["gene_name"]
                    alias_str = all_aliases.get(feat_no, feat_info["feature_name"])

                    if not dbxref_id:
                        continue

                    for annot in annotations[feat_no]:
                        go_no = annot["go_no"]
                        go_ref_no = annot["go_ref_no"]

                        if go_no not in go_no_to_info:
                            continue

                        goid = go_no_to_info[go_no]["goid"]
                        aspect = go_no_to_info[go_no]["aspect"]

                        # Create unique key
                        key = (feat_no, goid, go_ref_no)
                        if key in seen:
                            continue
                        seen.add(key)

                        # Get qualifier (NOT)
                        qualifier = qualifiers.get(go_ref_no, "")

                        # Get reference
                        ref_str = f"{PROJECT_ACRONYM}_REF:{annot['ref_dbxref_id']}"
                        if annot["pubmed"]:
                            ref_str += f"|PMID:{annot['pubmed']}"

                        # Get supporting evidence
                        with_from = supports.get(go_ref_no, "")

                        # GAF 2.0 columns
                        columns = [
                            PROJECT_ACRONYM,           # 1. DB
                            dbxref_id,                 # 2. DB_Object_ID
                            gene_name,                 # 3. DB_Object_Symbol
                            qualifier,                 # 4. Qualifier
                            f"GO:{zero_pad_goid(goid)}", # 5. GO_ID
                            ref_str,                   # 6. DB:Reference
                            annot["go_evidence"],      # 7. Evidence_Code
                            with_from,                 # 8. With/From
                            aspect,                    # 9. Aspect
                            "",                        # 10. DB_Object_Name
                            alias_str,                 # 11. DB_Object_Synonym
                            "gene_product",            # 12. DB_Object_Type
                            f"taxon:{taxon_id}",       # 13. Taxon
                            annot["date_created"],     # 14. Date
                            annot["source"],           # 15. Assigned_By
                            "",                        # 16. Annotation_Extension
                            "",                        # 17. Gene_Product_Form_ID
                        ]

                        f.write("\t".join(columns) + "\n")
                        annotation_count += 1

            logger.info(f"Wrote {annotation_count} annotations to {temp_file}")

            # Validate the generated file
            is_valid, validation_msg = validate_output_file(
                temp_file,
                final_file if final_file.exists() else None,
            )

            if not is_valid:
                error_msg = f"Validation failed: {validation_msg}"
                logger.error(error_msg)
                send_slack_message(error_msg, is_error=True)
                return False

            # Archive existing file before replacing
            if final_file.exists():
                archive_file = ARCHIVE_DIR / f"{gaf_filename}_{today_tag}"
                try:
                    shutil.copy(str(final_file), str(archive_file))
                    logger.info(f"Archived {final_file} to {archive_file}")
                except Exception as e:
                    logger.warning(f"Could not archive {final_file}: {e}")

            # Copy validated file to final location
            shutil.copy(str(temp_file), str(final_file))
            logger.info(f"Copied validated file to {final_file}")

            print(f"Generated: {final_file}")
            print(f"  Annotations: {annotation_count}")
            print(f"  Features: {len(all_features)}")

            # Send success notification
            send_slack_message(
                f"Successfully generated {gaf_filename} with "
                f"{annotation_count} annotations"
            )

            return True

    except Exception as e:
        logger.exception(f"Error generating GAF: {e}")
        send_slack_message(f"Error generating GAF: {e}", is_error=True)
        return False

    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump GO annotations to GAF format"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory",
    )

    args = parser.parse_args()

    success = dump_annotation(args.output_dir)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
