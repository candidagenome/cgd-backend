#!/usr/bin/env python3
from __future__ import annotations

"""
Generate gp2protein mapping files for the External_id_mappings download area.

Produces, for every CGD species, a two-column mapping of CGD feature IDs to
UniProtKB accessions, matching the files published at
https://www.candidagenome.org/download/External_id_mappings/

Outputs (written gzipped, matching the download site):
  - gp2protein_<strain_abbrev>.gz   one per species
  - gp2protein.cgd.gz               master file (union of all species)

Format (one line per feature, no header, tab-separated):
  CGD:<CGDID>\tUniProtKB:<accession>

Each feature is emitted once. When a feature has more than one UniProt
cross-reference, a reviewed Swiss-Prot accession is preferred over an
unreviewed TrEMBL one; ties are broken by lowest accession for determinism.

Data source: the dbxref table (source='EBI', dbxref_type IN
('SwissProt','TrEMBL')) linked to features of type ORF / allele / pseudogene,
i.e. the same selection the legacy uniprot_split.pl used. C. albicans B
alleles are picked up via feature_type='allele'.

Validation before replacing published files (per species):
  - writes to a temp directory first
  - requires a minimum feature count
  - rejects a >10% swing vs. the existing file
  - Slack notification on success / failure

Based on uniprot_split.pl / SGD-gp2protein.pl; structured after make_gpi.py.

Usage:
    python make_gp2protein.py C_albicans_SC5314
    python make_gp2protein.py --all
    python make_gp2protein.py --all --output-dir /tmp/gp2protein

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DOWNLOAD_DIR: Base directory for download output
    PROJECT_ACRONYM: Project acronym (default: CGD)
    SLACK_WEBHOOK_URL: Slack webhook URL for notifications
    ENV_STATE: Environment state (dev/prod) for Slack labels
"""

import argparse
import gzip
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
GP2PROTEIN_DIR = DOWNLOAD_DIR / "External_id_mappings"
ARCHIVE_DIR = GP2PROTEIN_DIR / "archive"

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_FEATURES = 100  # Absolute minimum features expected per species
MAX_FEATURE_CHANGE_PERCENT = 10.0  # Maximum allowed change from previous file

# Feature types included in gp2protein (matches legacy uniprot_split.pl)
FEATURE_TYPES = ("ORF", "allele", "pseudogene")

# UniProt dbxref selection (matches how the other species were loaded)
UNIPROT_SOURCE = "EBI"
SWISSPROT_TYPE = "SwissProt"
TREMBL_TYPE = "TrEMBL"

# Master (all-species) output file name, e.g. gp2protein.cgd
MASTER_FILE = f"gp2protein.{PROJECT_ACRONYM.lower()}"

# The six CGD species (mirrors make_gpi.py STRAIN_CONFIGS)
STRAIN_CONFIGS = {
    "C_albicans_SC5314": {"taxon_id": "237561"},
    "C_auris_B8441": {"taxon_id": "498019"},
    "C_dubliniensis_CD36": {"taxon_id": "573826"},
    "C_glabrata_CBS138": {"taxon_id": "284593"},
    "C_parapsilosis_CDC317": {"taxon_id": "578454"},
    "C_tropicalis": {"taxon_id": "294747"},
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

    payload = {"text": f"{emoji} {env_prefix}gp2protein Generation: {message}"}

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Slack notification failed: {response.status}")
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")


def count_lines_in_file(file_path: Path) -> int:
    """Count non-header data lines in a (optionally gzipped) gp2protein file."""
    if not file_path.exists():
        return 0

    opener = gzip.open if file_path.suffix == ".gz" else open
    count = 0
    with opener(file_path, "rt") as f:
        for line in f:
            if line.strip() and not line.startswith("!"):
                count += 1
    return count


def validate_output_file(
    new_file: Path,
    existing_file: Path | None,
    label: str,
) -> tuple[bool, str]:
    """Validate a generated gp2protein file.

    Returns (is_valid, message).
    """
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_lines_in_file(new_file)

    if new_count < MIN_FEATURES:
        return False, (
            f"Too few mappings for {label}: {new_count} (minimum: {MIN_FEATURES})"
        )

    if existing_file and existing_file.exists():
        existing_count = count_lines_in_file(existing_file)
        if existing_count > 0:
            change_pct = abs(new_count - existing_count) / existing_count * 100
            if change_pct > MAX_FEATURE_CHANGE_PERCENT:
                return False, (
                    f"Mapping count changed too much for {label}: "
                    f"{existing_count} -> {new_count} ({change_pct:.1f}% change, "
                    f"max: {MAX_FEATURE_CHANGE_PERCENT}%)"
                )

    return True, f"Validation passed for {label}: {new_count} mappings"


def get_uniprot_mappings(session, organism_abbrev: str) -> dict[str, str]:
    """Return {CGDID: UniProt accession} for one species, one entry per feature.

    A reviewed Swiss-Prot accession is preferred over TrEMBL; ties are broken by
    lowest accession so the output is deterministic.
    """
    query = text(f"""
        SELECT f.dbxref_id AS cgdid, d.dbxref_type, d.dbxref_id AS accession
        FROM {DB_SCHEMA}.dbxref d
        JOIN {DB_SCHEMA}.dbxref_feat df ON d.dbxref_no = df.dbxref_no
        JOIN {DB_SCHEMA}.feature f ON df.feature_no = f.feature_no
        JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
        WHERE o.organism_abbrev = :abbrev
          AND d.source = :src
          AND d.dbxref_type IN (:sp, :tr)
          AND f.feature_type IN ({", ".join(f"'{t}'" for t in FEATURE_TYPES)})
    """)

    # best[cgdid] = (rank, accession); rank 0 = Swiss-Prot (preferred), 1 = TrEMBL
    best: dict[str, tuple[int, str]] = {}
    rows = session.execute(
        query,
        {"abbrev": organism_abbrev, "src": UNIPROT_SOURCE,
         "sp": SWISSPROT_TYPE, "tr": TREMBL_TYPE},
    )
    for cgdid, dbxref_type, accession in rows:
        if not cgdid or not accession:
            continue
        rank = 0 if dbxref_type == SWISSPROT_TYPE else 1
        candidate = (rank, accession)
        current = best.get(cgdid)
        # Lower rank wins; within a rank, lexicographically smaller accession wins.
        if current is None or candidate < current:
            best[cgdid] = candidate

    return {cgdid: acc for cgdid, (_, acc) in best.items()}


def write_gp2protein_file(mappings: dict[str, str], out_path: Path) -> int:
    """Write a gzipped gp2protein file, sorted by CGDID. Returns line count."""
    with gzip.open(out_path, "wt") as f:
        for cgdid in sorted(mappings):
            f.write(f"{PROJECT_ACRONYM}:{cgdid}\tUniProtKB:{mappings[cgdid]}\n")
    return len(mappings)


def archive_and_replace(temp_file: Path, final_file: Path, today_tag: str) -> None:
    """Archive an existing published file, then move the new one into place."""
    final_file.parent.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    if final_file.exists():
        archive_file = ARCHIVE_DIR / f"{final_file.name}_{today_tag}"
        try:
            shutil.copy(str(final_file), str(archive_file))
            logger.info(f"Archived {final_file.name} -> {archive_file}")
        except Exception as e:
            logger.warning(f"Could not archive {final_file}: {e}")

    shutil.move(str(temp_file), str(final_file))


def generate(strains: list[str], output_dir: Path) -> bool:
    """Generate per-species files (+ master) for the given strains.

    Returns True if every species succeeded.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    today_tag = datetime.now().strftime("%Y%m%d")

    temp_dir = Path(tempfile.mkdtemp(prefix="gp2protein_"))
    success = True
    master: dict[str, str] = {}
    # Only build/replace the master when the full species set is generated.
    build_master = set(strains) == set(STRAIN_CONFIGS)

    try:
        with SessionLocal() as session:
            for strain in strains:
                if strain not in STRAIN_CONFIGS:
                    logger.error(f"Unknown species: {strain}")
                    success = False
                    continue

                mappings = get_uniprot_mappings(session, strain)
                logger.info(f"{strain}: {len(mappings)} UniProt mappings")
                master.update(mappings)

                filename = f"gp2protein_{strain}.gz"
                temp_file = temp_dir / filename
                final_file = output_dir / filename

                write_gp2protein_file(mappings, temp_file)

                is_valid, msg = validate_output_file(
                    temp_file,
                    final_file if final_file.exists() else None,
                    strain,
                )
                if not is_valid:
                    logger.error(f"Validation failed: {msg}")
                    send_slack_message(f"Validation failed: {msg}", is_error=True)
                    success = False
                    continue

                logger.info(msg)
                archive_and_replace(temp_file, final_file, today_tag)
                logger.info(f"Wrote {final_file}")

            if build_master:
                filename = f"{MASTER_FILE}.gz"
                temp_file = temp_dir / filename
                final_file = output_dir / filename
                write_gp2protein_file(master, temp_file)

                is_valid, msg = validate_output_file(
                    temp_file,
                    final_file if final_file.exists() else None,
                    MASTER_FILE,
                )
                if not is_valid:
                    logger.error(f"Validation failed: {msg}")
                    send_slack_message(f"Validation failed: {msg}", is_error=True)
                    success = False
                else:
                    logger.info(msg)
                    archive_and_replace(temp_file, final_file, today_tag)
                    logger.info(f"Wrote {final_file} ({len(master)} mappings)")

    except Exception as e:
        logger.exception(f"Error generating gp2protein files: {e}")
        send_slack_message(f"Error generating gp2protein files: {e}", is_error=True)
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    if success:
        n = len(strains)
        send_slack_message(
            f"Generated gp2protein for {n} species"
            + (f" + master ({len(master)} mappings)" if build_master else "")
        )

    return success


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate gp2protein mapping files"
    )
    parser.add_argument(
        "strain",
        nargs="?",
        help="Species abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate files for all species (also builds the master file)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=GP2PROTEIN_DIR,
        help=f"Output directory (default: {GP2PROTEIN_DIR})",
    )

    args = parser.parse_args()

    if args.all:
        strains = list(STRAIN_CONFIGS.keys())
    elif args.strain:
        strains = [args.strain]
    else:
        parser.print_help()
        return 1

    ok = generate(strains, args.output_dir)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
