#!/usr/bin/env python3
from __future__ import annotations

"""
Generate CGDID_2_GeneID.tab for the External_id_mappings download area.

Produces a two-column mapping of CGD feature IDs to NCBI Entrez Gene IDs,
matching the file published at
https://www.candidagenome.org/download/External_id_mappings/CGDID_2_GeneID.tab.gz

Output (gzipped, matching the download site):
  - CGDID_2_GeneID.tab.gz

Format (tab-separated, one row per Entrez Gene ID, with a header line):
  CGDID<TAB>Entrez GeneID
  CAL0002379<TAB>3639833
  ...

The CGDID is the raw CGD identifier (no "CGD:" prefix), matching the legacy
file. Data source is the dbxref table (dbxref_type='Entrez Gene ID',
source='NCBI') linked to features. Only C. albicans carries Entrez Gene IDs,
so the file is C. albicans in practice, but no organism filter is applied.

One row per Entrez Gene ID
--------------------------
An Entrez Gene ID links to more than one CGD feature (the orf19.* systematic
gene and its assembly-22 allele feature). To emit exactly one row per Entrez
ID -- reproducing the legacy file -- the orf19.* feature is preferred; ties (or
the absence of an orf19.* feature) fall back to the lowest CGDID for
determinism.

Validation before replacing the published file:
  - writes to a temp directory first
  - requires a minimum row count
  - rejects a >10% swing vs. the existing file
  - Slack notification on success / failure

Structured after make_gpi.py / make_gp2protein.py.

Usage:
    python make_cgdid_2_geneid.py
    python make_cgdid_2_geneid.py --output-dir /tmp/mappings

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DOWNLOAD_DIR: Base directory for download output
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
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data")))

# Output
MAPPINGS_DIR = DOWNLOAD_DIR / "External_id_mappings"
ARCHIVE_DIR = MAPPINGS_DIR / "archive"
OUTPUT_FILE = "CGDID_2_GeneID.tab"
HEADER = "CGDID\tEntrez GeneID"

# dbxref selection
GENEID_TYPE = "Entrez Gene ID"
GENEID_SOURCE = "NCBI"

# Assembly-22 A-allele systematic name, e.g. C1_09470C_A / CR_01234W_A
SYSTEMATIC_A_RE = re.compile(r"^C[1-7R]_\d{5}[WC]_A$")

# Slack webhook for notifications
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
ENV_STATE = os.getenv("ENV_STATE", "dev")

# Validation thresholds
MIN_ROWS = 1000
MAX_ROW_CHANGE_PERCENT = 10.0

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
    payload = {"text": f"{emoji} {env_prefix}CGDID_2_GeneID Generation: {message}"}

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


def count_rows_in_file(file_path: Path) -> int:
    """Count data rows (excluding header/blank lines) in a gzipped tab file."""
    if not file_path.exists():
        return 0

    opener = gzip.open if file_path.suffix == ".gz" else open
    count = 0
    with opener(file_path, "rt") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or line.startswith("!") or stripped.startswith("CGDID\t"):
                continue
            count += 1
    return count


def validate_output_file(new_file: Path, existing_file: Path | None) -> tuple[bool, str]:
    """Validate the generated file. Returns (is_valid, message)."""
    if not new_file.exists():
        return False, f"Output file does not exist: {new_file}"

    new_count = count_rows_in_file(new_file)

    if new_count < MIN_ROWS:
        return False, f"Too few rows: {new_count} (minimum: {MIN_ROWS})"

    if existing_file and existing_file.exists():
        existing_count = count_rows_in_file(existing_file)
        if existing_count > 0:
            change_pct = abs(new_count - existing_count) / existing_count * 100
            if change_pct > MAX_ROW_CHANGE_PERCENT:
                return False, (
                    f"Row count changed too much: {existing_count} -> {new_count} "
                    f"({change_pct:.1f}% change, max: {MAX_ROW_CHANGE_PERCENT}%)"
                )

    return True, f"Validation passed: {new_count} rows"


def get_geneid_mappings(session) -> dict[str, str]:
    """Return {Entrez Gene ID: CGDID}, one entry per Entrez ID.

    A current Entrez Gene ID is linked to both the assembly-22 A-allele feature
    and its paired orf19.* feature. The A-allele (systematic) CGDID is preferred
    for the download files; ties fall back to the lowest CGDID.
    """
    query = text(f"""
        SELECT d.dbxref_id AS entrez_id, f.dbxref_id AS cgdid, f.feature_name
        FROM {DB_SCHEMA}.dbxref d
        JOIN {DB_SCHEMA}.dbxref_feat df ON d.dbxref_no = df.dbxref_no
        JOIN {DB_SCHEMA}.feature f ON df.feature_no = f.feature_no
        WHERE d.dbxref_type = :dtype AND d.source = :src
    """)

    # best[entrez_id] = (rank, cgdid); rank 0 = assembly-22 A-allele (preferred), 1 = other.
    best: dict[str, tuple[int, str]] = {}
    rows = session.execute(query, {"dtype": GENEID_TYPE, "src": GENEID_SOURCE})
    for entrez_id, cgdid, feature_name in rows:
        if not entrez_id or not cgdid:
            continue
        is_systematic_a = bool(feature_name) and bool(SYSTEMATIC_A_RE.match(feature_name))
        rank = 0 if is_systematic_a else 1
        candidate = (rank, cgdid)
        current = best.get(entrez_id)
        # Lower rank wins; within a rank, lexicographically smaller CGDID wins.
        if current is None or candidate < current:
            best[entrez_id] = candidate

    return {entrez_id: cgdid for entrez_id, (_, cgdid) in best.items()}


def write_geneid_file(mappings: dict[str, str], out_path: Path) -> int:
    """Write the gzipped CGDID_2_GeneID.tab file. Returns data-row count.

    Sorted by (CGDID, Entrez ID) so a gene's multiple Entrez IDs group together.
    """
    rows = sorted(
        ((cgdid, entrez_id) for entrez_id, cgdid in mappings.items()),
        key=lambda r: (r[0], int(r[1]) if r[1].isdigit() else r[1]),
    )
    with gzip.open(out_path, "wt") as f:
        f.write(f"{HEADER}\n")
        for cgdid, entrez_id in rows:
            f.write(f"{cgdid}\t{entrez_id}\n")
    return len(rows)


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


def generate(output_dir: Path) -> bool:
    """Generate CGDID_2_GeneID.tab.gz. Returns True on success."""
    output_dir.mkdir(parents=True, exist_ok=True)
    today_tag = datetime.now().strftime("%Y%m%d")
    temp_dir = Path(tempfile.mkdtemp(prefix="cgdid_geneid_"))

    try:
        with SessionLocal() as session:
            mappings = get_geneid_mappings(session)
            logger.info(
                "%d Entrez Gene IDs mapped to %d distinct CGDIDs",
                len(mappings), len(set(mappings.values())),
            )

            filename = f"{OUTPUT_FILE}.gz"
            temp_file = temp_dir / filename
            final_file = output_dir / filename
            write_geneid_file(mappings, temp_file)

            is_valid, msg = validate_output_file(
                temp_file, final_file if final_file.exists() else None
            )
            if not is_valid:
                logger.error(f"Validation failed: {msg}")
                send_slack_message(f"Validation failed: {msg}", is_error=True)
                return False

            logger.info(msg)
            archive_and_replace(temp_file, final_file, today_tag)
            logger.info(f"Wrote {final_file}")

    except Exception as e:
        logger.exception(f"Error generating CGDID_2_GeneID: {e}")
        send_slack_message(f"Error generating CGDID_2_GeneID: {e}", is_error=True)
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    send_slack_message(f"Generated {OUTPUT_FILE} ({len(mappings)} rows)")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate CGDID_2_GeneID.tab mapping file"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=MAPPINGS_DIR,
        help=f"Output directory (default: {MAPPINGS_DIR})",
    )
    args = parser.parse_args()

    return 0 if generate(args.output_dir) else 1


if __name__ == "__main__":
    sys.exit(main())
