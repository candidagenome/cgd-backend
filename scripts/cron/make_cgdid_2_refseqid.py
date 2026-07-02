#!/usr/bin/env python3
from __future__ import annotations

"""
Generate CGDID_2_RefSeqID.tab for the External_id_mappings download area.

Produces a two-column mapping of CGD feature IDs to NCBI Entrez RefSeq
nucleotide (mRNA) accessions, matching the file published at
https://www.candidagenome.org/download/External_id_mappings/CGDID_2_RefSeqID.tab.gz

Output (gzipped, matching the download site):
  - CGDID_2_RefSeqID.tab.gz

Format (tab-separated, with a header line):
  CGDID<TAB>RefSeq ID
  CAL0000502<TAB>XM_705076.1
  ...

Derivation (per the External_id_mappings README)
------------------------------------------------
The RefSeq nucleotide accessions are NOT stored against features in CGD (the
'Entrez RefSeq ID' dbxrefs are orphaned), so they are derived from NCBI:

  1. Build the CGDID -> Entrez Gene ID mapping from the dbxref table (identical
     to make_cgdid_2_geneid.py: orf19.* feature preferred, one CGDID per Gene ID).
  2. Read NCBI's gene2refseq (ftp.ncbi.nlm.nih.gov/gene/DATA/gene2refseq.gz),
     which maps Entrez Gene ID -> RefSeq RNA_nucleotide_accession.version.
  3. Join on Gene ID to emit CGDID -> RefSeq nucleotide accession.

Because NCBI RefSeq drifts over time (accession versions bump, a few genes are
discontinued), the generated file reflects *current* NCBI data and will not be
byte-identical to the legacy 2009 file.

gene2refseq is ~2.3 GB. By default it is streamed and filtered on the fly (no
full copy stored). Pass --gene2refseq-file to reuse a local cached copy.

Validation before replacing the published file:
  - writes to a temp directory first
  - requires a minimum row count
  - rejects a >10% swing vs. the existing file
  - Slack notification on success / failure

Structured after make_gpi.py / make_gp2protein.py / make_cgdid_2_geneid.py.

Usage:
    python make_cgdid_2_refseqid.py
    python make_cgdid_2_refseqid.py --gene2refseq-file /data/HTS/gene2refseq.gz
    python make_cgdid_2_refseqid.py --output-dir /tmp/mappings

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
import shutil
import sys
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Project root directory (cgd-backend/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load environment variables BEFORE importing cgd modules (settings validation)
load_dotenv(PROJECT_ROOT / ".env")

sys.path.insert(0, str(PROJECT_ROOT))
# Allow importing the sibling generator regardless of package layout.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cgd.db.engine import SessionLocal
# Reuse the exact CGDID <-> Entrez Gene ID logic so the two files stay parallel.
from make_cgdid_2_geneid import get_geneid_mappings

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", str(PROJECT_ROOT / "data")))

# Output
MAPPINGS_DIR = DOWNLOAD_DIR / "External_id_mappings"
ARCHIVE_DIR = MAPPINGS_DIR / "archive"
OUTPUT_FILE = "CGDID_2_RefSeqID.tab"
HEADER = "CGDID\tRefSeq ID"

# NCBI gene2refseq
GENE2REFSEQ_URL = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2refseq.gz"
# Columns (0-based) in gene2refseq: 1=GeneID, 3=RNA_nucleotide_accession.version
COL_GENEID = 1
COL_RNA_ACC = 3

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
    payload = {"text": f"{emoji} {env_prefix}CGDID_2_RefSeqID Generation: {message}"}

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


def _open_gene2refseq(source: str | None):
    """Return a text stream over gene2refseq, from a local file or the NCBI URL."""
    if source:
        logger.info("Reading gene2refseq from local file: %s", source)
        return gzip.open(source, "rt")

    logger.info("Streaming gene2refseq from %s (~2.3 GB)", GENE2REFSEQ_URL)
    req = urllib.request.Request(
        GENE2REFSEQ_URL, headers={"User-Agent": "CGD-gp2refseq/1.0"}
    )
    response = urllib.request.urlopen(req, timeout=600)
    return gzip.open(response, "rt")


def get_refseq_for_genes(wanted: set[str], source: str | None) -> dict[str, set[str]]:
    """Map Entrez Gene ID -> set of RefSeq RNA accessions, restricted to `wanted`.

    Streams gene2refseq; keeps only rows whose GeneID is wanted and that have an
    RNA nucleotide accession (column 4 != '-').
    """
    rna_by_gene: dict[str, set[str]] = {}
    scanned = 0
    with _open_gene2refseq(source) as stream:
        for line in stream:
            if line.startswith("#"):
                continue
            scanned += 1
            cols = line.rstrip("\n").split("\t")
            if len(cols) <= COL_RNA_ACC:
                continue
            geneid = cols[COL_GENEID]
            if geneid not in wanted:
                continue
            rna_acc = cols[COL_RNA_ACC]
            if rna_acc and rna_acc != "-":
                rna_by_gene.setdefault(geneid, set()).add(rna_acc)
            if scanned % 5_000_000 == 0:
                logger.info(
                    "  scanned %d gene2refseq rows, matched %d genes so far",
                    scanned, len(rna_by_gene),
                )
    logger.info(
        "Scanned %d gene2refseq rows; found RefSeq RNA for %d of %d Gene IDs",
        scanned, len(rna_by_gene), len(wanted),
    )
    return rna_by_gene


def write_refseq_file(rows: list[tuple[str, str]], out_path: Path) -> int:
    """Write the gzipped CGDID_2_RefSeqID.tab file. Returns data-row count.

    `rows` is a list of (CGDID, RefSeq accession); sorted by (CGDID, RefSeq).
    """
    rows = sorted(set(rows))
    with gzip.open(out_path, "wt") as f:
        f.write(f"{HEADER}\n")
        for cgdid, refseq in rows:
            f.write(f"{cgdid}\t{refseq}\n")
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


def generate(output_dir: Path, gene2refseq_file: str | None) -> bool:
    """Generate CGDID_2_RefSeqID.tab.gz. Returns True on success."""
    output_dir.mkdir(parents=True, exist_ok=True)
    today_tag = datetime.now().strftime("%Y%m%d")
    temp_dir = Path(tempfile.mkdtemp(prefix="cgdid_refseq_"))

    try:
        with SessionLocal() as session:
            geneid_to_cgdid = get_geneid_mappings(session)
        logger.info(
            "%d Entrez Gene IDs -> %d distinct CGDIDs",
            len(geneid_to_cgdid), len(set(geneid_to_cgdid.values())),
        )

        rna_by_gene = get_refseq_for_genes(set(geneid_to_cgdid), gene2refseq_file)

        rows: list[tuple[str, str]] = []
        for geneid, cgdid in geneid_to_cgdid.items():
            for refseq in rna_by_gene.get(geneid, ()):  # skip genes with no RNA acc
                rows.append((cgdid, refseq))

        genes_without_refseq = len(geneid_to_cgdid) - len(
            [g for g in geneid_to_cgdid if g in rna_by_gene]
        )
        logger.info(
            "Emitting %d rows; %d Gene IDs had no RefSeq RNA accession",
            len(set(rows)), genes_without_refseq,
        )

        filename = f"{OUTPUT_FILE}.gz"
        temp_file = temp_dir / filename
        final_file = output_dir / filename
        write_refseq_file(rows, temp_file)

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
        logger.exception(f"Error generating CGDID_2_RefSeqID: {e}")
        send_slack_message(f"Error generating CGDID_2_RefSeqID: {e}", is_error=True)
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    send_slack_message(f"Generated {OUTPUT_FILE} ({len(set(rows))} rows)")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate CGDID_2_RefSeqID.tab mapping file"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=MAPPINGS_DIR,
        help=f"Output directory (default: {MAPPINGS_DIR})",
    )
    parser.add_argument(
        "--gene2refseq-file",
        help="Local gene2refseq.gz to use instead of streaming from NCBI",
    )
    args = parser.parse_args()

    return 0 if generate(args.output_dir, args.gene2refseq_file) else 1


if __name__ == "__main__":
    sys.exit(main())
