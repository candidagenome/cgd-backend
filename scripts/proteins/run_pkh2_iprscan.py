#!/usr/bin/env python3
"""
Run InterProScan for a single locus (default: PKH2) via the EBI web service.

Pulls the current protein sequence for one locus directly from the database
(resolved by gene name, e.g. PKH2, or systematic feature name), submits it to
the EBI InterProScan REST API, waits for the job to finish, and writes the
result (TSV by default). This reuses the same EBI submission code already used
for C. tropicalis proteins (scripts/C_tropicalis/iprscan_submit.py), so no local
InterProScan install is needed.

Use it after a feature merge (e.g. PKH1 / C1_12400C merged into PKH2) to
regenerate domain/motif hits for the surviving locus. The TSV output can then be
loaded with run_pkh2_load_domains.py.

Usage:
    python run_pkh2_iprscan.py --email you@example.org -o results/PKH2_iprscan.tsv
    python run_pkh2_iprscan.py --gene PKH2 --strain-abbrev C_albicans_SC5314 \
        --email you@example.org -o results/PKH2_iprscan.tsv
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Resolve imports: the `cgd` package lives at the repo root; the EBI InterProScan
# submission helpers live in scripts/C_tropicalis/.
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "C_tropicalis"))
sys.path.insert(0, str(REPO_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402
import iprscan_submit as ebi  # noqa: E402

load_dotenv()

logger = logging.getLogger(__name__)

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROTEIN_SEQ_TYPES = ("protein",)


def resolve_feature(session, gene: str, strain_abbrev: str):
    """Resolve a gene name or systematic feature name to (feature_no, feature_name, gene_name).

    Excludes old Assembly-19/21 features (orf19.* / orf21.*) so a gene name
    resolves to the current Assembly-22 feature -- the same rule the locus page
    uses. Raises if the name is still ambiguous.
    """
    rows = session.execute(
        text(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE (UPPER(f.gene_name) = UPPER(:gene)
                   OR UPPER(f.feature_name) = UPPER(:gene))
              AND o.organism_abbrev = :strain
              AND f.feature_type = 'ORF'
              AND UPPER(f.feature_name) NOT LIKE 'ORF19.%'
              AND UPPER(f.feature_name) NOT LIKE 'ORF21.%'
        """),
        {"gene": gene, "strain": strain_abbrev},
    ).fetchall()

    if not rows:
        raise ValueError(
            f"No current-assembly ORF feature found for '{gene}' in {strain_abbrev}"
        )
    if len(rows) > 1:
        cands = ", ".join(f"{r[1]} (feature_no={r[0]})" for r in rows)
        raise ValueError(
            f"'{gene}' is ambiguous in {strain_abbrev}: {cands}. "
            "Pass the systematic feature name via --gene."
        )
    return rows[0][0], rows[0][1], rows[0][2]


def fetch_current_protein(session, feature_no: int) -> str | None:
    """Fetch the most recent current protein sequence for a feature."""
    placeholders = ", ".join(f":t{i}" for i in range(len(PROTEIN_SEQ_TYPES)))
    params = {f"t{i}": t for i, t in enumerate(PROTEIN_SEQ_TYPES)}
    params["fno"] = feature_no

    row = session.execute(
        text(f"""
            SELECT residues
            FROM {DB_SCHEMA}.seq
            WHERE feature_no = :fno
              AND is_seq_current = 'Y'
              AND LOWER(seq_type) IN ({placeholders})
            ORDER BY seq_version DESC
        """),
        params,
    ).fetchone()
    return row[0] if row else None


def main():
    parser = argparse.ArgumentParser(
        description="Run InterProScan for a single locus (default: PKH2) via the EBI web service"
    )
    parser.add_argument(
        "--gene",
        default="PKH2",
        help="Gene name or systematic feature name (default: PKH2)",
    )
    parser.add_argument(
        "--strain-abbrev",
        default="C_albicans_SC5314",
        help="Organism abbreviation (default: C_albicans_SC5314)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Output result file",
    )
    parser.add_argument(
        "-e", "--email",
        required=True,
        help="Your email address (required by the EBI service)",
    )
    parser.add_argument(
        "-f", "--format",
        default="tsv",
        choices=["tsv", "json", "xml", "gff"],
        help="Result format (default: tsv)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between status checks (default: 30)",
    )
    parser.add_argument(
        "--max-wait",
        type=int,
        default=3600,
        help="Max seconds to wait for the job (default: 3600)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger.info(f"Started at {datetime.now()}")

    with SessionLocal() as session:
        feature_no, feature_name, gene_name = resolve_feature(
            session, args.gene, args.strain_abbrev
        )
        logger.info(
            f"Resolved '{args.gene}' -> feature_no={feature_no}, "
            f"feature_name={feature_name}, gene_name={gene_name}"
        )

        protein_seq = fetch_current_protein(session, feature_no)
        if not protein_seq:
            raise ValueError(f"No current protein sequence found for {feature_name}")

    # EBI rejects a trailing stop; use the systematic feature_name as the job
    # title / record id so downstream loaders can map hits back to the feature.
    protein_seq = protein_seq.rstrip("*")

    logger.info(f"Submitting {feature_name} ({len(protein_seq)} aa) to EBI InterProScan")
    job_id = ebi.submit_job(protein_seq, feature_name, args.email)
    if not job_id:
        logger.error("Submission failed; no job id returned")
        sys.exit(1)

    logger.info(f"Job {job_id} submitted; polling every {args.poll_interval}s")
    status = ebi.wait_for_job(job_id, max_wait=args.max_wait, poll_interval=args.poll_interval)
    if status != "FINISHED":
        logger.error(f"Job {job_id} did not finish cleanly: {status}")
        sys.exit(1)

    result = ebi.get_result(job_id, args.format)
    if result is None:
        logger.error(f"Could not retrieve {args.format} result for job {job_id}")
        sys.exit(1)

    # For a single-sequence job, force the TSV id column to the systematic
    # feature_name so run_pkh2_load_domains.py can map the hits to the feature
    # (EBI populates column 0 with its own query id).
    if args.format == "tsv":
        fixed = []
        for line in result.splitlines():
            if not line.strip():
                continue
            parts = line.split("\t")
            parts[0] = feature_name
            fixed.append("\t".join(parts))
        result = "\n".join(fixed) + ("\n" if fixed else "")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as fh:
        fh.write(result)

    line_count = result.count("\n")
    logger.info("=" * 50)
    logger.info("Summary:")
    logger.info(f"  Locus: {gene_name or feature_name} ({feature_name})")
    logger.info(f"  Job id: {job_id}")
    logger.info(f"  Result rows: {line_count}")
    logger.info(f"  Output: {args.output}")
    logger.info("=" * 50)
    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
