#!/usr/bin/env python3
"""
Run InterProScan for a single locus (default: PKH2).

Pulls the current protein sequence for one locus directly from the database
(resolved by gene name, e.g. PKH2, or systematic feature name), writes it to a
FASTA file, and reuses the shared ``submit_iprscan`` module to run InterProScan
and write a TSV result.

Use it after a feature merge (e.g. PKH1 / C1_12400C merged into PKH2) to
regenerate domain/motif hits for the surviving locus. The TSV output can then be
loaded with the domain-loading scripts (e.g. load_domain_data.py /
parse_iprscan_data.py).

Usage:
    python run_pkh2_iprscan.py --created-by DBUSER -o results/PKH2_iprscan.tsv
    python run_pkh2_iprscan.py --gene PKH2 --strain-abbrev SC5314 \
        -o results/PKH2_iprscan.tsv --iprscan-path /opt/interproscan/interproscan.sh
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from dotenv import load_dotenv
from sqlalchemy import text

# Resolve imports: the generic protein modules live in scripts/untested/proteins/;
# the `cgd` package lives at the repo root.
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "untested" / "proteins"))
sys.path.insert(0, str(REPO_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402
from submit_iprscan import (  # noqa: E402
    APPS_DOMAIN,
    APPS_SIGNAL,
    CHUNK_SIZE,
    process_in_chunks,
    setup_logging,
)

load_dotenv()

logger = logging.getLogger(__name__)

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROTEIN_SEQ_TYPES = ("protein",)


def resolve_feature(session, gene: str, strain_abbrev: str):
    """Resolve a gene name or systematic feature name to (feature_no, feature_name, gene_name)."""
    row = session.execute(
        text(f"""
            SELECT f.feature_no, f.feature_name, f.gene_name
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE (UPPER(f.gene_name) = UPPER(:gene)
                   OR UPPER(f.feature_name) = UPPER(:gene))
              AND o.organism_abbrev = :strain
              AND f.feature_type = 'ORF'
        """),
        {"gene": gene, "strain": strain_abbrev},
    ).fetchone()

    if not row:
        raise ValueError(
            f"No ORF feature found for '{gene}' in strain {strain_abbrev}"
        )
    return row[0], row[1], row[2]


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
        description="Run InterProScan for a single locus (default: PKH2)"
    )
    parser.add_argument(
        "--gene",
        default="PKH2",
        help="Gene name or systematic feature name (default: PKH2)",
    )
    parser.add_argument(
        "--strain-abbrev",
        default="SC5314",
        help="Strain abbreviation (default: SC5314)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        required=True,
        help="Output TSV file",
    )
    parser.add_argument(
        "--iprscan-path",
        default="interproscan.sh",
        help="Path to interproscan.sh",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE,
        help=f"Sequences per chunk (default: {CHUNK_SIZE})",
    )
    parser.add_argument(
        "--domain-only",
        action="store_true",
        help="Only run domain applications (no SignalP/TMHMM)",
    )
    parser.add_argument(
        "--signal-only",
        action="store_true",
        help="Only run signal applications (SignalP/TMHMM)",
    )
    parser.add_argument(
        "--applications",
        nargs="+",
        help="Specific applications to run",
    )
    parser.add_argument("--log-file", type=Path, help="Log file path")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    setup_logging(args.verbose, args.log_file)
    logger.info(f"Started at {datetime.now()}")

    # Determine applications (mirrors submit_iprscan.main).
    if args.applications:
        applications = args.applications
    elif args.domain_only:
        applications = APPS_DOMAIN
    elif args.signal_only:
        applications = APPS_SIGNAL
    else:
        applications = APPS_DOMAIN + APPS_SIGNAL
    logger.info(f"Applications: {', '.join(applications)}")

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

    # InterProScan rejects a trailing stop; use the systematic feature_name as
    # the record id so downstream loaders can map hits back to the feature.
    record = SeqRecord(
        Seq(protein_seq.rstrip("*")),
        id=feature_name,
        description=gene_name or "",
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    stats = process_in_chunks(
        [record],
        args.output,
        args.iprscan_path,
        args.chunk_size,
        applications,
    )

    logger.info("=" * 50)
    logger.info("Summary:")
    logger.info(f"  Locus: {gene_name or feature_name} ({feature_name})")
    logger.info(f"  Total sequences: {stats['total']}")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info(f"  Output: {args.output}")
    logger.info("=" * 50)
    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
