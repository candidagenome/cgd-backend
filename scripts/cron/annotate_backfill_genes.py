#!/usr/bin/env python3
"""
Annotate a backfill review TSV with the CGD genes each paper mentions.

Companion to backfill_ref_temp.py: given its --review-tsv output, fetch each
candidate's Medline record and match the title+abstract text against all CGD
gene names and systematic names (case variants: NAME, Name, and the Namep /
NAMEp protein forms common in Candida literature). Adds n_genes and
genes_mentioned columns so curators can separate gene-bearing papers from
the clinical/epidemiology residue.

2026-09 backfill result for context: only 112 of 5,326 candidates mentioned
any CGD gene — expected, because the windowless per-gene reference search
never had the pdat-window bug, so gene-naming papers were being caught all
along; the species-sweep backlog is by construction mostly gene-free.

Usage:
    python annotate_backfill_genes.py --review backfill_review.tsv \
        --out backfill_review_genes.tsv
"""
from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sqlalchemy import text  # noqa: E402

from load_ref_temp import RefTempLoader, DB_SCHEMA  # noqa: E402
from cgd.db.engine import SessionLocal  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.\-]{2,}")
MAX_SHOWN = 25


def build_variant_map(session) -> dict[str, str]:
    """Every acceptable text token -> canonical CGD name."""
    rows = session.execute(text(f"""
        SELECT f.gene_name, f.feature_name FROM {DB_SCHEMA}.feature f
        WHERE f.feature_type IN ('ORF','tRNA','rRNA','snoRNA','snRNA','ncRNA',
                                 'retrotransposon','DNA_transposon')
    """)).fetchall()
    variants: dict[str, str] = {}

    def add(name: str) -> None:
        if not name or len(name) < 3:
            return
        cap = name.capitalize()
        for v in (name, cap, cap + "p", name + "p"):
            variants.setdefault(v, name)

    for gene_name, feature_name in rows:
        add(gene_name)
        if feature_name:
            variants.setdefault(feature_name, feature_name)
    return variants


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--review", required=True,
                    help="review TSV from backfill_ref_temp.py --review-tsv")
    ap.add_argument("--out", required=True, help="annotated TSV to write")
    args = ap.parse_args()

    with open(args.review, newline="") as fh:
        reader = csv.reader(fh, delimiter="\t")
        header = next(reader)
        rows = list(reader)
    pmid_idx = header.index("pmid")
    insert_at = header.index("LOAD") if "LOAD" in header else len(header)

    with SessionLocal() as session:
        variants = build_variant_map(session)
        logger.info("name variants: %d", len(variants))

        loader = RefTempLoader(session, "gene-annotation pass")
        records = loader.fetch_records([int(r[pmid_idx]) for r in rows])
        logger.info("fetched %d Medline records", len(records))

    n_with = 0
    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(header[:insert_at] + ["n_genes", "genes_mentioned"]
                   + header[insert_at:])
        for r in rows:
            rec = records.get(int(r[pmid_idx]), {})
            txt = f"{rec.get('TI', '')} {rec.get('AB', '')}"
            hits = sorted({variants[t] for t in set(TOKEN_RE.findall(txt))
                           if t in variants})
            n_with += bool(hits)
            shown = ",".join(hits[:MAX_SHOWN]) + (
                f" (+{len(hits) - MAX_SHOWN} more)" if len(hits) > MAX_SHOWN else "")
            w.writerow(r[:insert_at] + [len(hits), shown] + r[insert_at:])

    logger.info("papers mentioning >=1 CGD gene: %d / %d -> %s",
                n_with, len(rows), args.out)
    return 0


if __name__ == "__main__":
    main()
