#!/usr/bin/env python3
"""
Precompute per-organism expression matrices for large matrix downloads.

The expression matrix download (/api/expression/matrix/download, used by the
Gene List Workbench) reads every gene's bigWig signal live, ~85 ms per gene,
so live downloads are capped at 200 genes. This script computes every gene's
fold changes once, with the same service code the live path uses, and saves
a genes x conditions matrix per organism:

    /data/cache/expression/expression_matrix_<HTS key>.npz

The API serves downloads of any size from it while its fingerprint (study
configuration, library sizes, and the size/mtime of every configured bigWig)
still matches; otherwise it falls back to live reads (200-gene cap). Rerun
after adding a study or reprocessing a dataset — `--status` shows which
matrices are current.

Covered genes: ORFs, pseudogenes and RNA genes; for C. albicans the current
Assembly 22 records (orf19/orf21 and _B alleles with an _A twin are skipped;
the download reads any such gene live).

Usage:
    python scripts/expression/build_expression_matrix_cache.py               # all organisms
    python scripts/expression/build_expression_matrix_cache.py --organism C_auris_B8441
    python scripts/expression/build_expression_matrix_cache.py --status
    python scripts/expression/build_expression_matrix_cache.py --organism C_auris_B8441 \
        --limit 300 --output-dir /tmp/matrix_test                              # quick test
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np  # noqa: E402

from cgd.db.engine import SessionLocal  # noqa: E402
from cgd.models.models import Feature  # noqa: E402
from cgd.api.services import expression_service as es  # noqa: E402

GENE_TYPES = (
    "ORF", "pseudogene", "blocked_reading_frame",
    "tRNA", "rRNA", "snoRNA", "snRNA", "ncRNA",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("build_expression_matrix_cache")
# Per-gene service warnings (e.g. "Multiple controls found") would repeat
# thousands of times
logging.getLogger(es.__name__).setLevel(logging.ERROR)


def target_features(db, hts_key):
    organism_no = es._get_organism_no_from_key(db, hts_key)
    features = (
        db.query(Feature)
        .filter(Feature.organism_no == organism_no, Feature.feature_type.in_(GENE_TYPES))
        .all()
    )
    names = {f.feature_name for f in features}
    return sorted(
        (
            f for f in features
            if not f.feature_name.upper().startswith(("ORF19.", "ORF21."))
            # _B alleles with an _A twin; B-only genes (e.g. MTLalpha) stay
            and not (f.feature_name.endswith("_B") and f.feature_name[:-2] + "_A" in names)
        ),
        key=lambda f: f.feature_name,
    )


def build(db, hts_key, limit=None):
    organism = es.HTS_KEY_TO_ORGANISM[hts_key]
    fingerprint = es.expression_data_fingerprint(hts_key)
    features = target_features(db, hts_key)
    if limit:
        features = features[:limit]
    logger.info("%s: computing %d genes", organism, len(features))

    columns = {}   # (study_id, condition_id) -> (study_id, condition_id, label, category)
    rows = []      # (feature_no, {(study_id, condition_id): fold_change})
    start = time.time()
    with es._BigWigPool() as pool:
        for i, feature in enumerate(features, 1):
            data = es._get_expression_for_organism(db, feature, organism, pool=pool)
            if data:
                fcs = {}
                for study in data.studies:
                    for condition in study.conditions:
                        key = (study.study_id, condition.condition_id)
                        fcs[key] = condition.fold_change
                        columns.setdefault(key, (study.study_id, condition.condition_id,
                                                 condition.label, study.category))
                rows.append((feature.feature_no, fcs))
            if i % 1000 == 0:
                logger.info("%s: %d/%d genes (%.0f s)", organism, i, len(features), time.time() - start)

    if es.expression_data_fingerprint(hts_key) != fingerprint:
        logger.error("%s: expression data changed during the build; not saving — rerun", organism)
        return False

    column_list = sorted(columns.values(), key=lambda c: (c[0], c[1]))
    column_index = {(c[0], c[1]): j for j, c in enumerate(column_list)}
    matrix = np.full((len(rows), len(column_list)), np.nan)
    for i, (_, fcs) in enumerate(rows):
        for key, fc in fcs.items():
            matrix[i, column_index[key]] = fc

    meta = {
        "version": es.MATRIX_CACHE_VERSION,
        "organism": organism,
        "hts_key": hts_key,
        "fingerprint": fingerprint,
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "genes_considered": len(features),
        "columns": column_list,
    }
    final = es.matrix_cache_path(hts_key)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(f".{final.stem}.tmp.npz")
    np.savez_compressed(
        tmp,
        fold_changes=matrix,
        feature_nos=np.array([f for f, _ in rows], dtype=np.int64),
        meta=np.array(json.dumps(meta)),
    )
    os.replace(tmp, final)   # atomic: the API never sees a partial file
    logger.info("%s: saved %s — %d genes with data x %d conditions (%.0f s, %.1f MB)",
                organism, final, len(rows), len(column_list), time.time() - start,
                final.stat().st_size / 1e6)
    return True


def status():
    for hts_key, organism in es.HTS_KEY_TO_ORGANISM.items():
        path = es.matrix_cache_path(hts_key)
        if not path.exists():
            print(f"{organism}: MISSING ({path})")
            continue
        with np.load(path, allow_pickle=False) as data:
            meta = json.loads(str(data["meta"]))
            shape = data["fold_changes"].shape
        current = meta["fingerprint"] == es.expression_data_fingerprint(hts_key)
        print(f"{organism}: {'CURRENT' if current else 'STALE — rebuild'}; built {meta['built_at']}; "
              f"{shape[0]} genes x {shape[1]} conditions")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--organism", choices=sorted(es.HTS_KEY_TO_ORGANISM),
                        help="HTS key of one organism (default: all)")
    parser.add_argument("--limit", type=int, help="only the first N genes (testing; needs --output-dir)")
    parser.add_argument("--output-dir", help=f"write here instead of {es.EXPRESSION_CACHE_DIR}")
    parser.add_argument("--status", action="store_true", help="report which matrices are current")
    args = parser.parse_args()
    if args.limit and not args.output_dir:
        # A partial matrix in the live cache directory would be served
        parser.error("--limit needs --output-dir")
    if args.output_dir:
        es.EXPRESSION_CACHE_DIR = Path(args.output_dir)

    if args.status:
        status()
        return
    keys = [args.organism] if args.organism else sorted(es.HTS_KEY_TO_ORGANISM)
    failed = []
    with SessionLocal() as db:
        for hts_key in keys:
            if not es.EXPRESSION_STUDIES.get(hts_key):
                logger.info("%s: no expression studies configured; skipped", hts_key)
                continue
            if not build(db, hts_key, limit=args.limit):
                failed.append(hts_key)
    if failed:
        sys.exit(f"failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
