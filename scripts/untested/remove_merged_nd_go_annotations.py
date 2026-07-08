#!/usr/bin/env python3
"""
Remove ND ("No Biological Data Available") GO annotations from a locus.

When two features are merged (e.g. PKH1 / C1_12400C merged into PKH2), the
retired feature's GO annotations are transferred onto the surviving feature.
An ND annotation is only a placeholder meaning "no data" for an aspect; once the
surviving locus carries real annotations, the inherited ND rows are spurious and
should be removed so they stop showing on the Gene Ontology tab.

This script resolves the surviving locus, lists its ND (go_evidence = 'ND') GO
annotations, and deletes them. The go_ref / go_qualifier child rows are removed
by the ON DELETE CASCADE foreign keys.

It is DRY-RUN by default -- it only reports what it would delete. Pass --commit
to actually delete.

Usage:
    # Preview what would be removed (safe, no changes):
    python remove_merged_nd_go_annotations.py --gene PKH2

    # Actually delete, noting the merge provenance for the log:
    python remove_merged_nd_go_annotations.py --gene PKH2 \
        --merged-from C1_12400C --commit
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

# Resolve imports: repo root for `cgd`.
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent.parent))

from cgd.db.engine import SessionLocal  # noqa: E402

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ND_EVIDENCE = "ND"


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
        """),
        {"gene": gene, "strain": strain_abbrev},
    ).fetchone()

    if not row:
        raise ValueError(
            f"No feature found for '{gene}' in strain {strain_abbrev}"
        )
    return row[0], row[1], row[2]


def find_nd_annotations(session, feature_no: int):
    """Return ND GO annotations for a feature as a list of row mappings."""
    rows = session.execute(
        text(f"""
            SELECT ga.go_annotation_no, g.goid, g.go_term, g.go_aspect,
                   ga.annotation_type, ga.source
            FROM {DB_SCHEMA}.go_annotation ga
            JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
            WHERE ga.feature_no = :fno
              AND ga.go_evidence = :ev
            ORDER BY g.go_aspect, g.goid
        """),
        {"fno": feature_no, "ev": ND_EVIDENCE},
    ).fetchall()
    return rows


def delete_annotations(session, annotation_nos: list[int]) -> None:
    """Delete GO annotations by primary key (go_ref/go_qualifier cascade in the DB)."""
    for ann_no in annotation_nos:
        session.execute(
            text(f"""
                DELETE FROM {DB_SCHEMA}.go_annotation
                WHERE go_annotation_no = :ann_no
            """),
            {"ann_no": ann_no},
        )


def main():
    parser = argparse.ArgumentParser(
        description="Remove ND (No Biological Data) GO annotations from a locus"
    )
    parser.add_argument(
        "--gene",
        default="PKH2",
        help="Surviving gene name or systematic feature name (default: PKH2)",
    )
    parser.add_argument(
        "--strain-abbrev",
        default="SC5314",
        help="Strain abbreviation (default: SC5314)",
    )
    parser.add_argument(
        "--merged-from",
        help="Systematic name of the retired/merged feature, for the log (e.g. C1_12400C)",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually delete. Without this flag the script only reports (dry run).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.info(f"Started at {datetime.now()}")
    if args.merged_from:
        logger.info(f"Cleaning ND annotations merged from {args.merged_from}")
    if not args.commit:
        logger.info("DRY RUN - no changes will be made (pass --commit to delete)")

    with SessionLocal() as session:
        feature_no, feature_name, gene_name = resolve_feature(
            session, args.gene, args.strain_abbrev
        )
        logger.info(
            f"Resolved '{args.gene}' -> feature_no={feature_no}, "
            f"feature_name={feature_name}, gene_name={gene_name}"
        )

        nd_rows = find_nd_annotations(session, feature_no)
        if not nd_rows:
            logger.info("No ND GO annotations found; nothing to do.")
            logger.info(f"Completed at {datetime.now()}")
            return

        logger.info(f"Found {len(nd_rows)} ND GO annotation(s):")
        for row in nd_rows:
            ann_no, goid, go_term, go_aspect, ann_type, source = row
            logger.info(
                f"  [{ann_no}] GO:{int(goid):07d} ({go_aspect}) "
                f"'{go_term}' | type={ann_type} | source={source}"
            )

        if not args.commit:
            logger.info(
                f"DRY RUN - would delete {len(nd_rows)} annotation(s). "
                "Re-run with --commit to apply."
            )
            logger.info(f"Completed at {datetime.now()}")
            return

        annotation_nos = [row[0] for row in nd_rows]
        delete_annotations(session, annotation_nos)
        session.commit()
        logger.info(f"Deleted {len(annotation_nos)} ND GO annotation(s) and committed.")

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
