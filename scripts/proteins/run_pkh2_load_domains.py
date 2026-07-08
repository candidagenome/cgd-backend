#!/usr/bin/env python3
"""
Load InterProScan domain/motif data for a single locus (default: PKH2).

This is the targeted counterpart to ``load_domain_data.py`` (which loads a whole
strain). It resolves one locus -- by gene name (e.g. PKH2) or systematic feature
name -- deletes that locus's existing DOMAIN / MOTIF / STRUCTURAL REGION rows,
and loads the domain hits for it from a raw InterProScan TSV.

Pair it with ``run_pkh2_iprscan.py`` (which writes the raw TSV using the
systematic feature_name as each record id). Use it after a feature merge
(e.g. PKH1 / C1_12400C merged into PKH2) to refresh the surviving locus's
protein-domain data.

Note:
    Feed the RAW InterProScan TSV (the output of run_pkh2_iprscan.py /
    submit_iprscan.py), not the reformatted file from parse_iprscan_data.py --
    load_domain_data expects the standard InterProScan column layout.

Usage:
    python run_pkh2_load_domains.py --data results/PKH2_iprscan.tsv \
        --created-by DBUSER --dry-run
    python run_pkh2_load_domains.py --gene PKH2 --strain-abbrev C_albicans_SC5314 \
        --data results/PKH2_iprscan.tsv --created-by DBUSER
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Resolve imports: the generic protein modules live in scripts/untested/proteins/;
# the `cgd` package lives at the repo root.
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "untested" / "proteins"))
sys.path.insert(0, str(REPO_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402
from load_domain_data import (  # noqa: E402
    get_organism,
    load_domain_data,
    setup_logging,
)

load_dotenv()

logger = logging.getLogger(__name__)


def resolve_feature_name(session, gene: str, organism) -> tuple[str, str]:
    """Resolve a gene name or systematic feature name to (feature_name, gene_name).

    Excludes old Assembly-19/21 features (orf19.* / orf21.*) so a gene name
    resolves to the current Assembly-22 feature -- the same rule the locus page
    uses. Raises if the name is still ambiguous.
    """
    rows = session.execute(
        text("""
            SELECT f.feature_no, f.feature_name, f.gene_name
            FROM feature f
            WHERE (UPPER(f.gene_name) = UPPER(:gene)
                   OR UPPER(f.feature_name) = UPPER(:gene))
              AND f.organism_no = :org_no
              AND f.feature_type = 'ORF'
              AND UPPER(f.feature_name) NOT LIKE 'ORF19.%'
              AND UPPER(f.feature_name) NOT LIKE 'ORF21.%'
        """),
        {"gene": gene, "org_no": organism.organism_no},
    ).fetchall()

    if not rows:
        raise ValueError(
            f"No current-assembly ORF feature found for '{gene}' in {organism.organism_abbrev}"
        )
    if len(rows) > 1:
        cands = ", ".join(f"{r[1]} (feature_no={r[0]})" for r in rows)
        raise ValueError(
            f"'{gene}' is ambiguous in {organism.organism_abbrev}: {cands}. "
            "Pass the systematic feature name via --gene."
        )
    return rows[0][1], rows[0][2]


def delete_existing_domains(session, organism, feature_name: str) -> int:
    """Delete the feature's existing DOMAIN/MOTIF/STRUCTURAL REGION protein_detail rows.

    Local replacement for load_domain_data.delete_existing_domain_data, whose
    ``IN :groups`` bind does not work with the installed SQLAlchemy. The group
    names are fixed literals here (no user input), so this is injection-safe.
    """
    result = session.execute(
        text("""
            DELETE FROM protein_detail
            WHERE protein_detail_no IN (
                SELECT pd.protein_detail_no
                FROM protein_detail pd
                JOIN protein_info pi ON pd.protein_info_no = pi.protein_info_no
                JOIN feature f ON pi.feature_no = f.feature_no
                WHERE f.feature_name = :feat_name
                  AND f.organism_no = :org_no
                  AND pd.protein_detail_group IN ('DOMAIN', 'MOTIF', 'STRUCTURAL REGION')
            )
        """),
        {"feat_name": feature_name, "org_no": organism.organism_no},
    )
    return result.rowcount


def main():
    parser = argparse.ArgumentParser(
        description="Load InterProScan domain/motif data for a single locus (default: PKH2)"
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
        "--data",
        type=Path,
        required=True,
        help="Raw InterProScan TSV file (from run_pkh2_iprscan.py)",
    )
    parser.add_argument(
        "--created-by",
        required=True,
        help="Database user for audit",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without modifying the database",
    )
    args = parser.parse_args()

    setup_logging(args.verbose)
    logger.info(f"Started at {datetime.now()}")

    if not args.data.exists():
        logger.error(f"Data file not found: {args.data}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            organism = get_organism(session, args.strain_abbrev)
            logger.info(f"Processing organism: {organism.organism_name}")

            feature_name, gene_name = resolve_feature_name(session, args.gene, organism)
            logger.info(
                f"Resolved '{args.gene}' -> feature_name={feature_name}, "
                f"gene_name={gene_name}"
            )
            # load_domain_data matches the systematic feature_name against the TSV
            # record ids, so scope everything to that single name.
            feature_list = {feature_name}

            deleted = delete_existing_domains(session, organism, feature_name)
            logger.info(f"Deleted {deleted} existing domain/motif row(s) for {feature_name}")

            stats = load_domain_data(
                session, args.data, organism, args.created_by, feature_list
            )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info(f"Locus: {gene_name or feature_name} ({feature_name})")
            logger.info(f"  Processed: {stats['processed']}")
            logger.info(f"  Inserted: {stats['inserted']}")
            logger.info(f"  Skipped (existing): {stats['skipped']}")
            logger.info(f"  Not found: {stats['not_found']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
