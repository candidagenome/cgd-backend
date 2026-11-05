#!/usr/bin/env python3
"""Transfer RNA accumulation phenotypes into MULTI.REGULATION as expression rows.

CGD's "RNA accumulation" phenotype annotations record how a single-gene mutant
changes other genes' expression: the mutated gene is the regulator, the
Reporter experiment property names the target genes. For each organism, this
adds a REGULATION row (evidence Indirect, source "CGD phenotype" on the locus
Regulation tab) for every phenotype annotation, reporter gene and reference
when

  * the mutated gene is a regulator (GO DNA-binding transcription factor,
    PathoYeastract regulator, or admitted by a curator),
  * the qualifier gives a direction (increased / decreased / absent), and
  * the reporter is a CGD gene (reporters that do not resolve are listed on
    the Curate Regulation page's review section).

The effect (Positive / Negative) follows from the mutant type and the
direction. Rows a phenotype no longer calls for (its reporter or qualifier
changed, or the gene stopped being a regulator) are removed; rows already
there keep curators' edits. Rows of a deleted phenotype annotation go with it
(ON DELETE CASCADE). See cgd/api/services/regulation_phenotype.py.

Run daily from cron (scripts/cron/cgd_transfer_phenotype_regulation.sh):

    python scripts/regulation/transfer_phenotype_regulation.py --dry-run
    python scripts/regulation/transfer_phenotype_regulation.py [--organism C_albicans_SC5314] [--email ...]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from sqlalchemy import func  # noqa: E402

from cgd.api.services import regulation_phenotype  # noqa: E402
from cgd.db.engine import SessionLocal  # noqa: E402
from cgd.models.models import Feature, Organism, PhenoAnnotation, Phenotype, Regulation  # noqa: E402

logger = logging.getLogger("transfer_phenotype_regulation")


def organisms(db, abbrev: str | None) -> list[Organism]:
    """Organisms with RNA accumulation phenotypes or phenotype-derived rows."""
    if abbrev:
        organism = db.query(Organism).filter(Organism.organism_abbrev == abbrev).first()
        if organism is None:
            raise SystemExit(f"Unknown organism {abbrev}")
        return [organism]
    with_phenotypes = (
        db.query(Feature.organism_no)
        .join(PhenoAnnotation, PhenoAnnotation.feature_no == Feature.feature_no)
        .join(Phenotype, Phenotype.phenotype_no == PhenoAnnotation.phenotype_no)
        .filter(func.lower(Phenotype.observable) == regulation_phenotype.OBSERVABLE.lower())
    )
    with_rows = (
        db.query(Feature.organism_no)
        .join(Regulation, Regulation.tf_feature_no == Feature.feature_no)
        .filter(Regulation.pheno_annotation_no.isnot(None))
    )
    nos = {r[0] for r in with_phenotypes.distinct()} | {r[0] for r in with_rows.distinct()}
    return (db.query(Organism).filter(Organism.organism_no.in_(nos)).order_by(Organism.organism_order).all()
            if nos else [])


def describe(row: dict) -> str:
    return (f"{row['tf']} -> {row['target']}  PMID {row['pubmed']}  {row['experiment']}  "
            f"({row['association_type']})")


def email(recipients: list[str], changes: dict[str, dict], dry_run: bool) -> None:
    from cgd.utils.notifications import send_email

    added = sum(len(c["added"]) for c in changes.values())
    removed = sum(len(c["removed"]) for c in changes.values())
    lines = [
        "The nightly transfer of RNA accumulation phenotypes into regulation data "
        f"{'would have added' if dry_run else 'added'} {added} and removed {removed} row(s).",
        "",
        "Added rows are shown on the locus Regulation tabs as 'CGD phenotype' evidence and can be edited in "
        "Curate Regulation. To change or drop one, correct the phenotype annotation (its reporter or "
        "qualifier); the next run follows it. Reporters that are not CGD genes are listed in the review "
        "section of Curate Regulation.",
    ]
    for abbrev, change in changes.items():
        for label in ("added", "removed"):
            if change[label]:
                lines += ["", f"{abbrev}: {label} {len(change[label])}"]
                lines += [f"  {describe(row)}" for row in change[label]]
    if not send_email(recipients, f"CGD regulation: {added} added, {removed} removed from phenotypes",
                      "\n".join(lines)):
        logger.error("Email to %s failed", ", ".join(recipients))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--organism", help="one organism_abbrev (default: every organism with RNA accumulation "
                        "phenotypes)")
    parser.add_argument("--dry-run", action="store_true", help="report what would change; change nothing")
    parser.add_argument("--max-delete-fraction", type=float, default=0.2,
                        help="refuse an organism whose removals exceed this fraction of its phenotype-derived "
                             "rows (when more than 20); default 0.2")
    parser.add_argument("--email", default="", help="comma-separated recipients for a summary when rows change")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    changes: dict[str, dict] = {}
    failed = []
    with SessionLocal() as db:
        for organism in organisms(db, args.organism):
            try:
                result = regulation_phenotype.transfer_phenotypes(
                    db, organism.organism_no, organism.organism_abbrev, dry_run=args.dry_run,
                    max_delete_fraction=args.max_delete_fraction)
            except Exception as exc:  # one organism's failure must not stop the others
                db.rollback()
                logger.error("%s: %s", organism.organism_abbrev, exc)
                failed.append(organism.organism_abbrev)
                continue
            logger.info("%s: %s %d, removed %d, kept %d", organism.organism_abbrev,
                        "would add" if args.dry_run else "added", len(result["added"]), len(result["removed"]),
                        result["kept"])
            for label in ("added", "removed"):
                for row in result[label]:
                    logger.info("  %s %s", label, describe(row))
            if result["added"] or result["removed"]:
                changes[organism.organism_abbrev] = result

    recipients = [r.strip() for r in args.email.split(",") if r.strip()]
    if changes and recipients:
        email(recipients, changes, args.dry_run)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
