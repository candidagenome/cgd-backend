#!/usr/bin/env python3
"""
Add BioGRID interaction experiment types to the experiment_type CV (cv_term).

The INTERACTION_BIUR trigger validates experiment_type against cv_term. These
BioGRID physical/genetic types were missing from CGD's experiment_type CV, so
curators could not use them in the Curate Interactions tool. Adding them here
makes them valid; the curation service already offers any of its known types
that are present in the CV, so no code change is needed.

Idempotent: safe to run multiple times. This is a DB write that does NOT travel
through git, so it MUST be run on EVERY database (dev and prod).

Usage:
    python scripts/add_interaction_experiment_type_cv_terms.py [--dry-run]
"""
import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import func, text  # noqa: E402

from cgd.db.engine import SessionLocal  # noqa: E402
from cgd.models.models import Cv, CvTerm  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
log = logging.getLogger(__name__)

CV_NAME = "experiment_type"

# BioGRID interaction experiment types missing from CGD's experiment_type CV.
TERMS = [
    "Affinity Capture-Luminescence",  # physical
    "Affinity Capture-RNA",           # physical
    "Co-fractionation",               # physical
    "Proximity Label-MS",             # physical
    "Dosage Lethality",               # genetic
]


def _free_cv_term_no(db) -> int:
    """Get a cv_term_no not already in use.

    CVTERM_BIUR assigns cv_term_no from cv_term_seq only when NULL; on
    data-loaded environments that sequence can lag max(cv_term_no), so pull
    nextval until a free value is found (which also re-syncs the sequence).
    """
    for _ in range(100000):
        candidate = int(db.execute(text("SELECT MULTI.cv_term_seq.NEXTVAL FROM dual")).scalar())
        in_use = db.execute(
            text("SELECT 1 FROM MULTI.cv_term WHERE cv_term_no = :n"), {"n": candidate}
        ).first()
        if not in_use:
            return candidate
    raise RuntimeError("Could not obtain a free cv_term_no from cv_term_seq")


def main(dry_run: bool = False) -> int:
    db = SessionLocal()
    try:
        cv = (
            db.query(Cv)
            .filter(func.lower(Cv.cv_name) == CV_NAME.lower())
            .first()
        )
        if not cv:
            log.error("CV %r not found", CV_NAME)
            return 1
        log.info("CV %r -> cv_no=%s", CV_NAME, cv.cv_no)

        added = 0
        for term in TERMS:
            existing = (
                db.query(CvTerm)
                .filter(CvTerm.cv_no == cv.cv_no, CvTerm.term_name == term)
                .first()
            )
            if existing:
                log.info("  exists: %s (cv_term_no=%s)", term, existing.cv_term_no)
                continue

            log.info("  adding: %s", term)
            if dry_run:
                continue

            cv_term_no = _free_cv_term_no(db)
            # created_by is populated by the trigger/default.
            db.add(CvTerm(cv_term_no=cv_term_no, cv_no=cv.cv_no, term_name=term))
            db.commit()
            log.info("    added (cv_term_no=%s)", cv_term_no)
            added += 1

        log.info("Done. %d term(s) added.", added)
        return 0
    except Exception:
        db.rollback()
        log.exception("Failed to add CV terms")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main(dry_run="--dry-run" in sys.argv))
