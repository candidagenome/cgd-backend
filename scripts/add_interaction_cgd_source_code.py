#!/usr/bin/env python3
"""
Add the 'CGD' source code to the CODE table for interactions.

Curator-entered interactions use source='CGD'. The CODE table
(tab_name='INTERACTION', col_name='SOURCE') is the controlled vocabulary for
the interaction source column and currently contains only 'BioGRID'.

Idempotent: safe to run multiple times. This is a DB write that does NOT travel
through git, so it MUST be run on EVERY database (dev and prod).

Usage:
    python scripts/add_interaction_cgd_source_code.py [--dry-run]
"""
import os
import sys
import logging

# Allow running directly: add project root to path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from cgd.db.engine import SessionLocal  # noqa: E402
from cgd.models.models import Code  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
log = logging.getLogger(__name__)

TAB_NAME = "INTERACTION"
COL_NAME = "SOURCE"
CODE_VALUE = "CGD"
DESCRIPTION = "Interaction curated by CGD"


def main(dry_run: bool = False) -> int:
    db = SessionLocal()
    try:
        existing = (
            db.query(Code)
            .filter(
                Code.tab_name == TAB_NAME,
                Code.col_name == COL_NAME,
                Code.code_value == CODE_VALUE,
            )
            .first()
        )
        if existing:
            log.info(
                "CODE %s/%s/%s already exists (code_no=%s); nothing to do.",
                TAB_NAME, COL_NAME, CODE_VALUE, existing.code_no,
            )
            return 0

        log.info("Adding CODE row: %s/%s/%s", TAB_NAME, COL_NAME, CODE_VALUE)
        if dry_run:
            log.info("--dry-run: not committing.")
            return 0

        # code_no and created_by are populated by Oracle defaults/triggers.
        code = Code(
            tab_name=TAB_NAME,
            col_name=COL_NAME,
            code_value=CODE_VALUE,
            description=DESCRIPTION,
        )
        db.add(code)
        db.commit()
        log.info("Added CODE row (code_no=%s).", code.code_no)
        return 0
    except Exception:
        db.rollback()
        log.exception("Failed to add CODE row")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main(dry_run="--dry-run" in sys.argv))
