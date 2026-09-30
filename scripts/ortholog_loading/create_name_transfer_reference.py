#!/usr/bin/env python3
"""
Create the CGD methods reference cited by ortholog gene-name transfers.

Modeled on the reference used for computed gene descriptions
("CGD (2010) Description lines for gene products, based on orthologs and
predicted Gene Ontology (GO) annotations.", CAL0139843): a
'Curator non-PubMed reference' authored by "CGD" with an abstract that
explains the methodology. apply_name_transfers.py attaches this reference
to every gene name it sets, via REF_LINK (FEATURE / GENE_NAME), so the
citation appears next to the name on locus pages.

The REFERENCE insert triggers do most of the wiring: REFERENCE_BIUR assigns
reference_no and mints the CAL dbxref_id (MakeDbid / DBID_SEQ), and
REFERENCE_AIUDR creates the matching DBXREF ('CGDID Primary') and DBXREF_REF
rows. This script adds only the satellite rows the triggers don't:
AUTHOR_EDITOR (author "CGD"), REF_REFTYPE ('Personal communication to CGD'),
REF_PROPERTY (curation_status), and ABSTRACT.

The dbxref_id is allocated per-database, so dev and prod will carry different
CAL ids for the same reference; scripts must therefore look it up by title
(REF_TITLE below), never by a hard-coded CAL id.

Idempotent: exits successfully if a reference with REF_TITLE already exists.
Dry-run by default.

Usage:
    python create_name_transfer_reference.py           # dry-run
    python create_name_transfer_reference.py --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()

# The stable lookup key for this reference on every database (dev and prod
# mint different CAL dbxref ids). apply_name_transfers.py imports this.
REF_TITLE = ("Conserved name transfer based on orthology, as determined by the "
             "Candida Gene Order Browser and the Yeast Gene Order Browser")
REF_YEAR = 2026
# House format for curator non-PubMed references: "CGD (year) title  "
# (empty journal/volume slots leave two trailing spaces, as in CAL0139843).
REF_CITATION = f"CGD ({REF_YEAR}) {REF_TITLE}  "

REF_ABSTRACT = (
    "Standard gene names are transferred at CGD to unnamed genes from their named "
    "orthologs in other CGD species and in Saccharomyces cerevisiae. Orthology "
    "relationships are derived from the synteny-based pillars of the Candida Gene "
    "Order Browser (CGOB) and, for Candida glabrata, the Yeast Gene Order Browser "
    "(YGOB). A name is transferred only when it is conserved and unambiguous across "
    "the ortholog group: all named members of the group agree on the name (following "
    "the S. cerevisiae standard name where an S. cerevisiae ortholog exists), and the "
    "transferred name does not conflict with any existing standard name, systematic "
    "name, or alias in the target species. Candidate transfers are generated and "
    "checked computationally, and are reviewed by CGD curators before they are "
    "applied. For C. albicans, a name transferred to an Assembly 22 feature is also "
    "assigned to the corresponding unnamed Assembly 21 feature."
)

REF_SOURCE = "Curator non-PubMed reference"
REF_STATUS = "Unpublished"
REF_PDF_STATUS = "NAP"
REF_TYPE = ("CGD", "Personal communication to CGD")
AUTHOR_NAME = "CGD"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def find_reference(session):
    """Return (reference_no, dbxref_id) for the name-transfer reference, or None."""
    row = session.execute(text(
        f"SELECT reference_no, dbxref_id FROM {DB_SCHEMA}.reference WHERE title = :t"),
        {"t": REF_TITLE}).fetchone()
    return tuple(row) if row else None


def ensure_seq_ahead(session, seq_name: str, table: str, pk_col: str) -> None:
    """Verify a PK-minting sequence is ahead of its table's MAX(pk); repair if not.

    The insert triggers assign PKs from these sequences, so a sequence that
    lags the table (e.g., after rows were inserted with hand-computed ids)
    mints duplicate keys and the load dies on ORA-00001 partway through.
    Consumes one sequence value per check; call only before real inserts.
    """
    max_pk = session.execute(text(
        f"SELECT NVL(MAX({pk_col}), 0) FROM {DB_SCHEMA}.{table}")).scalar()
    nextval = session.execute(text(
        f"SELECT {DB_SCHEMA}.{seq_name}.NEXTVAL FROM DUAL")).scalar()
    if nextval > max_pk:
        return
    gap = max_pk - nextval + 1
    logger.warning("%s is behind %s.%s (%d <= %d) — advancing by %d",
                   seq_name, table, pk_col, nextval, max_pk, gap)
    session.execute(text(f"ALTER SEQUENCE {DB_SCHEMA}.{seq_name} INCREMENT BY {gap}"))
    session.execute(text(f"SELECT {DB_SCHEMA}.{seq_name}.NEXTVAL FROM DUAL"))
    session.execute(text(f"ALTER SEQUENCE {DB_SCHEMA}.{seq_name} INCREMENT BY 1"))
    fixed = session.execute(text(
        f"SELECT {DB_SCHEMA}.{seq_name}.NEXTVAL FROM DUAL")).scalar()
    if fixed <= max_pk:
        raise RuntimeError(f"{seq_name} repair failed: nextval {fixed} <= max {max_pk}")
    logger.info("%s repaired: nextval now %d", seq_name, fixed)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--apply", action="store_true",
                    help="actually create the reference (default: dry-run)")
    args = ap.parse_args()
    dry = not args.apply

    with SessionLocal() as session:
        existing = find_reference(session)
        if existing:
            logger.info("reference already exists: reference_no=%d dbxref_id=%s",
                        existing[0], existing[1])
            return

        author_no = session.execute(text(
            f"SELECT author_no FROM {DB_SCHEMA}.author WHERE author_name = :n"),
            {"n": AUTHOR_NAME}).scalar()
        ref_type_no = session.execute(text(
            f"SELECT ref_type_no FROM {DB_SCHEMA}.ref_type"
            f" WHERE source = :s AND ref_type = :t"),
            {"s": REF_TYPE[0], "t": REF_TYPE[1]}).scalar()
        if not author_no or not ref_type_no:
            sys.exit(f"missing prerequisites: author {AUTHOR_NAME!r} -> {author_no}, "
                     f"ref_type {REF_TYPE[1]!r} -> {ref_type_no}")

        logger.info("citation: %r", REF_CITATION)
        if dry:
            logger.info("[DRY-RUN] would create reference (author_no=%d, ref_type_no=%d)"
                        " + author_editor + ref_reftype + ref_property + abstract",
                        author_no, ref_type_no)
            return

        ensure_seq_ahead(session, "REFERENCE_SEQ", "reference", "reference_no")
        ensure_seq_ahead(session, "DBXREF_SEQ", "dbxref", "dbxref_no")

        # reference_no and dbxref_id are trigger-assigned; the after-insert
        # trigger also creates the DBXREF and DBXREF_REF rows.
        session.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.reference
                (source, status, pdf_status, citation, year, title, created_by)
            VALUES (:src, :st, :pdf, :cit, :yr, :ti, :u)
        """), {"src": REF_SOURCE, "st": REF_STATUS, "pdf": REF_PDF_STATUS,
               "cit": REF_CITATION, "yr": REF_YEAR, "ti": REF_TITLE, "u": ADMIN_USER})
        ref_no, dbxref_id = find_reference(session)

        session.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.author_editor
                (author_no, reference_no, author_order, author_type)
            VALUES (:a, :r, 1, 'Author')
        """), {"a": author_no, "r": ref_no})
        session.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.ref_reftype (reference_no, ref_type_no)
            VALUES (:r, :t)
        """), {"r": ref_no, "t": ref_type_no})
        session.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.ref_property
                (reference_no, source, property_type, property_value,
                 date_last_reviewed, created_by)
            VALUES (:r, 'CGD', 'curation_status', 'Not yet curated', SYSDATE, :u)
        """), {"r": ref_no, "u": ADMIN_USER})
        session.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.abstract (reference_no, abstract)
            VALUES (:r, :a)
        """), {"r": ref_no, "a": REF_ABSTRACT})
        session.commit()

        dbx = session.execute(text(f"""
            SELECT d.dbxref_no FROM {DB_SCHEMA}.dbxref d
            JOIN {DB_SCHEMA}.dbxref_ref dr ON dr.dbxref_no = d.dbxref_no
            WHERE dr.reference_no = :r
        """), {"r": ref_no}).scalar()
        logger.info("created reference_no=%d dbxref_id=%s (dbxref wiring %s)",
                    ref_no, dbxref_id, "OK" if dbx else "MISSING — check triggers")


if __name__ == "__main__":
    main()
