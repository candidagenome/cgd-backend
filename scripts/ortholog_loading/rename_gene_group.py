#!/usr/bin/env python3
"""
Rename every gene carrying one standard name to another (e.g. HEX3 -> SLX5).

For an ortholog group whose name turns out to be an outdated S. cerevisiae
alias: every feature (any organism, Assembly 21 records included) whose
gene_name is --old gets gene_name --new, and:

  - ORF features keep the old name as a 'Uniform' alias (the curators'
    convention for former standard names);
  - GENE_NAME ref_links: the new name cites --cite <organism>=<PMID> for that
    organism, otherwise the CGD name-transfer methods reference (see
    create_name_transfer_reference.py). Literature references that cited the
    old name move to the old-name alias; an old name-transfer citation is
    dropped where it no longer applies;
  - --headline-sub OLD=NEW rewrites matching text in the renamed features'
    headlines (e.g. Hex3p=Slx5p).

Refuses to run if any feature already carries the new name. All changes go in
one transaction through the normal triggers (update_log keeps the audit
trail). Dry-run by default.

Usage:
    python rename_gene_group.py --old HEX3 --new SLX5 \\
        --cite C_glabrata_CBS138=39642165 \\
        --headline-sub Hex3p=Slx5p --headline-sub HEX3=SLX5            # dry-run
    python rename_gene_group.py ... --apply
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

from cgd.api.services.curation.reference_curation_service import ReferenceCurationService  # noqa: E402
from cgd.db.engine import SessionLocal  # noqa: E402
from scripts.ortholog_loading.create_name_transfer_reference import (  # noqa: E402
    REF_TITLE,
    ensure_seq_ahead,
)

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()
ALIAS_TYPE = "Uniform"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_pairs(values: list[str], what: str) -> dict[str, str]:
    pairs = {}
    for value in values:
        key, sep, val = value.partition("=")
        if not sep or not key or not val:
            sys.exit(f"bad {what} {value!r}: expected KEY=VALUE")
        pairs[key] = val
    return pairs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--old", required=True, help="current gene name")
    ap.add_argument("--new", required=True, help="new gene name")
    ap.add_argument("--cite", action="append", default=[],
                    help="ORGANISM_ABBREV=PMID: reference for the new name in that organism")
    ap.add_argument("--headline-sub", action="append", default=[],
                    help="OLD=NEW text replacement in the renamed features' headlines")
    ap.add_argument("--apply", action="store_true", help="actually update (default: dry-run)")
    args = ap.parse_args()
    dry = not args.apply
    cites = {org: int(pmid) for org, pmid in parse_pairs(args.cite, "--cite").items()}
    headline_subs = parse_pairs(args.headline_sub, "--headline-sub")
    tag = " [DRY-RUN]" if dry else ""

    with SessionLocal() as session:
        def q(sql, **kw):
            return session.execute(text(sql), kw).fetchall()

        def ex(sql, **kw):
            if not dry:
                session.execute(text(sql), kw)

        targets = q(f"""SELECT f.feature_no, f.feature_name, f.feature_type, f.headline, o.organism_abbrev
                        FROM {DB_SCHEMA}.feature f JOIN {DB_SCHEMA}.organism o ON o.organism_no = f.organism_no
                        WHERE UPPER(f.gene_name) = UPPER(:old) ORDER BY o.organism_abbrev, f.feature_name""",
                    old=args.old)
        clashes = q(f"SELECT feature_name FROM {DB_SCHEMA}.feature WHERE UPPER(gene_name) = UPPER(:new)",
                    new=args.new)
        if clashes:
            sys.exit(f"{args.new} is already the name of {', '.join(r[0] for r in clashes)}; resolve by hand")
        if not targets:
            logger.info("no features named %s — nothing to do (already applied?)", args.old)
            return
        unknown = set(cites) - {t.organism_abbrev for t in targets}
        if unknown:
            sys.exit(f"--cite organisms not among the {args.old} features: {', '.join(sorted(unknown))}")

        transfer_ref = session.execute(text(
            f"SELECT reference_no FROM {DB_SCHEMA}.reference WHERE title = :t"), {"t": REF_TITLE}).scalar()
        if not transfer_ref:
            sys.exit("name-transfer reference not found — run create_name_transfer_reference.py --apply first")

        ref_service = ReferenceCurationService(session)
        cite_refs = {}
        for org, pmid in cites.items():
            ref = ref_service.get_reference_by_pubmed(pmid)
            if ref:
                cite_refs[org] = ref.reference_no
            elif dry:
                logger.info("PMID:%s is not in CGD; --apply will create it from PubMed", pmid)
                cite_refs[org] = None
            else:
                cite_refs[org] = ref_service.create_reference_from_pubmed(
                    pubmed=pmid, reference_status="Published", curator_userid=ADMIN_USER)
                logger.info("created reference %s for PMID:%s", cite_refs[org], pmid)

        if not dry:
            ensure_seq_ahead(session, "REF_LINK_SEQ", "ref_link", "ref_link_no")
            ensure_seq_ahead(session, "FEAT_ALIAS_SEQ", "feat_alias", "feat_alias_no")
            ensure_seq_ahead(session, "ALIAS_SEQ", "alias", "alias_no")

        def ensure_ref_link(ref_no, tab, col, pk) -> None:
            if ref_no is None or q(f"""SELECT 1 FROM {DB_SCHEMA}.ref_link WHERE reference_no = :r
                                      AND tab_name = :t AND col_name = :c AND primary_key = :p""",
                                   r=ref_no, t=tab, c=col, p=pk):
                return
            ex(f"""INSERT INTO {DB_SCHEMA}.ref_link (reference_no, tab_name, primary_key, col_name, created_by)
                   VALUES (:r, :t, :p, :c, :u)""", r=ref_no, t=tab, p=pk, c=col, u=ADMIN_USER)

        def ensure_alias(feature_no: int):
            """The old name as a Uniform alias of the feature; returns its feat_alias_no (None in a dry run)."""
            alias_no = session.execute(text(
                f"""SELECT alias_no FROM {DB_SCHEMA}.alias
                    WHERE alias_name = :a AND alias_type = :t"""), {"a": args.old, "t": ALIAS_TYPE}).scalar()
            if alias_no is None:
                if dry:
                    return None
                ex(f"INSERT INTO {DB_SCHEMA}.alias (alias_name, alias_type, created_by) VALUES (:a, :t, :u)",
                   a=args.old, t=ALIAS_TYPE, u=ADMIN_USER)
                alias_no = session.execute(text(
                    f"SELECT alias_no FROM {DB_SCHEMA}.alias WHERE alias_name = :a AND alias_type = :t"),
                    {"a": args.old, "t": ALIAS_TYPE}).scalar()
            link = session.execute(text(
                f"SELECT feat_alias_no FROM {DB_SCHEMA}.feat_alias WHERE feature_no = :f AND alias_no = :a"),
                {"f": feature_no, "a": alias_no}).scalar()
            if link is None and not dry:
                ex(f"INSERT INTO {DB_SCHEMA}.feat_alias (feature_no, alias_no) VALUES (:f, :a)",
                   f=feature_no, a=alias_no)
                link = session.execute(text(
                    f"SELECT feat_alias_no FROM {DB_SCHEMA}.feat_alias WHERE feature_no = :f AND alias_no = :a"),
                    {"f": feature_no, "a": alias_no}).scalar()
            return link

        for t in targets:
            new_ref = cite_refs.get(t.organism_abbrev, transfer_ref)
            old_links = q(f"""SELECT rl.ref_link_no, rl.reference_no, r.pubmed FROM {DB_SCHEMA}.ref_link rl
                              JOIN {DB_SCHEMA}.reference r ON r.reference_no = rl.reference_no
                              WHERE rl.tab_name = 'FEATURE' AND rl.col_name = 'GENE_NAME'
                                AND rl.primary_key = :f""", f=t.feature_no)
            new_headline = t.headline
            for old_text, new_text in headline_subs.items():
                if new_headline:
                    new_headline = new_headline.replace(old_text, new_text)

            logger.info("%s %s (%s): gene_name %s -> %s%s", t.organism_abbrev, t.feature_name, t.feature_type,
                        args.old, args.new, tag)
            ex(f"UPDATE {DB_SCHEMA}.feature SET gene_name = :g WHERE feature_no = :f",
               g=args.new, f=t.feature_no)

            if t.feature_type == "allele":
                # Allele-level records carry no aliases or name citations
                continue

            feat_alias_no = ensure_alias(t.feature_no)
            logger.info("    + alias %s (%s)%s", args.old, ALIAS_TYPE, tag)
            if new_ref is not None and new_ref not in {link.reference_no for link in old_links}:
                logger.info("    + GENE_NAME cites reference %s%s", new_ref, tag)
            ensure_ref_link(new_ref, "FEATURE", "GENE_NAME", t.feature_no)
            for link in old_links:
                if link.reference_no == new_ref:
                    continue
                if link.reference_no != transfer_ref:
                    # A paper that cited the old name now supports the alias
                    logger.info("    ~ reference %s (PMID %s) moves from GENE_NAME to alias %s%s",
                                link.reference_no, link.pubmed, args.old, tag)
                    if feat_alias_no is not None:
                        ensure_ref_link(link.reference_no, "FEAT_ALIAS", "FEAT_ALIAS_NO", feat_alias_no)
                else:
                    logger.info("    - GENE_NAME name-transfer citation dropped%s", tag)
                ex(f"DELETE FROM {DB_SCHEMA}.ref_link WHERE ref_link_no = :n", n=link.ref_link_no)

            if new_headline != t.headline:
                logger.info("    headline: %s%s", new_headline, tag)
                ex(f"UPDATE {DB_SCHEMA}.feature SET headline = :h WHERE feature_no = :f",
                   h=new_headline, f=t.feature_no)

        if dry:
            session.rollback()
        else:
            session.commit()
    logger.info("done: %d features renamed %s -> %s%s", len(targets), args.old, args.new,
                " [DRY-RUN — no changes]" if dry else "")


if __name__ == "__main__":
    main()
