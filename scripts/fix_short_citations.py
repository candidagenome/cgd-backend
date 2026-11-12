#!/usr/bin/env python3
"""
Repair references created with Curator Central's "create from PubMed".

Until 2026-11, that tool
  - built citations as "Ropars et al. (2018) Nat Commun 9:2253" (no author
    initials, title or issue) where the PubMed loaders write
    "Ropars J et al. (2018) <title> Nat Commun 9(1):2253";
  - did not store the issue;
  - cut titles and abstracts at the first markup tag, e.g. "Survival
    Strategies of Pathogenic " for "... Pathogenic <i>Candida</i> Species ...",
    and kept only the first section of a structured abstract.

For each such reference this fetches the PubMed record, restores a title or
abstract that the stored one is a truncated prefix of (anything else, e.g. a
curator's edit, is left alone and reported), stores the issue, and rebuilds
the citation in the loaders' format (cgd.core.citation.format_citation) from
the title, year, journal, volume, issue and pages and the authors in order.

Only 'Curator PubMed reference' rows whose citation does not contain the
start of their title are touched, so reruns are no-ops. Dry-run by default.

Usage:
    python scripts/fix_short_citations.py            # dry-run
    python scripts/fix_short_citations.py --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.api.services.curation.reference_curation_service import (  # noqa: E402
    ReferenceCurationError,
    ReferenceCurationService,
)
from cgd.db.engine import SessionLocal  # noqa: E402
from cgd.core.citation import format_citation  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
SOURCE = "Curator PubMed reference"
PUBMED_DELAY = 0.4  # NCBI allows 3 requests/s without an API key

SECTION_LABEL = re.compile(r"^[A-Z][A-Z /&,-]+: ")


def same_text(stored: str, full: str) -> bool:
    """
    Equal, allowing for characters outside the database's Latin-1 character set,
    which Oracle stores as a substitute (Δ as ¿, β as ß, − as -).
    """
    return len(stored) == len(full) and all(a == b or ord(b) > 255 for a, b in zip(stored, full))


def truncated_from(stored: str, full: str) -> bool:
    """True when the stored text is a cut-off start of the PubMed text (or of its first labelled section)."""
    stored = stored.strip()
    for candidate in (full, SECTION_LABEL.sub("", full, count=1)):
        if len(candidate) > len(stored) and same_text(stored, candidate[:len(stored)]):
            return True
    return False


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--apply", action="store_true", help="actually update (default: dry-run)")
    args = ap.parse_args()
    tag = "" if args.apply else " [DRY-RUN]"

    with SessionLocal() as session:
        refs = session.execute(text(f"""
            SELECT r.reference_no, r.pubmed, r.year, r.title, r.volume, r.issue, r.page, r.citation,
                   j.abbreviation AS journal
              FROM {DB_SCHEMA}.reference r
              LEFT JOIN {DB_SCHEMA}.journal j ON j.journal_no = r.journal_no
             WHERE r.source = :source AND r.title IS NOT NULL
               AND INSTR(r.citation, SUBSTR(r.title, 1, 20)) = 0
             ORDER BY r.reference_no"""), {"source": SOURCE}).fetchall()
        logger.info("%d short citations from Curator Central%s", len(refs), tag)

        pubmed = ReferenceCurationService(session)
        changed = titles = abstracts = 0
        for ref in refs:
            authors = [row[0] for row in session.execute(text(f"""
                SELECT a.author_name FROM {DB_SCHEMA}.author_editor ae
                  JOIN {DB_SCHEMA}.author a ON a.author_no = ae.author_no
                 WHERE ae.reference_no = :r AND ae.author_type = 'Author'
                 ORDER BY ae.author_order"""), {"r": ref.reference_no})]
            title, issue = ref.title, ref.issue
            abstract = session.execute(text(f"SELECT abstract FROM {DB_SCHEMA}.abstract WHERE reference_no = :r"),
                                       {"r": ref.reference_no}).scalar()
            new_abstract = None
            if ref.pubmed:
                try:
                    record = pubmed.fetch_pubmed_metadata(ref.pubmed)
                except ReferenceCurationError as exc:
                    logger.warning("reference %s: PubMed fetch failed (%s); only the citation is rebuilt",
                                   ref.reference_no, exc)
                    record = {}
                time.sleep(PUBMED_DELAY)
                issue = issue or record.get("issue") or None
                full_title = (record.get("title") or "")[:400]
                if full_title and not same_text(title, full_title):
                    if truncated_from(title, full_title):
                        title = full_title
                    else:
                        logger.warning("reference %s: stored title differs from PubMed, kept:\n    %s\n    %s",
                                       ref.reference_no, ref.title, full_title)
                full_abstract = (record.get("abstract") or "")[:4000]
                if full_abstract and abstract and not same_text(abstract, full_abstract):
                    if truncated_from(abstract, full_abstract):
                        new_abstract = full_abstract
                    else:
                        logger.warning("reference %s: stored abstract differs from PubMed, kept", ref.reference_no)

            citation = format_citation(ref.year, title, ref.journal, ref.volume, issue, ref.page, authors)
            if not citation:
                logger.warning("reference %s: no authors, left as %r", ref.reference_no, ref.citation)
                continue
            logger.info("reference %s (PMID %s)%s\n    old: %s\n    new: %s", ref.reference_no, ref.pubmed, tag,
                        ref.citation, citation)
            if title != ref.title:
                logger.info("    title restored: %s", title)
                titles += 1
            if new_abstract:
                logger.info("    abstract restored: %d -> %d characters", len(abstract), len(new_abstract))
                abstracts += 1
            if args.apply:
                session.execute(text(f"""UPDATE {DB_SCHEMA}.reference SET citation = :c, issue = :i, title = :t
                                         WHERE reference_no = :r"""),
                                {"c": citation, "i": issue[:40] if issue else None, "t": title,
                                 "r": ref.reference_no})
                if new_abstract:
                    session.execute(text(f"UPDATE {DB_SCHEMA}.abstract SET abstract = :a WHERE reference_no = :r"),
                                    {"a": new_abstract, "r": ref.reference_no})
            changed += 1

        if args.apply:
            session.commit()
    logger.info("done: %d citations rebuilt, %d titles and %d abstracts restored%s", changed, titles, abstracts,
                " [DRY-RUN — no changes]" if not args.apply else "")


if __name__ == "__main__":
    main()
