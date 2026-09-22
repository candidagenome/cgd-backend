#!/usr/bin/env python3
"""
One-off ref_temp backfill sweep over a long Entrez-date window.

Why: until mid-2026 the weekly triage search filtered by publication date
(pdat), so a paper whose print date lay months in the future at the moment
it became searchable was invisible to every contemporaneous window, and the
current 21-day edat windows never look back (the PMID 41033005 class of
miss). This sweep re-runs the species-wide triage searches over the whole
suspect era; anything not already in reference/ref_bad/ref_temp lands in
ref_temp for normal curator triage.

Mirrors the weekly wrapper's REF_TEMP_QUERIES (including exclude lists) and
reuses RefTempLoader for query building, Medline fetch, and inserts. Its own
esearch paginates with an explicit edat mindate/maxdate range, because a
two-year "Candida" window exceeds the loader's single-call retmax; known
PMIDs are bulk-filtered against the database BEFORE any Medline fetching.

Dry-run by default: reports per-query found/new counts and writes the
would-insert PMID list, no NCBI record fetching, no writes.

Usage:
    python backfill_ref_temp.py --mindate 2024/10/01                # dry-run
    python backfill_ref_temp.py --mindate 2024/10/01 --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from Bio import Entrez  # noqa: E402
from sqlalchemy import text  # noqa: E402

from load_ref_temp import RefTempLoader, DB_SCHEMA  # noqa: E402
from cgd.db.engine import SessionLocal  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# The weekly wrapper's species-wide triage queries, verbatim
QUERIES: list[tuple[str, list[str]]] = [
    ("albicans", []),
    ("glabrata", ["Biomphalaria", "Arachis", "Vitex", "Littorinopsis", "Pera",
                  "Velleia", "Magonia", "Ficus", "Serjania", "Disonycha",
                  "Lasiosphaeria"]),
    ("dubliniensis", []),
    ("parapsilosis", []),
    ("auris", []),
    ("tropicalis", []),
    ("Torulopsis", []),
    ("Candida", ["Folsomia"]),
    ("Nakaseomyces AND glabratus", []),
    ("Nakaseomyces AND glabrata", []),
    ("Candidozyma AND auris", []),
    ("Candida AND krusei", []),
    ("Pichia AND kudriavzevii", []),
]

PAGE = 10000


def fetch_summaries(pmids: list[int]) -> dict[int, dict]:
    """Batch esummary metadata (year, journal, author, title, entrez date)."""
    out: dict[int, dict] = {}
    for i in range(0, len(pmids), 200):
        chunk = pmids[i:i + 200]
        try:
            handle = Entrez.esummary(db="pubmed",
                                     id=",".join(str(p) for p in chunk))
            for rec in Entrez.read(handle):
                pmid = int(rec.get("Id", 0))
                hist = rec.get("History", {})
                out[pmid] = {
                    "pubdate": str(rec.get("PubDate", "")),
                    "edat": (str(hist.get("entrez", ""))[:10]
                             if hist else str(rec.get("EPubDate", ""))),
                    "journal": str(rec.get("Source", "")),
                    "author": (rec.get("AuthorList") or [""])[0],
                    "title": str(rec.get("Title", "")),
                }
            handle.close()
        except Exception as e:
            logger.warning("esummary batch failed at %d: %s", i, e)
        time.sleep(0.4)
    return out


def search_window(query: str, mindate: str, maxdate: str) -> list[int]:
    """Paginated esearch over an explicit edat date range."""
    pmids: list[int] = []
    retstart = 0
    while True:
        handle = Entrez.esearch(
            db="pubmed", term=query, datetype="edat",
            mindate=mindate, maxdate=maxdate,
            retstart=retstart, retmax=PAGE,
        )
        record = Entrez.read(handle)
        handle.close()
        batch = [int(p) for p in record.get("IdList", [])]
        pmids.extend(batch)
        total = int(record.get("Count", 0))
        retstart += len(batch)
        if retstart >= total or not batch:
            return pmids
        time.sleep(0.4)


def known_pmids(session, pmids: list[int]) -> set[int]:
    """PMIDs already present in reference, ref_bad, or ref_temp."""
    known: set[int] = set()
    for table in ("reference", "ref_bad", "ref_temp"):
        for i in range(0, len(pmids), 900):
            chunk = pmids[i:i + 900]
            rows = session.execute(text(
                f"SELECT pubmed FROM {DB_SCHEMA}.{table}"
                f" WHERE pubmed IN ({','.join(str(p) for p in chunk)})"
            )).fetchall()
            known.update(r[0] for r in rows)
    return known


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--mindate", default="2024/10/01",
                    help="Window start, YYYY/MM/DD by Entrez date")
    ap.add_argument("--maxdate", default=date.today().strftime("%Y/%m/%d"),
                    help="Window end (default today)")
    ap.add_argument("--apply", action="store_true",
                    help="fetch Medline records and insert (default: dry-run)")
    ap.add_argument("--out", default="backfill_ref_temp_pmids.txt",
                    help="dry-run: where to write the would-insert PMID list")
    ap.add_argument("--review-tsv",
                    help="dry-run: also write a curator review TSV with "
                         "per-paper metadata (pmid, dates, journal, author, "
                         "title, matched query)")
    ap.add_argument("--pmids-file",
                    help="apply mode: load exactly these PMIDs (one per "
                         "line; the curator-approved subset of a review "
                         "file) instead of searching")
    args = ap.parse_args()
    dry = not args.apply

    with SessionLocal() as session:
        if args.apply and args.pmids_file:
            wanted = [int(line.split()[0]) for line in open(args.pmids_file)
                      if line.strip() and line.split()[0].isdigit()]
            logger.info("loading %d curator-approved PMIDs from %s",
                        len(wanted), args.pmids_file)
            loader = RefTempLoader(session, "curator-approved backfill subset")
            records = loader.fetch_records(wanted)
            loader.load_records(records)
            logger.info("done: %d inserted, %d failed, %d already known",
                        loader.insert_count, loader.fail_count,
                        loader.exclude_count)
            return 0 if not loader.fail_count else 1

        # one loader for query formatting + fetch/insert machinery
        all_new: set[int] = set()
        seen: set[int] = set()
        loaders: list[tuple[RefTempLoader, list[int]]] = []

        for query, excludes in QUERIES:
            loader = RefTempLoader(session, query, excludes or None)
            term = loader.build_query()
            found = search_window(term, args.mindate, args.maxdate)
            fresh = [p for p in found if p not in seen]
            seen.update(found)
            known = known_pmids(session, fresh) if fresh else set()
            new = [p for p in fresh if p not in known]
            all_new.update(new)
            loaders.append((loader, new))
            logger.info("%-32s found=%6d new-to-this-sweep=%5d unknown=%4d",
                        query, len(found), len(fresh), len(new))
            time.sleep(0.4)

        logger.info("TOTAL unknown PMIDs across all queries: %d", len(all_new))

        if dry:
            with open(args.out, "w") as fh:
                fh.write("\n".join(str(p) for p in sorted(all_new)))
            logger.info("[DRY-RUN] would-insert list written to %s — rerun "
                        "with --apply to fetch and load", args.out)
            if args.review_tsv:
                import csv
                first_query: dict[int, str] = {}
                for loader, new in loaders:
                    for pmid in new:
                        first_query.setdefault(pmid, loader.species_query)
                meta = fetch_summaries(sorted(all_new, reverse=True))
                with open(args.review_tsv, "w", newline="") as fh:
                    w = csv.writer(fh, delimiter="\t", lineterminator="\n")
                    w.writerow(["pmid", "entrez_date", "pubdate", "journal",
                                "first_author", "title", "matched_query",
                                "pubmed_url", "LOAD"])
                    for pmid in sorted(all_new, reverse=True):
                        m = meta.get(pmid, {})
                        w.writerow([
                            pmid, m.get("edat", ""), m.get("pubdate", ""),
                            m.get("journal", ""), m.get("author", ""),
                            m.get("title", ""), first_query.get(pmid, ""),
                            f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/", "",
                        ])
                logger.info("[DRY-RUN] curator review TSV written to %s "
                            "(%d rows)", args.review_tsv, len(all_new))
            return 0

        inserted = failed = 0
        for loader, new in loaders:
            if not new:
                continue
            records = loader.fetch_records(new)
            loader.load_records(records)   # re-checks per-PMID, commits per row
            inserted += loader.insert_count
            failed += loader.fail_count
        logger.info("backfill done: %d inserted into ref_temp, %d failed",
                    inserted, failed)
        return 0 if not failed else 1


if __name__ == "__main__":
    main()
