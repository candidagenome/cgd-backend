#!/usr/bin/env python3
"""
Fix 'Reference Data' URLs that do not point at their dataset directory.

The archived datasets are served from /download/systematic_results/<dir>/.
An audit of every Reference Data link on candidagenome.org (2026-11-09) found
15 broken ones: 11 with a '/downloads/' prefix, one with no leading slash, and
three naming a directory that does not exist (a typo, or the wrong year). Each
fix below is checked against the live directory listing.

The URL row is updated in place; if the corrected URL already exists as its
own row, the reference's REF_URL is re-pointed to it instead (URL.url is
unique). A row already holding the corrected URL is left alone, so reruns are
no-ops. --verify fetches each corrected URL from --site first and refuses to
apply one that is not a directory listing. Dry-run by default.

Usage:
    python scripts/fix_reference_data_urls.py --verify            # dry-run
    python scripts/fix_reference_data_urls.py --verify --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")

DATA_DIR = "/download/systematic_results"

# reference dbxref_id -> (stored URL, corrected URL)
FIXES = {
    "CAL0000246390": ("/downloads/systematic_results/Avelar_2024", f"{DATA_DIR}/Avelar_2024"),
    "CAL0000246374": ("/downloads/systematic_results/Chow_2024", f"{DATA_DIR}/Chow_2024"),
    "CAL0000246584": ("/downloads/systematic_results/Khemiri_2024", f"{DATA_DIR}/Khemiri_2024"),
    "CAL0000246529": ("/downloads/systematic_results/Kumar_2024", f"{DATA_DIR}/Kumar_2024"),
    "CAL0000246446": ("/downloads/systematic_results/Srague_2024", f"{DATA_DIR}/Sprague_2024"),
    "CAL0000246485": ("/downloads/systematic_results/Wakade_2024", f"{DATA_DIR}/Wakade_2024"),
    "CAL0000246470": ("/downloads/systematic_results/Xiong_2024", f"{DATA_DIR}/Xiong_2024"),
    "CAL0000246086": ("download/systematic_results/Alam_2023", f"{DATA_DIR}/Alam_2023"),
    "CAL0000245642": ("/downloads/systematic_results/Pais2_2022", f"{DATA_DIR}/Pais2_2022"),
    "CAL0000245670": ("/downloads/systematic_results/Sun_2022", f"{DATA_DIR}/Sun_2022"),
    "CAL0000229189": ("/downloads/systematic_results/Fu_2021", f"{DATA_DIR}/Fu_2021"),
    "CAL0000228547": ("/downloads/systematic_results/Gale_2020", f"{DATA_DIR}/Gale_2020"),
    # Pais et al. 2022 CgMar1 paper (PMID 35050001); Pais2_2022 is the other 2022 paper
    "CAL0000228859": ("/download/systematic_results/Paisi_2022/", f"{DATA_DIR}/Pais_2022"),
    # Shor et al. 2020 (PMID 33323516)
    "CAL0000229242": ("/download/systematic_results/Shor_2022/", f"{DATA_DIR}/Shor_2020"),
    # The directory itself is spelled Rogers_Baker_2003
    "CAL0080946": ("/download/systematic_results/Rogers_Barker_2003", f"{DATA_DIR}/Rogers_Baker_2003"),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def is_listing(site: str, url: str) -> bool:
    try:
        with urllib.request.urlopen(f"{site}{url}/", timeout=30) as resp:
            body = resp.read(20000).decode("utf-8", "replace")
    except OSError as exc:
        logger.warning("%s%s/: %s", site, url, exc)
        return False
    return "Index of" in body and 'id="root"' not in body


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--apply", action="store_true", help="actually update (default: dry-run)")
    ap.add_argument("--verify", action="store_true", help="check each corrected URL on --site first")
    ap.add_argument("--site", default="https://www.candidagenome.org")
    args = ap.parse_args()
    dry = not args.apply
    tag = " [DRY-RUN]" if dry else ""
    fixed = already = skipped = 0

    with SessionLocal() as session:
        def q(sql, **kw):
            return session.execute(text(sql), kw).fetchall()

        for dbxref_id, (old, new) in FIXES.items():
            if args.verify and not is_listing(args.site, new):
                logger.warning("SKIP %s: %s%s/ is not a dataset directory", dbxref_id, args.site, new)
                skipped += 1
                continue
            rows = q(f"""SELECT ru.ref_url_no, u.url_no, u.url FROM {DB_SCHEMA}.reference r
                         JOIN {DB_SCHEMA}.ref_url ru ON ru.reference_no = r.reference_no
                         JOIN {DB_SCHEMA}.url u ON u.url_no = ru.url_no
                         WHERE r.dbxref_id = :d AND u.url_type = 'Reference Data'""", d=dbxref_id)
            current = [row for row in rows if row.url in (old, new)]
            if any(row.url == new for row in current):
                already += 1
                continue
            if len(current) != 1:
                logger.warning("SKIP %s: expected one Reference Data URL %s, found %s",
                               dbxref_id, old, [row.url for row in rows])
                skipped += 1
                continue
            row = current[0]
            sharers = session.execute(text(f"SELECT COUNT(*) FROM {DB_SCHEMA}.ref_url WHERE url_no = :n"),
                                      {"n": row.url_no}).scalar()
            if sharers != 1:
                logger.warning("SKIP %s: URL row %s is shared by %d references", dbxref_id, row.url_no, sharers)
                skipped += 1
                continue
            existing = session.execute(text(f"SELECT url_no FROM {DB_SCHEMA}.url WHERE url = :u"),
                                       {"u": new}).scalar()
            if existing:
                logger.info("%s: re-point to existing URL row %s (%s)%s", dbxref_id, existing, new, tag)
                if not dry:
                    session.execute(text(f"UPDATE {DB_SCHEMA}.ref_url SET url_no = :n WHERE ref_url_no = :r"),
                                    {"n": existing, "r": row.ref_url_no})
            else:
                logger.info("%s: %s -> %s%s", dbxref_id, old, new, tag)
                if not dry:
                    session.execute(text(f"UPDATE {DB_SCHEMA}.url SET url = :u WHERE url_no = :n"),
                                    {"u": new, "n": row.url_no})
            fixed += 1

        if dry:
            session.rollback()
        else:
            session.commit()
    logger.info("done: %d fixed, %d already correct, %d skipped%s", fixed, already, skipped,
                " [DRY-RUN — no changes]" if dry else "")
    if skipped:
        sys.exit(1)


if __name__ == "__main__":
    main()
