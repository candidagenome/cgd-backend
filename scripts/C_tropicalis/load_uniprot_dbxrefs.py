#!/usr/bin/env python3
"""
Load UniProt (Swiss-Prot / TrEMBL) cross-references for C. tropicalis MYA-3404.

C. tropicalis features were imported into CGD with only their CGDID and no
external cross-references, so they have no UniProt dbxrefs (unlike the other
five CGD species).  As a result C. tropicalis is absent from the gp2protein
download files.  This script backfills those mappings so C. tropicalis matches
the other species and can be included in gp2protein generation.

Approach
--------
The C. tropicalis MYA-3404 UniProt proteome (NCBI taxon 294747) records each
protein under its ORF name (the CTRG_##### locus tag), which is identical to
the CGD feature_name.  We fetch the proteome from the UniProt REST API and,
for every entry whose ORF name matches a CGD C. tropicalis feature, create:

  - a dbxref row      (source='EBI', dbxref_type='SwissProt'|'TrEMBL',
                       dbxref_id=<UniProt accession>)
  - a dbxref_feat row (linking the dbxref to the feature)
  - a dbxref_url row   (linking the dbxref to the standard UniProt URL, so the
                       accession is clickable on the locus page)

These conventions were read directly from the existing UniProt dbxrefs of the
other five species (source 'EBI'; SwissProt->url_no 44473, TrEMBL->url_no
44475).  The script is idempotent: existing dbxref / dbxref_feat / dbxref_url
rows are reused, never duplicated.

Usage:
    python scripts/C_tropicalis/load_uniprot_dbxrefs.py --dry-run
    python scripts/C_tropicalis/load_uniprot_dbxrefs.py

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA:    Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")

# C. tropicalis MYA-3404
TAXON_ID = 294747
ORGANISM_ABBREV = "C_tropicalis"

# dbxref conventions, matching the existing UniProt dbxrefs of the other species
SOURCE = "EBI"
SWISSPROT_TYPE = "SwissProt"
TREMBL_TYPE = "TrEMBL"
# Standard UniProt URL rows (MULTI.url) used by existing SwissProt/TrEMBL dbxrefs
SWISSPROT_URL_NO = 44473
TREMBL_URL_NO = 44475

UNIPROT_STREAM_URL = "https://rest.uniprot.org/uniprotkb/stream"
UNIPROT_FIELDS = "accession,id,gene_orf,gene_names,reviewed"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_uniprot_tsv(taxon_id: int) -> str:
    """Fetch the full UniProt proteome for a taxon as TSV."""
    query = urllib.parse.urlencode({
        "query": f"organism_id:{taxon_id}",
        "format": "tsv",
        "fields": UNIPROT_FIELDS,
    })
    url = f"{UNIPROT_STREAM_URL}?{query}"
    logger.info("Fetching UniProt proteome for taxon %s", taxon_id)
    logger.debug("URL: %s", url)
    with urllib.request.urlopen(url, timeout=300) as response:
        return response.read().decode("utf-8")


def parse_uniprot_tsv(data: str) -> list[dict]:
    """Parse the UniProt TSV into records.

    Columns (from UNIPROT_FIELDS):
        Entry, Entry Name, Gene Names (ORF), Gene Names, Reviewed
    """
    records: list[dict] = []
    lines = data.strip().split("\n")
    for line in lines[1:]:  # skip header
        parts = line.split("\t")
        if len(parts) < 5:
            continue
        accession = parts[0].strip()
        entry_name = parts[1].strip()
        orf_names = parts[2].strip()
        gene_names = parts[3].strip()
        reviewed = parts[4].strip().lower()
        if not accession:
            continue

        # Candidate CGD feature names: prefer the ORF/locus-tag names, then any
        # other gene-name tokens (deduped, order preserved).
        candidates: list[str] = []
        for token in orf_names.split() + gene_names.split():
            up = token.upper()
            if up and up not in candidates:
                candidates.append(up)

        records.append({
            "accession": accession,
            "entry_name": entry_name,
            "candidates": candidates,
            "is_swissprot": reviewed == "reviewed",
        })
    return records


def get_feature_map(session) -> dict[str, int]:
    """Map C. tropicalis feature_name (uppercase) -> feature_no."""
    rows = session.execute(
        text(f"""
            SELECT f.feature_name, f.feature_no
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_abbrev = :abbrev
              AND f.feature_name IS NOT NULL
        """),
        {"abbrev": ORGANISM_ABBREV},
    ).fetchall()
    return {r[0].upper(): r[1] for r in rows}


def get_existing_dbxrefs(session) -> dict[tuple[str, str], int]:
    """Map (dbxref_type, dbxref_id) -> dbxref_no for existing EBI UniProt rows."""
    rows = session.execute(
        text(f"""
            SELECT dbxref_type, dbxref_id, dbxref_no
            FROM {DB_SCHEMA}.dbxref
            WHERE source = :src AND dbxref_type IN (:sp, :tr)
        """),
        {"src": SOURCE, "sp": SWISSPROT_TYPE, "tr": TREMBL_TYPE},
    ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def get_existing_feat_links(session, dbxref_nos: set[int]) -> set[tuple[int, int]]:
    """Existing (dbxref_no, feature_no) pairs among the given dbxref_nos."""
    if not dbxref_nos:
        return set()
    rows = session.execute(
        text(f"""
            SELECT df.dbxref_no, df.feature_no
            FROM {DB_SCHEMA}.dbxref_feat df
            JOIN {DB_SCHEMA}.dbxref d ON df.dbxref_no = d.dbxref_no
            WHERE d.source = :src AND d.dbxref_type IN (:sp, :tr)
        """),
        {"src": SOURCE, "sp": SWISSPROT_TYPE, "tr": TREMBL_TYPE},
    ).fetchall()
    return {(r[0], r[1]) for r in rows}


def get_existing_url_links(session) -> set[tuple[int, int]]:
    """Existing (dbxref_no, url_no) pairs for EBI UniProt dbxrefs."""
    rows = session.execute(
        text(f"""
            SELECT du.dbxref_no, du.url_no
            FROM {DB_SCHEMA}.dbxref_url du
            JOIN {DB_SCHEMA}.dbxref d ON du.dbxref_no = d.dbxref_no
            WHERE d.source = :src AND d.dbxref_type IN (:sp, :tr)
        """),
        {"src": SOURCE, "sp": SWISSPROT_TYPE, "tr": TREMBL_TYPE},
    ).fetchall()
    return {(r[0], r[1]) for r in rows}


def nextval(session, seq_name: str) -> int:
    return session.execute(
        text(f"SELECT {DB_SCHEMA}.{seq_name}.NEXTVAL FROM dual")
    ).scalar()


def insert_dbxref(session, dbxref_type: str, accession: str, created_by: str) -> int:
    dbxref_no = nextval(session, "dbxref_seq")
    session.execute(
        text(f"""
            INSERT INTO {DB_SCHEMA}.dbxref
                (dbxref_no, source, dbxref_type, dbxref_id, created_by)
            VALUES (:n, :src, :dt, :did, :cb)
        """),
        {"n": dbxref_no, "src": SOURCE, "dt": dbxref_type,
         "did": accession, "cb": created_by},
    )
    return dbxref_no


def insert_dbxref_feat(session, dbxref_no: int, feature_no: int) -> None:
    df_no = nextval(session, "dbxref_feat_seq")
    session.execute(
        text(f"""
            INSERT INTO {DB_SCHEMA}.dbxref_feat (dbxref_feat_no, dbxref_no, feature_no)
            VALUES (:n, :dx, :fn)
        """),
        {"n": df_no, "dx": dbxref_no, "fn": feature_no},
    )


def insert_dbxref_url(session, dbxref_no: int, url_no: int) -> None:
    du_no = nextval(session, "dbxref_url_seq")
    session.execute(
        text(f"""
            INSERT INTO {DB_SCHEMA}.dbxref_url (dbxref_url_no, dbxref_no, url_no)
            VALUES (:n, :dx, :un)
        """),
        {"n": du_no, "dx": dbxref_no, "un": url_no},
    )


def load(session, created_by: str, dry_run: bool) -> dict:
    stats = {
        "records_fetched": 0,
        "swissprot_created": 0,
        "trembl_created": 0,
        "dbxref_reused": 0,
        "feat_links_created": 0,
        "url_links_created": 0,
        "features_not_found": 0,
        "multi_match": 0,
    }

    feature_map = get_feature_map(session)
    logger.info("Loaded %d C. tropicalis features", len(feature_map))

    existing_dbxrefs = get_existing_dbxrefs(session)
    existing_feat_links = get_existing_feat_links(
        session, set(existing_dbxrefs.values())
    )
    existing_url_links = get_existing_url_links(session)
    logger.info(
        "Existing EBI UniProt dbxrefs=%d, feat_links=%d, url_links=%d",
        len(existing_dbxrefs), len(existing_feat_links), len(existing_url_links),
    )

    data = fetch_uniprot_tsv(TAXON_ID)
    records = parse_uniprot_tsv(data)
    stats["records_fetched"] = len(records)
    logger.info("Parsed %d UniProt records", len(records))

    for rec in records:
        accession = rec["accession"]
        dbxref_type = SWISSPROT_TYPE if rec["is_swissprot"] else TREMBL_TYPE
        url_no = SWISSPROT_URL_NO if rec["is_swissprot"] else TREMBL_URL_NO

        # Resolve to matching CGD feature(s)
        feat_nos = []
        for name in rec["candidates"]:
            fno = feature_map.get(name)
            if fno and fno not in feat_nos:
                feat_nos.append(fno)
        if not feat_nos:
            stats["features_not_found"] += 1
            logger.debug("No CGD feature for %s (%s)", accession, rec["candidates"])
            continue
        if len(feat_nos) > 1:
            stats["multi_match"] += 1

        key = (dbxref_type, accession)
        dbxref_no = existing_dbxrefs.get(key)

        if dbxref_no is None:
            if dry_run:
                # Use a sentinel so downstream dry-run bookkeeping stays consistent
                dbxref_no = -(stats["swissprot_created"] + stats["trembl_created"] + 1)
            else:
                dbxref_no = insert_dbxref(session, dbxref_type, accession, created_by)
            existing_dbxrefs[key] = dbxref_no
            if rec["is_swissprot"]:
                stats["swissprot_created"] += 1
            else:
                stats["trembl_created"] += 1
        else:
            stats["dbxref_reused"] += 1

        # Link to feature(s)
        for fno in feat_nos:
            if (dbxref_no, fno) in existing_feat_links:
                continue
            if not dry_run:
                insert_dbxref_feat(session, dbxref_no, fno)
            existing_feat_links.add((dbxref_no, fno))
            stats["feat_links_created"] += 1

        # Link to UniProt URL
        if (dbxref_no, url_no) not in existing_url_links:
            if not dry_run:
                insert_dbxref_url(session, dbxref_no, url_no)
            existing_url_links.add((dbxref_no, url_no))
            stats["url_links_created"] += 1

        created = stats["swissprot_created"] + stats["trembl_created"]
        if not dry_run and created and created % 500 == 0:
            session.flush()
            logger.info("Created %d dbxrefs so far...", created)

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load C. tropicalis UniProt dbxrefs into CGD"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without modifying the database",
    )
    parser.add_argument(
        "--created-by", default=os.getenv("ADMIN_USER", "SWENG").upper()[:12],
        help="created_by value for new rows (default: SWENG / $ADMIN_USER)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("Load C. tropicalis UniProt dbxrefs (taxon %s)", TAXON_ID)
    if args.dry_run:
        logger.info("[DRY RUN] no database changes will be committed")
    logger.info("created_by=%s  started=%s", args.created_by, datetime.now())
    logger.info("=" * 60)

    try:
        with SessionLocal() as session:
            stats = load(session, args.created_by, args.dry_run)
            if args.dry_run:
                session.rollback()
            else:
                session.commit()
                logger.info("Transaction committed")
    except Exception as e:  # noqa: BLE001
        logger.exception("Load failed: %s", e)
        return 1

    logger.info("=" * 60)
    logger.info("Summary:")
    logger.info("  UniProt records fetched : %d", stats["records_fetched"])
    logger.info("  SwissProt dbxrefs created: %d", stats["swissprot_created"])
    logger.info("  TrEMBL dbxrefs created   : %d", stats["trembl_created"])
    logger.info("  dbxrefs reused (existing): %d", stats["dbxref_reused"])
    logger.info("  dbxref_feat links created: %d", stats["feat_links_created"])
    logger.info("  dbxref_url links created : %d", stats["url_links_created"])
    logger.info("  UniProt entries w/o CGD  : %d", stats["features_not_found"])
    logger.info("  entries matching >1 feat : %d", stats["multi_match"])
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
