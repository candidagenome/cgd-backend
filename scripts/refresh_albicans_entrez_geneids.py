#!/usr/bin/env python3
from __future__ import annotations

"""
Refresh C. albicans SC5314 Entrez Gene ID cross-references from current NCBI data.

CGD stored its C. albicans Entrez Gene IDs circa 2009. NCBI has since
reannotated C. albicans SC5314 and discontinued ~52% of those Gene IDs, so the
stored IDs are half dead links -- which also makes the CGDID_2_GeneID.tab and
CGDID_2_RefSeqID.tab download files stale/incomplete.

This script rebuilds the 'Entrez Gene ID' dbxrefs from the current NCBI
annotation:

  1. Read NCBI's Fungi gene_info (tax_id 237561), which lists each current gene
     as (GeneID, LocusTag=CAALFM_########). ~115 MB.
  2. Transform each CAALFM locus tag to the CGD assembly-22 systematic name
     (CAALFM_C109470CA -> C1_09470C_A) and match it to a CGD feature. The
     Gene ID is attached to that assembly-22 A-allele feature.
  3. In one transaction: delete all existing 'Entrez Gene ID' (source 'NCBI')
     dbxrefs / dbxref_feat / dbxref_url rows, then insert the current mappings
     (dbxref + dbxref_feat + dbxref_url to the NCBI Gene URL, url_no 31538).

After this runs, regenerate the download files:
    python scripts/cron/make_cgdid_2_geneid.py --all-in-place  (or default)
    python scripts/cron/make_cgdid_2_refseqid.py

The mapping is deterministic and idempotent (re-running reproduces the same
state). Always --dry-run first.

Usage:
    python scripts/refresh_albicans_entrez_geneids.py --dry-run
    python scripts/refresh_albicans_entrez_geneids.py --gene-info-file /data/HTS/All_Fungi.gene_info.gz
    python scripts/refresh_albicans_entrez_geneids.py

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""

import argparse
import gzip
import logging
import os
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")

# C. albicans SC5314
TAXON_ID = 237561
ORGANISM_ABBREV = "C_albicans_SC5314"

# dbxref conventions (from existing Entrez Gene ID rows)
DBXREF_TYPE = "Entrez Gene ID"
DBXREF_SOURCE = "NCBI"
NCBI_GENE_URL_NO = 31538  # http://www.ncbi.nlm.nih.gov/.../db=gene...list_uids=_SUBSTITUTE_THIS_

# NCBI Fungi gene_info
GENE_INFO_URL = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Fungi/All_Fungi.gene_info.gz"
# gene_info columns (0-based): 0=tax_id, 1=GeneID, 3=LocusTag
COL_TAX, COL_GENEID, COL_LOCUSTAG = 0, 1, 3

# CAALFM_C109470CA -> chrom=C1, digits=09470, orient=C, allele=A -> C1_09470C_A
LOCUSTAG_RE = re.compile(r"^CAALFM_C([1-7R])(\d{5})([WC])([AB])$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def locustag_to_systematic(locustag: str) -> str | None:
    """CAALFM_C109470CA -> C1_09470C_A (None if not a nuclear CAALFM locus tag)."""
    m = LOCUSTAG_RE.match(locustag)
    if not m:
        return None
    chrom, digits, orient, allele = m.groups()
    return f"C{chrom}_{digits}{orient}_{allele}"


def open_gene_info(source: str | None):
    """Return a text stream over Fungi gene_info (local file or NCBI URL)."""
    if source:
        logger.info("Reading gene_info from local file: %s", source)
        return gzip.open(source, "rt")
    logger.info("Streaming gene_info from %s (~115 MB)", GENE_INFO_URL)
    req = urllib.request.Request(
        GENE_INFO_URL, headers={"User-Agent": "CGD-geneid-refresh/1.0"}
    )
    return gzip.open(urllib.request.urlopen(req, timeout=300), "rt")


def parse_gene_info(source: str | None) -> dict[str, str]:
    """Return {systematic_name (upper): current Entrez GeneID} for tax 237561."""
    mapping: dict[str, str] = {}
    mito = other = 0
    with open_gene_info(source) as stream:
        for line in stream:
            if line.startswith("#"):
                continue
            cols = line.rstrip("\n").split("\t")
            if len(cols) <= COL_LOCUSTAG or cols[COL_TAX] != str(TAXON_ID):
                continue
            geneid, locustag = cols[COL_GENEID], cols[COL_LOCUSTAG]
            sysname = locustag_to_systematic(locustag)
            if sysname is None:
                if locustag.startswith("Caalf"):
                    mito += 1
                else:
                    other += 1
                continue
            mapping[sysname.upper()] = geneid
    logger.info(
        "NCBI tax %s: %d nuclear genes parsed (skipped mito=%d, other=%d)",
        TAXON_ID, len(mapping), mito, other,
    )
    return mapping


def get_feature_map(session) -> dict[str, int]:
    """Map C. albicans assembly-22 A-allele feature_name (upper) -> feature_no."""
    rows = session.execute(
        text(f"""
            SELECT UPPER(f.feature_name), f.feature_no
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_abbrev = :abbrev
              AND REGEXP_LIKE(f.feature_name, '^C[1-7R]_[0-9]{{5}}[WC]_A$')
        """),
        {"abbrev": ORGANISM_ABBREV},
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def get_orf19_pairing(session) -> dict[str, list[int]]:
    """Map assembly-22 A-allele feature_name (upper) -> paired orf19 feature_no(s).

    Intrinsic pairing via the feature/alias tables (independent of the dbxref
    state, so it is stable across re-runs): an A-allele feature carries its
    partner's orf19.NNNN name as an alias, and that name is itself an orf19
    feature. This preserves the "Entrez Gene" locus-page link on both the
    A-allele and orf19 pages.
    """
    # orf19.NNNN features: feature_name (upper) -> feature_no
    orf19_rows = session.execute(
        text(f"""
            SELECT UPPER(f.feature_name), f.feature_no
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_abbrev = :abbrev
              AND REGEXP_LIKE(f.feature_name, '^orf19\\.[0-9]+$')
        """),
        {"abbrev": ORGANISM_ABBREV},
    ).fetchall()
    orf19_fno = {name: fno for name, fno in orf19_rows}

    # A-allele features and their orf19.NNNN aliases
    rows = session.execute(
        text(f"""
            SELECT UPPER(f.feature_name), UPPER(a.alias_name)
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            JOIN {DB_SCHEMA}.feat_alias fa ON fa.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.alias a ON a.alias_no = fa.alias_no
            WHERE o.organism_abbrev = :abbrev
              AND REGEXP_LIKE(f.feature_name, '^C[1-7R]_[0-9]{{5}}[WC]_A$')
              AND REGEXP_LIKE(a.alias_name, '^orf19\\.[0-9]+$', 'i')
        """),
        {"abbrev": ORGANISM_ABBREV},
    ).fetchall()

    pairing: dict[str, list[int]] = {}
    for sys_name, alias_name in rows:
        ofno = orf19_fno.get(alias_name)
        if ofno is None:
            continue  # alias is not itself an orf19 feature (e.g. a secondary orf19.1xxxx)
        pairing.setdefault(sys_name, [])
        if ofno not in pairing[sys_name]:
            pairing[sys_name].append(ofno)
    return pairing


def nextval(session, seq_name: str) -> int:
    return session.execute(
        text(f"SELECT {DB_SCHEMA}.{seq_name}.NEXTVAL FROM dual")
    ).scalar()


def delete_existing(session) -> dict[str, int]:
    """Delete all existing Entrez Gene ID (NCBI) dbxrefs and their links."""
    where = "dbxref_type = :dt AND source = :src"
    params = {"dt": DBXREF_TYPE, "src": DBXREF_SOURCE}
    sub = f"SELECT dbxref_no FROM {DB_SCHEMA}.dbxref WHERE {where}"

    n_url = session.execute(
        text(f"DELETE FROM {DB_SCHEMA}.dbxref_url WHERE dbxref_no IN ({sub})"), params
    ).rowcount
    n_feat = session.execute(
        text(f"DELETE FROM {DB_SCHEMA}.dbxref_feat WHERE dbxref_no IN ({sub})"), params
    ).rowcount
    n_dbxref = session.execute(
        text(f"DELETE FROM {DB_SCHEMA}.dbxref WHERE {where}"), params
    ).rowcount
    return {"dbxref": n_dbxref, "dbxref_feat": n_feat, "dbxref_url": n_url}


def insert_mapping(session, geneid: str, feature_nos: list[int], created_by: str) -> int:
    """Insert one dbxref for the Gene ID, link it to each feature, add the URL.

    Returns the number of dbxref_feat links created.
    """
    dbxref_no = nextval(session, "dbxref_seq")
    session.execute(
        text(f"""
            INSERT INTO {DB_SCHEMA}.dbxref
                (dbxref_no, source, dbxref_type, dbxref_id, created_by)
            VALUES (:n, :src, :dt, :did, :cb)
        """),
        {"n": dbxref_no, "src": DBXREF_SOURCE, "dt": DBXREF_TYPE,
         "did": geneid, "cb": created_by},
    )
    for feature_no in feature_nos:
        df_no = nextval(session, "dbxref_feat_seq")
        session.execute(
            text(f"""
                INSERT INTO {DB_SCHEMA}.dbxref_feat (dbxref_feat_no, dbxref_no, feature_no)
                VALUES (:n, :dx, :fn)
            """),
            {"n": df_no, "dx": dbxref_no, "fn": feature_no},
        )
    du_no = nextval(session, "dbxref_url_seq")
    session.execute(
        text(f"""
            INSERT INTO {DB_SCHEMA}.dbxref_url (dbxref_url_no, dbxref_no, url_no)
            VALUES (:n, :dx, :un)
        """),
        {"n": du_no, "dx": dbxref_no, "un": NCBI_GENE_URL_NO},
    )
    return len(feature_nos)


def refresh(session, source: str | None, created_by: str, dry_run: bool) -> dict:
    stats = {
        "ncbi_genes": 0,
        "matched": 0,
        "unmatched": 0,
        "with_orf19_partner": 0,
        "deleted_dbxref": 0,
        "deleted_feat": 0,
        "deleted_url": 0,
        "inserted": 0,
        "feat_links": 0,
    }

    ncbi = parse_gene_info(source)
    stats["ncbi_genes"] = len(ncbi)

    feature_map = get_feature_map(session)
    logger.info("CGD A-allele features: %d", len(feature_map))

    # Capture the A-allele <-> orf19 pairing BEFORE the rebuild deletes it.
    pairing = get_orf19_pairing(session)
    logger.info("A-allele features with an orf19 partner: %d", len(pairing))

    # Resolve NCBI genes to CGD features (A-allele + paired orf19)
    resolved: list[tuple[str, list[int]]] = []  # (geneid, [feature_no, ...])
    unmatched_examples: list[str] = []
    for sysname, geneid in ncbi.items():
        fno = feature_map.get(sysname)
        if fno is None:
            stats["unmatched"] += 1
            if len(unmatched_examples) < 8:
                unmatched_examples.append(sysname)
            continue
        feature_nos = [fno]
        orf19_partners = pairing.get(sysname, [])
        if orf19_partners:
            stats["with_orf19_partner"] += 1
            feature_nos.extend(orf19_partners)
        resolved.append((geneid, feature_nos))
    stats["matched"] = len(resolved)
    logger.info("Matched %d NCBI genes (%d with orf19 partner); %d unmatched %s",
                stats["matched"], stats["with_orf19_partner"], stats["unmatched"],
                unmatched_examples if unmatched_examples else "")

    if stats["matched"] < 1000:
        raise RuntimeError(
            f"Refusing to proceed: only {stats['matched']} matches (expected ~6000+)"
        )

    # Rebuild
    deleted = delete_existing(session)
    stats["deleted_dbxref"] = deleted["dbxref"]
    stats["deleted_feat"] = deleted["dbxref_feat"]
    stats["deleted_url"] = deleted["dbxref_url"]
    logger.info("Deleted existing: dbxref=%d dbxref_feat=%d dbxref_url=%d",
                deleted["dbxref"], deleted["dbxref_feat"], deleted["dbxref_url"])

    for geneid, feature_nos in resolved:
        stats["feat_links"] += insert_mapping(session, geneid, feature_nos, created_by)
        stats["inserted"] += 1
        if stats["inserted"] % 1000 == 0:
            session.flush()
            logger.info("Inserted %d mappings so far...", stats["inserted"])

    if dry_run:
        session.rollback()
        logger.info("[DRY RUN] rolled back")
    else:
        session.commit()
        logger.info("Transaction committed")

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh C. albicans Entrez Gene ID dbxrefs from current NCBI data"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without committing")
    parser.add_argument("--gene-info-file",
                        help="Local Fungi gene_info.gz (instead of streaming from NCBI)")
    parser.add_argument("--created-by",
                        default=os.getenv("ADMIN_USER", "SWENG").upper()[:12],
                        help="created_by value for new rows")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("Refresh C. albicans Entrez Gene IDs (NCBI tax %s)", TAXON_ID)
    if args.dry_run:
        logger.info("[DRY RUN] no database changes will be committed")
    logger.info("created_by=%s  started=%s", args.created_by, datetime.now())
    logger.info("=" * 60)

    try:
        with SessionLocal() as session:
            stats = refresh(session, args.gene_info_file, args.created_by, args.dry_run)
    except Exception as e:  # noqa: BLE001
        logger.exception("Refresh failed: %s", e)
        return 1

    logger.info("=" * 60)
    logger.info("Summary:")
    logger.info("  NCBI nuclear genes         : %d", stats["ncbi_genes"])
    logger.info("  matched to CGD features    : %d", stats["matched"])
    logger.info("    of which w/ orf19 partner: %d", stats["with_orf19_partner"])
    logger.info("  unmatched (NCBI, no CGD)   : %d", stats["unmatched"])
    logger.info("  deleted old dbxref/feat/url: %d / %d / %d",
                stats["deleted_dbxref"], stats["deleted_feat"], stats["deleted_url"])
    logger.info("  inserted dbxrefs           : %d", stats["inserted"])
    logger.info("  inserted dbxref_feat links : %d", stats["feat_links"])
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
