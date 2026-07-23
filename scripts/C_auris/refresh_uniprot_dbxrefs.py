#!/usr/bin/env python3
"""
Refresh UniProt (Swiss-Prot / TrEMBL) cross-references for C. auris B8441.

Motivation
----------
GOA reported that the C. auris entries in CGD's GPI file could not be mapped:
the UniProtKB accessions CGD emits for C. auris are overwhelmingly *obsolete*.
Spot-checking them against the UniProt REST API shows entryType "Inactive" with
inactiveReason DELETED ("Not part of a reference proteome" / "Redundant
sequence").  They were loaded from EBI years ago and UniProt has since retired
them, so GOA (which maps against the current reference proteome) rejects them.

Why matching is by protein sequence
------------------------------------
CGD's C. auris feature names use the 6-digit RefSeq locus tag B9J08_######.  The
current UniProt B8441 reference proteome (UP000230249, assembly
GCA_002759435.3) records almost every protein under the 5-digit GenBank locus
tag B9J08_#####, which is an *independent* numbering scheme -- not zero-padding.
Validated against the 21 reviewed entries that carry both tags, a naive
6->5-digit transform is 0/21 correct (the 5-digit ID space is dense, so every
transformed tag collides with an unrelated gene).  Locus-tag matching would
therefore mis-map ~2,300 genes.

Instead we match on the protein sequence itself: exact (normalised) amino-acid
identity between CGD's stored protein residues and the UniProt proteome FASTA.
This is immune to locus-tag renumbering.  Sequences that do not match exactly
are left untouched and reported (never guessed).

What the refresh does
---------------------
  * ADD    - link each C. auris feature to the accession(s) whose UniProt
             sequence exactly equals the feature's protein sequence (idempotent;
             existing dbxref / dbxref_feat / dbxref_url rows are reused).
  * REMOVE - drop the EBI UniProt dbxref links whose accession is no longer in
             the current proteome (the deleted/obsolete ones).  A dbxref row
             left with no remaining feature links (and its dbxref_url rows) is
             deleted too, so it stops appearing in gp2protein / GPI output.

Removal matters because make_gpi.get_uniprot_id() picks the lowest accession
string among a feature's linked UniProt dbxrefs; a stale accession that sorts
below the fresh one would keep being emitted.  Pass --keep-stale for additive-
only mode (add current accessions, leave obsolete links for curator review).

dbxref conventions match the existing UniProt dbxrefs of the other species
(source 'EBI'; SwissProt->url_no 44473, TrEMBL->url_no 44475).

Usage:
    python scripts/C_auris/refresh_uniprot_dbxrefs.py --dry-run
    python scripts/C_auris/refresh_uniprot_dbxrefs.py --dry-run --report-unmatched
    python scripts/C_auris/refresh_uniprot_dbxrefs.py
    python scripts/C_auris/refresh_uniprot_dbxrefs.py --keep-stale

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

from cgd.db.engine import SessionLocal  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")

# C. auris B8441, current UniProt reference proteome (assembly GCA_002759435.3).
TAXON_ID = 498019
ORGANISM_ABBREV = "C_auris_B8441"
PROTEOME_ID = "UP000230249"

# CGD stores the protein sequence in MULTI.SEQ; C. auris uses the lowercase
# 'protein' seq_type (kept broad to match the sequence_service convention).
PROTEIN_SEQ_TYPES = ("protein", "Protein")

# dbxref conventions, matching the existing UniProt dbxrefs of the other species.
# make_gpi.py accepts all three types under source 'EBI'; current EBI loads use
# SwissProt / TrEMBL, and 'UniProtKB' is the older generic type.
SOURCE = "EBI"
SWISSPROT_TYPE = "SwissProt"
TREMBL_TYPE = "TrEMBL"
UNIPROT_TYPES = (SWISSPROT_TYPE, TREMBL_TYPE, "UniProtKB")
# Standard UniProt URL rows (MULTI.url) used by existing SwissProt/TrEMBL dbxrefs
SWISSPROT_URL_NO = 44473
TREMBL_URL_NO = 44475

UNIPROT_STREAM_URL = "https://rest.uniprot.org/uniprotkb/stream"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# UniProt proteome (FASTA)                                                     #
# --------------------------------------------------------------------------- #
def fetch_proteome_fasta(proteome_id: str) -> str:
    """Fetch the current UniProt proteome as FASTA (canonical sequences only)."""
    query = urllib.parse.urlencode({
        "query": f"proteome:{proteome_id}",
        "format": "fasta",
        "includeIsoform": "false",
    })
    url = f"{UNIPROT_STREAM_URL}?{query}"
    logger.info("Fetching UniProt proteome %s (FASTA)", proteome_id)
    logger.debug("URL: %s", url)
    with urllib.request.urlopen(url, timeout=300) as response:
        return response.read().decode("utf-8")


def normalize_seq(seq: str | None) -> str:
    """Canonicalise a protein sequence for comparison.

    Uppercase, drop all whitespace, and strip a single trailing stop codon '*'
    (CGD occasionally stores it; UniProt never does).
    """
    if not seq:
        return ""
    s = "".join(seq.split()).upper()
    if s.endswith("*"):
        s = s[:-1]
    return s


def parse_proteome_fasta(fasta: str) -> list[dict]:
    """Parse UniProt FASTA into [{accession, is_swissprot, seq}] records.

    Header format: '>sp|ACC|ENTRY ...' (reviewed) or '>tr|ACC|ENTRY ...'.
    """
    records: list[dict] = []
    acc = None
    is_sp = False
    chunks: list[str] = []

    def flush() -> None:
        if acc is not None:
            records.append({
                "accession": acc,
                "is_swissprot": is_sp,
                "seq": normalize_seq("".join(chunks)),
            })

    for line in fasta.splitlines():
        if line.startswith(">"):
            flush()
            chunks = []
            header = line[1:]
            parts = header.split("|")
            if len(parts) >= 2:
                is_sp = parts[0] == "sp"
                acc = parts[1].strip()
            else:  # unexpected header shape; skip gracefully
                acc = None
        else:
            chunks.append(line.strip())
    flush()
    return records


def build_seq_index(records: list[dict]) -> dict[str, list[dict]]:
    """Map normalised sequence -> list of UniProt records sharing that sequence."""
    index: dict[str, list[dict]] = {}
    for rec in records:
        if rec["seq"]:
            index.setdefault(rec["seq"], []).append(rec)
    return index


# --------------------------------------------------------------------------- #
# CGD reads                                                                    #
# --------------------------------------------------------------------------- #
def get_cgd_protein_seqs(session) -> list[tuple[int, str, str]]:
    """Return [(feature_no, feature_name, normalised_protein_seq)] for C. auris."""
    types_clause = ", ".join(f"'{t}'" for t in PROTEIN_SEQ_TYPES)
    rows = session.execute(
        text(f"""
            SELECT f.feature_no, f.feature_name, s.residues
            FROM {DB_SCHEMA}.feature f
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            JOIN {DB_SCHEMA}.seq s ON s.feature_no = f.feature_no
            WHERE o.organism_abbrev = :abbrev
              AND s.seq_type IN ({types_clause})
              AND s.is_seq_current = 'Y'
        """),
        {"abbrev": ORGANISM_ABBREV},
    ).fetchall()
    out: list[tuple[int, str, str]] = []
    for feature_no, feature_name, residues in rows:
        # residues may be a CLOB/LOB; coerce to str before normalising.
        out.append((feature_no, feature_name, normalize_seq(str(residues or ""))))
    return out


def get_existing_uniprot_dbxrefs(session) -> dict[tuple[str, str], int]:
    """Map (dbxref_type, dbxref_id) -> dbxref_no for existing EBI UniProt rows."""
    types_clause = ", ".join(f"'{t}'" for t in UNIPROT_TYPES)
    rows = session.execute(
        text(f"""
            SELECT dbxref_type, dbxref_id, dbxref_no
            FROM {DB_SCHEMA}.dbxref
            WHERE source = :src AND dbxref_type IN ({types_clause})
        """),
        {"src": SOURCE},
    ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def get_auris_feat_links(session) -> list[tuple[int, int, str, str]]:
    """EBI UniProt (dbxref_no, feature_no, dbxref_type, dbxref_id) for C. auris."""
    types_clause = ", ".join(f"'{t}'" for t in UNIPROT_TYPES)
    rows = session.execute(
        text(f"""
            SELECT df.dbxref_no, df.feature_no, d.dbxref_type, d.dbxref_id
            FROM {DB_SCHEMA}.dbxref_feat df
            JOIN {DB_SCHEMA}.dbxref d ON df.dbxref_no = d.dbxref_no
            JOIN {DB_SCHEMA}.feature f ON df.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no
            WHERE o.organism_abbrev = :abbrev
              AND d.source = :src
              AND d.dbxref_type IN ({types_clause})
        """),
        {"abbrev": ORGANISM_ABBREV, "src": SOURCE},
    ).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def get_existing_url_links(session) -> set[tuple[int, int]]:
    """Existing (dbxref_no, url_no) pairs for EBI UniProt dbxrefs."""
    types_clause = ", ".join(f"'{t}'" for t in UNIPROT_TYPES)
    rows = session.execute(
        text(f"""
            SELECT du.dbxref_no, du.url_no
            FROM {DB_SCHEMA}.dbxref_url du
            JOIN {DB_SCHEMA}.dbxref d ON du.dbxref_no = d.dbxref_no
            WHERE d.source = :src AND d.dbxref_type IN ({types_clause})
        """),
        {"src": SOURCE},
    ).fetchall()
    return {(r[0], r[1]) for r in rows}


def count_feat_links(session, dbxref_no: int) -> int:
    """How many features still reference this dbxref (across all organisms)."""
    return session.execute(
        text(f"SELECT COUNT(*) FROM {DB_SCHEMA}.dbxref_feat WHERE dbxref_no = :dx"),
        {"dx": dbxref_no},
    ).scalar()


# --------------------------------------------------------------------------- #
# CGD writes                                                                   #
# --------------------------------------------------------------------------- #
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


def delete_dbxref_feat(session, dbxref_no: int, feature_no: int) -> None:
    session.execute(
        text(f"""
            DELETE FROM {DB_SCHEMA}.dbxref_feat
            WHERE dbxref_no = :dx AND feature_no = :fn
        """),
        {"dx": dbxref_no, "fn": feature_no},
    )


def delete_orphan_dbxref(session, dbxref_no: int) -> None:
    """Delete a dbxref with no remaining feature links, plus its URL rows."""
    session.execute(
        text(f"DELETE FROM {DB_SCHEMA}.dbxref_url WHERE dbxref_no = :dx"),
        {"dx": dbxref_no},
    )
    session.execute(
        text(f"DELETE FROM {DB_SCHEMA}.dbxref WHERE dbxref_no = :dx"),
        {"dx": dbxref_no},
    )


# --------------------------------------------------------------------------- #
# Refresh                                                                      #
# --------------------------------------------------------------------------- #
def refresh(session, created_by: str, dry_run: bool, keep_stale: bool,
            report_unmatched: bool) -> dict:
    stats = {
        "proteome_records": 0,
        "current_accessions": 0,
        "cgd_features_with_protein": 0,
        "features_matched": 0,
        "features_unmatched": 0,
        "swissprot_created": 0,
        "trembl_created": 0,
        "dbxref_reused": 0,
        "feat_links_created": 0,
        "url_links_created": 0,
        "stale_links_removed": 0,
        "orphan_dbxrefs_deleted": 0,
        "features_left_without_uniprot": 0,
    }

    # ---- current UniProt proteome, indexed by sequence -----------------------
    records = parse_proteome_fasta(fetch_proteome_fasta(PROTEOME_ID))
    stats["proteome_records"] = len(records)
    seq_index = build_seq_index(records)
    current_accessions = {r["accession"] for r in records}
    stats["current_accessions"] = len(current_accessions)
    logger.info(
        "Proteome: %d entries, %d distinct sequences, %d accessions",
        len(records), len(seq_index), len(current_accessions),
    )

    # ---- CGD state -----------------------------------------------------------
    cgd_seqs = get_cgd_protein_seqs(session)
    stats["cgd_features_with_protein"] = len(cgd_seqs)
    existing_dbxrefs = get_existing_uniprot_dbxrefs(session)
    existing_url_links = get_existing_url_links(session)
    auris_links = get_auris_feat_links(session)
    existing_feat_links = {(dx, fn) for dx, fn, _dt, _did in auris_links}
    logger.info(
        "CGD: %d C. auris features with protein seq; existing EBI UniProt "
        "dbxrefs=%d, C. auris feat_links=%d, url_links=%d",
        len(cgd_seqs), len(existing_dbxrefs), len(auris_links),
        len(existing_url_links),
    )

    # ---- ADD: match each feature by exact protein sequence -------------------
    features_with_current: set[int] = set()
    unmatched: list[str] = []
    for feature_no, feature_name, seq in cgd_seqs:
        matches = seq_index.get(seq) if seq else None
        if not matches:
            stats["features_unmatched"] += 1
            if report_unmatched:
                unmatched.append(feature_name or str(feature_no))
            continue
        stats["features_matched"] += 1
        features_with_current.add(feature_no)

        for rec in matches:
            accession = rec["accession"]
            dbxref_type = SWISSPROT_TYPE if rec["is_swissprot"] else TREMBL_TYPE
            url_no = SWISSPROT_URL_NO if rec["is_swissprot"] else TREMBL_URL_NO

            key = (dbxref_type, accession)
            dbxref_no = existing_dbxrefs.get(key)
            if dbxref_no is None:
                if dry_run:
                    dbxref_no = -(stats["swissprot_created"]
                                  + stats["trembl_created"] + 1)
                else:
                    dbxref_no = insert_dbxref(
                        session, dbxref_type, accession, created_by)
                existing_dbxrefs[key] = dbxref_no
                if rec["is_swissprot"]:
                    stats["swissprot_created"] += 1
                else:
                    stats["trembl_created"] += 1
            else:
                stats["dbxref_reused"] += 1

            if (dbxref_no, feature_no) not in existing_feat_links:
                if not dry_run:
                    insert_dbxref_feat(session, dbxref_no, feature_no)
                existing_feat_links.add((dbxref_no, feature_no))
                stats["feat_links_created"] += 1

            if (dbxref_no, url_no) not in existing_url_links:
                if not dry_run:
                    insert_dbxref_url(session, dbxref_no, url_no)
                existing_url_links.add((dbxref_no, url_no))
                stats["url_links_created"] += 1

    # ---- REMOVE: drop links whose accession is gone from the proteome --------
    touched_dbxref_nos: set[int] = set()
    features_losing_all: set[int] = set()
    for dbxref_no, feature_no, _dtype, accession in auris_links:
        if accession in current_accessions:
            continue  # still valid
        if keep_stale:
            logger.debug("STALE (kept): feature %s -> %s", feature_no, accession)
            continue
        logger.debug("Removing stale link feature %s -> %s", feature_no, accession)
        if not dry_run:
            delete_dbxref_feat(session, dbxref_no, feature_no)
        stats["stale_links_removed"] += 1
        touched_dbxref_nos.add(dbxref_no)
        if feature_no not in features_with_current:
            features_losing_all.add(feature_no)

    # Orphan-clean dbxref rows that now have no feature links at all.
    if not keep_stale:
        for dbxref_no in touched_dbxref_nos:
            if dry_run:
                remaining = count_feat_links(session, dbxref_no)
                removed_here = sum(
                    1 for dx, _fn, _dt, acc in auris_links
                    if dx == dbxref_no and acc not in current_accessions
                )
                if remaining - removed_here <= 0:
                    stats["orphan_dbxrefs_deleted"] += 1
                continue
            if count_feat_links(session, dbxref_no) == 0:
                delete_orphan_dbxref(session, dbxref_no)
                stats["orphan_dbxrefs_deleted"] += 1

    stats["features_left_without_uniprot"] = len(features_losing_all)

    if report_unmatched and unmatched:
        logger.info("Features with no exact-sequence UniProt match (%d):",
                    len(unmatched))
        for name in sorted(unmatched):
            logger.info("  UNMATCHED %s", name)

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh C. auris B8441 UniProt dbxrefs in CGD against the "
                    "current UniProt reference proteome, matching by protein "
                    "sequence identity"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without modifying the database",
    )
    parser.add_argument(
        "--keep-stale", action="store_true",
        help="Additive only: add current accessions but do NOT remove obsolete "
             "links (for curator review before deletion)",
    )
    parser.add_argument(
        "--report-unmatched", action="store_true",
        help="List C. auris features with no exact-sequence UniProt match",
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
    logger.info("Refresh C. auris UniProt dbxrefs (taxon %s, proteome %s)",
                TAXON_ID, PROTEOME_ID)
    logger.info("Matching by exact protein-sequence identity")
    if args.dry_run:
        logger.info("[DRY RUN] no database changes will be committed")
    if args.keep_stale:
        logger.info("[KEEP-STALE] obsolete links will be left in place")
    logger.info("created_by=%s  started=%s", args.created_by, datetime.now())
    logger.info("=" * 60)

    try:
        with SessionLocal() as session:
            stats = refresh(session, args.created_by, args.dry_run,
                            args.keep_stale, args.report_unmatched)
            if args.dry_run:
                session.rollback()
            else:
                session.commit()
                logger.info("Transaction committed")
    except Exception as e:  # noqa: BLE001
        logger.exception("Refresh failed: %s", e)
        return 1

    logger.info("=" * 60)
    logger.info("Summary:")
    logger.info("  Proteome entries         : %d", stats["proteome_records"])
    logger.info("  Distinct current accns   : %d", stats["current_accessions"])
    logger.info("  CGD features w/ protein  : %d", stats["cgd_features_with_protein"])
    logger.info("  features matched by seq  : %d", stats["features_matched"])
    logger.info("  features unmatched       : %d", stats["features_unmatched"])
    logger.info("  SwissProt dbxrefs created: %d", stats["swissprot_created"])
    logger.info("  TrEMBL dbxrefs created   : %d", stats["trembl_created"])
    logger.info("  dbxrefs reused (existing): %d", stats["dbxref_reused"])
    logger.info("  dbxref_feat links created: %d", stats["feat_links_created"])
    logger.info("  dbxref_url links created : %d", stats["url_links_created"])
    logger.info("  stale links removed      : %d", stats["stale_links_removed"])
    logger.info("  orphan dbxrefs deleted   : %d", stats["orphan_dbxrefs_deleted"])
    logger.info("  features now w/o UniProt : %d", stats["features_left_without_uniprot"])
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
