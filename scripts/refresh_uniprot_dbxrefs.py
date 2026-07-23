#!/usr/bin/env python3
"""
Refresh UniProt (Swiss-Prot / TrEMBL) cross-references for CGD species.

Motivation
----------
GOA reported that many CGD IDs in the GPI download could not be mapped: the
UniProtKB accessions CGD emitted were obsolete.  Checked against the UniProt
REST API they return entryType "Inactive" with inactiveReason DELETED ("Not
part of a reference proteome" / "Redundant sequence").  They were loaded from
EBI years ago and UniProt has since retired them, so GOA (which maps against
current UniProt) rejects them.  The failures were dominated by C. auris
(~2,300) with smaller tails in C. glabrata and C. albicans.

What the refresh does (per species, idempotent)
------------------------------------------------
  * REMOVE - drop EBI UniProt dbxref links whose accession is genuinely DELETED
             in UniProt.  A dbxref row left with no remaining feature links (and
             its dbxref_url rows) is deleted too, so it stops appearing in
             gp2protein / GPI output.
  * ADD    - for features that are left with NO active UniProt link by the
             removal (i.e. their only accession(s) were deleted), restore a
             current cross-reference by matching the feature's protein sequence
             to the current UniProt reference proteome.  Existing dbxref /
             dbxref_feat / dbxref_url rows are reused.

Two design choices that matter
------------------------------
1. Deletion is decided per accession via the UniProt entry's own status, NOT by
   "absent from the reference proteome".  C. albicans SC5314 has several UniProt
   proteomes (A21 reference + A22 haplotypes) and its accessions are spread
   across taxon ids, so many valid, active accessions are absent from the single
   reference proteome; removing those would destroy good cross-references.  Only
   entries that are actually Inactive/DELETED are removed.  (A secondary
   accession that now redirects to an active entry resolves to active and is
   kept.)

2. Matching for the ADD step is by exact protein-sequence identity, never locus
   tag.  For C. auris CGD's 6-digit RefSeq tag B9J08_###### and UniProt's
   5-digit GenBank tag B9J08_##### are independent numbering schemes (a
   6->5-digit transform scored 0/21 against the reviewed entries carrying both),
   so tag matching would mis-map thousands of genes.

make_gpi.get_uniprot_id() emits the lowest accession string among a feature's
linked UniProt dbxrefs, so a deleted accession sorting below a good one would
keep being emitted -- hence the removal.  Pass --keep-stale to skip removal
(diagnostic only).

dbxref conventions match the existing UniProt dbxrefs of the other species
(source 'EBI'; SwissProt->url_no 44473, TrEMBL->url_no 44475).

Usage:
    python scripts/refresh_uniprot_dbxrefs.py C_auris_B8441 --dry-run
    python scripts/refresh_uniprot_dbxrefs.py C_glabrata_CBS138
    python scripts/refresh_uniprot_dbxrefs.py --all --dry-run --report-unrestored

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA:    Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")

# Per-species config: CGD organism_abbrev -> NCBI taxon + UniProt *reference*
# proteome id (used only for the ADD/sequence-match step).  Proteome ids were
# confirmed against the UniProt proteomes API (proteomeType "Reference
# proteome", matching CGD's strain/assembly lineage).
SPECIES_CONFIGS = {
    "C_auris_B8441": {"taxon_id": 498019, "proteome_id": "UP000230249"},
    "C_glabrata_CBS138": {"taxon_id": 284593, "proteome_id": "UP000002428"},
    "C_albicans_SC5314": {"taxon_id": 237561, "proteome_id": "UP000000559"},
}

# CGD stores the protein sequence in MULTI.SEQ; species use either the lowercase
# 'protein' or mixed-case 'Protein' seq_type (per sequence_service convention).
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
UNIPROT_ENTRY_URL = "https://rest.uniprot.org/uniprotkb"
STATUS_WORKERS = 16

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# UniProt: reference proteome (FASTA) + per-accession deletion status          #
# --------------------------------------------------------------------------- #
def fetch_proteome_fasta(proteome_id: str) -> str:
    """Fetch the current UniProt reference proteome as FASTA (canonical only)."""
    query = urllib.parse.urlencode({
        "query": f"proteome:{proteome_id}",
        "format": "fasta",
        "includeIsoform": "false",
    })
    url = f"{UNIPROT_STREAM_URL}?{query}"
    logger.info("Fetching UniProt proteome %s (FASTA)", proteome_id)
    with urllib.request.urlopen(url, timeout=300) as response:
        return response.read().decode("utf-8")


def is_accession_deleted(accession: str) -> bool | None:
    """Return True if the accession is Inactive/DELETED in UniProt.

    The per-accession endpoint follows merges: a secondary accession that now
    redirects to an active entry reports as active and returns False.  Returns
    None on lookup error (treated as "keep" by the caller, never removed).
    """
    url = (f"{UNIPROT_ENTRY_URL}/{urllib.parse.quote(accession)}"
           f"?fields=accession&format=json")
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            d = json.load(r)
    except urllib.error.HTTPError as e:
        # 400/404 for a genuinely unknown accession is rare here (these came
        # from UniProt originally); be conservative and do not remove.
        logger.debug("status lookup HTTP %s for %s", e.code, accession)
        return None
    except Exception as e:  # noqa: BLE001
        logger.debug("status lookup failed for %s: %s", accession, e)
        return None
    if d.get("entryType") != "Inactive":
        return False
    reason = (d.get("inactiveReason") or {}).get("inactiveReasonType")
    return reason == "DELETED"


def classify_deleted(accessions: set[str]) -> set[str]:
    """Return the subset of accessions that are genuinely DELETED in UniProt."""
    if not accessions:
        return set()
    logger.info("Checking UniProt status of %d candidate accessions...",
                len(accessions))
    accs = list(accessions)
    deleted: set[str] = set()
    with ThreadPoolExecutor(max_workers=STATUS_WORKERS) as ex:
        for acc, result in zip(accs, ex.map(is_accession_deleted, accs)):
            if result is True:
                deleted.add(acc)
    return deleted


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
            parts = line[1:].split("|")
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
def get_species_feat_links(
    session, organism_abbrev: str
) -> list[tuple[int, int, str, str]]:
    """EBI UniProt (dbxref_no, feature_no, dbxref_type, dbxref_id) for a species."""
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
        {"abbrev": organism_abbrev, "src": SOURCE},
    ).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def get_protein_seqs_for_features(
    session, feature_nos: set[int]
) -> dict[int, list[str]]:
    """Map feature_no -> list of distinct normalised protein sequences.

    A feature may have more than one current protein seq row; all are returned
    so the ADD step can match on any of them.
    """
    out: dict[int, list[str]] = {}
    if not feature_nos:
        return out
    types_clause = ", ".join(f"'{t}'" for t in PROTEIN_SEQ_TYPES)
    ids = list(feature_nos)
    for i in range(0, len(ids), 900):  # stay under the Oracle IN-list limit
        chunk = ids[i:i + 900]
        binds = {f"f{j}": v for j, v in enumerate(chunk)}
        in_clause = ", ".join(f":{k}" for k in binds)
        rows = session.execute(
            text(f"""
                SELECT s.feature_no, s.residues
                FROM {DB_SCHEMA}.seq s
                WHERE s.feature_no IN ({in_clause})
                  AND s.seq_type IN ({types_clause})
                  AND s.is_seq_current = 'Y'
            """),
            binds,
        ).fetchall()
        for feature_no, residues in rows:
            seq = normalize_seq(str(residues or ""))
            if not seq:
                continue
            seqs = out.setdefault(feature_no, [])
            if seq not in seqs:
                seqs.append(seq)
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
# Refresh (one species)                                                        #
# --------------------------------------------------------------------------- #
def refresh_species(session, organism_abbrev: str, config: dict, created_by: str,
                    dry_run: bool, keep_stale: bool,
                    report_unrestored: bool) -> dict:
    stats = {
        "existing_links": 0,
        "candidates_checked": 0,
        "accessions_deleted": 0,
        "accessions_active_kept": 0,
        "stale_links_removed": 0,
        "orphan_dbxrefs_deleted": 0,
        "features_broken": 0,
        "features_restored": 0,
        "features_unrestored": 0,
        "swissprot_created": 0,
        "trembl_created": 0,
        "dbxref_reused": 0,
        "feat_links_created": 0,
        "url_links_created": 0,
    }

    species_links = get_species_feat_links(session, organism_abbrev)
    stats["existing_links"] = len(species_links)

    # ---- classify which linked accessions are genuinely deleted --------------
    # Everything present in the reference proteome is active by definition; only
    # the remainder needs a per-accession status check.
    proteome_records = parse_proteome_fasta(fetch_proteome_fasta(config["proteome_id"]))
    seq_index = build_seq_index(proteome_records)
    ref_accessions = {r["accession"] for r in proteome_records}

    linked_accs = {acc for _dx, _fn, _dt, acc in species_links}
    candidates = {a for a in linked_accs if a not in ref_accessions}
    stats["candidates_checked"] = len(candidates)
    deleted_accs = set() if keep_stale else classify_deleted(candidates)
    stats["accessions_deleted"] = len(deleted_accs)
    stats["accessions_active_kept"] = len(candidates) - len(deleted_accs)
    logger.info(
        "[%s] %d links, %d accns outside ref proteome, %d DELETED, %d active-kept",
        organism_abbrev, len(species_links), len(candidates),
        len(deleted_accs), stats["accessions_active_kept"],
    )

    # ---- REMOVE deleted links ------------------------------------------------
    touched_dbxref_nos: set[int] = set()
    features_with_active: set[int] = set()
    features_touched_by_removal: set[int] = set()
    for dbxref_no, feature_no, _dtype, accession in species_links:
        if accession in deleted_accs:
            features_touched_by_removal.add(feature_no)
            if not dry_run:
                delete_dbxref_feat(session, dbxref_no, feature_no)
            stats["stale_links_removed"] += 1
            touched_dbxref_nos.add(dbxref_no)
        else:
            features_with_active.add(feature_no)

    if not keep_stale:
        for dbxref_no in touched_dbxref_nos:
            if dry_run:
                remaining = count_feat_links(session, dbxref_no)
                removed_here = sum(
                    1 for dx, _fn, _dt, acc in species_links
                    if dx == dbxref_no and acc in deleted_accs
                )
                if remaining - removed_here <= 0:
                    stats["orphan_dbxrefs_deleted"] += 1
                continue
            if count_feat_links(session, dbxref_no) == 0:
                delete_orphan_dbxref(session, dbxref_no)
                stats["orphan_dbxrefs_deleted"] += 1

    # ---- ADD (gap-fill): restore UniProt to features left with no active link
    broken_features = features_touched_by_removal - features_with_active
    stats["features_broken"] = len(broken_features)

    existing_dbxrefs = get_existing_uniprot_dbxrefs(session)
    existing_url_links = get_existing_url_links(session)
    existing_feat_links = {(dx, fn) for dx, fn, _dt, _did in species_links}
    seqs_by_feature = get_protein_seqs_for_features(session, broken_features)

    unrestored: list[int] = []
    for feature_no in broken_features:
        matches: list[dict] = []
        seen_acc: set[str] = set()
        for seq in seqs_by_feature.get(feature_no, []):
            for rec in seq_index.get(seq, []):
                if rec["accession"] not in seen_acc:
                    seen_acc.add(rec["accession"])
                    matches.append(rec)
        if not matches:
            stats["features_unrestored"] += 1
            if report_unrestored:
                unrestored.append(feature_no)
            continue
        stats["features_restored"] += 1

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

    if report_unrestored and unrestored:
        logger.info("[%s] features left without UniProt after cleanup (%d): %s",
                    organism_abbrev, len(unrestored), sorted(unrestored))

    return stats


def log_summary(organism_abbrev: str, stats: dict) -> None:
    logger.info("-" * 60)
    logger.info("Summary [%s]:", organism_abbrev)
    logger.info("  existing EBI UniProt links : %d", stats["existing_links"])
    logger.info("  accns checked (non-ref)    : %d", stats["candidates_checked"])
    logger.info("  accns genuinely DELETED    : %d", stats["accessions_deleted"])
    logger.info("  accns active (kept)        : %d", stats["accessions_active_kept"])
    logger.info("  stale links removed        : %d", stats["stale_links_removed"])
    logger.info("  orphan dbxrefs deleted     : %d", stats["orphan_dbxrefs_deleted"])
    logger.info("  features broken by removal : %d", stats["features_broken"])
    logger.info("  features restored (seq)    : %d", stats["features_restored"])
    logger.info("  features left w/o UniProt  : %d", stats["features_unrestored"])
    logger.info("  SwissProt dbxrefs created  : %d", stats["swissprot_created"])
    logger.info("  TrEMBL dbxrefs created     : %d", stats["trembl_created"])
    logger.info("  dbxrefs reused (existing)  : %d", stats["dbxref_reused"])
    logger.info("  dbxref_feat links created  : %d", stats["feat_links_created"])
    logger.info("  dbxref_url links created   : %d", stats["url_links_created"])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh CGD UniProt dbxrefs: remove deleted accessions and "
                    "restore current ones by protein-sequence identity"
    )
    parser.add_argument(
        "strain", nargs="*",
        help="One or more organism abbrevs (%s)" % ", ".join(SPECIES_CONFIGS),
    )
    parser.add_argument(
        "--all", action="store_true", help="Refresh all configured species",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without modifying the database",
    )
    parser.add_argument(
        "--keep-stale", action="store_true",
        help="Diagnostic only: skip the UniProt status check and remove nothing "
             "(also disables the ADD step, which only fills removal gaps)",
    )
    parser.add_argument(
        "--report-unrestored", action="store_true",
        help="List features left without a UniProt link after cleanup",
    )
    parser.add_argument(
        "--created-by", default=os.getenv("ADMIN_USER", "SWENG").upper()[:12],
        help="created_by value for new rows (default: SWENG / $ADMIN_USER)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.all:
        strains = list(SPECIES_CONFIGS)
    elif args.strain:
        unknown = [s for s in args.strain if s not in SPECIES_CONFIGS]
        if unknown:
            logger.error("Unknown strain(s) %s; choose from: %s",
                         unknown, ", ".join(SPECIES_CONFIGS))
            return 1
        strains = list(dict.fromkeys(args.strain))  # de-dupe, keep order
    else:
        parser.print_help()
        return 1

    logger.info("=" * 60)
    logger.info("Refresh CGD UniProt dbxrefs")
    logger.info("Species: %s", ", ".join(strains))
    if args.dry_run:
        logger.info("[DRY RUN] no database changes will be committed")
    if args.keep_stale:
        logger.info("[KEEP-STALE] no removals, no additions (diagnostic)")
    logger.info("created_by=%s  started=%s", args.created_by, datetime.now())
    logger.info("=" * 60)

    all_stats: dict[str, dict] = {}
    try:
        with SessionLocal() as session:
            for strain in strains:
                all_stats[strain] = refresh_species(
                    session, strain, SPECIES_CONFIGS[strain], args.created_by,
                    args.dry_run, args.keep_stale, args.report_unrestored,
                )
            if args.dry_run:
                session.rollback()
            else:
                session.commit()
                logger.info("Transaction committed")
    except Exception as e:  # noqa: BLE001
        logger.exception("Refresh failed: %s", e)
        return 1

    logger.info("=" * 60)
    for strain in strains:
        log_summary(strain, all_stats[strain])
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
