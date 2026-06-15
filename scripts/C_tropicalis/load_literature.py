#!/usr/bin/env python3
"""
Load curated C. tropicalis literature into CGD.

Mirrors the tested LitReviewCurationService insert logic (reference + abstract,
curation_status ref_property, refprop_feat gene links) and additionally writes
the 'Candida tropicalis' literature_topic ref_property.

Two input sets (from ctrop_build_loadsets.py):
  - literal  -> brand-new papers get 'High Priority' + CTRG gene links + topic
  - keyword  -> brand-new papers get 'Done:Abstract curated, full text not
                curated' + topic (no gene links)

Papers already present in the REFERENCE table are NOT re-created. For those we
only add the 'Candida tropicalis' literature_topic if it is missing (their
curation_status and gene links are left untouched).

Usage:
    python load_literature.py [--set literal|keyword|all] [--dry-run]
                              [--data-dir DIR] [--created-by USERID] [--limit N]

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA:    Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv
from sqlalchemy import bindparam, text
from sqlalchemy.exc import IntegrityError

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402

# Configuration / constants (mirror litreview_curation_service)
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DEFAULT_DATA_DIR = "/data/C_tropicalis"

REF_SOURCE = "Curator PubMed reference"   # Coded value in CODE table
REF_STATUS = "Published"
REF_PDF_STATUS = "N"
PROP_SOURCE = "CGD"
STATUS_TYPE = "curation_status"
TOPIC_TYPE = "literature_topic"
TOPIC_VALUE = "Candida tropicalis"
HIGH_PRIORITY = "High Priority"
KEYWORD_STATUS = "Done:Abstract curated, full text not curated"
ORGANISM_ABBREV = "C_tropicalis"

LITERAL_FILE = "c_tropicalis_load_literal_highpriority.tsv"
KEYWORD_FILE = "c_tropicalis_load_keyword_mesh.tsv"
ABSTRACT_FILE = "c_tropicalis_pubmed_papers.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------
def load_abstracts(path: Path) -> Dict[int, str]:
    """Map pmid -> abstract text from the PubMed jsonl pull."""
    abstracts: Dict[int, str] = {}
    if not path.exists():
        logger.warning("Abstract file not found: %s (abstracts will be skipped)", path)
        return abstracts
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            pmid = int(d["pmid"])
            text_ = (d.get("abstract") or "").strip()
            if text_:
                abstracts[pmid] = text_
    return abstracts


def parse_literal(path: Path) -> Dict[int, dict]:
    """Group literal rows by pmid; collect the CTRG systematic ids per paper."""
    papers: Dict[int, dict] = {}
    with open(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            pmid = int(row["pmid"])
            p = papers.setdefault(pmid, {
                "pmid": pmid,
                "year": row.get("year", "").strip(),
                "journal": row.get("journal", "").strip(),
                "title": row.get("title", "").strip(),
                "status": HIGH_PRIORITY,
                "features": [],
            })
            ctrg = (row.get("ct_systematic") or "").strip()
            if ctrg and ctrg not in p["features"]:
                p["features"].append(ctrg)
    return papers


def parse_keyword(path: Path) -> Dict[int, dict]:
    """One row per paper for the keyword/MeSH set."""
    papers: Dict[int, dict] = {}
    with open(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            pmid = int(row["pmid"])
            papers[pmid] = {
                "pmid": pmid,
                "year": row.get("year", "").strip(),
                "journal": row.get("journal", "").strip(),
                "title": row.get("title", "").strip(),
                "status": KEYWORD_STATUS,
                "features": [],
            }
    return papers


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
class Loader:
    def __init__(self, db, created_by: str, dry_run: bool):
        self.db = db
        self.created_by = created_by[:12]
        self.dry_run = dry_run
        self.feature_map: Dict[str, int] = {}
        self.stats = {
            "refs_created": 0,
            "abstracts_added": 0,
            "status_set": 0,
            "links_added": 0,
            "topics_added": 0,
            "existing_topic_added": 0,
            "skipped_existing": 0,
            "missing_features": 0,
            "errors": 0,
        }

    def validate_created_by(self) -> None:
        """Fail fast if created_by is not a real dbuser.

        The MULTI.CHECKUSER trigger rejects any created_by that is absent from
        the dbuser table, rolling back the insert. Because Oracle sequences are
        non-transactional, such rollbacks burn sequence values (PK gaps). We
        therefore verify the userid up front and adopt the canonical casing
        stored in dbuser so the trigger matches exactly.
        """
        row = self.db.execute(
            text(f"SELECT userid FROM {DB_SCHEMA}.dbuser WHERE UPPER(userid) = :u"),
            {"u": self.created_by.upper()},
        ).first()
        if not row:
            valid = [
                r[0]
                for r in self.db.execute(
                    text(f"SELECT userid FROM {DB_SCHEMA}.dbuser ORDER BY userid")
                ).fetchall()
            ]
            raise SystemExit(
                f"created_by '{self.created_by}' is not a valid dbuser "
                f"(MULTI.CHECKUSER would reject every insert). "
                f"Valid userids: {', '.join(valid)}"
            )
        if row[0] != self.created_by:
            logger.info("Normalized created_by '%s' -> '%s'", self.created_by, row[0])
        self.created_by = row[0]
        logger.info("created_by validated against dbuser: %s", self.created_by)

    def load_feature_map(self) -> None:
        rows = self.db.execute(
            text(
                f"SELECT f.feature_name, f.feature_no "
                f"FROM {DB_SCHEMA}.feature f "
                f"JOIN {DB_SCHEMA}.organism o ON f.organism_no = o.organism_no "
                f"WHERE o.organism_abbrev = :oa"
            ),
            {"oa": ORGANISM_ABBREV},
        ).fetchall()
        self.feature_map = {r[0]: r[1] for r in rows}
        logger.info("Loaded %d %s features", len(self.feature_map), ORGANISM_ABBREV)

    def reference_no_for(self, pmid: int) -> Optional[int]:
        r = self.db.execute(
            text(f"SELECT reference_no FROM {DB_SCHEMA}.reference WHERE pubmed = :p"),
            {"p": pmid},
        ).first()
        return r[0] if r else None

    def citation_exists(self, citation: str) -> bool:
        r = self.db.execute(
            text(f"SELECT 1 FROM {DB_SCHEMA}.reference WHERE citation = :c"),
            {"c": citation},
        ).first()
        return r is not None

    def build_citation(self, paper: dict) -> str:
        parts = [p for p in (paper["title"], paper["journal"]) if p]
        base = " ".join(parts)
        if paper["year"]:
            base = f"{base} ({paper['year']})"
        base = base[:480]
        # Citation column is UNIQUE; disambiguate on collision.
        if self.citation_exists(base):
            suffix = f" (PMID:{paper['pmid']})"
            base = base[: 480 - len(suffix)] + suffix
        return base

    def topic_exists(self, reference_no: int) -> bool:
        r = self.db.execute(
            text(
                f"SELECT 1 FROM {DB_SCHEMA}.ref_property "
                f"WHERE reference_no = :rn AND property_type = :pt "
                f"AND property_value = :pv AND source = :src"
            ),
            {"rn": reference_no, "pt": TOPIC_TYPE, "pv": TOPIC_VALUE, "src": PROP_SOURCE},
        ).first()
        return r is not None

    def add_topic(self, reference_no: int, existing_ref: bool) -> None:
        if self.topic_exists(reference_no):
            return
        if self.dry_run:
            logger.info("  [dry-run] would add topic '%s'", TOPIC_VALUE)
        else:
            seq = self.db.execute(
                text(f"SELECT {DB_SCHEMA}.ref_property_seq.NEXTVAL FROM dual")
            ).scalar()
            self.db.execute(
                text(
                    f"INSERT INTO {DB_SCHEMA}.ref_property "
                    f"(ref_property_no, reference_no, source, property_type, "
                    f"property_value, created_by) VALUES "
                    f"(:n, :rn, :src, :pt, :pv, :cb)"
                ),
                {"n": seq, "rn": reference_no, "src": PROP_SOURCE,
                 "pt": TOPIC_TYPE, "pv": TOPIC_VALUE, "cb": self.created_by},
            )
        self.stats["topics_added"] += 1
        if existing_ref:
            self.stats["existing_topic_added"] += 1

    def create_reference(self, paper: dict, abstract: Optional[str]) -> int:
        citation = self.build_citation(paper)
        year = int(paper["year"]) if paper["year"].isdigit() else 0
        if self.dry_run:
            logger.info(
                "  [dry-run] would create reference PMID:%s year=%s cite=%r",
                paper["pmid"], year, citation[:80],
            )
            self.stats["refs_created"] += 1
            if abstract:
                self.stats["abstracts_added"] += 1
            return -1
        ref_no = self.db.execute(
            text(f"SELECT {DB_SCHEMA}.reference_seq.NEXTVAL FROM dual")
        ).scalar()
        self.db.execute(
            text(
                f"INSERT INTO {DB_SCHEMA}.reference "
                f"(reference_no, pubmed, source, status, pdf_status, dbxref_id, "
                f"citation, year, created_by) VALUES "
                f"(:rn, :pm, :src, :st, :pdf, :dx, :cit, :yr, :cb)"
            ),
            {"rn": ref_no, "pm": paper["pmid"], "src": REF_SOURCE,
             "st": REF_STATUS, "pdf": REF_PDF_STATUS,
             "dx": f"PMID:{paper['pmid']}", "cit": citation,
             "yr": year, "cb": self.created_by},
        )
        self.stats["refs_created"] += 1
        if abstract:
            self.db.execute(
                text(
                    f"INSERT INTO {DB_SCHEMA}.abstract (reference_no, abstract) "
                    f"VALUES (:rn, :ab)"
                ),
                {"rn": ref_no, "ab": abstract[:4000]},
            )
            self.stats["abstracts_added"] += 1
        return ref_no

    def set_status(self, reference_no: int, status: str) -> int:
        if self.dry_run:
            logger.info("  [dry-run] would set status '%s'", status)
            self.stats["status_set"] += 1
            return -1
        seq = self.db.execute(
            text(f"SELECT {DB_SCHEMA}.ref_property_seq.NEXTVAL FROM dual")
        ).scalar()
        self.db.execute(
            text(
                f"INSERT INTO {DB_SCHEMA}.ref_property "
                f"(ref_property_no, reference_no, source, property_type, "
                f"property_value, created_by) VALUES "
                f"(:n, :rn, :src, :pt, :pv, :cb)"
            ),
            {"n": seq, "rn": reference_no, "src": PROP_SOURCE,
             "pt": STATUS_TYPE, "pv": status, "cb": self.created_by},
        )
        self.stats["status_set"] += 1
        return seq

    def link_feature(self, ref_property_no: int, ctrg: str) -> None:
        feature_no = self.feature_map.get(ctrg)
        if not feature_no:
            logger.warning("  feature not found for %s", ctrg)
            self.stats["missing_features"] += 1
            return
        if self.dry_run:
            logger.info("  [dry-run] would link %s (feature_no=%s)", ctrg, feature_no)
            self.stats["links_added"] += 1
            return
        seq = self.db.execute(
            text(f"SELECT {DB_SCHEMA}.refprop_feat_seq.NEXTVAL FROM dual")
        ).scalar()
        self.db.execute(
            text(
                f"INSERT INTO {DB_SCHEMA}.refprop_feat "
                f"(refprop_feat_no, ref_property_no, feature_no, created_by) "
                f"VALUES (:n, :rp, :fn, :cb)"
            ),
            {"n": seq, "rp": ref_property_no, "fn": feature_no, "cb": self.created_by},
        )
        self.stats["links_added"] += 1

    def process(self, paper: dict, abstract: Optional[str]) -> None:
        pmid = paper["pmid"]
        existing = self.reference_no_for(pmid)
        if existing:
            # Decision: topic-only for existing references.
            self.add_topic(existing, existing_ref=True)
            self.stats["skipped_existing"] += 1
            if not self.dry_run:
                self.db.commit()
            return

        # Brand-new paper: full treatment.
        ref_no = self.create_reference(paper, abstract)
        rp_no = self.set_status(ref_no, paper["status"])
        for ctrg in paper["features"]:
            self.link_feature(rp_no, ctrg)
        self.add_topic(ref_no, existing_ref=False)
        if not self.dry_run:
            self.db.commit()


def main() -> int:
    ap = argparse.ArgumentParser(description="Load C. tropicalis literature into CGD")
    ap.add_argument("--set", choices=["literal", "keyword", "all"], default="all")
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    ap.add_argument("--created-by", default="ctroplit")
    ap.add_argument("--limit", type=int, default=0, help="process at most N papers per set")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    abstracts = load_abstracts(data_dir / ABSTRACT_FILE)

    sets: List[Dict[int, dict]] = []
    if args.set in ("literal", "all"):
        lit = parse_literal(data_dir / LITERAL_FILE)
        logger.info("literal: %d papers", len(lit))
        sets.append(lit)
    if args.set in ("keyword", "all"):
        kw = parse_keyword(data_dir / KEYWORD_FILE)
        logger.info("keyword: %d papers", len(kw))
        sets.append(kw)

    db = SessionLocal()
    loader = Loader(db, created_by=args.created_by, dry_run=args.dry_run)
    loader.validate_created_by()
    loader.load_feature_map()

    try:
        for papers in sets:
            count = 0
            for pmid in sorted(papers):
                if args.limit and count >= args.limit:
                    break
                count += 1
                try:
                    loader.process(papers[pmid], abstracts.get(pmid))
                except IntegrityError as e:
                    db.rollback()
                    loader.stats["errors"] += 1
                    logger.error("PMID %s integrity error: %s", pmid, str(e)[:200])
                except Exception as e:  # noqa: BLE001
                    db.rollback()
                    loader.stats["errors"] += 1
                    logger.error("PMID %s failed: %s", pmid, str(e)[:200])
    finally:
        db.close()

    logger.info("==== summary (%s) ====", "DRY-RUN" if args.dry_run else "COMMITTED")
    for k, v in loader.stats.items():
        logger.info("  %-20s %d", k, v)
    return 0


if __name__ == "__main__":
    sys.exit(main())
