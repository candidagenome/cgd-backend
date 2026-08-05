#!/usr/bin/env python3
"""
Insert ncRNA features for a species from a plan (subtask-3 of the "missing
non-coding features" project).

BACKGROUND / why this is an INSERT:
  C. tropicalis (org 12) had feature_type='ncRNA' count = 0, while every sister
  species carries at least the rDNA internal transcribed spacers ITS1/ITS2, and
  C. albicans additionally has the conserved housekeeping RNAs RPR1 (RNase P RNA)
  and SCR1 (SRP RNA). None of these existed in CGD for C. tropicalis, so they are
  inserted as new features (they are not mislabeled ORFs -- verified zero overlap
  with any existing feature).

  The ITS1/ITS2 are derived exactly as the spacer gaps between the already-loaded
  rRNA subunits (18S->5.8S = ITS1, 5.8S->25S = ITS2; see load_rrnas.py subtask-2).
  RPR1/SCR1 are located by sequence homology (blastn) to the C. albicans orthologs.

WHAT THIS SCRIPT DOES, per planned ncRNA (identical storage to the sister-species
ncRNAs and to load_rrnas.py):
  1. INSERT parent FEATURE (feature_type='ncRNA', gene_name, headline, dbxref CAL)
  2. INSERT parent genomic SEQ + FEAT_LOCATION (root=contig, seq_no=own genomic
     seq; minus strand stored start>stop with strand='C')
  3. INSERT FEAT_PROPERTY feature_qualifier='Uncharacterized'
  4. INSERT one noncoding_exon child FEATURE (+ SEQ + FEAT_LOCATION + 'part of'
     rank=2)
  Idempotent: a planned feature whose feature_name already exists is skipped.

INPUT:
  --plan : JSON list from build_ncrna_plan.py. Each entry:
           {organism_name, seq_source, feature_name, gene_name, headline,
            ncrna_class, contig, start, stop, strand}  (strand 'W'/'C';
            start/stop in CGD 1-based coords on `contig`)

Usage:
    python load_ncrnas.py --plan ctrop_ncrna_plan.json [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Optional

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal  # noqa: E402

DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()

COMPLEMENT = str.maketrans("ACGTNacgtn", "TGCANtgcan")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
def extract_residues(contig_seq: str, start: int, stop: int, strand: str) -> str:
    lo, hi = sorted((start, stop))
    seq = contig_seq[lo - 1:hi]
    if strand == "C":
        seq = seq.translate(COMPLEMENT)[::-1]
    return seq.upper()


def ensure_code(session, tab, col, val, desc, dry) -> None:
    row = session.execute(text(f"""
        SELECT code_no FROM {DB_SCHEMA}.code
        WHERE tab_name=:t AND col_name=:c AND code_value=:v
    """), {"t": tab, "c": col, "v": val}).first()
    if row:
        return
    if dry:
        logger.info("[DRY RUN] would add CODE %s.%s=%s", tab, col, val)
        return
    session.execute(text(f"""
        INSERT INTO {DB_SCHEMA}.code (tab_name, col_name, code_value, description, created_by)
        VALUES (:t, :c, :v, :d, :u)
    """), {"t": tab, "c": col, "v": val, "d": desc, "u": ADMIN_USER})
    session.commit()
    logger.info("Created CODE %s.%s=%s", tab, col, val)


def get_scalar(session, sql, **kw):
    return session.execute(text(sql), kw).scalar()


def get_max_cal_id(session) -> int:
    def m(tbl):
        return session.execute(text(f"""
            SELECT MAX(TO_NUMBER(SUBSTR(dbxref_id, 4))) FROM {DB_SCHEMA}.{tbl}
            WHERE dbxref_id LIKE 'CAL%' AND REGEXP_LIKE(SUBSTR(dbxref_id, 4), '^[0-9]+$')
        """)).scalar() or 0
    return max(m("feature"), m("dbxref"))


def get_contig_map(session, organism_no: int) -> Dict[str, Dict]:
    rows = session.execute(text(f"""
        SELECT f.feature_name, s.seq_no, s.residues
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.seq s ON s.feature_no = f.feature_no
        WHERE f.organism_no = :o AND f.feature_type IN ('contig','chromosome')
          AND s.seq_type='genomic' AND s.is_seq_current='Y'
    """), {"o": organism_no}).fetchall()
    return {name: {"seq_no": seq_no,
                   "residues": str(res) if res is not None else ""}
            for name, seq_no, res in rows}


# --------------------------------------------------------------------------- #
class Inserter:
    def __init__(self, session, organism_no, genome_version_no, seq_source,
                 contigs_by_name, next_cal, dry):
        self.s = session
        self.org = organism_no
        self.gv = genome_version_no
        self.src = seq_source
        self.contigs = contigs_by_name
        self.next_cal = next_cal
        self.dry = dry

    def _cal(self) -> str:
        x = f"CAL{self.next_cal[0]:010d}"
        self.next_cal[0] += 1
        return x

    def _feature_no(self, name: str) -> Optional[int]:
        row = self.s.execute(text(
            f"SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name=:n"),
            {"n": name}).first()
        return row[0] if row else None

    def exists(self, name: str) -> bool:
        return self._feature_no(name) is not None

    def _insert_feature(self, name, ftype, gene_name, headline) -> Optional[int]:
        dbxref = self._cal()
        if self.dry:
            return None
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feature
                (organism_no, feature_name, dbxref_id, feature_type, gene_name,
                 headline, source, created_by)
            VALUES (:o, :n, :x, :ft, :g, :h, 'CGD', :u)
        """), {"o": self.org, "n": name, "x": dbxref, "ft": ftype,
               "g": gene_name, "h": (headline[:240] if headline else None),
               "u": ADMIN_USER})
        return self._feature_no(name)

    def _insert_seq_and_location(self, feature_no, root_seq_no, residues,
                                 start, stop, strand) -> None:
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.seq
                (feature_no, genome_version_no, seq_version, seq_type, source,
                 is_seq_current, seq_length, residues, created_by)
            VALUES (:f, :gv, SYSDATE, 'genomic', :src, 'Y', :len, :res, :u)
        """), {"f": feature_no, "gv": self.gv, "src": self.src,
               "len": len(residues), "res": residues, "u": ADMIN_USER})
        seq_no = self.s.execute(text(f"""
            SELECT seq_no FROM {DB_SCHEMA}.seq
            WHERE feature_no=:f AND seq_type='genomic' AND is_seq_current='Y'
        """), {"f": feature_no}).scalar()
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feat_location
                (feature_no, root_seq_no, seq_no, coord_version, start_coord,
                 stop_coord, strand, is_loc_current, created_by)
            VALUES (:f, :r, :sq, SYSDATE, :a, :b, :s, 'Y', :u)
        """), {"f": feature_no, "r": root_seq_no, "sq": seq_no,
               "a": start, "b": stop, "s": strand, "u": ADMIN_USER})

    def _ensure_property(self, feature_no) -> None:
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feat_property
                (feature_no, property_type, property_value, source, created_by)
            VALUES (:f, 'feature_qualifier', 'Uncharacterized', 'CGD', :u)
        """), {"f": feature_no, "u": ADMIN_USER})

    def load_one(self, spec: Dict) -> None:
        name = spec["feature_name"]
        contig = self.contigs[spec["contig"]]
        root_seq_no = contig["seq_no"]
        strand = spec["strand"]
        a, b = spec["start"], spec["stop"]
        if strand == "C":
            start, stop = max(a, b), min(a, b)
        else:
            start, stop = min(a, b), max(a, b)
        res = extract_residues(contig["residues"], a, b, strand)

        if self.dry:
            logger.info("[DRY RUN] ncRNA %-12s %-6s %s:%d..%d %s len=%d gene=%s",
                        name, spec.get("ncrna_class", ""), spec["contig"],
                        start, stop, strand, len(res), spec.get("gene_name"))
            self._cal()          # parent CAL
            self._cal()          # child CAL (keeps numbering in step)
            return

        parent_no = self._insert_feature(name, "ncRNA", spec.get("gene_name"),
                                         spec.get("headline"))
        self._insert_seq_and_location(parent_no, root_seq_no, res, start, stop, strand)
        self._ensure_property(parent_no)

        child_name = f"{name}_exon1"
        child_no = self._insert_feature(child_name, "noncoding_exon", None, None)
        self._insert_seq_and_location(child_no, root_seq_no, res, start, stop, strand)
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feat_relationship
                (parent_feature_no, child_feature_no, relationship_type, rank, created_by)
            VALUES (:p, :c, 'part of', 2, :u)
        """), {"p": parent_no, "c": child_no, "u": ADMIN_USER})
        logger.info("inserted ncRNA %-12s (feat %d) gene=%-6s %s:%d..%d %s len=%d",
                    name, parent_no, spec.get("gene_name"), spec["contig"],
                    start, stop, strand, len(res))


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Insert ncRNA features from a plan")
    ap.add_argument("--plan", required=True, type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    plan = json.loads(args.plan.read_text())
    if not plan:
        logger.error("empty plan")
        sys.exit(1)

    organism_name = plan[0]["organism_name"]
    seq_source = plan[0]["seq_source"]
    logger.info("=" * 64)
    logger.info("Insert %d ncRNA features for %s%s", len(plan), organism_name,
                " [DRY RUN]" if args.dry_run else "")
    logger.info("=" * 64)

    with SessionLocal() as session:
        for tab, col, val, desc in [
            ("FEATURE", "FEATURE_TYPE", "ncRNA", "Non-coding RNA"),
            ("FEATURE", "FEATURE_TYPE", "noncoding_exon", "Noncoding exon"),
            ("SEQ", "SEQ_TYPE", "genomic", "Genomic DNA sequence"),
            ("SEQ", "SOURCE", seq_source, f"Source for {organism_name} data"),
            ("FEAT_PROPERTY", "PROPERTY_TYPE", "feature_qualifier", "Feature qualifier"),
        ]:
            ensure_code(session, tab, col, val, desc, args.dry_run)

        organism_no = get_scalar(session,
            f"SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_name=:n",
            n=organism_name)
        genome_version_no = get_scalar(session,
            f"SELECT genome_version_no FROM {DB_SCHEMA}.genome_version "
            f"WHERE organism_no=:o AND is_ver_current='Y'", o=organism_no)
        contigs = get_contig_map(session, organism_no)
        logger.info("organism_no=%d genome_version_no=%d contigs=%d",
                    organism_no, genome_version_no, len(contigs))

        next_cal = [get_max_cal_id(session) + 1]
        logger.info("next CAL id: CAL%010d", next_cal[0])
        ins = Inserter(session, organism_no, genome_version_no, seq_source,
                       contigs, next_cal, args.dry_run)

        inserted = skipped = 0
        for spec in plan:
            if spec["contig"] not in contigs:
                logger.error("contig %s not found for %s", spec["contig"],
                             spec["feature_name"])
                sys.exit(1)
            if ins.exists(spec["feature_name"]):
                logger.info("SKIP %s (already exists)", spec["feature_name"])
                skipped += 1
                continue
            ins.load_one(spec)
            inserted += 1
            if not args.dry_run:
                session.commit()

        if not args.dry_run:
            session.commit()

        logger.info("=" * 64)
        logger.info("planned:  %d", len(plan))
        logger.info("inserted: %d", inserted)
        logger.info("skipped:  %d", skipped)
        logger.info("=" * 64)


if __name__ == "__main__":
    main()
