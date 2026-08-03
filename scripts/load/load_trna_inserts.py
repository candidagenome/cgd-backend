#!/usr/bin/env python3
"""
Insert brand-new C. tropicalis tRNA features from a plan (subtask-1 follow-up).

BACKGROUND / why this is an INSERT (not a reclassify):
  The bulk of the C. tropicalis tRNA work was a RECLASSIFY: 185 tRNAs already
  existed in CGD mislabeled as feature_type='ORF' (they were lifted from ASM633v3
  as gene lines and swept in by the ORF loader). Those were fixed in place by
  scripts/C_tropicalis/load_trnas.py.
  That loader also REPORTED a handful of tRNAscan-SE predictions that matched no
  existing CGD feature at all -- genuinely new tRNAs absent from the database
  (verified: zero coordinate overlap with ANY org-12 feature, not just the tRNA
  tag set). The curator approved adding these as new features with systematic
  CTRG_XXXX.5-style names (the same convention used for the curated gene EFG1 =
  CTRG_00421.5). This loader inserts them.

WHAT THIS SCRIPT DOES, per planned tRNA (mirrors how the reclassified 185 + the
existing glabrata/albicans tRNAs are stored):
  1. INSERT parent FEATURE (feature_type='tRNA', gene_name t<AA>(anticodon)<n>,
     headline, dbxref CAL..........)
  2. INSERT parent genomic SEQ over the whole gene span (intron included) +
     FEAT_LOCATION (root_seq_no=contig, seq_no=the parent's own genomic seq,
     minus strand stored start>stop with strand='C')
  3. INSERT FEAT_PROPERTY feature_qualifier='Uncharacterized'
  4. INSERT exon/intron child FEATUREs (noncoding_exon [+ intron]) each with its
     own genomic SEQ, FEAT_LOCATION (seq_no=child's own seq) and FEAT_RELATIONSHIP
     'part of' rank=2 -- required by dump_gff.py's subfeature query.
  Idempotent: a planned feature whose feature_name already exists is skipped.

INPUT:
  --plan : JSON list from build_trna_insert_plan.py. Each entry:
           {organism_name, seq_source, feature_name, gene_name, headline,
            contig, begin, end, intron_begin, intron_end}
           begin/end are in tRNAscan orientation (begin>end encodes minus strand)
           in CGD contig coordinates; intron_begin/intron_end 0 if none.

Usage:
    python load_trna_inserts.py --plan ctrop_novel_trna_plan.json [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
# coordinate helpers (kept identical to scripts/C_tropicalis/load_trnas.py)
# --------------------------------------------------------------------------- #
def extract_residues(contig_seq: str, start: int, stop: int, strand: str) -> str:
    lo, hi = sorted((start, stop))
    seq = contig_seq[lo - 1:hi]
    if strand == "-":
        seq = seq.translate(COMPLEMENT)[::-1]
    return seq.upper()


def compute_segments(begin: int, end: int, i_begin: int, i_end: int
                     ) -> Tuple[str, List[Tuple[int, int, str]]]:
    """(strand, [(start, stop, subtype)...]) 5'->3'; start>stop encodes Crick."""
    strand = "+" if begin < end else "-"
    t_lo, t_hi = sorted((begin, end))
    has_intron = bool(i_begin) and bool(i_end) and i_begin != 0
    if has_intron:
        i_lo, i_hi = sorted((i_begin, i_end))
        fwd = [(t_lo, i_lo - 1, "noncoding_exon"),
               (i_lo, i_hi, "intron"),
               (i_hi + 1, t_hi, "noncoding_exon")]
        fwd = [(lo, hi, st) for lo, hi, st in fwd if lo <= hi]
    else:
        fwd = [(t_lo, t_hi, "noncoding_exon")]
    segs = [(lo, hi, st) if strand == "+" else (hi, lo, st) for lo, hi, st in fwd]
    if strand == "-":
        segs.reverse()
    return strand, segs


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
                                 start, stop, db_strand) -> None:
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
               "a": start, "b": stop, "s": db_strand, "u": ADMIN_USER})

    def _ensure_property(self, feature_no) -> None:
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feat_property
                (feature_no, property_type, property_value, source, created_by)
            VALUES (:f, 'feature_qualifier', 'Uncharacterized', 'CGD', :u)
        """), {"f": feature_no, "u": ADMIN_USER})

    def _add_child(self, parent_no, parent_name, idx, root_seq_no, residues_src,
                   start, stop, subtype) -> None:
        label = "exon" if subtype == "noncoding_exon" else "intron"
        child_name = f"{parent_name}_{label}{idx}"
        db_strand = "W" if start < stop else "C"
        res = extract_residues(residues_src, start, stop,
                               "+" if start < stop else "-")
        if self.dry:
            logger.info("[DRY RUN]   child %-22s (%s) %d..%d %s len=%d",
                        child_name, subtype, start, stop, db_strand, len(res))
            self._cal()  # keep CAL numbering in step with a real run
            return
        child_no = self._insert_feature(child_name, subtype, None, None)
        self._insert_seq_and_location(child_no, root_seq_no, res,
                                      start, stop, db_strand)
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feat_relationship
                (parent_feature_no, child_feature_no, relationship_type, rank, created_by)
            VALUES (:p, :c, 'part of', 2, :u)
        """), {"p": parent_no, "c": child_no, "u": ADMIN_USER})

    def load_one(self, spec: Dict) -> None:
        name = spec["feature_name"]
        contig = self.contigs[spec["contig"]]
        root_seq_no = contig["seq_no"]
        residues_src = contig["residues"]
        begin, end = spec["begin"], spec["end"]
        strand = "+" if begin < end else "-"
        db_strand = "W" if strand == "+" else "C"
        t_lo, t_hi = sorted((begin, end))
        # parent stored start>stop for Crick (CGD convention)
        p_start, p_stop = (t_lo, t_hi) if strand == "+" else (t_hi, t_lo)
        p_res = extract_residues(residues_src, t_lo, t_hi, strand)

        _, segs = compute_segments(begin, end,
                                   spec.get("intron_begin", 0) or 0,
                                   spec.get("intron_end", 0) or 0)

        if self.dry:
            logger.info("[DRY RUN] tRNA %-14s gene=%-10s %s:%d..%d %s len=%d "
                        "intron=%s segs=%d",
                        name, spec.get("gene_name"), spec["contig"],
                        p_start, p_stop, db_strand, len(p_res),
                        bool(spec.get("intron_begin")), len(segs))
            self._cal()  # parent CAL (property carries no dbxref)
            for idx, (s, e, subtype) in enumerate(segs, start=1):
                self._add_child(None, name, idx, root_seq_no, residues_src,
                                s, e, subtype)
            return

        parent_no = self._insert_feature(name, "tRNA", spec.get("gene_name"),
                                         spec.get("headline"))
        self._insert_seq_and_location(parent_no, root_seq_no, p_res,
                                      p_start, p_stop, db_strand)
        self._ensure_property(parent_no)
        for idx, (s, e, subtype) in enumerate(segs, start=1):
            self._add_child(parent_no, name, idx, root_seq_no, residues_src,
                            s, e, subtype)
        logger.info("inserted tRNA %-14s (feat %d) gene=%-10s %s:%d..%d %s "
                    "len=%d children=%d",
                    name, parent_no, spec.get("gene_name"), spec["contig"],
                    p_start, p_stop, db_strand, len(p_res), len(segs))


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Insert new tRNA features from a plan")
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
    logger.info("Insert %d NEW tRNA features for %s%s", len(plan), organism_name,
                " [DRY RUN]" if args.dry_run else "")
    logger.info("=" * 64)

    with SessionLocal() as session:
        for tab, col, val, desc in [
            ("FEATURE", "FEATURE_TYPE", "tRNA", "Transfer RNA"),
            ("FEATURE", "FEATURE_TYPE", "noncoding_exon", "Noncoding exon"),
            ("FEATURE", "FEATURE_TYPE", "intron", "Intron"),
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
