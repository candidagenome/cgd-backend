#!/usr/bin/env python3
"""
Reclassify + enrich C. tropicalis tRNA features (subtask-1).

BACKGROUND / why this is a reclassify (not an insert):
  The loaded C. tropicalis assembly (GCA_013177555.1) is unannotated, so the
  CTRG_* gene models in CGD were lifted over (Liftoff) from the annotated
  assembly ASM633v3 (GCF_000006335.3). That ORF loader typed EVERY GFF 'gene'
  line as feature_type='ORF', so the 186 tRNA genes were swept in as ORFs:
  185 of them already exist in CGD as feature_type='ORF' with correct CP-contig
  coordinates and a genomic (not protein) sequence, but no gene_name / headline
  / tRNA modeling. 1 tRNA (CTRG_06445) is absent entirely.

  So the fix is to RECLASSIFY those 185 ORF rows to 'tRNA' and enrich them,
  rather than insert new features (a fresh insert would collide on feature_name,
  which is unique DB-wide).

WHAT THIS SCRIPT DOES, per mislabeled tRNA locus:
  1. UPDATE feature SET feature_type='tRNA', gene_name='tX(anticodon)N',
     headline='tRNA-Xxx, predicted by tRNAscan-SE; NNN anticodon'
  2. INSERT FEAT_PROPERTY feature_qualifier='Uncharacterized' (if absent)
  3. INSERT one noncoding_exon child FEATURE (+ FEAT_LOCATION + genomic SEQ +
     FEAT_RELATIONSHIP 'part of'); multi-exon (introned) tRNAs get exon+intron
     children. The parent's existing FEAT_LOCATION and genomic SEQ are left
     as-is (already correct).

INPUTS:
  --asm633-gff : ASM633v3 GFF (GCF_000006335.3_ASM633v3_genomic.gff.gz).
                 Authoritative source of the CTRG_ tRNA locus-tag set + isotype.
  --trnascan   : tRNAscan-SE tabular (.out) run on the CP-accession assembly
                 (GCA_013177555.1). Supplies anticodon + intron structure, and
                 is matched to each existing CGD feature by coordinate overlap.

Predicted tRNAs with no overlapping CGD feature (e.g. the missing CTRG_06445)
are REPORTED, not inserted -- inserting a brand-new tRNA needs a curator-assigned
systematic name and is handled separately.

Usage:
    python load_trnas.py --asm633-gff FILE --trnascan FILE [--include-pseudo] [--dry-run]
"""
from __future__ import annotations

import argparse
import gzip
import logging
import os
import re
import sys
from collections import defaultdict
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

ORGANISM_NAME = "Candida tropicalis MYA-3404"
SEQ_SOURCE = "C. tropicalis MYA-3404"      # matches the existing genomic SEQ source
OVERLAP_MIN = 0.5                          # min reciprocal overlap to call a match

AA_TO_LETTER = {
    "Ala": "A", "Arg": "R", "Asn": "N", "Asp": "D", "Cys": "C",
    "Gln": "Q", "Glu": "E", "Gly": "G", "His": "H", "Ile": "I",
    "Leu": "L", "Lys": "K", "Met": "M", "Phe": "F", "Pro": "P",
    "Ser": "S", "Thr": "T", "Trp": "W", "Tyr": "Y", "Val": "V",
    "SeC": "Z", "Sec": "Z", "iMet": "M", "fMet": "M",
    "Sup": "X", "Undet": "X", "Pseudo": "X",
}

COMPLEMENT = str.maketrans("ACGTNacgtn", "TGCANtgcan")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# parsing
# --------------------------------------------------------------------------- #
def _open(path: Path):
    return gzip.open(path, "rt") if str(path).endswith(".gz") else open(path)


def parse_asm633_trna_tags(gff: Path) -> Dict[str, str]:
    """Return {CTRG_ locus_tag: isotype} for gene lines with gene_biotype=tRNA."""
    tags: Dict[str, str] = {}
    with _open(gff) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            f = line.rstrip("\n").split("\t")
            if len(f) < 9 or f[2] != "gene" or "gene_biotype=tRNA" not in f[8]:
                continue
            m = re.search(r"locus_tag=([^;]+)", f[8])
            if m:
                tags[m.group(1)] = "tRNA"
    logger.info("ASM633v3 tRNA locus tags: %d", len(tags))
    return tags


def parse_trnascan(path: Path) -> List[Dict]:
    """Parse the standard tRNAscan-SE tabular (.out) output.

    Leading columns are stable across versions:
      0 seqid  1 tRNA#  2 begin  3 end  4 type  5 anticodon
      6 intron_begin  7 intron_end  8 inf_score  [extra...] [Note]
    """
    out: List[Dict] = []
    for line in _open(path):
        f = line.split()
        if len(f) < 9:
            continue
        if not (f[2].lstrip("-").isdigit() and f[3].lstrip("-").isdigit()):
            continue                                   # header lines
        note = " ".join(f[9:]).lower()
        out.append({
            "seqid": f[0],
            "begin": int(f[2]),
            "end": int(f[3]),
            "aa": f[4],
            "codon": f[5].upper().replace("T", "U"),   # display anticodon as RNA
            "intron_begin": int(f[6]),
            "intron_end": int(f[7]),
            "is_pseudo": "pseudo" in note,
        })
    logger.info("Parsed %d tRNAscan predictions from %s", len(out), path.name)
    return out


# --------------------------------------------------------------------------- #
# coordinate helpers
# --------------------------------------------------------------------------- #
def reciprocal_overlap(a_lo: int, a_hi: int, b_lo: int, b_hi: int) -> float:
    inter = max(0, min(a_hi, b_hi) - max(a_lo, b_lo) + 1)
    if inter == 0:
        return 0.0
    return inter / max(a_hi - a_lo + 1, b_hi - b_lo + 1)


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


# --------------------------------------------------------------------------- #
# DB helpers
# --------------------------------------------------------------------------- #
def ensure_code(session, tab: str, col: str, val: str, desc: str, dry: bool) -> None:
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


def get_scalar(session, sql: str, **kw):
    return session.execute(text(sql), kw).scalar()


def get_max_cal_id(session) -> int:
    def m(tbl):
        return session.execute(text(f"""
            SELECT MAX(TO_NUMBER(SUBSTR(dbxref_id, 4))) FROM {DB_SCHEMA}.{tbl}
            WHERE dbxref_id LIKE 'CAL%' AND REGEXP_LIKE(SUBSTR(dbxref_id, 4), '^[0-9]+$')
        """)).scalar() or 0
    return max(m("feature"), m("dbxref"))


def get_contig_map(session, organism_no: int) -> Tuple[Dict[str, Dict], Dict[int, str]]:
    """name -> {seq_no, residues}; and root_seq_no -> contig name."""
    rows = session.execute(text(f"""
        SELECT f.feature_name, s.seq_no, s.residues
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.seq s ON s.feature_no = f.feature_no
        WHERE f.organism_no = :o AND f.feature_type IN ('contig','chromosome')
          AND s.seq_type='genomic' AND s.is_seq_current='Y'
    """), {"o": organism_no}).fetchall()
    by_name, by_seqno = {}, {}
    for name, seq_no, residues in rows:
        by_name[name] = {"seq_no": seq_no,
                         "residues": str(residues) if residues is not None else ""}
        by_seqno[seq_no] = name
    return by_name, by_seqno


def get_current_features(session, organism_no: int) -> Dict[str, Dict]:
    """feature_name -> {feature_no, feature_type, root_seq_no, lo, hi, strand}."""
    rows = session.execute(text(f"""
        SELECT f.feature_name, f.feature_no, f.feature_type, f.gene_name,
               l.root_seq_no, l.start_coord, l.stop_coord, l.strand
        FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.feat_location l
          ON l.feature_no = f.feature_no AND l.is_loc_current='Y'
        WHERE f.organism_no = :o
    """), {"o": organism_no}).fetchall()
    out = {}
    for name, fno, ftype, gname, root, a, b, strand in rows:
        out[name] = {"feature_no": fno, "feature_type": ftype, "gene_name": gname,
                     "root_seq_no": root, "lo": min(a, b), "hi": max(a, b),
                     "strand": strand}
    return out


# --------------------------------------------------------------------------- #
class Enricher:
    def __init__(self, session, organism_no, genome_version_no,
                 contigs_by_name, next_cal, dry):
        self.s = session
        self.org = organism_no
        self.gv = genome_version_no
        self.contigs = contigs_by_name
        self.next_cal = next_cal
        self.dry = dry

    def _cal(self) -> str:
        x = f"CAL{self.next_cal[0]:010d}"
        self.next_cal[0] += 1
        return x

    def reclassify(self, feature_no: int, gene_name: str, headline: str) -> None:
        if self.dry:
            return
        self.s.execute(text(f"""
            UPDATE {DB_SCHEMA}.feature
            SET feature_type='tRNA', gene_name=:g, headline=:h
            WHERE feature_no=:f
        """), {"g": gene_name, "h": headline[:240], "f": feature_no})

    def ensure_property(self, feature_no: int) -> None:
        exists = self.s.execute(text(f"""
            SELECT 1 FROM {DB_SCHEMA}.feat_property
            WHERE feature_no=:f AND property_type='feature_qualifier'
        """), {"f": feature_no}).first()
        if exists or self.dry:
            return
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feat_property
                (feature_no, property_type, property_value, source, created_by)
            VALUES (:f, 'feature_qualifier', 'Uncharacterized', 'CGD', :u)
        """), {"f": feature_no, "u": ADMIN_USER})

    def has_children(self, parent_no: int) -> bool:
        return self.s.execute(text(f"""
            SELECT 1 FROM {DB_SCHEMA}.feat_relationship
            WHERE parent_feature_no=:p AND relationship_type='part of'
        """), {"p": parent_no}).first() is not None

    def _feature_no(self, name: str) -> Optional[int]:
        row = self.s.execute(text(f"""
            SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name=:n
        """), {"n": name}).first()
        return row[0] if row else None

    def add_child(self, parent_no: int, parent_name: str, idx: int,
                  root_seq_no: int, residues_src: str,
                  start: int, stop: int, subtype: str) -> None:
        label = "exon" if subtype == "noncoding_exon" else "intron"
        child_name = f"{parent_name}_{label}{idx}"
        db_strand = "W" if start < stop else "C"
        res = extract_residues(residues_src, start, stop, "+" if start < stop else "-")
        if self.dry:
            logger.info("[DRY RUN]   child %s (%s) %d..%d len=%d",
                        child_name, subtype, start, stop, len(res))
            return
        dbxref = self._cal()
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feature
                (organism_no, feature_name, dbxref_id, feature_type, source, created_by)
            VALUES (:o, :n, :x, :ft, 'CGD', :u)
        """), {"o": self.org, "n": child_name, "x": dbxref, "ft": subtype, "u": ADMIN_USER})
        child_no = self._feature_no(child_name)
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feat_location
                (feature_no, root_seq_no, coord_version, start_coord, stop_coord,
                 strand, is_loc_current, created_by)
            VALUES (:f, :r, SYSDATE, :a, :b, :s, 'Y', :u)
        """), {"f": child_no, "r": root_seq_no, "a": start, "b": stop,
               "s": db_strand, "u": ADMIN_USER})
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.seq
                (feature_no, genome_version_no, seq_version, seq_type, source,
                 is_seq_current, seq_length, residues, created_by)
            VALUES (:f, :gv, SYSDATE, 'genomic', :src, 'Y', :len, :res, :u)
        """), {"f": child_no, "gv": self.gv, "src": SEQ_SOURCE,
               "len": len(res), "res": res, "u": ADMIN_USER})
        self.s.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.feat_relationship
                (parent_feature_no, child_feature_no, relationship_type, created_by)
            VALUES (:p, :c, 'part of', :u)
        """), {"p": parent_no, "c": child_no, "u": ADMIN_USER})


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Reclassify + enrich C. tropicalis tRNAs")
    ap.add_argument("--asm633-gff", required=True, type=Path,
                    help="GCF_000006335.3_ASM633v3_genomic.gff(.gz) (tRNA tag set)")
    ap.add_argument("--trnascan", required=True, type=Path,
                    help="tRNAscan-SE .out on the CP-accession assembly (anticodons)")
    ap.add_argument("--include-pseudo", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    for p in (args.asm633_gff, args.trnascan):
        if not p.exists():
            logger.error("Input not found: %s", p)
            sys.exit(1)

    logger.info("=" * 64)
    logger.info("Reclassify + enrich C. tropicalis tRNAs%s",
                " [DRY RUN]" if args.dry_run else "")
    logger.info("=" * 64)

    trna_tags = parse_asm633_trna_tags(args.asm633_gff)
    preds = parse_trnascan(args.trnascan)
    if not args.include_pseudo:
        n = len(preds)
        preds = [p for p in preds if not p["is_pseudo"]]
        if n != len(preds):
            logger.info("Excluded %d pseudogene predictions", n - len(preds))

    with SessionLocal() as session:
        for tab, col, val, desc in [
            ("FEATURE", "FEATURE_TYPE", "tRNA", "Transfer RNA"),
            ("FEATURE", "FEATURE_TYPE", "noncoding_exon", "Noncoding exon"),
            ("FEATURE", "FEATURE_TYPE", "intron", "Intron"),
            ("SEQ", "SEQ_TYPE", "genomic", "Genomic DNA sequence"),
            ("SEQ", "SOURCE", SEQ_SOURCE, f"Source for {ORGANISM_NAME} data"),
            ("FEAT_PROPERTY", "PROPERTY_TYPE", "feature_qualifier", "Feature qualifier"),
        ]:
            ensure_code(session, tab, col, val, desc, args.dry_run)

        organism_no = get_scalar(session,
            f"SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_name=:n",
            n=ORGANISM_NAME)
        genome_version_no = get_scalar(session,
            f"SELECT genome_version_no FROM {DB_SCHEMA}.genome_version "
            f"WHERE organism_no=:o AND is_ver_current='Y'", o=organism_no)
        contigs_by_name, seqno_to_name = get_contig_map(session, organism_no)
        current = get_current_features(session, organism_no)
        logger.info("organism_no=%d genome_version_no=%d contigs=%d org-features=%d",
                    organism_no, genome_version_no, len(contigs_by_name), len(current))

        # index predictions by contig name for overlap lookup
        preds_by_contig: Dict[str, List[Dict]] = defaultdict(list)
        for p in preds:
            preds_by_contig[p["seqid"]].append(p)

        # Build the target list: CGD features for each ASM633v3 tRNA tag.
        targets: List[Dict] = []
        absent_tags: List[str] = []
        for tag in trna_tags:
            feat = current.get(tag)
            if not feat:
                absent_tags.append(tag)
                continue
            contig_name = seqno_to_name.get(feat["root_seq_no"])
            match, best = None, 0.0
            for p in preds_by_contig.get(contig_name, []):
                ov = reciprocal_overlap(feat["lo"], feat["hi"],
                                        min(p["begin"], p["end"]),
                                        max(p["begin"], p["end"]))
                if ov > best:
                    best, match = ov, p
            targets.append({"tag": tag, "feat": feat, "contig": contig_name,
                            "pred": match if best >= OVERLAP_MIN else None})

        # assign gene_name copy numbers over matched targets, in genomic order
        def order_key(t):
            return (t["contig"] or "", t["feat"]["lo"])
        counters: Dict[Tuple[str, str], int] = defaultdict(int)
        for t in sorted(targets, key=order_key):
            p = t["pred"]
            if not p:
                continue
            letter = AA_TO_LETTER.get(p["aa"], "X")
            counters[(letter, p["codon"])] += 1
            t["gene_name"] = f"t{letter}({p['codon']}){counters[(letter, p['codon'])]}"
            t["headline"] = (f"tRNA-{p['aa']}, predicted by tRNAscan-SE; "
                             f"{p['codon']} anticodon"
                             + ("; possible pseudogene" if p["is_pseudo"] else ""))

        next_cal = [get_max_cal_id(session) + 1]
        enr = Enricher(session, organism_no, genome_version_no,
                       contigs_by_name, next_cal, args.dry_run)

        reclassified = enriched_props = children_created = 0
        skipped_done = no_anticodon = conflict = 0
        for i, t in enumerate(sorted(targets, key=order_key)):
            feat, p = t["feat"], t["pred"]
            # safety: never touch something that already looks like a real ORF
            if feat["feature_type"] not in ("ORF", "tRNA"):
                logger.warning("SKIP %s: unexpected feature_type=%s",
                               t["tag"], feat["feature_type"])
                conflict += 1
                continue
            if feat["feature_type"] == "tRNA" and feat["gene_name"]:
                skipped_done += 1
                continue
            if not p:
                logger.warning("No tRNAscan anticodon match for %s (%s) - "
                               "reclassifying without gene_name", t["tag"], t["contig"])
                no_anticodon += 1

            gene_name = t.get("gene_name")
            headline = t.get("headline",
                             "tRNA, predicted by tRNAscan-SE")
            logger.info("%s (feat %d) -> tRNA  gene_name=%s  | %s",
                        t["tag"], feat["feature_no"], gene_name, headline)
            enr.reclassify(feat["feature_no"], gene_name or None, headline)
            reclassified += 1
            enr.ensure_property(feat["feature_no"])
            enriched_props += 1

            # children (exon/intron); skip if already present (idempotent re-run)
            if p and not enr.has_children(feat["feature_no"]):
                _, segs = compute_segments(p["begin"], p["end"],
                                           p["intron_begin"], p["intron_end"])
                residues = contigs_by_name[t["contig"]]["residues"]
                for idx, (start, stop, subtype) in enumerate(segs, start=1):
                    enr.add_child(feat["feature_no"], t["tag"], idx,
                                  feat["root_seq_no"], residues, start, stop, subtype)
                    children_created += 1

            if not args.dry_run and (i + 1) % 50 == 0:
                session.commit()
                logger.info("Committed through %d/%d", i + 1, len(targets))

        if not args.dry_run:
            session.commit()

        # predictions that matched no target tag (candidate new tRNAs, e.g. CTRG_06445)
        matched_pred_ids = {id(t["pred"]) for t in targets if t["pred"]}
        unmatched_preds = [p for p in preds if id(p) not in matched_pred_ids]

        logger.info("=" * 64)
        logger.info("tRNA tags (ASM633v3):        %d", len(trna_tags))
        logger.info("reclassified ORF->tRNA:      %d", reclassified)
        logger.info("feature_qualifier props set: %d", enriched_props)
        logger.info("exon/intron children added:  %d", children_created)
        logger.info("already-done (skipped):      %d", skipped_done)
        logger.info("reclassified w/o anticodon:  %d", no_anticodon)
        logger.info("type conflicts (skipped):    %d", conflict)
        logger.info("tags absent from CGD:        %d  %s",
                    len(absent_tags), absent_tags)
        logger.info("tRNAscan preds unmatched:    %d (candidate NEW tRNAs - report only)",
                    len(unmatched_preds))
        logger.info("=" * 64)


if __name__ == "__main__":
    main()
