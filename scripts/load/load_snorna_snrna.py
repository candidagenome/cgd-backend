#!/usr/bin/env python3
"""
Insert snoRNA/snRNA features for a species from a plan (subtask-4).

Only C. albicans had feature_type in ('snoRNA','snRNA'); the other 5 species had
0. These are predicted de novo with Rfam/Infernal cmscan (--cut_ga) against the
CGD-coordinate genome, plus U1 by homology to the albicans U1 (Rfam misses the
divergent Candida U1). Genuinely absent -> INSERT.

Per planned feature (storage mirrors load_ncrnas.py / the albicans sno/snRNAs):
  parent FEATURE (feature_type from the plan: 'snoRNA' or 'snRNA') + genomic SEQ
  + FEAT_LOCATION (seq_no=own, root=contig, minus=start>stop/C) + feat_property
  feature_qualifier='Uncharacterized' + one noncoding_exon child rank=2.
Idempotent by feature_name.

Usage: python load_snorna_snrna.py --plan plan.json [--dry-run]
"""
from __future__ import annotations
import argparse, json, logging, os, sys
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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def extract_residues(contig_seq, start, stop, strand):
    lo, hi = sorted((start, stop))
    seq = contig_seq[lo - 1:hi]
    if strand == "C":
        seq = seq.translate(COMPLEMENT)[::-1]
    return seq.upper()


def ensure_code(session, tab, col, val, desc, dry):
    if session.execute(text(f"SELECT code_no FROM {DB_SCHEMA}.code WHERE tab_name=:t AND col_name=:c AND code_value=:v"),
                       {"t": tab, "c": col, "v": val}).first():
        return
    if dry:
        logger.info("[DRY RUN] would add CODE %s.%s=%s", tab, col, val); return
    session.execute(text(f"INSERT INTO {DB_SCHEMA}.code (tab_name,col_name,code_value,description,created_by) VALUES (:t,:c,:v,:d,:u)"),
                    {"t": tab, "c": col, "v": val, "d": desc, "u": ADMIN_USER})
    session.commit()
    logger.info("Created CODE %s.%s=%s", tab, col, val)


def get_scalar(session, sql, **kw):
    return session.execute(text(sql), kw).scalar()


def get_max_cal_id(session):
    def m(tbl):
        return session.execute(text(f"SELECT MAX(TO_NUMBER(SUBSTR(dbxref_id,4))) FROM {DB_SCHEMA}.{tbl} "
                                    f"WHERE dbxref_id LIKE 'CAL%' AND REGEXP_LIKE(SUBSTR(dbxref_id,4),'^[0-9]+$')")).scalar() or 0
    return max(m("feature"), m("dbxref"))


def get_contig_map(session, organism_no):
    rows = session.execute(text(f"""
        SELECT f.feature_name, s.seq_no, s.residues FROM {DB_SCHEMA}.feature f
        JOIN {DB_SCHEMA}.seq s ON s.feature_no=f.feature_no
        WHERE f.organism_no=:o AND f.feature_type IN ('contig','chromosome')
          AND s.seq_type='genomic' AND s.is_seq_current='Y'"""), {"o": organism_no}).fetchall()
    return {name: {"seq_no": sq, "residues": str(res) if res is not None else ""} for name, sq, res in rows}


class Inserter:
    def __init__(self, session, org, gv, src, contigs, next_cal, dry):
        self.s, self.org, self.gv, self.src = session, org, gv, src
        self.contigs, self.next_cal, self.dry = contigs, next_cal, dry

    def _cal(self):
        x = f"CAL{self.next_cal[0]:010d}"; self.next_cal[0] += 1; return x

    def _fno(self, name):
        r = self.s.execute(text(f"SELECT feature_no FROM {DB_SCHEMA}.feature WHERE feature_name=:n"), {"n": name}).first()
        return r[0] if r else None

    def exists(self, name):
        return self._fno(name) is not None

    def _insert_feature(self, name, ftype, gene, headline):
        dbx = self._cal()
        if self.dry:
            return None
        self.s.execute(text(f"""INSERT INTO {DB_SCHEMA}.feature
            (organism_no,feature_name,dbxref_id,feature_type,gene_name,headline,source,created_by)
            VALUES (:o,:n,:x,:ft,:g,:h,'CGD',:u)"""),
            {"o": self.org, "n": name, "x": dbx, "ft": ftype, "g": gene,
             "h": (headline[:240] if headline else None), "u": ADMIN_USER})
        return self._fno(name)

    def _seq_loc(self, fno, root, res, start, stop, strand):
        self.s.execute(text(f"""INSERT INTO {DB_SCHEMA}.seq
            (feature_no,genome_version_no,seq_version,seq_type,source,is_seq_current,seq_length,residues,created_by)
            VALUES (:f,:gv,SYSDATE,'genomic',:src,'Y',:len,:res,:u)"""),
            {"f": fno, "gv": self.gv, "src": self.src, "len": len(res), "res": res, "u": ADMIN_USER})
        seqno = self.s.execute(text(f"SELECT seq_no FROM {DB_SCHEMA}.seq WHERE feature_no=:f AND seq_type='genomic' AND is_seq_current='Y'"), {"f": fno}).scalar()
        self.s.execute(text(f"""INSERT INTO {DB_SCHEMA}.feat_location
            (feature_no,root_seq_no,seq_no,coord_version,start_coord,stop_coord,strand,is_loc_current,created_by)
            VALUES (:f,:r,:sq,SYSDATE,:a,:b,:s,'Y',:u)"""),
            {"f": fno, "r": root, "sq": seqno, "a": start, "b": stop, "s": strand, "u": ADMIN_USER})

    def _prop(self, fno):
        self.s.execute(text(f"""INSERT INTO {DB_SCHEMA}.feat_property
            (feature_no,property_type,property_value,source,created_by)
            VALUES (:f,'feature_qualifier','Uncharacterized','CGD',:u)"""), {"f": fno, "u": ADMIN_USER})

    def load_one(self, spec):
        name, ftype = spec["feature_name"], spec["feature_type"]
        contig = self.contigs[spec["contig"]]
        root, strand = contig["seq_no"], spec["strand"]
        a, b = spec["start"], spec["stop"]
        start, stop = (max(a, b), min(a, b)) if strand == "C" else (min(a, b), max(a, b))
        res = extract_residues(contig["residues"], a, b, strand)
        if self.dry:
            logger.info("[DRY RUN] %-7s %-16s %-9s %s:%d..%d %s len=%d",
                        ftype, name, spec.get("gene_name"), spec["contig"], start, stop, strand, len(res))
            self._cal(); self._cal(); return
        pno = self._insert_feature(name, ftype, spec.get("gene_name"), spec.get("headline"))
        self._seq_loc(pno, root, res, start, stop, strand)
        self._prop(pno)
        cno = self._insert_feature(f"{name}_exon1", "noncoding_exon", None, None)
        self._seq_loc(cno, root, res, start, stop, strand)
        self.s.execute(text(f"""INSERT INTO {DB_SCHEMA}.feat_relationship
            (parent_feature_no,child_feature_no,relationship_type,rank,created_by)
            VALUES (:p,:c,'part of',2,:u)"""), {"p": pno, "c": cno, "u": ADMIN_USER})
        logger.info("inserted %-7s %-16s (feat %d) %-9s %s:%d..%d %s len=%d",
                    ftype, name, pno, spec.get("gene_name"), spec["contig"], start, stop, strand, len(res))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True, type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    plan = json.loads(args.plan.read_text())
    if not plan:
        logger.error("empty plan"); sys.exit(1)
    organism_name, seq_source = plan[0]["organism_name"], plan[0]["seq_source"]
    logger.info("=" * 64)
    logger.info("Insert %d sno/snRNA for %s%s", len(plan), organism_name, " [DRY RUN]" if args.dry_run else "")
    logger.info("=" * 64)
    with SessionLocal() as s:
        for tab, col, val, desc in [
            ("FEATURE", "FEATURE_TYPE", "snoRNA", "Small nucleolar RNA"),
            ("FEATURE", "FEATURE_TYPE", "snRNA", "Small nuclear RNA"),
            ("FEATURE", "FEATURE_TYPE", "noncoding_exon", "Noncoding exon"),
            ("SEQ", "SEQ_TYPE", "genomic", "Genomic DNA sequence"),
            ("SEQ", "SOURCE", seq_source, f"Source for {organism_name} data"),
            ("FEAT_PROPERTY", "PROPERTY_TYPE", "feature_qualifier", "Feature qualifier"),
        ]:
            ensure_code(s, tab, col, val, desc, args.dry_run)
        org = get_scalar(s, f"SELECT organism_no FROM {DB_SCHEMA}.organism WHERE organism_name=:n", n=organism_name)
        gv = get_scalar(s, f"SELECT genome_version_no FROM {DB_SCHEMA}.genome_version WHERE organism_no=:o AND is_ver_current='Y'", o=org)
        contigs = get_contig_map(s, org)
        next_cal = [get_max_cal_id(s) + 1]
        logger.info("organism_no=%d gv=%d contigs=%d next CAL=CAL%010d", org, gv, len(contigs), next_cal[0])
        ins = Inserter(s, org, gv, seq_source, contigs, next_cal, args.dry_run)
        inserted = skipped = 0
        for spec in plan:
            if spec["contig"] not in contigs:
                logger.error("contig %s not found (%s)", spec["contig"], spec["feature_name"]); sys.exit(1)
            if ins.exists(spec["feature_name"]):
                logger.info("SKIP %s (exists)", spec["feature_name"]); skipped += 1; continue
            ins.load_one(spec); inserted += 1
            if not args.dry_run:
                s.commit()
        if not args.dry_run:
            s.commit()
        logger.info("planned:%d inserted:%d skipped:%d", len(plan), inserted, skipped)


if __name__ == "__main__":
    main()
