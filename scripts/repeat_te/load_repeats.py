#!/usr/bin/env python3
"""Load repeat/TE features from load_plan_<species>.json into the dev DB.

Features are simple (albicans rho-3b precedent): FEATURE (type, source,
headline, CAL dbxref) + genomic SEQ + FEAT_LOCATION (with seq_no) + one
'Non-uniform' alias. Names are provisional <PREFIX>_RPT_NNNN serials.
Usage: python load_repeats.py --species <sp> [--commit]
"""
import os
import json
import argparse
from dotenv import load_dotenv
load_dotenv(".env")
from sqlalchemy import text
from cgd.db.engine import SessionLocal

CFG = {
    "C_tropicalis":          dict(org=12, fsrc="C. tropicalis MYA-3404"),
    "C_albicans_SC5314":     dict(org=3,  fsrc="CGD",
                                  seq_src="C. albicans SC5314 Assembly 22",
                                  gv_prefix="A22"),
    "C_auris_B8441":         dict(org=11, fsrc="CGD", gv_prefix="s"),
    "C_parapsilosis_CDC317": dict(org=7,  fsrc="CGD"),
    "C_glabrata_CBS138":     dict(org=5,  fsrc="CGD"),
    "C_dubliniensis_CD36":   dict(org=9,  fsrc="CGD"),
}
S = os.getenv("DB_SCHEMA", "MULTI")
U = os.getenv("ADMIN_USER", "cgdadmin").upper()

ap = argparse.ArgumentParser()
ap.add_argument("--species", required=True, choices=list(CFG))
ap.add_argument("--commit", action="store_true")
args = ap.parse_args()
C = CFG[args.species]

db = SessionLocal()
q = lambda sql, **k: db.execute(text(sql), k).fetchall()
ex = lambda sql, **k: db.execute(text(sql), k)

COMP = str.maketrans("ACGTNacgtn", "TGCANtgcan")
rc = lambda s: s.translate(COMP)[::-1]

# Some organisms keep several is_ver_current='Y' rows (albicans: one per
# assembly A19/A20/A21/A22; auris: nuclear + mito) — filter by gv_prefix so
# the load never attaches to the wrong version (blind [0][0] picked A20 /
# mito before; dev was retagged 2026-08-18).
gv_rows = q(f"SELECT genome_version_no, genome_version FROM {S}.genome_version "
            f"WHERE organism_no=:o AND is_ver_current='Y' ORDER BY genome_version_no DESC",
            o=C["org"])
if C.get("gv_prefix"):
    gv_rows = [r for r in gv_rows if r[1].startswith(C["gv_prefix"])]
assert gv_rows, f"no current genome_version matching prefix {C.get('gv_prefix')!r}"
if len(gv_rows) > 1:
    print(f"WARNING: {len(gv_rows)} matching current genome_versions {gv_rows}; using {gv_rows[0]}")
gv = gv_rows[0][0]
seq_src = C.get("seq_src") or q(f"""SELECT s.source FROM {S}.seq s JOIN {S}.feature f ON f.feature_no=s.feature_no
    WHERE f.organism_no=:o AND f.feature_type='ORF' AND s.seq_type='genomic'
    AND s.is_seq_current='Y' GROUP BY s.source ORDER BY COUNT(*) DESC FETCH FIRST 1 ROWS ONLY""",
            o=C["org"])[0][0]


def maxcal():
    m = lambda t: q(f"SELECT MAX(TO_NUMBER(SUBSTR(dbxref_id,4))) FROM {S}.{t} "
                    f"WHERE dbxref_id LIKE 'CAL%' AND REGEXP_LIKE(SUBSTR(dbxref_id,4),'^[0-9]+$')")[0][0] or 0
    return max(m("feature"), m("dbxref"))


cal = [maxcal() + 1]


def newcal():
    x = f"CAL{cal[0]:010d}"
    cal[0] += 1
    return x


roots = {}
contigs = {}


def root_for(contig_name):
    if contig_name not in roots:
        r = q(f"""SELECT s.seq_no FROM {S}.seq s JOIN {S}.feature f ON f.feature_no=s.feature_no
            WHERE f.feature_name=:n AND f.organism_no=:o AND s.seq_type='genomic'
            AND s.is_seq_current='Y'""", n=contig_name, o=C["org"])
        assert len(r) == 1, f"root lookup failed for {contig_name}: {r}"
        roots[contig_name] = r[0][0]
    return roots[contig_name]


def contig_seq(seq_no):
    if seq_no not in contigs:
        contigs[seq_no] = str(q(f"SELECT residues FROM {S}.seq WHERE seq_no=:s", s=seq_no)[0][0])
    return contigs[seq_no]


plan = json.load(open(f"/data/HTS/repeat_te/load_plan_{args.species}.json"))
rows = plan["rows"]
exists = q(f"SELECT COUNT(*) FROM {S}.feature WHERE feature_name LIKE :p", p=rows[0]["feature_name"][:9] + "%")[0][0]
assert exists == 0, f"{exists} {rows[0]['feature_name'][:9]}* features already exist — aborting"

if not args.commit:
    types = {}
    for r in rows:
        types[r["ftype"]] = types.get(r["ftype"], 0) + 1
    print(f"[DRY] {args.species}: {len(rows)} features {types}; seq_src='{seq_src}' fsrc='{C['fsrc']}' gv={gv}")
    for r in rows[:3]:
        print(f"  e.g. {r['feature_name']} ({r['alias']}) {r['ftype']} {r['contig']}:{r['start']}-{r['end']}{r['strand']}")
    db.close()
    raise SystemExit

n = 0
for r in rows:
    root = root_for(r["contig"])
    lo, hi = int(r["start"]), int(r["end"])
    res = contig_seq(root)[lo - 1:hi]
    if r["strand"] == "-":
        res = rc(res)
    ex(f"INSERT INTO {S}.feature (organism_no,feature_name,dbxref_id,feature_type,source,headline,"
       f"created_by) VALUES (:o,:n,:x,:t,:src,:h,:u)",
       o=C["org"], n=r["feature_name"], x=newcal(), t=r["ftype"], src=C["fsrc"],
       h=r["headline"][:240], u=U)
    fno = q(f"SELECT feature_no FROM {S}.feature WHERE feature_name=:n", n=r["feature_name"])[0][0]
    ex(f"INSERT INTO {S}.seq (feature_no,genome_version_no,seq_version,seq_type,source,is_seq_current,"
       f"seq_length,residues,created_by) VALUES (:f,:gv,SYSDATE,'genomic',:s,'Y',:l,:r,:u)",
       f=fno, gv=gv, s=seq_src, l=len(res), r=res, u=U)
    sq = q(f"SELECT seq_no FROM {S}.seq WHERE feature_no=:f AND is_seq_current='Y'", f=fno)[0][0]
    a, b = (lo, hi) if r["strand"] == "+" else (hi, lo)
    ex(f"INSERT INTO {S}.feat_location (feature_no,root_seq_no,seq_no,coord_version,start_coord,"
       f"stop_coord,strand,is_loc_current,created_by) VALUES (:f,:r,:sq,SYSDATE,:a,:b,:st,'Y',:u)",
       f=fno, r=root, sq=sq, a=a, b=b, st=("W" if r["strand"] == "+" else "C"), u=U)
    hit = q(f"SELECT alias_no FROM {S}.alias WHERE alias_name=:n AND alias_type='Non-uniform'",
            n=r["alias"])
    if hit:
        ano = hit[0][0]  # A/B allele pairs share one alias row
    else:
        ex(f"INSERT INTO {S}.alias (alias_name, alias_type, created_by) VALUES (:n,'Non-uniform',:u)",
           n=r["alias"], u=U)
        ano = q(f"SELECT MAX(alias_no) FROM {S}.alias WHERE alias_name=:n", n=r["alias"])[0][0]
    ex(f"INSERT INTO {S}.feat_alias (feature_no, alias_no) VALUES (:f,:a)", f=fno, a=ano)
    n += 1

check = q(f"SELECT COUNT(*) FROM {S}.feature WHERE feature_name LIKE :p AND organism_no=:o",
          p=rows[0]["feature_name"][:9] + "%", o=C["org"])[0][0]
if check == len(rows):
    db.commit()
    print(f"COMMITTED {args.species}: {check} repeat/TE features")
else:
    db.rollback()
    print(f"ROLLBACK {args.species}: inserted {check} != planned {len(rows)}")
db.close()
