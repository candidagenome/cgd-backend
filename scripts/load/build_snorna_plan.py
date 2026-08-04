#!/usr/bin/env python3
"""Build a snoRNA/snRNA insert plan for one species from a cmscan fmt-2 tblout.

Computes systematic feature_name (<lower-numbered flanking locus tag>.5, clusters
-> .5/.6/.7..), gene_name (Rfam family, -1/-2 for multi-copy), extracts genomic
sequence, and writes a plan JSON for load_snorna_snrna.py. U1 (Rfam-invisible in
Candida) can be injected via --u1 from a homology hit. QC-flagged hits excluded
via --exclude.

Usage:
  python build_snorna_plan.py --organism "Candida tropicalis MYA-3404" \
    --tblout ctrop_sno_sn.tblout --contigs ctrop_contigs.fna \
    --locus-regex 'CTRG_(\\d+)' --out ctrop_snorna_plan.json \
    [--u1 CP047871.1:1766428:1766692:W] [--exclude CP047870.1:1984041]
"""
import argparse, json, os, re, sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path("scripts/load/load_rrnas.py").resolve().parents[2]))
from sqlalchemy import text
from cgd.db.engine import SessionLocal
S = os.getenv("DB_SCHEMA", "MULTI")
RFAM = Path("/data/HTS/snorna_rfam")


def load_family():
    fam = {}
    for line in open(RFAM / "family.txt", encoding="latin-1"):
        f = line.rstrip("\n").split("\t")
        if len(f) >= 19 and f[0].startswith("RF"):
            typ = f[18]
            fam[f[0]] = {"id": f[1],
                         "cls": "snoRNA" if "snoRNA" in typ else ("snRNA" if "splicing" in typ else "?"),
                         "box": "CD" if "CD-box" in typ else "HACA" if "HACA-box" in typ else "-"}
    return fam


def read_fasta(path):
    d, name, buf = {}, None, []
    for line in open(path):
        if line.startswith(">"):
            if name:
                d[name] = "".join(buf)
            name, buf = line[1:].split()[0], []
        else:
            buf.append(line.strip())
    if name:
        d[name] = "".join(buf)
    return d


COMP = str.maketrans("ACGTNacgtn", "TGCANtgcan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--organism", required=True)
    ap.add_argument("--tblout", required=True, type=Path)
    ap.add_argument("--contigs", required=True, type=Path)
    ap.add_argument("--locus-regex", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--u1")            # contig:lo:hi:W/C   homology U1
    ap.add_argument("--exclude", action="append", default=[])  # contig:lo
    ap.add_argument("--skip-existing-rna", action="store_true",
                    help="drop hits overlapping an existing RNA-gene feature "
                         "(ncRNA/snoRNA/snRNA/tRNA/rRNA) -- avoids duplicating "
                         "features already annotated (e.g. glabrata's ncRNA-typed snoRNAs)")
    args = ap.parse_args()

    fam = load_family()
    contigs = read_fasta(args.contigs)
    excl = set(tuple(e.split(":")) for e in args.exclude)  # (contig, lo-as-str)

    hits = []
    for line in open(args.tblout):
        if line.startswith("#"):
            continue
        c = line.split()
        if len(c) < 20 or c[18] != "!":
            continue
        acc = c[2]
        m = fam.get(acc, {"id": c[1], "cls": "?", "box": "-"})
        sfrom, sto = int(c[9]), int(c[10])
        lo, hi = min(sfrom, sto), max(sfrom, sto)
        if (c[3], str(lo)) in excl:
            continue
        hits.append({"fid": c[1], "cls": m["cls"], "box": m["box"], "contig": c[3],
                     "lo": lo, "hi": hi, "strand": "W" if c[11] == "+" else "C",
                     "src": "Rfam/Infernal cmscan"})
    if args.u1:
        cn, lo, hi, st = args.u1.split(":")
        hits.append({"fid": "U1", "cls": "snRNA", "box": "-", "contig": cn,
                     "lo": int(lo), "hi": int(hi), "strand": st,
                     "src": "homology to C. albicans U1"})

    with SessionLocal() as s:
        org = s.execute(text(f"SELECT organism_no FROM {S}.organism WHERE organism_name=:n"), {"n": args.organism}).scalar()
        seq_source = s.execute(text(f"""SELECT source FROM (
            SELECT sq.source, COUNT(*) c FROM {S}.feature f JOIN {S}.seq sq ON sq.feature_no=f.feature_no
            WHERE f.organism_no=:o AND f.feature_type IN ('contig','chromosome') AND sq.seq_type='genomic' AND sq.is_seq_current='Y'
            GROUP BY sq.source ORDER BY c DESC) WHERE ROWNUM=1"""), {"o": org}).scalar()
        seqno_to_name, current = {}, {}
        for r in s.execute(text(f"""SELECT f.feature_name, f.feature_type, l.root_seq_no, l.start_coord, l.stop_coord
            FROM {S}.feature f JOIN {S}.feat_location l ON l.feature_no=f.feature_no AND l.is_loc_current='Y'
            WHERE f.organism_no=:o"""), {"o": org}).fetchall():
            current[r[0]] = {"ftype": r[1], "root": r[2], "lo": min(r[3], r[4]), "hi": max(r[3], r[4])}
        for r in s.execute(text(f"""SELECT cf.feature_name, sq.seq_no FROM {S}.feature cf
            JOIN {S}.seq sq ON sq.feature_no=cf.feature_no
            WHERE cf.organism_no=:o AND cf.feature_type IN ('contig','chromosome') AND sq.seq_type='genomic' AND sq.is_seq_current='Y'"""), {"o": org}).fetchall():
            seqno_to_name[r[1]] = r[0]

    # drop hits overlapping an existing RNA gene (avoid duplicates)
    if args.skip_existing_rna:
        RNA = {"ncRNA", "snoRNA", "snRNA", "tRNA", "rRNA"}
        rna_by_contig = defaultdict(list)
        for name, fd in current.items():
            if fd["ftype"] in RNA:
                cn = seqno_to_name.get(fd["root"])
                if cn:
                    rna_by_contig[cn].append((fd["lo"], fd["hi"], name, fd["ftype"]))
        kept, dropped = [], []
        for h in hits:
            ov = next(((lo, hi, nm, ft) for lo, hi, nm, ft in rna_by_contig.get(h["contig"], [])
                       if min(h["hi"], hi) - max(h["lo"], lo) + 1 > 0), None)
            (dropped if ov else kept).append((h, ov))
        for h, ov in dropped:
            print(f"  SKIP {h['fid']:12s} {h['contig']}:{h['lo']}-{h['hi']} overlaps {ov[2]}[{ov[3]}]", file=sys.stderr)
        print(f"skip-existing-rna: dropped {len(dropped)} of {len(hits)} hits (overlap existing RNA gene)", file=sys.stderr)
        hits = [h for h, ov in kept]

    # locus features per contig for flanking naming
    rx = re.compile(args.locus_regex)
    loci = defaultdict(list)
    for name, fd in current.items():
        mm = rx.fullmatch(name)
        cn = seqno_to_name.get(fd["root"])
        if mm and cn:
            loci[cn].append((fd["lo"], fd["hi"], int(mm.group(1)), name))
    for cn in loci:
        loci[cn].sort()

    def base_tag(h):
        lst = loci.get(h["contig"], [])
        left = [o for o in lst if o[1] < h["lo"]]
        right = [o for o in lst if o[0] > h["hi"]]
        cand = left[-1:] + right[:1]
        if not cand:
            return h["contig"]  # fallback
        return min(cand, key=lambda o: o[2])[3]   # feature_name of lower-numbered flank

    hits.sort(key=lambda h: (h["contig"], h["lo"]))
    grp = defaultdict(list)
    for h in hits:
        grp[base_tag(h)].append(h)
    for tag, hs in grp.items():
        for i, h in enumerate(sorted(hs, key=lambda x: (x["contig"], x["lo"]))):
            h["feature_name"] = f"{tag}.{5 + i}"

    famtotal, famcount = defaultdict(int), defaultdict(int)
    for h in hits:
        famtotal[h["fid"]] += 1
    plan = []
    for h in sorted(hits, key=lambda x: (x["fid"], x["contig"], x["lo"])):
        famcount[h["fid"]] += 1
        base = "U3" if h["fid"] == "Fungi_U3" else h["fid"]
        gene = f"{base}-{famcount[h['fid']]}" if famtotal[h["fid"]] > 1 else base
        if h["cls"] == "snRNA":
            hl = f"{base} spliceosomal small nuclear RNA (snRNA); {h['src']}"
        else:
            box = {"CD": "C/D box", "HACA": "H/ACA box"}.get(h["box"], "")
            hl = f"{box} small nucleolar RNA (snoRNA), Rfam family {base}; {h['src']}".strip()
        seq = contigs[h["contig"]][h["lo"] - 1:h["hi"]]
        if h["strand"] == "C":
            seq = seq.translate(COMP)[::-1]
        h["gene_name"], h["headline"], h["seq_ok"] = gene, hl, ("N" not in seq.upper() and len(seq) == h["hi"] - h["lo"] + 1)
        plan.append({"organism_name": args.organism, "seq_source": seq_source,
                     "feature_name": h["feature_name"], "gene_name": gene,
                     "feature_type": h["cls"], "ncrna_class": h["box"],
                     "contig": h["contig"], "start": h["lo"], "stop": h["hi"],
                     "strand": h["strand"], "headline": hl})

    # guards
    for e in plan:
        if e["feature_type"] not in ("snoRNA", "snRNA"):
            print("ERROR unclassified:", e["feature_name"], file=sys.stderr); sys.exit(1)
    seen = set()
    for e in plan:
        if e["feature_name"] in seen:
            print("ERROR dup feature_name:", e["feature_name"], file=sys.stderr); sys.exit(1)
        seen.add(e["feature_name"])

    args.out.write_text(json.dumps(plan, indent=2))
    nsno = sum(1 for e in plan if e["feature_type"] == "snoRNA")
    nsn = sum(1 for e in plan if e["feature_type"] == "snRNA")
    print(f"wrote {len(plan)} ({nsno} snoRNA + {nsn} snRNA) -> {args.out}  seq_source='{seq_source}'")
    for e in plan:
        print(f"  {e['feature_name']:18s} {e['gene_name']:10s} {e['feature_type']:6s} "
              f"{e['contig']}:{e['start']}-{e['stop']} {e['strand']}")


if __name__ == "__main__":
    main()
