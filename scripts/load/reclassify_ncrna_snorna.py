#!/usr/bin/env python3
"""
Reclassify existing ncRNA features to snoRNA/snRNA using Rfam/Infernal cmscan hits
(subtask-4, glabrata). Glabrata annotated its snoRNAs/snRNAs as generic
feature_type='ncRNA' (CAGL####r loci); this matches each cmscan hit to the ncRNA
feature it overlaps and, in place:
  UPDATE feature SET feature_type = 'snoRNA'|'snRNA', gene_name = <Rfam family>,
         headline = <descriptive> (only if the feature has no headline yet)
Coordinates / sequence / noncoding_exon child / CGDID / Uncharacterized property
are already present and left untouched. Idempotent (skips rows already sno/snRNA).

Usage: python reclassify_ncrna_snorna.py --organism "Candida glabrata CBS138" \
         --tblout cgla_sno_sn.tblout [--dry-run]
"""
import argparse, os, sys
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))
from cgd.db.engine import SessionLocal  # noqa
S = os.getenv("DB_SCHEMA", "MULTI")
ADMIN = os.getenv("ADMIN_USER", "cgdadmin").upper()
RFAM = Path("/data/HTS/snorna_rfam")

UNAME = {"U1_yeast": "U1", "U1": "U1", "U2": "U2", "U4": "U4", "U5": "U5", "U6": "U6"}


def load_family():
    fam = {}
    for line in open(RFAM / "family.txt", encoding="latin-1"):
        f = line.rstrip("\n").split("\t")
        if len(f) >= 19 and f[0].startswith("RF"):
            t = f[18]
            fam[f[0]] = {"cls": "snoRNA" if "snoRNA" in t else ("snRNA" if "splicing" in t else "?"),
                         "box": "CD" if "CD-box" in t else "HACA" if "HACA-box" in t else "-"}
    return fam


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--organism", required=True)
    ap.add_argument("--tblout", required=True, type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    fam = load_family()

    hits = []
    for line in open(args.tblout):
        if line.startswith("#"):
            continue
        c = line.split()
        if len(c) < 20 or c[18] != "!":
            continue
        m = fam.get(c[2], {"cls": "?", "box": "-"})
        sfrom, sto = int(c[9]), int(c[10])
        hits.append({"fid": c[1], "cls": m["cls"], "box": m["box"], "contig": c[3],
                     "lo": min(sfrom, sto), "hi": max(sfrom, sto), "score": float(c[16])})

    with SessionLocal() as s:
        org = s.execute(text(f"SELECT organism_no FROM {S}.organism WHERE organism_name=:n"),
                        {"n": args.organism}).scalar()
        seqno_to_name = {}
        for r in s.execute(text(f"""SELECT cf.feature_name, sq.seq_no FROM {S}.feature cf
            JOIN {S}.seq sq ON sq.feature_no=cf.feature_no
            WHERE cf.organism_no=:o AND cf.feature_type IN ('contig','chromosome')
              AND sq.seq_type='genomic' AND sq.is_seq_current='Y'"""), {"o": org}).fetchall():
            seqno_to_name[r[1]] = r[0]
        ncrna = defaultdict(list)   # contig -> [(lo,hi,feature_no,feature_name,gene_name,headline,ftype)]
        for r in s.execute(text(f"""SELECT f.feature_no, f.feature_name, f.gene_name, f.headline, f.feature_type,
            l.root_seq_no, LEAST(l.start_coord,l.stop_coord), GREATEST(l.start_coord,l.stop_coord)
            FROM {S}.feature f JOIN {S}.feat_location l ON l.feature_no=f.feature_no AND l.is_loc_current='Y'
            WHERE f.organism_no=:o AND f.feature_type IN ('ncRNA','snoRNA','snRNA')"""), {"o": org}).fetchall():
            cn = seqno_to_name.get(r[5])
            if cn:
                ncrna[cn].append((r[6], r[7], r[0], r[1], r[2], r[3], r[4]))

        # match each hit -> best-overlapping ncRNA feature; keep best hit per ncRNA
        best = {}   # feature_no -> (hit, ncRNA tuple)
        for h in hits:
            cand = [(min(h["hi"], b) - max(h["lo"], a) + 1, row) for a, b, *rest in
                    ((x[0], x[1], x) for x in ncrna.get(h["contig"], []))
                    for row in [rest[0]] if min(h["hi"], b) - max(h["lo"], a) + 1 > 0]
            if not cand:
                continue
            _, row = max(cand, key=lambda z: z[0])
            fno = row[2]
            if fno not in best or h["score"] > best[fno][0]["score"]:
                best[fno] = (h, row)

        # assign gene_name (U-name / family, multi-copy suffix in genomic order)
        pairs = sorted(best.values(), key=lambda z: (z[0]["contig"], z[0]["lo"]))
        total = defaultdict(int)
        for h, row in pairs:
            key = UNAME.get(h["fid"], "U3" if h["fid"] == "Fungi_U3" else h["fid"])
            total[key] += 1
        cnt = defaultdict(int)
        updates = []
        for h, row in pairs:
            lo, hi, fno, fname, gname, headline, ftype = row
            base = UNAME.get(h["fid"], "U3" if h["fid"] == "Fungi_U3" else h["fid"])
            cnt[base] += 1
            gene = f"{base}-{cnt[base]}" if total[base] > 1 else base
            new_type = "snRNA" if h["cls"] == "snRNA" else "snoRNA"
            if h["cls"] == "snRNA":
                hl = f"{base} spliceosomal small nuclear RNA (snRNA); Rfam/Infernal cmscan"
            else:
                box = {"CD": "C/D box", "HACA": "H/ACA box"}.get(h["box"], "")
                hl = f"{box} small nucleolar RNA (snoRNA), Rfam family {base}; Rfam/Infernal cmscan".strip()
            updates.append((fno, fname, ftype, gname, gene, new_type, headline, hl))

        n_recl = n_skip = 0
        for fno, fname, ftype, oldgene, gene, new_type, old_hl, new_hl in updates:
            if ftype in ("snoRNA", "snRNA"):
                n_skip += 1
                continue
            set_hl = new_hl if not (old_hl and old_hl.strip()) else old_hl
            print(f"  {fname:16s} ncRNA -> {new_type:6s} gene={gene:10s} (was {oldgene})"
                  + ("" if set_hl == old_hl else f"  +headline"))
            if not args.dry_run:
                s.execute(text(f"""UPDATE {S}.feature SET feature_type=:ft, gene_name=:g, headline=:h
                    WHERE feature_no=:f AND feature_type='ncRNA'"""),
                    {"ft": new_type, "g": gene, "h": set_hl[:240] if set_hl else None, "f": fno})
            n_recl += 1
        if not args.dry_run:
            s.commit()
        print(f"\nmatched ncRNA: {len(updates)}   reclassified: {n_recl}   "
              f"already sno/snRNA (skipped): {n_skip}{'  [DRY RUN]' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
