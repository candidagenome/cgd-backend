#!/usr/bin/env python3
"""Genome-wide sweep: verify the "+N kb Flanking" sequence is gap-free.

AUTHORITATIVE TEST (length-independent, no dependence on the flanking code):

    The displayed "Genomic DNA +N kb Flanking" sequence must equal the literal
    CONTIGUOUS chromosomal neighborhood of the gene's coordinates, i.e. the
    chromosome slice [ gene_span_low - N , gene_span_high + N ] (reverse
    complemented for Crick-strand genes).

The flanking bug anchored the flanks on the subfeature min/max (which includes
the UTRs) while the sequence it flanked was only the CDS span -- so the UTR
region was excised, leaving a gap. A gapped sequence is NOT a contiguous
chromosomal slice, so it FAILS this test. The fixed code anchors on the
FeatLocation span, producing an exact contiguous slice -> PASS.

This is independent of UTR length (unlike a "UTRs sit flush against the CDS"
check, which gives false positives whenever a UTR is longer than the flank).

By default only ORFs that carry an annotated 5'/3' UTR are tested -- the only
genes the bug can affect. Use --all to test every feature with a current
location (also catches the multi-assembly subfeature bug).

A failure where the stored genomic sequence ALSO disagrees with the chromosome
is flagged as a likely sequence DESYNC rather than a flanking bug.

Usage:
    python scripts/sweep_flank_utr_contiguity.py
    python scripts/sweep_flank_utr_contiguity.py --organism "Candida glabrata"
    python scripts/sweep_flank_utr_contiguity.py --all --flank 1000
    python scripts/sweep_flank_utr_contiguity.py --out /tmp/flank_failures.csv
"""
import argparse
import csv
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy.orm import aliased  # noqa: E402

from cgd.db.engine import SessionLocal  # noqa: E402
from cgd.models.models import Feature, Seq, FeatLocation, FeatRelationship, Organism  # noqa: E402
from cgd.api.services import sequence_service as ss  # noqa: E402
from cgd.schemas.sequence_schema import SeqType  # noqa: E402

UTR_TYPES = ("five_prime_UTR", "three_prime_UTR")
COMPLEMENT_MAP = str.maketrans("ACGTacgt", "TGCAtgca")


def rc(s):
    return s.translate(COMPLEMENT_MAP)[::-1]


def find_features(db, organism=None, test_all=False):
    """ORFs carrying a current UTR subfeature (default), or all located features.

    Single server-side join (no Python-side IN list) so it scales past Oracle's
    1000-expression IN limit.
    """
    if test_all:
        q = (
            db.query(Feature)
            .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(FeatLocation.is_loc_current == "Y")
            .distinct()
        )
    else:
        parent = aliased(Feature)
        child = aliased(Feature)
        q = (
            db.query(parent)
            .join(FeatRelationship, FeatRelationship.parent_feature_no == parent.feature_no)
            .join(child, FeatRelationship.child_feature_no == child.feature_no)
            .join(FeatLocation, FeatLocation.feature_no == child.feature_no)
            .outerjoin(Organism, parent.organism_no == Organism.organism_no)
            .filter(
                FeatRelationship.rank == 2,
                child.feature_type.in_(UTR_TYPES),
                FeatLocation.is_loc_current == "Y",
            )
            .distinct()
        )
        Feature_alias = parent
        if organism:
            q = q.filter(Organism.organism_name == organism)
        return q.order_by(Feature_alias.feature_name).all()

    if organism:
        q = q.filter(Organism.organism_name == organism)
    return q.order_by(Feature.feature_name).all()


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--organism", help="Restrict to this organism_name")
    ap.add_argument("--flank", type=int, default=1000, help="Flank size bp (default 1000)")
    ap.add_argument("--all", action="store_true", dest="test_all",
                    help="Test every located feature, not just UTR-bearing ORFs")
    ap.add_argument("--limit", type=int, help="Only test the first N features")
    ap.add_argument("--out", help="Write failing genes to this CSV path")
    ap.add_argument("--verbose", action="store_true", help="Print every gene, not just fails")
    args = ap.parse_args()
    flank = args.flank

    db = SessionLocal()
    feats = find_features(db, args.organism, args.test_all)
    if args.limit:
        feats = feats[: args.limit]
    total = len(feats)
    pop = "located feature(s)" if args.test_all else "UTR-bearing ORF(s)"
    print(f"Testing {total} {pop}"
          + (f" in {args.organism}" if args.organism else "")
          + f" with +/-{flank} bp flanking...\n")

    chr_cache = {}  # root_seq_no -> residues

    def chr_residues(root_seq_no):
        if root_seq_no not in chr_cache:
            r = db.query(Seq).filter(Seq.seq_no == root_seq_no,
                                     Seq.is_seq_current == "Y").first()
            chr_cache[root_seq_no] = r.residues if r and r.residues else None
        return chr_cache[root_seq_no]

    fails, skipped, desync, errors, ok = [], 0, [], [], 0

    for i, feat in enumerate(feats, 1):
        name = feat.feature_name
        try:
            loc = (db.query(FeatLocation)
                   .filter(FeatLocation.feature_no == feat.feature_no,
                           FeatLocation.is_loc_current == "Y")
                   .first())
            if not loc or loc.start_coord is None or loc.stop_coord is None:
                skipped += 1
                continue
            chr_seq = chr_residues(loc.root_seq_no)
            if not chr_seq:
                skipped += 1
                continue

            lo = min(loc.start_coord, loc.stop_coord)
            hi = max(loc.start_coord, loc.stop_coord)

            resp = ss.get_sequence_by_feature(db, name, SeqType.GENOMIC, flank, flank)
            if not resp or not resp.sequence:
                skipped += 1
                continue
            actual = resp.sequence.upper()
            actual_plus = actual if loc.strand == "W" else rc(actual)

            # Ground truth: the contiguous chromosomal neighborhood.
            left = max(0, lo - 1 - flank)
            expected_plus = chr_seq[left:hi + flank].upper()

            if actual_plus == expected_plus:
                ok += 1
                if args.verbose:
                    print(f"  OK   {name}  ({loc.strand}, flank len {len(actual)})")
                continue

            # Failure -- diagnose: is the stored genomic seq itself off the chromosome?
            stored = (db.query(Seq)
                      .filter(Seq.feature_no == feat.feature_no,
                              Seq.seq_type.in_(["genomic", "Genomic DNA"]),
                              Seq.is_seq_current == "Y")
                      .first())
            span = chr_seq[lo - 1:hi].upper()
            if loc.strand != "W":
                span = rc(span)
            gen_ok = bool(stored and stored.residues
                          and stored.residues.upper() == span)
            row = (name, feat.gene_name or "", loc.strand,
                   len(actual), len(expected_plus),
                   "FLANK GAP/MISMATCH" if gen_ok
                   else "stored genomic disagrees with chromosome (likely DESYNC)")
            if gen_ok:
                fails.append(row)
                print(f"  FAIL {name}  flank != contiguous chromosome slice "
                      f"(got {len(actual)} bp, expected {len(expected_plus)} bp)")
            else:
                desync.append(row)
                print(f"  DESYNC {name}  stored genomic seq differs from chromosome")
        except Exception as exc:  # noqa: BLE001
            errors.append((name, repr(exc)))
            print(f"  ERR  {name}  {exc!r}")

        if i % 250 == 0:
            print(f"  ... {i}/{total} done "
                  f"(ok={ok}, fail={len(fails)}, desync={len(desync)}, "
                  f"skip={skipped}, err={len(errors)})", flush=True)

    print("\n==== SUMMARY ====")
    print(f"  tested:                 {total}")
    print(f"  contiguous (OK):        {ok}")
    print(f"  FLANK GAP (BUG):        {len(fails)}")
    print(f"  desync (not flank bug): {len(desync)}")
    print(f"  skipped (no loc/seq):   {skipped}")
    print(f"  errors:                 {len(errors)}")

    if args.out and (fails or desync):
        with open(args.out, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["feature_name", "gene_name", "strand",
                        "flank_len", "expected_len", "reason"])
            w.writerows(fails + desync)
        print(f"\n  Failing genes written to {args.out}")

    db.close()
    sys.exit(1 if fails or errors else 0)


if __name__ == "__main__":
    main()
