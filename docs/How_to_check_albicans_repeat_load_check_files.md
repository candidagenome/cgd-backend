# How to check `C_albicans_SC5314_repeat_load_check.tsv`

**File:** `cgd-frontend-dev:/data/HTS/repeat_te/C_albicans_SC5314_repeat_load_check.tsv`
(tab-separated; copies in `../cgd/` and `~/Downloads/`; opens in Excel/Sheets)

## What this file is

One row per **repeat/TE family loaded to the dev database** on 2026-08-14 —
the de novo candidates that had zero overlap with any curated repeat
feature. Albicans now follows the same review workflow as the other five
species: the features are live on dev, you review real locus pages, and
mark **Accept** or **Revert** per family. Nothing is on prod.

**Load summary:** 31 families → 240 features (`CALB_RPT_NNNN_A/_B`),
121 loci; 9 dispersed families below the D6 floor were not loaded
(bottom rows, `NOT_LOADED`).

## Albicans-specific: haplotype pairing

The A22 assembly carries both haplotypes, and curated albicans features
exist as `_A`/`_B` allele pairs. The load mirrors that: copies of the same
family at syntenic positions on sister chromosomes (Ca22chrXA/Ca22chrXB)
were paired into one locus — `CALB_RPT_0001_A` + `CALB_RPT_0001_B` share a
serial and an alias (`CaRLX1-1`), like curated alleles. 119 of 121 loci
paired cleanly (sister-haplotype starts typically within ~100 bp); 2 are
haplotype singletons — themselves potentially interesting (real A/B
differences). `n_copies` in this file counts **loci**, not features.

## Columns

| Column | Meaning |
|---|---|
| `family_display` | Wicker-style family id: `Ca` + class + number (RLG=Gypsy, RLC=Copia, RLX=other LTR, RIL=LINE, RPT=unclassified) — e.g. `CaRLX1`. Individual loci are `CaRLX1-1`, `CaRLX1-2`, … (the alias on each feature) |
| `status` | `LOADED` (on dev, awaiting your verdict) or `NOT_LOADED` (below D6 floor, listed for completeness) |
| `feature_type` | `retrotransposon` / `long_terminal_repeat` / `repeat_region` as loaded |
| `n_copies` | Number of loci (A/B pairs count once) |
| `total_bp` / `max_len` | Total bp across all features / longest single feature |
| `first_feature` | First feature of the family — click through via `example_url` |
| `example_url` | **Live dev locus page** — the main review tool |
| `evidence` | RepeatModeler family + class, TEsorter domain call, % divergence, feature/locus counts, and a flag when features overlap annotated genes |
| `REVIEW(Accept/Revert)` | **Fill this in** per family |

## How to review a family

1. Open `example_url` — a real locus summary page: headline (family,
   class, divergence, provenance), coordinates, sequence.
2. Check the location in context (the locus page's map, or JBrowse at the
   same coordinates): intergenic/subtelomeric = expected repeat territory;
   inside an ORF = suspicious.
3. **Gene-overlap flag**: families whose evidence says "features overlap
   annotated genes (paralog check)" are the likeliest Reverts —
   RepeatModeler sometimes builds "families" from paralogous gene clusters.
   The headline of each affected feature names the overlapped gene(s).
4. Weigh the classification: TEsorter protein-domain calls (Gypsy/Copia)
   are strong evidence; `Unknown` families rest on copy number and length.
5. Write `Accept` or `Revert` (plus notes) in the REVIEW column.
   Reverting a family removes all its `CALB_RPT_*` features from dev —
   nothing reaches prod either way until this review completes.

## Suggested triage order

- `long_terminal_repeat` families first (top of file, by locus count):
  `CaRLX4` (9 loci) and `CaRLX1` (6 loci) are solo-LTR families the
  curated annotation lacks entirely.
- `retrotransposon` families next (few, high value — includes the
  TEsorter-confirmed Copia family).
- `repeat_region` last (largest section); the 9 `NOT_LOADED` rows need no
  action unless you want one rescued.

## Companion files (same directory)

- `C_albicans_SC5314_repeat_gap_diff.tsv` — every de novo hit (492) with
  NOVEL/PARTIAL/KNOWN status vs curation and per-copy gene overlaps.
- `C_albicans_SC5314_novel_family_summary.tsv` — raw per-family summary.
- `C_albicans_SC5314_curated_not_recovered.tsv` — reverse diff: 105
  curated repeat features de novo did not recover (separate question:
  degenerate-but-real, or mis-annotation?).
- `load_plan_C_albicans_SC5314.json` — exact per-feature load manifest.
- `CURATOR_REVIEW_REPEAT_TE.md` — the six-species decision doc (D1–D6).

## What happens after review

Accepted families stay; Reverted families get their features deleted from
dev. Then the standard path continues: ES reindex (so search finds them) →
JBrowse GFF injection → flat files → prod rollout with the rest of the
non-coding release (genome-version bump consideration).

*Loaded 2026-08-14 from the 2026-08-13 gap-diff run
(`albicans_gap_diff.py` → `build_albicans_load_plan.py` → `load_repeats.py
--species C_albicans_SC5314` → `make_albicans_check_file.py`).*
