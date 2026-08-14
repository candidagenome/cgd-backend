# How to check `C_albicans_SC5314_repeat_load_check.tsv`

**File:** `cgd-frontend-dev:/data/HTS/repeat_te/C_albicans_SC5314_repeat_load_check.tsv`
(tab-separated; opens directly in Excel/Numbers/Google Sheets)

## What this file is

One row per **candidate repeat/TE family** that the de novo analysis found in
the C. albicans A22 genome but that has **zero overlap with any curated
repeat feature** (`repeat_region`, `long_terminal_repeat`, `retrotransposon`).
These are the proposed *additions* to the existing 366 curated albicans
repeat/TE features — 67 families covering 270 genomic copies (~208 kb).

**Key difference from the other species' check files:** for the 5 other
species the rows say `LOADED` and link to live locus pages, because their
features are already in the dev database — you are *verifying* a load.
For albicans **nothing has been loaded yet** — you are *approving or
rejecting* families before any FEATURE rows are written. That is why
`first_feature` is `-` and the links go to JBrowse regions instead of
locus pages.

## Columns

| Column | Meaning |
|---|---|
| `family_display` | RepeatModeler family id (raw; display names like the Wicker `CaRLG…` ids get assigned at load time per decision D5) |
| `status` | `CANDIDATE` = meets the proposed D6 floor (≥5 copies or ≥5 kb total). `CANDIDATE_BELOW_FLOOR` = fails both — default recommendation is *reject* unless something makes it interesting |
| `feature_type` | Proposed CGD feature type from the evidence crosswalk: `retrotransposon` (full-length/LINE), `long_terminal_repeat` (solo LTR), `repeat_region` (unclassified dispersed) |
| `n_copies` | Number of novel genomic copies of this family (copies already covered by curation are not counted) |
| `total_bp` / `max_len` | Total novel bp across copies / longest single copy |
| `first_feature` | `-` (no features exist yet; will become `CALB_RPT_NNNN`-style names on load) |
| `example_url` | JBrowse link to a representative copy — **this is the main review tool** |
| `evidence` | RepeatModeler class, TEsorter protein-domain call (if any), LTR-structural evidence, and whether copies overlap annotated genes |
| `REVIEW(Accept/Reject)` | **Fill this in** — Accept / Reject / notes |

## How to review a row

1. Click `example_url` — the region opens in dev JBrowse.
2. Look at what's there: an intergenic hit with no annotation is a clean
   candidate; a hit sitting inside an ORF deserves suspicion (RepeatModeler
   sometimes builds "families" from paralogous gene clusters — the evidence
   column flags how many copies overlap genes, e.g. `rnd-1_family-56` has
   4/20 copies over genes at the chr1 left end, a subtelomere signature).
3. Weigh the evidence column: a TEsorter domain call (Gypsy/Copia RT/INT/RH)
   or LTR-structural support is strong; `Unknown` dispersed families rest on
   copy number and length only.
4. Sanity-check scale: high copy number + long copies (kb-scale) = likely
   real dispersed repeat; 2 copies × 200 bp = probably noise (these are
   already marked `CANDIDATE_BELOW_FLOOR`).
5. Write `Accept` or `Reject` (plus any notes) in the REVIEW column. If a
   family should load under a different feature_type than proposed, note
   that too.

## Suggested triage order

- `long_terminal_repeat` rows first (top of file, sorted by copy count) —
  17-copy `ltr-1_family-7` and 12-copy `ltr-1_family-1` are solo-LTR
  families the current annotation lacks entirely.
- Then `retrotransposon` rows (few, high value).
- Then `repeat_region` (largest section; the D6 floor already splits it —
  the 23 BELOW_FLOOR rows can be batch-rejected unless one catches your eye).

## Companion files (same directory)

- `C_albicans_SC5314_repeat_gap_diff.tsv` — every individual de novo hit
  (492 rows) with NOVEL/PARTIAL/KNOWN status vs curation, per-copy
  coordinates, and gene overlaps. Use it to see *all* copies of a family.
- `C_albicans_SC5314_novel_family_summary.tsv` — the raw per-family summary
  this check file was built from.
- `C_albicans_SC5314_curated_not_recovered.tsv` — the **reverse** diff: 105
  curated repeat features the de novo run did not recover. A separate
  question from loading: are these degenerate-but-real (keep), or
  mis-annotations (curator judgment)? Not part of Accept/Reject here.
- `CURATOR_REVIEW_REPEAT_TE.md` — the six-species decision doc (D1–D6);
  D4/D5 naming and D6 floor apply to albicans loads too.

## What happens after review

Accepted families go through the same path as the other five species:
`build_repeat_load_plans.py` → `load_plan_C_albicans_SC5314.json` → dev DB
load (assigning `_RPT_` feature names) → ES reindex → JBrowse GFF injection
→ a LOADED-status check file like the other species' for final verification
→ prod with the rest of the non-coding release.

*Generated 2026-08-14 from the 2026-08-13 albicans gap-diff run
(`albicans_gap_diff.py` → `make_albicans_check_file.py`).*
