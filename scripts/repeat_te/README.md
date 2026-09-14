# CGD repeat / transposable-element annotation pipeline (2026)

Scripts behind CGD's 2026 systematic repeat/TE annotation of all six species
(*C. albicans*, *C. dubliniensis*, *C. tropicalis*, *C. glabrata*,
*C. parapsilosis*, *C. auris*). User-facing documentation of the released
annotation: <http://www.candidagenome.org/help/repeat-te>. Design rationale
(tooling, feature-type crosswalk, naming):
[`docs/REPEAT_TE_ANNOTATION_STRATEGY.md`](../../docs/REPEAT_TE_ANNOTATION_STRATEGY.md).

All discovery steps run in the Dfam TE Tools Docker container (`dfam/tetools`,
bundling RepeatModeler 2.x, RepeatMasker 4.x, LTR_retriever), plus
[TEsorter](https://github.com/zhangrengang/TEsorter) against REXdb for
protein-domain classification. Paths are hard-coded to the CGD analysis server
(`/data/HTS/repeat_te/`, genomes in `/data/genomes/`) — adapt as needed.

## Repeat libraries (downloadable)

The libraries the pipeline uses/produces are available at
<http://www.candidagenome.org/download/repeat_te/>:

- `albicans_te_seeds.fasta` — the 366 curated *C. albicans* SC5314 repeat/TE
  sequences (Tca retrotransposons, Greek-letter LTR families, MRS repeat
  regions, Zorro LINEs) extracted from CGD's curated annotation, formatted as a
  RepeatMasker library (`>name#class` headers). Used for the homology survey
  (Phase 1 of `run_repeat_pipeline.sh`).
- Per-species RepeatModeler2 consensus libraries (`*-families.fa`) and their
  TEsorter-classified versions (`*_tesorter.cls.lib`).

## Pipeline (discovery → candidates → curator review → load)

1. **Discovery** — one driver script per batch, all the same recipe
   (RepeatModeler2 `-LTRStruct` → TEsorter → RepeatMasker with the genome's own
   library):
   - `run_repeat_pipeline.sh` — albicans-seed RepeatMasker homology survey of
     all 6 genomes (Phase 1) + the *C. tropicalis* de novo pilot (Phase 2).
   - `run_repeat_2sp.sh` — de novo runs for *C. auris* + *C. parapsilosis*.
   - `run_repeat_2sp_b.sh` — de novo runs for *C. glabrata* + *C. dubliniensis*.
   - `run_repeat_albicans.sh` — de novo run for *C. albicans* SC5314 (gap-fill
     of the curated reference set).
2. **Candidate extraction** — parse RepeatMasker `.out` into candidate-feature
   TSVs, applying the feature-type crosswalk and load/mask split from the
   strategy doc:
   - `make_repeat_candidates.py` — the original *C. tropicalis* parser.
   - `parameterize_parser.py` — generates the species-parameterized variant
     from it.
   - `make_repeat_candidates_sp.py` — the generated parameterized parser
     (`<species> <tag>` args), used for the other five species.
   - `make_dub_homology_candidates.py` / `make_dub_homology_candidates2.py` —
     *C. dubliniensis* homology route: candidates named by their *C. albicans*
     homolog from the seed-library survey (v2 supersedes v1).
   - `albicans_gap_diff.py` — *C. albicans* only: classifies de novo hits as
     NOVEL / PARTIAL / KNOWN against the existing curated annotation.
3. **Curator review** — per-family ACCEPT/REVERT check files:
   - `make_repeat_check_files.py`, `make_albicans_check_file.py`.
4. **Load** — build per-species load plans from accepted candidates and write
   `FEATURE`/`FEAT_LOCATION`/`SEQ` rows:
   - `build_repeat_load_plans.py`, `build_albicans_load_plan.py` (haplotype
     A/B allele pairing), `load_repeats.py`.

Family floor: ≥5 genomic copies or ≥5 kb total sequence per family; simple and
low-complexity repeats are never loaded as features. Every family was reviewed
by a CGD curator before loading.
