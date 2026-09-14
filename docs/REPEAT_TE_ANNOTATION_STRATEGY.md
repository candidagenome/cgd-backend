# Repeat / Transposable-Element Annotation Strategy (Subtask 5)

> **Publication note (2026-09):** This is the pre-implementation decision document for CGD's
> 2026 repeat/TE annotation, preserved as written (2026-08-04). The project as executed grew
> beyond the scope below: all six CGD species were annotated (including gap-fill of the curated
> *C. albicans* reference set and *C. dubliniensis*/*C. glabrata* legacy sets), and the
> `DNA_transposon` (SO:0000182) feature type — left "curator-gated" in §5 — was ultimately
> introduced for the Tc1/mariner and MULE-MuDR elements found in *C. parapsilosis* and
> *C. dubliniensis*. For the released annotation, methods summary, and per-species results, see
> <http://www.candidagenome.org/help/repeat-te>. The pipeline scripts live in
> [`scripts/repeat_te/`](../scripts/repeat_te/) in this repository, and the repeat libraries
> (curated *C. albicans* seed library and de novo consensus libraries) are downloadable at
> <http://www.candidagenome.org/download/repeat_te/>.

**Status:** Decision document — *decides tooling, feature types, and naming before any loading.* No data is loaded by this ticket.
**Scope species:** *Candida tropicalis*, *Candida auris*, *Candida parapsilosis* (the three with **zero** repeat/TE annotation).
**Author:** CGD backend / Claude · **Date:** 2026-08-04 · Part of the "missing non-coding features in 6 species" project (siblings: subtask-1 tRNA, subtask-2 rRNA, subtask-3 ncRNA, subtask-4 snoRNA/snRNA).

---

## 1. Why this needs a decision doc first

Repeats/TEs are the **largest and most curation-sensitive** gap of the non-coding project. Unlike tRNA/rRNA/snoRNA (where the answer is a discrete, well-bounded gene set from a covariance-model or homology search), repeat/TE annotation is:

- **Open-ended** — a de novo repeat library produces hundreds of families of wildly varying quality and a large "Unknown" fraction; what gets *loaded as curated features* is a deliberate subset, not the raw tool output.
- **Nomenclature-heavy** — CGD's existing repeat/TE model is a hand-built, literature-derived scheme (see §3) that cannot be mechanically transferred to a new species.
- **Type-ambiguous** — the same biological element can be modeled under several Sequence Ontology terms; CGD has historically used a **narrow subset** of SO and deliberately never uses the generic `transposable_element`.

Getting tooling, feature types, and naming agreed **before** loading avoids a large, hard-to-reverse curation mess.

---

## 2. Current state — the gap (measured 2026-08-04)

Feature counts by `feature_type` × organism (CGD dev DB). These are the **only** repeat/TE-relevant types CGD uses; `transposable_element` (SO:0000101) is present **nowhere**.

| feature_type | SO term | albicans | glabrata | dubliniensis | **tropicalis** | **auris** | **parapsilosis** |
|---|---|---:|---:|---:|---:|---:|---:|
| `long_terminal_repeat` | SO:0000286 | 292 | 4 | 0 | **0** | **0** | **0** |
| `repeat_region` | SO:0000657 | 90 | 2 | 10 | **0** | **0** | **0** |
| `retrotransposon` | SO:0000180 | 34 | 0 | 0 | **0** | **0** | **0** |
| `centromere` | SO:0000577 | 16 | 11 | 0 | **0** | **0** | **0** |
| `telomeric_repeat` | SO:0001496 | 2 | 0 | 0 | **0** | **0** | **0** |
| `transposable_element` | SO:0000101 | 0 | 0 | 0 | 0 | 0 | 0 |

**Reading:** repeat/TE annotation is effectively **albicans-only** (434 features). glabrata (17) and dubliniensis (10) have small legacy sets. **The three scope species have literally nothing.** They are the target of this ticket.

`FEATURE.feature_type` is constrained only `NOT NULL` — there is **no controlled-vocabulary table or FK**, so new type strings are *technically* possible. This makes curatorial discipline (not the schema) the thing that keeps the vocabulary consistent, which is why this ticket fixes the allowed set explicitly.

---

## 3. How CGD models repeats/TEs today (the albicans precedent we must stay consistent with)

Examined from live albicans records:

- **`retrotransposon`** — named `Tca<N>` (Transposon of *C. albicans*), with a chromosome suffix for individual copies/fragments: `Tca2-7` = Tca2 on Chr 7; `Tca12-R` = "Fragment of copia-like retrotransposon Tca12 on Chr R". Headlines record family (gypsy/copia), length, and LTR length. The 16 Tca families are from Goodwin & Poulter 2000 (*Genome Res* 10:174).
- **`long_terminal_repeat`** — named with **Greek-letter families** + chromosome-arm suffix: `rho-3b`, `iota-4a`, `zeta-4b`, `sampi-4b`, `lambda-4a`. Covers both solo LTRs and LTRs flanking full elements.
- **`repeat_region`** — named by known major repeat families: `Ca3` (large dispersed repeat / MRS), `RPS-1`, `RB2-4a`, `27A`.
- **`centromere`**, **`telomeric_repeat`** — small hand-curated sets.

**Two implications:**
1. This nomenclature is **albicans-specific published biology**. We cannot reuse `Tca<N>` / Greek-letter names for tropicalis/auris/parapsilosis — those names denote *C. albicans* elements.
2. The two legacy non-albicans species show the *systematic* fallback CGD already uses: glabrata → locus-tag style with a class-suffix letter (`CAGL0G07166t` for LTR/repeat, `...s` for centromere); dubliniensis → descriptive per-chromosome (`Cd36.chrom4.MRS_1`). Both are curator-entered (`PRACHI` 2010, `BINKLEY` 2013).

---

## 4. Decision 1 — Tooling

### Recommendation
Run a **per-genome de novo pipeline in the Dfam TE Tools container**, then annotate; do **not** rely on public libraries alone, and do **not** use EDTA.

```
# one container, pinned versions (RepeatModeler 2.0.9 / RepeatMasker 4.2.4)
docker run … dfam/tetools

# 1. de novo family discovery (LTRStruct bundles LTR_retriever)
BuildDatabase -name CTROP ctrop_genome.fa
RepeatModeler -database CTROP -threads 16 -LTRStruct    # -> CTROP-families.fa

# 2. classify consensi by protein domains (Wicker order/superfamily/clade)
TEsorter CTROP-families.fa            # refines RepeatClassifier labels

# 3. annotate the genome with the custom library (+ known Candida seeds)
RepeatMasker -lib CTROP-families.fa -xsmall -pa 4 ctrop_genome.fa
```

### Rationale
- **De novo is necessary, not optional.** Dfam 3.9 is CC0/free but **metazoan-biased** with *no curated Candida/fungal reference set*; RepBase (the historically fungal-richer library) has been **paywalled since April 2019** (~$1,395/yr academic, 2019 figure — *unverified for 2026*). So a public-library-only RepeatMasker run would miss most Candida elements. Standard fungal practice (e.g. funannotate) is a per-genome RepeatModeler2 library. Caveat: Candida TEs are **low copy number**, which makes de novo consensus-building harder — so **seed** the library with published *C. albicans* elements (16 Tca families; Zorro LINEs) and CGD's albicans TE consensus.
- **`-LTRStruct`** runs the community-standard LTRharvest→LTR_retriever path internally, giving high-quality full-length LTR-retrotransposon models — the dominant Candida TE class.
- **Avoid EDTA.** EDTA is plant-benchmarked and **not validated on fungi**; it will run (`--species others`) but with no fungal tuning. The LTR_retriever path (via `-LTRStruct`) is the safer, better-supported route.
- **Solo-LTR detection needs a homology post-step** — no tool emits solo LTRs directly. Take the LTR sequences of validated full-length elements and RepeatMasker/BLAST them back; LTR-length hits with no internal region = solo LTRs (→ `long_terminal_repeat`).
- **Zorro correction:** *C. albicans* Zorro1–3 are **non-LTR LINEs (L1 clade)**, not LTR elements — they are found by the RECON/RepeatScout core, not the LTR module. Class them as retrotransposon (see §5), not LTR.

### Environment note
The `trnascan`/Infernal micromamba env used in subtasks 1–4 does **not** include RepeatModeler. Install the `dfam/tetools` container on the dev box; write large intermediates to `/data/HTS` (root `/` is full — see the dev-server-disk note). Expected runtime on a ~14 Mb genome is **~1–4 h RepeatModeler + minutes for RepeatMasker** (*extrapolated from the 164 Mb Drosophila benchmark; no published 14 Mb benchmark*).

---

## 5. Decision 2 — Feature types (SO)

### Recommendation
**Reuse CGD's existing narrow set; introduce no new types.** Map every load-worthy element into one of three existing CGD types, and preserve the finer classification (Wicker superfamily, RepeatModeler family, SO accession) in the `headline`/`Note`, exactly as albicans does. Do **not** use `transposable_element`, and do **not** introduce the finer SO subtypes (`LTR_retrotransposon`, `DNA_transposon`, `satellite_DNA`, `low_complexity_region`, `tandem_repeat`) — none are currently in CGD and adding them would fork the vocabulary away from the albicans reference set.

**Load-time crosswalk (RepeatMasker/TESorter class → CGD `feature_type`):**

| Tool class (Wicker / RepeatMasker) | CGD `feature_type` | CGD SO | Load? |
|---|---|---|---|
| LTR retrotransposon, full-length (Gypsy `RLG`, Copia `RLC`) | `retrotransposon` | SO:0000180 | **Yes** |
| Solo LTR / LTR flanking an element | `long_terminal_repeat` | SO:0000286 | **Yes** |
| Non-LTR LINE (Zorro-like, `RIx`) | `retrotransposon` | SO:0000180 | **Yes** (note "non-LTR/LINE" in headline) |
| DNA transposon (TIR / Helitron) | `repeat_region` *(no CGD precedent; flag each for curator)* | SO:0000657 | **Curator-gated** |
| Major dispersed / tandem repeat (MRS, RPS-like, subtelomeric) | `repeat_region` | SO:0000657 | **Yes** |
| Simple_repeat / microsatellite | — | — | **No — mask only** |
| Low_complexity | — | — | **No — mask only** |
| Satellite (short) | `repeat_region` *(only if biologically notable)* | SO:0000657 | **Curator-gated** |
| Unclassified low-copy fragment below length/identity threshold | — | — | **No — mask only** |

### Rationale
- CGD lumps LTR retrotransposons under `retrotransposon` (the Tca precedent uses `retrotransposon`, **not** the finer SO `LTR_retrotransposon`). Staying with `retrotransposon` keeps the 3 new species consistent with the 34 albicans records. (For reference, **SGD** uses the finer `LTR_retrotransposon` SO:0000186 for full Ty elements and `long_terminal_repeat` for solo LTRs — a defensible alternative if curators would rather align to SGD than to CGD-albicans. **This is the one SO choice worth an explicit curator ruling.**)
- **Simple/low-complexity/microsatellite are excluded from loading.** albicans never loaded these as features; they are numerous, low-curation-value masking artifacts. We still generate them (for the masked genome / JBrowse repeat track) but do not create DB features.
- **DNA transposons are curator-gated** because CGD has no precedent type for them and Candida genomes carry few-to-none (Candida mobile DNA is overwhelmingly LTR retrotransposons + Zorro LINEs). If any real ones appear, curators decide between `repeat_region` and introducing a new type.
- There is **no official RepeatMasker→SO crosswalk** (RepeatMasker's own `rmOutToGFF3.pl` collapses everything to `dispersed_repeat` SO:0000658 in column 3). The table above is a CGD-specific mapping we author and own.

### Explicitly out of scope
`centromere` and `telomeric_repeat` are **not** products of a RepeatMasker pipeline. Candida centromeres are largely unique-sequence / epigenetically defined, not repeat-defined; telomeric repeats are a separate small hand-curated class. Leave both to dedicated curator entry, as in albicans/glabrata.

---

## 6. Decision 3 — Naming

We cannot reuse albicans' `Tca<N>` / Greek-letter names. Proposed **two-tier** scheme (mirrors how subtasks 1–4 separated systematic `feature_name` from biological `gene_name`, and how glabrata/dubliniensis legacy repeats were named):

**(a) `gene_name` / display name — family-based, biological.**
- For families that classify to a known Candida element by homology, use a **species-tagged Wicker-style family id**: `<Sp><3-letter Wicker code><n>`, e.g. `CtRLG1` = *C. tropicalis* LTR/Gypsy family 1, `CauRLC2` = *C. auris* LTR/Copia family 2, `CpRIL1` = *C. parapsilosis* LINE family 1. Individual copies get a chromosome/copy suffix like albicans (`CtRLG1-3` = copy on Chr 3).
- For major dispersed/tandem repeats, use descriptive names in the dubliniensis idiom: `<Assembly>.chrom<N>.MRS`, `…RPS`, etc.

**(b) `feature_name` — systematic, stable, unique.**
Two options; **curator picks one**:
- **Option B1 (dedicated repeat serial):** `<LOCUSPREFIX>_RPT_NNNN` per species (`CTRG_RPT_0001`, `B9J08_RPT_0001`, `CPAR2_RPT_0001`). Cleanest for elements that sit in intergenic/subtelomeric gaps far from any ORF. **Recommended.**
- **Option B2 (flanking-locus `.N`):** reuse the convention subtasks 1–4 adopted (`<lower-numbered flanking locus tag>.5/.6/…`). Consistent with the very recent RNA work, but awkward for repeats in gene-poor regions.

**Recommendation:** B1 for `feature_name` + family-based `gene_name`. Record the RepeatModeler family id, Wicker classification, and %identity-to-seed in the `headline`/`Note` for provenance (as albicans does).

> Naming is inherently a **curator decision** and is presented here for sign-off, consistent with every prior subtask (all deferred final names to curators).

---

## 7. What gets loaded vs. only masked

- **Loaded as DB features (curation-worthy):** full-length LTR retrotransposons, solo/flanking LTRs, non-LTR LINEs, and major dispersed/tandem repeat regions — subject to a minimum length + identity threshold and manual review of the family list.
- **Generated but not loaded:** simple repeats, low-complexity, microsatellites, and low-copy unclassified fragments. These feed (i) the **soft-masked genome** (for downstream gene-prediction/BLAST) and optionally (ii) a **JBrowse "RepeatMasker" track** (bulk GFF), but create no `FEATURE` rows.
- Deliverables per species mirror subtasks 1–4: DB inserts → ES reindex → JBrowse CGD-DB GFF injection → per-species flat file (TSV) for curator review → prod after sign-off.

---

## 8. Proposed pilot

Pilot **C. tropicalis first** (best assembly; closest well-annotated relative in *C. albicans* for homology seeding; already the pilot species for subtasks 1–4). Sequence:
1. Build container + seed library (albicans Tca + Zorro + CGD albicans TE consensus).
2. RepeatModeler2 `-LTRStruct` → TESorter → curated family list (expect a high "Unknown" fraction — Candida is non-model).
3. RepeatMasker → parse to candidate features → apply §5 crosswalk + §7 load/mask split.
4. Produce the **candidate feature TSV** (counts by class, names, coords, classification) and **review counts + names with curators** — the same gate used in every prior subtask — before writing any `FEATURE` rows.
5. On sign-off: load → ES → JBrowse → scale to auris + parapsilosis.

---

## 9. Open questions for curator sign-off

1. **SO alignment (the one real type choice):** keep full-length LTR-RTs as CGD `retrotransposon` (albicans-consistent) — or adopt SGD's finer `LTR_retrotransposon` (SO:0000186)? *Recommendation: stay `retrotransposon`.*
2. **DNA transposons / short satellites** if any survive review: `repeat_region`, or introduce a new type? *Recommendation: `repeat_region`, case-by-case.*
3. **`feature_name` scheme:** B1 `<PREFIX>_RPT_NNNN` (recommended) vs B2 flanking-locus `.N`.
4. **`gene_name` family scheme:** species-tagged Wicker ids (`CtRLG1`) acceptable, or another convention?
5. **Load threshold:** minimum length / %identity / copy-number for a repeat family to become DB features vs mask-only.
6. **RepBase:** is a 2026 GIRI academic license worth purchasing to improve fungal coverage, or is de-novo-only acceptable? *Recommendation: de-novo-only + albicans seeds is sufficient for a first pass.*

---

## Appendix A — Sequence Ontology reference (verified via EBI OLS4)

| Concept | SO accession | Label | In CGD today? |
|---|---|---|---|
| retrotransposon | SO:0000180 | `retrotransposon` | ✅ (albicans) |
| long terminal repeat | SO:0000286 | `long_terminal_repeat` | ✅ |
| repeat region | SO:0000657 | `repeat_region` | ✅ |
| centromere | SO:0000577 | `centromere` | ✅ |
| telomeric repeat | SO:0001496 | `telomeric_repeat` | ✅ |
| LTR retrotransposon | SO:0000186 | `LTR_retrotransposon` | ❌ (SGD uses) |
| non-LTR retrotransposon | SO:0000189 | `non_LTR_retrotransposon` | ❌ |
| LINE element | SO:0000194 | `LINE_element` | ❌ |
| SINE element | SO:0000206 | `SINE_element` | ❌ |
| DNA transposon | SO:0000182 | `DNA_transposon` | ❌ |
| terminal inverted repeat element | SO:0000208 | `terminal_inverted_repeat_element` | ❌ |
| satellite DNA | SO:0000005 | `satellite_DNA` | ❌ |
| tandem repeat | SO:0000705 | `tandem_repeat` | ❌ |
| microsatellite | SO:0000289 | `microsatellite` | ❌ |
| low complexity region | SO:0001005 | `low_complexity_region` | ❌ |
| dispersed repeat | SO:0000658 | `dispersed_repeat` | ❌ (RepeatMasker default) |
| transposable_element (generic) | SO:0000101 | `transposable_element` | ❌ — **deliberately unused** |

Disambiguations: `DNA_transposon` (SO:0000182, the class) ≠ `terminal_inverted_repeat_element` (SO:0000208, TIR-defined element) ≠ `terminal_inverted_repeat` (SO:0000481, the end sub-feature). `low_complexity_region` (SO:0001005, region feature) ≠ `low_complexity` (SO:0001004, property).

## Appendix B — Tooling reference

- **Dfam TE Tools** container `dfam/tetools` (Docker/Singularity) — bundles RepeatModeler **2.0.9**, RepeatMasker **4.2.4**, RMBlast, TRF, RECON, RepeatScout, GenomeTools, **LTR_retriever 2.9.0**, MAFFT, CD-HIT. https://github.com/Dfam-consortium/TETools
- **RepeatModeler2** — Flynn et al. 2020, *PNAS* 117(17):9451. https://www.pnas.org/doi/10.1073/pnas.1921046117
- **Dfam 3.9** (2025-01, CC0) — Storer et al. 2021, *Mobile DNA* 12:2. Metazoan-biased; thin for fungi.
- **RepBase / GIRI** — paywalled since 2019-04. https://www.girinst.org/server/RepBase/
- **TESorter** — Zhang et al. 2022, *Hortic Res* 9:uhac017. https://github.com/zhangrengang/TEsorter
- **LTR_retriever** — Ou & Jiang 2018, *Plant Physiol* 176:1410. https://github.com/oushujun/LTR_retriever
- **EDTA** v2.3 — *not fungal-validated; avoid for Candida.*
- **Wicker TE classification** — Wicker et al. 2007, *Nat Rev Genet* 8:973. Class I (LTR/LINE/SINE/DIRS/PLE) vs Class II (TIR/Helitron/Maverick); 3-letter codes RLG=Gypsy, RLC=Copia, RIx=LINE, DTx=TIR, DHH=Helitron.
- **Candida TEs** — Goodwin & Poulter 2000 (*Genome Res* 10:174, the 16 Tca families); Muszewska et al. 2020 (*Mobile DNA* 11:20, fungal comparative); **MycoMobilome** 2026 (*NARGB*, fungal-kingdom TE DB — candidate seed/reference source).

*Flags carried from research:* 14 Mb runtime is extrapolated, not benchmarked; RepBase 2026 price unconfirmed; no official RepeatMasker→SO crosswalk (we author ours); expect a high "Unknown" classification fraction for Candida; Zorro = LINE (non-LTR), not an LTR element.
