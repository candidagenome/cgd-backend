# Virulence Factor Browser - Technical Documentation

This document explains how the Virulence Factor Browser works, including evidence classification, summary generation, confidence scoring, and how PMIDs are collected.

---

## Table of Contents

1. [Direct vs Indirect Evidence Classification](#1-direct-vs-indirect-evidence-classification)
2. [Summary Generation](#2-summary-generation)
3. [In Vivo and Confidence Evidence](#3-in-vivo-and-confidence-evidence)
4. [PMID Collection](#4-pmid-collection)
5. [Summary Header Statistics](#5-summary-header-statistics)
6. [Confidence Score Calculation](#6-confidence-score-calculation)

---

## 1. Direct vs Indirect Evidence Classification

**File:** `cgd/schemas/virulence_schema.py`

### Overview
Evidence is split into "direct" (strong virulence relevance) and "indirect" (supporting/weaker) categories.

### Key Code Locations

| Component | Lines | Description |
|-----------|-------|-------------|
| `DIRECT_VIRULENCE_GO_TERMS` | 82-85 | GO terms indicating direct virulence |
| `DIRECT_PHENOTYPE_PATTERNS` | 88-95 | Phenotype patterns for Tier 1 & 2 |
| `split_evidence()` | 98-159 | Main function splitting evidence |

### Classification Rules

**Direct Evidence includes:**
- `virulence model:` prefix (always direct) - line 127-129
- Phenotype evidence matching Tier 1 & 2 patterns - lines 132-142
- GO terms containing: pathogenesis, virulence, host, invasion, adhesion, biofilm, filament, hyphal, morphogenesis - lines 146-154

**Indirect Evidence includes:**
- Tier 3 & 4 phenotypes (Stress Response, Indirect) - line 141
- Gene pattern matches - line 157
- Headline matches - line 157
- Literature topic matches - line 157
- Non-virulence GO terms - line 153

### Phenotype Tier Definitions

**File:** `cgd/schemas/virulence_schema.py`, lines 20-55

| Tier | Name | Patterns | Score |
|------|------|----------|-------|
| 1 | Direct Virulence | virulence, pathogenesis, host killing, infection, lethality, colonization | +5 |
| 2 | Host Interaction | host cell, phagocytosis, macrophage, epithelial, galleria, mouse | +4 |
| 3 | Stress Response | oxidative stress, heat shock, antifungal, azole | +2 |
| 4 | Indirect | resistance, susceptibility, sensitivity | +1 |

---

## 2. Summary Generation

**File:** `cgd/schemas/virulence_schema.py`

### Overview
Summaries are generated using evidence-calibrated language - stronger evidence allows stronger verb choices.

### Key Code Locations

| Component | Lines | Description |
|-----------|-------|-------------|
| `EVIDENCE_LANGUAGE_TIERS` | 190-221 | Tier-to-verb mapping |
| `CATEGORY_VIRULENCE_PHRASES` | 224-232 | Category relevance phrases |
| `determine_evidence_language_tier()` | 235-280 | Determines language tier |
| `generate_summary()` | 723-840 | Short summary (~150 chars) |
| `generate_summary_full()` | 843-938 | Full tooltip summary |
| `_extract_function_from_headline()` | 416-506 | Extract protein function |
| `_extract_actions_from_headline()` | 509-562 | Extract biological actions |

### Evidence Language Tiers

| Tier | Description | Allowed Verbs |
|------|-------------|---------------|
| `in_vivo_strong` | in vivo + high confidence | "required for", "plays a key role in", "promotes", "drives" |
| `experimental_strong` | phenotype/model + high confidence | "contributes to", "is involved in", "supports", "promotes" |
| `experimental_moderate` | phenotype/model + medium confidence | "is associated with", "has been linked to", "may contribute to" |
| `annotation_supported` | GO/KW only | "is associated with", "is annotated to" |
| `indirect_low` | weak/indirect evidence | "has limited evidence for", "may be linked to" |

### Summary Template
```
[Gene] is a [core function] that [action], [virulence clause]. [evidence ending if strong]
```

Example outputs:
- Strong: "ALS1 is an adhesin that promotes host adhesion, contributing to biofilm formation. Virulence demonstrated in vivo."
- Weak: "ACT1 is an actin cytoskeletal protein, with limited evidence for host interaction."

---

## 3. In Vivo and Confidence Evidence

**File:** `cgd/schemas/virulence_schema.py`

### In Vivo Detection

**Function:** `determine_evidence_language_tier()`, lines 235-280

In vivo evidence is detected by searching for keywords in match reasons:
- "virulence model" (line 251)
- "mouse" (line 252)
- "galleria" (line 253)
- "in vivo" (line 254)

### Virulence Model Query

**File:** `cgd/api/services/virulence_service.py`, lines 450-518

The system queries for virulence model evidence in two places:

1. **Phenotype observables** (lines 472-490):
   - Patterns: `%virulence%`, `%mouse%`, `%animal model%`, `%galleria%`

2. **Experiment properties** (lines 493-516):
   - Queries `ExptProperty.property_value` for VIRULENCE, MOUSE, GALLERIA

---

## 4. PMID Collection

### Overview
PMIDs are collected from references linked to each gene feature via the `RefLink` table.

### Key Code Locations

**File:** `cgd/api/services/virulence_service.py`

| Function | Lines | Description |
|----------|-------|-------------|
| `_get_paper_count_and_pmids()` | 178-211 | Oracle-based PMID query |

**File:** `cgd/api/services/es_indexer.py`

| Function | Lines | Description |
|----------|-------|-------------|
| `_get_paper_count_and_pmids_es()` | 1078-1111 | ES indexing PMID query |

### Query Logic (virulence_service.py:192-205)

```python
refs = (
    db.query(Reference.pubmed)
    .join(RefLink, RefLink.reference_no == Reference.reference_no)
    .filter(
        RefLink.tab_name == "FEATURE",
        RefLink.primary_key == feature.feature_no,
        Reference.pubmed.isnot(None),
    )
    .distinct()
    .all()
)
```

### What are "Keywords" for PMID sources?

PMIDs come from multiple evidence types, tracked in `match_reasons`:

| Evidence Type | Prefix | Description |
|---------------|--------|-------------|
| `PHE` | `phenotype:` | Phenotype annotations |
| `GO` | `GO:` | Gene Ontology annotations |
| `KW` | `gene pattern:`, `headline:`, `literature topic:` | Keyword/text matches |

**File:** `cgd/schemas/virulence_schema.py`, lines 313-329

```python
EVIDENCE_TYPES = {
    "GO": {"match_prefixes": ["GO:"]},
    "PHE": {"match_prefixes": ["phenotype:", "virulence model:"]},
    "KW": {"match_prefixes": ["gene pattern:", "headline:", "literature topic:"]},
}
```

---

## 5. Summary Header Statistics

### Frontend Display

**File:** `cgd-frontend/src/pages/VirulenceFactorBrowserPage.jsx`, lines 941-950

```jsx
<span className="tier-item tier-experimental">
  <strong>{tierCounts.withExperimental}</strong> with experimental phenotype evidence
  {tierCounts.validatedInVivo > 0 && (
    <span className="tier-sub">({tierCounts.validatedInVivo} in vivo)</span>
  )}
</span>
```

### How Counts are Calculated

**File:** `cgd-frontend/src/pages/VirulenceFactorBrowserPage.jsx`, lines 380-420

```javascript
// For each result item:

// 1. Count with experimental evidence (phenotype or virulence model)
const hasExperimental = allEvidence.some((e) => {
  const eLower = e.toLowerCase();
  return eLower.startsWith('virulence model:') || eLower.startsWith('phenotype:');
});
if (hasExperimental) withExperimental++;

// 2. Count with GO annotations
const hasGO = allEvidence.some((e) => e.toLowerCase().startsWith('go:'));
if (hasGO) withGO++;

// 3. Count validated in vivo (importance_level === 'high')
if (item.importance_level === 'high') validatedInVivo++;
```

---

## 6. Confidence Score Calculation

**File:** `cgd/schemas/virulence_schema.py`

### Score Weights

**Lines:** 1022-1031

```python
EVIDENCE_WEIGHTS = {
    "virulence_model": 5,       # Tested in mouse/Galleria
    "tier1_phenotype": 4,       # Direct virulence phenotype
    "tier2_phenotype": 3,       # Host interaction phenotype
    "virulence_go": 3,          # Pathogenesis/host GO terms
    "disease_literature": 2,    # Disease literature topic
    "gene_pattern": 1,          # Gene name pattern match
    "keyword_match": 1,         # Headline keyword
    "housekeeping_penalty": -3, # Housekeeping gene penalty
}
```

### Calculation Function

**File:** `cgd/api/services/virulence_service.py`, lines 214-258

```python
def _calculate_confidence_score(match_reasons, evidence_tier, is_housekeeping):
    score = 0

    for reason in match_reasons:
        if "virulence model:" in reason_lower:
            score += 5  # virulence_model
        elif "phenotype:" in reason_lower:
            if evidence_tier == 1:
                score += 4  # tier1_phenotype
            elif evidence_tier == 2:
                score += 3  # tier2_phenotype
        elif "go:" in reason_lower:
            if any(t in reason_lower for t in ["pathogenesis", "host", "virulence"]):
                score += 3  # virulence_go
        elif "literature topic: disease" in reason_lower:
            score += 2  # disease_literature
        elif "gene pattern:" in reason_lower:
            score += 1  # gene_pattern
        elif "headline:" in reason_lower:
            score += 1  # keyword_match

    if is_housekeeping:
        score -= 3  # housekeeping_penalty

    return max(0, min(20, score))  # Clamp to 0-20
```

### Confidence Tiers

**File:** `cgd/schemas/virulence_schema.py`, lines 167-181

| Tier | Score Range | Description |
|------|-------------|-------------|
| High | 10-20 | Strong direct virulence evidence |
| Medium | 5-9 | Moderate evidence with host interaction |
| Low | 0-4 | Indirect or weak evidence |

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `cgd/schemas/virulence_schema.py` | Core definitions, evidence classification, summary generation |
| `cgd/api/services/virulence_service.py` | Oracle database queries, evidence collection |
| `cgd/api/services/es_indexer.py` | Elasticsearch indexing (pre-computes all fields) |
| `cgd/api/services/es_search_service.py` | Elasticsearch search queries (lines 2417-2720) |
| `cgd/api/routers/virulence_router.py` | API endpoints |
| `cgd-frontend/src/pages/VirulenceFactorBrowserPage.jsx` | Frontend display |

---

## Reindexing

To update summaries and evidence after code changes:

```bash
ssh cgd-backend-dev
cd work/cgd-backend
source .venv/bin/activate
python -c "
from cgd.db.deps import get_db_session
from cgd.core.elasticsearch import get_es_client
from cgd.api.services import es_indexer

db = next(get_db_session())
es = get_es_client()
print('ES connected:', es.ping())
print('Starting reindex...')
es_indexer.rebuild_index(db, es)
print('Done!')
"
```

---

## Questions or Changes?

Review the schema file (`virulence_schema.py`) for all configurable patterns, tiers, and scoring weights. Most changes can be made by updating the dictionaries at the top of the file.
