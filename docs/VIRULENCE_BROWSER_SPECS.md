# Virulence Factor Browser - Technical Specifications

This document provides detailed technical specifications for the key algorithms in the Virulence Factor Browser: gene summary generation, confidence scoring, and ortholog mapping.

---

## Table of Contents

1. [Gene Summary Generation](#1-gene-summary-generation)
2. [Confidence Scoring System](#2-confidence-scoring-system)
3. [Ortholog Mapping System](#3-ortholog-mapping-system)
4. [Virulence Categories and GO Terms](#4-virulence-categories-and-go-terms)

---

## 1. Gene Summary Generation

### Overview

The system generates two types of summaries:
- **`summary`** (~150 chars): Concise summary for table display
- **`summary_full`** (~400 chars): Detailed summary for tooltips/expansion

### Step 1: Determine Evidence Language Tier

The function `determine_evidence_language_tier()` classifies evidence strength:

| Tier | Criteria | Allowed Verbs |
|------|----------|---------------|
| **in_vivo_strong** | In vivo evidence + high confidence | "required for", "promotes", "drives", "mediates" |
| **experimental_strong** | Phenotype/model + high confidence | "required for", "contributes to", "promotes" |
| **experimental_moderate** | Phenotype/model + medium confidence | "is associated with", "may contribute to" |
| **annotation_supported** | GO/keyword only | "is linked to", "is annotated to" |
| **indirect_low** | Weak/indirect evidence | "has limited evidence for", "may be linked to" |

### Step 2: Extract Biological Information from Headline

**`_extract_function_from_headline()`** identifies the protein type:
- Checks for patterns like "transcription factor", "kinase", "adhesin", "protease"
- Returns role like "transcription factor", "secreted aspartyl protease", "kinase"

**`_extract_actions_from_headline()`** extracts actions:
- Finds patterns: "regulates X", "required for Y", "promotes Z"
- Returns up to 3 action phrases

### Step 3: Get Protein-Type-Specific Verb

`get_verb_for_protein_type()` maps protein types to appropriate verbs:

| Protein Type | Verb |
|--------------|------|
| Transcription factor | "regulating" |
| Kinase, phosphatase | "involved in" |
| Adhesin | "mediating" |
| Chaperone | "supporting" |
| Default | "involved in" |

### Step 4: Get Virulence Relevance Phrase

`get_virulence_phrase()` maps categories to virulence context:

| Category | Phrase |
|----------|--------|
| Adhesins | "host adhesion" |
| Secreted Enzymes | "secreted enzymatic activity" |
| Morphogenesis | "morphogenesis" |
| Biofilm Formation | "biofilm formation" |

### Step 5: Deduplicate Concepts

`dedupe_concepts()` normalizes similar concepts to avoid redundancy:
- "biofilm formation" -> "biofilm"
- "cell adhesion", "host adhesion" -> "adhesion"
- "immune evasion" -> "host interaction"

### Step 6: Build Summary Based on Evidence Tier

**Strong Evidence (in_vivo_strong, experimental_strong):**
```
{gene_name} is a {role} required for {concepts}, with multiple studies supporting a role in virulence.
```
- Uses "required for" **ONLY** with causal evidence (tier 1 phenotype, knockout)
- Otherwise uses protein-type-specific verb

**Moderate Evidence (experimental_moderate):**
```
{gene_name} is a {role} involved in {concepts}.
```

**Annotation Only (annotation_supported):**
```
{gene_name} is a {role} linked to {concept}.
```

**Weak Evidence (indirect_low):**
```
{gene_name} is a {role} with limited evidence linking it to {concept}.
```

### Step 7: Add Paper-Scaled Evidence Phrase

`_get_paper_scaled_phrase()` adds context based on publication count:

| Papers | Phrase Added |
|--------|--------------|
| 10+ | ", with multiple studies supporting a role in virulence" |
| 5-9 | ", with evidence supporting a role in virulence" |
| 1-4 + in vivo | ", with in vivo evidence supporting a role in virulence" |

### Step 8: Post-Processing Repairs

`repair_summary()` fixes common issues:
- "plays a role in X, contributing to Y" -> "required for X and Y"
- "is involved in X, involved in Y" -> "is involved in X and Y"
- Removes double spaces, "that that" stutters, fixes punctuation

### Example Outputs

| Gene | Evidence | Summary |
|------|----------|---------|
| ALS3 | High + in vivo | "ALS3 is an adhesin required for host adhesion and biofilm, with multiple studies supporting a role in virulence." |
| SAP2 | High | "SAP2 is a secreted aspartyl protease involved in host interaction." |
| ERG11 | Moderate | "ERG11 is a cytochrome P450 involved in drug response." |
| Unknown | Low | "XYZ1 is a protein with limited evidence linking it to morphogenesis." |

### Key Design Principles

1. **"Required for" is restricted** - Only used with causal evidence (knockout, virulence phenotype)
2. **Protein-type verbs** - TFs "regulate", enzymes "are involved in", adhesins "mediate"
3. **Semantic deduplication** - Avoids "adhesion and host adhesion"
4. **Paper-count scaling** - Stronger language for well-studied genes
5. **Evidence calibration** - Language strength matches evidence strength

---

## 2. Confidence Scoring System

### Overview

The confidence scoring system evaluates the **quality and strength of evidence** linking a gene to virulence. It produces:

1. **Confidence Score** (0-20): Numeric score based on evidence weights
2. **Confidence Tier** (High/Medium/Low): Human-readable classification
3. **Evidence Tier** (1-4): Phenotype-based evidence quality ranking
4. **Importance Level**: Combined assessment for prioritization

### Step 1: Evidence Tier Classification

`_classify_phenotype_tier()` classifies phenotype evidence into 4 tiers:

| Tier | Name | Patterns | Score Contribution |
|------|------|----------|-------------------|
| **1** | Direct Virulence | `virulence`, `pathogenesis`, `host killing`, `infection`, `lethality`, `colonization` | +5 |
| **2** | Host Interaction | `host cell`, `phagocytosis`, `macrophage`, `epithelial`, `endothelial`, `invasion`, `galleria`, `mouse`, `animal model` | +4 |
| **3** | Stress Response | `oxidative stress`, `heat shock`, `antifungal`, `azole`, `echinocandin` | +2 |
| **4** | Indirect | `resistance`, `susceptibility`, `sensitivity` | +1 |

**Virulence model evidence** (tested in mouse/Galleria) is automatically classified as **Tier 1**.

### Step 2: Housekeeping Gene Detection

`_is_housekeeping_gene()` identifies housekeeping genes using two methods:

**Method 1: GO Term Match**

Checks for housekeeping-related GO annotations:
- `GO:0006412` - translation
- `GO:0006414` - translational elongation
- `GO:0006260` - DNA replication
- `GO:0006350` - transcription
- `GO:0006457` - protein folding
- `GO:0007049` - cell cycle
- `GO:0006096` - glycolysis
- `GO:0006099` - tricarboxylic acid cycle
- `GO:0015031` - protein transport
- `GO:0006281` - DNA repair

**Method 2: Ortholog Conservation**

If gene has orthologs in **>=5 Candida species**, it's flagged as housekeeping (essential genes are highly conserved).

### Step 3: Calculate Confidence Score

`_calculate_confidence_score()` computes score (0-20) based on evidence weights:

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

**Scoring Logic:**

```python
for each match_reason:
    if "virulence model:" -> score += 5
    elif "phenotype:" and tier == 1 -> score += 4
    elif "phenotype:" and tier == 2 -> score += 3
    elif "GO:" with host interaction/symbiont terms -> score += 3
    elif "literature topic: disease" -> score += 2
    elif "gene pattern:" -> score += 1
    elif "headline:" -> score += 1

if is_housekeeping:
    score -= 3  # Penalty

return clamp(score, 0, 20)
```

### Step 4: Map Score to Confidence Tier

`get_confidence_tier()` converts numeric score to tier:

| Score Range | Confidence Tier | Description |
|-------------|-----------------|-------------|
| **10-20** | High | Strong direct virulence evidence |
| **5-9** | Medium | Moderate evidence with host interaction |
| **0-4** | Low | Indirect or weak evidence |

### Step 5: Calculate Importance Level

`calculate_importance_level()` combines multiple factors for prioritization:

**Input Factors:**
- Direct evidence count
- Virulence model evidence (yes/no)
- Paper count
- Phenotype count
- GO annotation count
- Confidence score

**Scoring:**

| Factor | Points |
|--------|--------|
| Has virulence model | +4 |
| >=3 direct evidence | +3 |
| >=1 direct evidence | +1 |
| >=10 papers | +3 |
| >=5 papers | +2 |
| >=2 papers | +1 |
| >=2 phenotypes + >=1 GO | +2 |
| Confidence score >=15 | +2 |
| Confidence score >=10 | +1 |

**Importance Levels:**

| Score | Level | Label Examples |
|-------|-------|----------------|
| **>=8** | High | "Core virulence factor", "Validated in vivo", "Well-characterized", "Strong evidence" |
| **4-7** | Medium | "Multiple studies", "Direct evidence", "Moderate evidence" |
| **0-3** | Low | "Phenotype support", "GO annotation", "Indirect evidence" |

### Example Calculations

#### Example 1: ALS3 (Well-studied adhesin)
```
Match reasons:
- "virulence model: mouse systemic infection" -> +5
- "phenotype: adhesion" (tier 1) -> +4
- "phenotype: biofilm formation" (tier 1) -> +4
- "GO: interaction with host" -> +3
- "gene pattern: ALS3" -> +1

Raw score: 17
Housekeeping: No
Final score: 17 -> High confidence

Papers: 45
Importance: 4 (virulence model) + 3 (direct>=3) + 3 (papers>=10) + 2 (phenotypes+GO) + 2 (score>=15) = 14 -> High
Label: "Core virulence factor"
```

#### Example 2: ERG11 (Drug resistance gene)
```
Match reasons:
- "phenotype: azole resistance" (tier 3) -> +0
- "GO: response to drug" -> +0 (not virulence GO)
- "gene pattern: ERG11" -> +1

Raw score: 1
Housekeeping: No
Final score: 1 -> Low confidence

Papers: 30
Importance: 0 + 0 + 3 (papers>=10) = 3 -> Low
Label: "Indirect evidence"
```

#### Example 3: ACT1 (Housekeeping gene)
```
Match reasons:
- "phenotype: virulence" (tier 1) -> +4
- "GO: cytoskeleton" -> +0

Raw score: 4
Housekeeping: Yes (GO: actin cytoskeleton)
Penalty: -3
Final score: 1 -> Low confidence

Label: "GO annotation"
```

### Data Flow Diagram

```
                    Match Reasons
                         |
         +---------------+---------------+
         v               v               v
   Phenotype         GO Terms      Gene Pattern
   Evidence                         Headline
         |               |               |
         v               |               |
  _classify_phenotype_tier()             |
         |               |               |
         v               v               v
    Evidence Tier    Virulence GO?   Weight: +1
       (1-4)         Weight: +3
         |               |
         v               v
  +--------------------------------------+
  |      _calculate_confidence_score()   |
  |  Sum weights + housekeeping penalty  |
  |           Clamp to 0-20              |
  +--------------------------------------+
                    |
                    v
         get_confidence_tier()
              High/Medium/Low
                    |
                    v
        calculate_importance_level()
         + paper count + direct count
                    |
                    v
         Importance Level + Label
```

### API Parameters for Filtering

The API supports filtering by confidence:

| Parameter | Type | Description |
|-----------|------|-------------|
| `max_evidence_tier` | int (1-4) | Only include genes with tier <= this value |
| `min_confidence_score` | int (0-20) | Only include genes with score >= this value |
| `sort_by` | string | Sort by `confidence_score`, `gene_name`, or `evidence_tier` |

---

## 3. Ortholog Mapping System

### Overview

The ortholog mapping system identifies and displays **cross-species ortholog relationships** for virulence factor genes across Candida species. It uses the **CGOB (Candida Gene Order Browser)** homology data to find orthologs and displays them with clinical priority ordering.

### Database Schema

#### HomologyGroup Table

Stores homology group metadata:

| Column | Type | Description |
|--------|------|-------------|
| `homology_group_no` | NUMBER(10) | Primary key |
| `homology_group_type` | VARCHAR(40) | Type: `ortholog`, `paralog`, etc. |
| `method` | VARCHAR(40) | Method used: `CGOB` |
| `homology_group_id` | VARCHAR(40) | Group name/ID |

#### FeatHomology Table

Links features to homology groups (many-to-many):

| Column | Type | Description |
|--------|------|-------------|
| `feat_homology_no` | NUMBER(10) | Primary key |
| `feature_no` | NUMBER(10) | FK to Feature table |
| `homology_group_no` | NUMBER(10) | FK to HomologyGroup table |

### Backend Implementation

#### Step 1: Find Homology Groups

`_get_ortholog_details()` first finds all homology groups the query gene belongs to:

```python
homology_group_nos = (
    db.query(FeatHomology.homology_group_no)
    .filter(FeatHomology.feature_no == feature.feature_no)
    .all()
)
```

#### Step 2: Query Orthologs

Then queries all other genes in those homology groups:

```python
orthologs = (
    db.query(
        Feature.feature_name,
        Feature.gene_name,
        Organism.organism_abbrev,
        Organism.organism_name,
    )
    .join(FeatHomology, FeatHomology.feature_no == Feature.feature_no)
    .join(HomologyGroup, FeatHomology.homology_group_no == HomologyGroup.homology_group_no)
    .join(Organism, Feature.organism_no == Organism.organism_no)
    .filter(FeatHomology.homology_group_no.in_(hg_nos))
    .filter(HomologyGroup.method == 'CGOB')                    # Only CGOB data
    .filter(HomologyGroup.homology_group_type == 'ortholog')   # Only orthologs
    .filter(Feature.feature_no != feature.feature_no)          # Exclude query gene
    .distinct()
    .all()
)
```

#### Step 3: Group by Organism

One representative gene per organism (handles cases where multiple paralogs exist):

```python
organism_orthologs = {}
for feat_name, gene_name, org_abbrev, org_name in orthologs:
    if org_abbrev not in organism_orthologs:
        organism_orthologs[org_abbrev] = {
            "organism_abbrev": org_abbrev,
            "organism_name": org_name,
            "gene_name": gene_name or feat_name,
            "feature_name": feat_name,
        }
```

#### Output Format

Returns list of ortholog dictionaries:

```json
[
  {
    "organism_abbrev": "C_auris_B8441",
    "organism_name": "Candida auris B8441",
    "gene_name": "ALS3",
    "feature_name": "B9J08_002761"
  },
  {
    "organism_abbrev": "C_glabrata_CBS138",
    "organism_name": "Candida glabrata CBS 138",
    "gene_name": "EPA1",
    "feature_name": "CAGL0E06644g"
  }
]
```

### Ortholog Count for Housekeeping Detection

`_get_ortholog_count()` counts distinct organisms with orthologs (used for housekeeping gene detection):

```python
organism_count = (
    db.query(func.count(distinct(Feature.organism_no)))
    .join(FeatHomology, ...)
    .join(HomologyGroup, ...)
    .filter(FeatHomology.homology_group_no.in_(hg_nos))
    .filter(HomologyGroup.method == 'CGOB')
    .filter(HomologyGroup.homology_group_type == 'ortholog')
    .scalar()
)
```

**Rule**: If ortholog_count >= 5 -> gene is flagged as **housekeeping** (highly conserved = essential function)

### Frontend Display

#### Clinical Priority Ordering

Orthologs are sorted by **clinical importance**, not alphabetically:

```javascript
const SPECIES_PRIORITY = {
  'auris': 1,       // Emerging multidrug-resistant pathogen
  'glabrata': 2,    // Common clinical isolate
  'albicans': 3,    // Most common pathogen
  'tropicalis': 4,  // Clinical relevance
  'parapsilosis': 5,
  'dubliniensis': 6,
  'lusitaniae': 7,
};
```

**Rationale**: *C. auris* is shown first because it's the most clinically urgent emerging pathogen; *C. glabrata* second due to increasing prevalence and drug resistance.

#### Display Formatting

`formatOrthologDisplay()` creates a compact display:

```javascript
// Sort by clinical importance
const sorted = [...orthologs].sort((a, b) =>
  getSpeciesPriority(a) - getSpeciesPriority(b)
);

// Create short species names: "Candida auris B8441" -> "C. auris"
const getShortSpecies = (org) => {
  const parts = org.organism_name.split(' ');
  return `${parts[0].charAt(0)}. ${parts[1]}`;
};

// Show first 3, then "+N more"
const displaySpecies = sorted.slice(0, 3).map(getShortSpecies);
const remaining = sorted.length - 3;

let text = displaySpecies.join(', ');
if (remaining > 0) {
  text += ` +${remaining} more`;
}
```

**Example Output**: `C. auris, C. glabrata, C. tropicalis +2 more`

#### Synteny Viewer Link

Links to the synteny browser for comparative genomics visualization:

```jsx
<a
  href={`/synteny-browser?gene=${params.data.feature_name}`}
  target="_blank"
  rel="noopener noreferrer"
  className="synteny-link"
  title={`View ${orthologs.length} orthologs in synteny browser`}
>
  -> View
</a>
```

### Data Flow Diagram

```
                    Query Gene (e.g., ALS3)
                           |
                           v
              +------------------------+
              |    FeatHomology Table  |
              |  Find homology_group_no|
              +------------------------+
                           |
                           v
              +------------------------+
              |   HomologyGroup Table  |
              |  Filter: method='CGOB' |
              |  Filter: type='ortholog'|
              +------------------------+
                           |
                           v
              +------------------------+
              |    FeatHomology Table  |
              |  Find all features in  |
              |  same homology groups  |
              +------------------------+
                           |
                           v
              +------------------------+
              |     Feature + Organism |
              |  Get gene_name, org_name|
              |  (exclude query gene)  |
              +------------------------+
                           |
                           v
              +------------------------+
              |  Group by Organism     |
              |  1 gene per organism   |
              +------------------------+
                           |
                           v
                    API Response
                           |
           +---------------+---------------+
           v                               v
    Ortholog Count                  Ortholog Details
    (for housekeeping              (for display)
     detection)
           |                               |
           v                               v
    If count >= 5                  Sort by Clinical
    -> Housekeeping                   Priority
    -> Score penalty -3                    |
                                           v
                                   Display: "C. auris,
                                   C. glabrata +2 more"
                                           |
                                           v
                                   Synteny Viewer Link
```

### Example Query

**Input**: ALS3 gene from *C. albicans*

**Database Query Flow**:
1. ALS3 (feature_no=12345) -> FeatHomology -> homology_group_no=789
2. HomologyGroup 789: method='CGOB', type='ortholog'
3. FeatHomology where homology_group_no=789 -> features: [12345, 23456, 34567, 45678, 56789]
4. Exclude 12345 (query gene)
5. Join Feature + Organism for remaining

**Output**:
```json
{
  "orthologs": [
    {"organism_abbrev": "C_auris_B8441", "organism_name": "Candida auris B8441", "gene_name": "ALS3", "feature_name": "B9J08_002761"},
    {"organism_abbrev": "C_glabrata_CBS138", "organism_name": "Candida glabrata CBS 138", "gene_name": "EPA6", "feature_name": "CAGL0L01419g"},
    {"organism_abbrev": "C_tropicalis_MYA3404", "organism_name": "Candida tropicalis MYA-3404", "gene_name": "ALS3", "feature_name": "CTRG_02293"},
    {"organism_abbrev": "C_parapsilosis_CDC317", "organism_name": "Candida parapsilosis CDC317", "gene_name": "ALS7", "feature_name": "CPAR2_204720"}
  ],
  "ortholog_count": 4
}
```

**Frontend Display**:
```
Orthologs: C. auris, C. glabrata, C. tropicalis +1 more -> View
```

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **CGOB method only** | CGOB provides curated Candida-specific ortholog data |
| **One gene per organism** | Simplifies display; avoids listing multiple paralogs |
| **Clinical priority sorting** | Highlights most medically relevant species first |
| **>=5 orthologs = housekeeping** | Highly conserved genes are likely essential, not virulence-specific |
| **Synteny viewer link** | Enables comparative genomics exploration |
| **Short species names** | "C. auris" is more readable than full strain names |

---

## 4. Virulence Categories and GO Terms

The virulence factor browser classifies genes into categories based on GO terms, phenotype patterns, and gene name patterns. Here are the GO terms used for each category:

| Category | GO Terms | Description |
|----------|----------|-------------|
| **Adhesins** | `GO:0007155`, `GO:0044406` | cell adhesion, adhesion to host |
| **Secreted Enzymes** | `GO:0008233`, `GO:0016298`, `GO:0008970` | protease, lipase, phospholipase |
| **Morphogenesis** | `GO:0001403` | invasive filamentous growth |
| **Host Interaction** | `GO:0051701`, `GO:0044114`, `GO:0044409` | interaction with host, development of symbiont in host, symbiont entry into host |
| **Biofilm Formation** | `GO:0044010`, `GO:0043709` | biofilm formation, biofilm matrix |
| **Immune Evasion** | `GO:0042783`, `GO:0052553`, `GO:0030682` | symbiont-mediated evasion/perturbation of host immune response/defenses |
| **Drug Resistance** | `GO:0042493`, `GO:0046677`, `GO:0071466` | response to drug, response to antibiotic, cellular response to xenobiotic stimulus |

**Note**: `GO:0009405` (pathogenesis) was obsoleted by GO in 2021 and is no longer used.

---

## File References

| File | Purpose |
|------|---------|
| `cgd/schemas/virulence_schema.py` | Core definitions, evidence classification, summary generation |
| `cgd/api/services/virulence_service.py` | Oracle database queries, confidence calculation, ortholog queries |
| `cgd/api/services/es_indexer.py` | Elasticsearch indexing (pre-computes all fields) |
| `cgd/api/services/es_search_service.py` | Elasticsearch search queries |
| `cgd-frontend/src/pages/VirulenceFactorBrowserPage.jsx` | Frontend display, clinical priority sorting |
