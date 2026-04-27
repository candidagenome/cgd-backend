# Virulence Factor Browser - Future Improvements Roadmap

## Completed Features
- [x] Confidence tier (High/Medium/Low) with scoring
- [x] Evidence type filter (GO/PHE/KW)
- [x] Paper count + PMID links to CGD reference pages
- [x] Split direct vs indirect evidence display
- [x] Expandable PMID list
- [x] Wider table layout with adjustable columns
- [x] #1 Auto-generated summary per gene (evidence-calibrated language)
- [x] #2 Evidence transparency/breakdown (GO annotations, phenotypes, papers)
- [x] AlphaFold structure links (🔬 icon) for genes with UniProt IDs
- [x] Cross-species orthologs display with clinical priority sorting
  - Species ordered by clinical importance: C. auris → C. glabrata → C. albicans → etc.
  - Links to Synteny Browser for detailed ortholog view
- [x] Gene links open in new window
- [x] GO evidence codes displayed (IDA, IMP, IEA, etc.) with manual evidence weighted higher
- [x] "Normal" qualifier phenotypes excluded from selection and scoring
- [x] Removed redundant evidence display (headline, gene pattern matches)
- [x] User-facing help page explaining methodology and limitations

## Removed Features
- ~~Importance badges ("In vivo", "Multi-study")~~ - Removed because computational labels were potentially misleading. Users should sort by confidence and review evidence directly.

## Future Improvements

### #3 Sorting / Ranking (Priority: High)
**Goal:** Let users answer "Show me the most important virulence genes"

**Implementation ideas:**
- Add sort options dropdown:
  - "Most studied" (sort by paper_count)
  - "Strongest experimental evidence" (sort by direct_evidence count)
  - "Core virulence genes" (genes with virulence model + multiple evidence types)
- Add filters:
  - "Has virulence model data"
  - "Has GO evidence"
  - "Min papers: X"
  - "Min confidence score: X"

### #4 Gene Relationships (Priority: High)
**Goal:** Show connections between genes (pathways, regulatory networks)

**What researchers want:**
- "What regulates this gene?"
- "What acts downstream?"
- Example: Bcr1 → ALS3 regulatory relationship

**Data sources to explore:**
- FeatRelationship table in database
- GO annotations for regulatory processes
- Literature-derived interactions

**UI ideas:**
- "Regulated by" / "Regulates" fields
- "Interacts with" list
- Mini network visualization

### #5 Condition / Strain Context (Priority: High)
**Goal:** Filter by strain, host, experimental condition

**Why critical:**
- Virulence effects are strain-specific
- Condition-specific (RPMI, catheter, Spider media, etc.)

**Implementation ideas:**
- Parse condition info from phenotype data
- Add strain filter (C. albicans SC5314, C. glabrata, C. auris, etc.)
- Add host filter (mouse, human, in vitro)
- Add condition tags from experiments

### #6 Multi-select / Compare View (Priority: Medium)
**Goal:** Side-by-side comparison of selected genes

**Features:**
- Checkbox to select multiple genes
- "Compare selected" button
- Side-by-side view showing:
  - Categories
  - Evidence breakdown
  - Phenotypes
  - Papers overlap

### #7 Derived Knowledge Layer (Priority: Medium-High)
**Goal:** Move from displaying evidence to interpreting it

**Computed scores to add:**
- "Virulence score" (weighted combination of evidence)
- "Biofilm importance score"
- "Host interaction strength"
- "Drug resistance relevance"

**Aggregation views:**
- "Top 10 adhesion genes in C. albicans"
- "Genes with both drug resistance + biofilm"
- Category overlap analysis

**Scoring rubric ideas:**
- Direct virulence model evidence: +5 points
- Multiple organism models: +3 points
- GO virulence terms: +2 points
- Phenotype evidence: +2 points
- High paper count (>10): +2 points
- Conservation across species: +1 point

---

## Technical Notes

### Current Evidence Weights (from virulence_schema.py)
```python
EVIDENCE_WEIGHTS = {
    "virulence_model": 5,       # Virulence phenotype (non-normal qualifier)
    "tier1_phenotype": 4,       # Direct virulence phenotype
    "tier2_phenotype": 3,       # Host interaction phenotype
    "virulence_go": 3,          # Pathogenesis/host GO terms (IEA)
    "virulence_go_manual": 4,   # Pathogenesis/host GO terms (IDA, IMP, etc.)
    "other_go_manual": 2,       # Other GO terms with manual evidence
    "other_go_iea": 1,          # Other GO terms with IEA evidence
    "disease_literature": 2,    # Disease literature topic
    "gene_pattern": 1,          # Gene name pattern match
    "keyword_match": 1,         # Headline keyword
    "housekeeping_penalty": -3, # Housekeeping gene penalty
}
```

**Note:** Phenotypes with "Normal" qualifier are excluded from scoring.

### Database Tables to Explore
- `FeatRelationship` - gene-gene relationships
- `PhenoAnnotation` - may have condition metadata
- `ExptProperty` / `ExptExptprop` - experimental conditions
- `RefProperty` - literature context

---

*Last updated: 2026-04-27*
