# Virulence Factor Browser - Future Improvements Roadmap

## Completed Features
- [x] Confidence tier (High/Medium/Low) with scoring
- [x] Evidence type filter (GO/PHE/KW)
- [x] Paper count + PMID links to CGD reference pages
- [x] Split direct vs indirect evidence display
- [x] Expandable PMID list
- [x] Wider table layout with adjustable columns

## In Progress
- [ ] #1 Auto-generated summary per gene
- [ ] #2 Evidence transparency/breakdown

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
    "virulence_model": 5,      # Direct virulence evidence
    "go_pathogenesis": 4,      # GO pathogenesis terms
    "go_host_interaction": 3,  # GO host interaction
    "phenotype_direct": 3,     # Direct phenotypes (virulence, killing)
    "phenotype_indirect": 2,   # Indirect phenotypes (biofilm, adhesion)
    "keyword_match": 1,        # Gene name/headline patterns
}
```

### Database Tables to Explore
- `FeatRelationship` - gene-gene relationships
- `PhenoAnnotation` - may have condition metadata
- `ExptProperty` / `ExptExptprop` - experimental conditions
- `RefProperty` - literature context

---

*Last updated: 2026-04-17*
