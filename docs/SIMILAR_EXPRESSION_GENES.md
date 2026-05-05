# Similar Expression Genes Feature

## Overview

This feature finds genes with correlated expression profiles to a query gene, inspired by [Microarray-GeneXplorer](https://metacpan.org/dist/Microarray-GeneXplorer). Given a gene of interest, it returns a ranked list of genes with similar expression patterns across experimental conditions.

## API Endpoint

```
GET /api/expression/gene/{gene_name}/similar
```

### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `organism` | string | `C_albicans_SC5314_A22` | Organism to search |
| `limit` | int | 20 | Maximum results to return (1-100) |
| `metric` | string | `pearson` | Similarity metric: `pearson`, `spearman`, or `cosine` |
| `min_conditions` | int | 5 | Minimum shared conditions required for comparison |
| `value_type` | string | `log2fc` | Value type for comparison (see below) |

### Example Requests

```bash
# Default: log2 fold change with Pearson correlation
curl "http://localhost:8000/api/expression/gene/HOG1/similar?organism=C_albicans_SC5314_A22&limit=20"

# Z-score normalized for better cross-study comparison
curl "http://localhost:8000/api/expression/gene/HOG1/similar?value_type=zscore&metric=pearson"

# Rank-based with Spearman correlation (most robust)
curl "http://localhost:8000/api/expression/gene/HOG1/similar?value_type=rank&metric=spearman"
```

### Response Format

```json
{
  "success": true,
  "query_gene": "HOG1",
  "query_feature_name": "orf19.5820",
  "organism": "C_albicans_SC5314_A22",
  "metric": "pearson",
  "value_type": "log2fc",
  "similar_genes": [
    {
      "gene_name": "PBS2",
      "feature_name": "orf19.4668",
      "description": "MAPKK of the HOG signaling pathway",
      "correlation": 0.892,
      "p_value": 1.2e-15,
      "shared_conditions": 58
    },
    {
      "gene_name": "SSK2",
      "feature_name": "orf19.4523",
      "description": "MAPKKK involved in HOG pathway",
      "correlation": 0.845,
      "p_value": 3.4e-12,
      "shared_conditions": 58
    }
  ],
  "total_genes_compared": 6215,
  "conditions_used": 62,
  "computation_time_ms": 1523.4
}
```

## Value Types for Comparison

The `value_type` parameter controls how expression values are transformed before correlation analysis. This is important because each study has its own control condition and sequencing depth.

| Value Type | Description | Best For |
|------------|-------------|----------|
| `log2fc` | Log2(fold change) - default | Cross-study comparison; symmetric scale for up/down regulation |
| `fold_change` | Raw fold change values | Within-study comparison; intuitive interpretation |
| `zscore` | Z-score normalized within each study | Cross-study comparison; removes study-specific scale differences |
| `rank` | Rank-transformed values | Robust to outliers; pairs well with Spearman correlation |

### Why Multiple Options?

**The Control Condition Challenge:**
- Each study defines its own control condition (e.g., "untreated", "wild-type")
- Fold changes are relative to that study's control
- Cross-study fold changes aren't directly comparable

**Recommended Combinations:**
- `log2fc` + `pearson`: Standard genomics approach, symmetric scale
- `zscore` + `pearson`: Best for cross-study analysis, removes batch effects
- `rank` + `spearman`: Most robust, handles outliers and non-linear patterns
- `fold_change` + `pearson`: Simple interpretation, best for single-study analysis

### Value Transformations

```
Raw expression value: 150.0 (treatment), 50.0 (control)

fold_change:  150 / 50 = 3.0
log2fc:       log2(3.0) = 1.585
zscore:       (value - study_mean) / study_std
rank:         position in sorted list (1 to N)
```

## Algorithm

### Expression Vector Construction

For each gene, we build a fold-change vector across all experimental conditions:

```
Gene A: [1.2, 0.8, 2.1, 0.5, 1.5, ...]  # ~60-100+ conditions
Gene B: [1.1, 0.9, 2.3, 0.4, 1.6, ...]
```

Each value represents the fold-change of expression relative to the control condition within that study.

### Similarity Metrics

**Pearson Correlation (default)**
- Measures linear relationship between expression profiles
- Range: -1 (anti-correlated) to +1 (perfectly correlated)
- Most commonly used in genomics for co-expression analysis

**Spearman Correlation**
- Rank-based correlation, more robust to outliers
- Good for detecting monotonic relationships
- Useful when data has non-linear patterns

**Cosine Similarity**
- Measures angle between expression vectors
- Focuses on pattern similarity, not magnitude
- Range: -1 to +1

### Comparison Process

1. Extract expression profile (fold-changes) for the query gene
2. Extract profiles for all other genes in the organism
3. For each gene pair, compute correlation using shared conditions
4. Require minimum overlap (default: 5 conditions)
5. Rank by correlation coefficient
6. Return top N similar genes with statistics

## Data Sources

Expression data comes from RNA-seq studies stored as BigWig files:

| Organism | Studies | Total Conditions |
|----------|---------|------------------|
| C. albicans SC5314 | 9 | ~60 |
| C. auris B8441 | 8 | ~120 |
| C. glabrata CBS138 | 5 | ~35 |
| C. dubliniensis CD36 | 2 | ~12 |
| C. parapsilosis CDC317 | 3 | ~55 |

## Performance

- **Cold cache** (first query): ~20-30 seconds
- **Warm cache** (subsequent queries): <2 seconds
- Expression profiles are cached in memory after first computation

## Implementation Files

- **Schema**: `cgd/schemas/expression_schema.py`
  - `SimilarGene` - individual similar gene result
  - `SimilarGenesResponse` - full API response

- **Service**: `cgd/api/services/expression_service.py`
  - `get_similar_expression_genes()` - main entry point
  - `_build_expression_profile()` - construct fold-change vector
  - `_compute_correlation()` - calculate similarity

- **Router**: `cgd/api/routers/expression_router.py`
  - `GET /gene/{gene_name}/similar` endpoint

## Dependencies

```
scipy>=1.11.0  # For pearsonr, spearmanr statistical functions
numpy>=1.24.0  # For array operations
```

## Future Enhancements

1. **Pre-computed matrices**: Store expression matrices for instant lookups
2. **Condition filtering**: Allow users to select specific studies or condition types
3. **Cross-organism similarity**: Find orthologs with similar expression patterns
4. **Clustering**: Group genes into co-expression clusters
5. **Visualization**: Heatmap and network views of similar genes
