# BioGRID Interaction Data Loader

This directory contains scripts for loading physical and genetic interaction data from BioGRID into CGD.

## Overview

BioGRID (Biological General Repository for Interaction Datasets) provides curated protein-protein and genetic interaction data. This loader fetches *Candida albicans* SC5314 interactions via the BioGRID REST API.

## Data Statistics (May 2026)

| Type | Raw Interactions |
|------|------------------|
| Physical | 1,568 |
| Genetic | 531 |
| **Total** | **2,099** |

## Setup

### 1. Get BioGRID API Key

Register for a free API key at: https://webservice.thebiogrid.org/

### 2. Set Environment Variable

Add to your `.env` file:
```
BIOGRID_ACCESS_KEY=your_32_character_key_here
```

## Usage

### Dry Run (Validate without writing)
```bash
cd /path/to/cgd-backend
source venv/bin/activate
python scripts/loading/biogrid/load_biogrid_interactions.py --dry-run
```

### Load Data
```bash
python scripts/loading/biogrid/load_biogrid_interactions.py
```

### Debug Mode
```bash
python scripts/loading/biogrid/load_biogrid_interactions.py --debug
```

## Database Tables

The loader populates three tables:

### `INTERACTION`
| Column | Description |
|--------|-------------|
| interaction_no | Primary key |
| experiment_type | BioGRID experimental system (e.g., "Affinity Capture-MS") |
| source | Always "BioGRID" |
| description | Optional description from BioGRID |

### `FEAT_INTERACT`
| Column | Description |
|--------|-------------|
| feat_interact_no | Primary key |
| feature_no | FK to FEATURE table |
| interaction_no | FK to INTERACTION table |
| action | "Bait" or "Hit" |

### `REF_LINK`
| Column | Description |
|--------|-------------|
| ref_link_no | Primary key |
| reference_no | FK to REFERENCE table |
| tab_name | "INTERACTION" |
| primary_key | interaction_no |
| col_name | "INTERACTION_NO" |

## BioGRID Experimental Systems

### Physical Interactions (25 types)
- Affinity Capture-MS
- Affinity Capture-Western
- Two-hybrid
- Co-purification
- Reconstituted Complex
- FRET
- PCA
- And more...

### Genetic Interactions (11 types)
- Synthetic Lethality
- Synthetic Growth Defect
- Negative Genetic
- Positive Genetic
- Dosage Rescue
- Phenotypic Suppression
- Phenotypic Enhancement
- And more...

## Gene Name Mapping

The loader maps BioGRID systematic names to CGD feature_no using:
1. Exact match on `feature_name` (e.g., C4_00430W_A)
2. Match without _A/_B suffix
3. Match on `gene_name` (e.g., MEP2)
4. Match on aliases from BioGRID data

## References

- BioGRID: https://thebiogrid.org/
- BioGRID REST API: https://wiki.thebiogrid.org/doku.php/biogridrest
- Oughtred R, et al. (2021) The BioGRID database. Nucleic Acids Research.
