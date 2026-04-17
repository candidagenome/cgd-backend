"""
Virulence Factor Browser Schemas - Pydantic models for virulence factor API.
"""
from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# =============================================================================
# PHENOTYPE EVIDENCE TIER DEFINITIONS
# =============================================================================

# Evidence tiers rank phenotype evidence by biological relevance to virulence
# Tier 1 = most direct virulence evidence, Tier 4 = indirect/weak evidence
PHENOTYPE_EVIDENCE_TIERS = {
    1: {
        "name": "Direct Virulence",
        "description": "Direct evidence of virulence/pathogenesis",
        "patterns": [
            "%virulence%", "%pathogenesis%", "%host killing%",
            "%infection%", "%lethality%", "%colonization%"
        ],
        "score_contribution": 5,
    },
    2: {
        "name": "Host Interaction",
        "description": "Host cell/tissue interactions",
        "patterns": [
            "%host cell%", "%phagocytosis%", "%macrophage%",
            "%epithelial%", "%endothelial%", "%invasion%",
            "%galleria%", "%mouse%", "%animal model%"
        ],
        "score_contribution": 4,
    },
    3: {
        "name": "Stress Response",
        "description": "Stress resistance that may enable survival in host",
        "patterns": [
            "%oxidative stress%", "%heat shock%", "%antifungal%",
            "%azole%", "%echinocandin%"
        ],
        "score_contribution": 2,
    },
    4: {
        "name": "Indirect",
        "description": "Broad phenotypes with indirect virulence relevance",
        "patterns": ["%resistance%", "%susceptibility%", "%sensitivity%"],
        "score_contribution": 1,
    },
}

# =============================================================================
# HOUSEKEEPING GENE GO TERMS
# =============================================================================

# GO terms that indicate housekeeping/essential cellular functions
HOUSEKEEPING_GO_TERMS = [
    "GO:0006412",  # translation
    "GO:0006414",  # translational elongation
    "GO:0006260",  # DNA replication
    "GO:0006350",  # transcription
    "GO:0006457",  # protein folding
    "GO:0007049",  # cell cycle
    "GO:0006096",  # glycolysis
    "GO:0006099",  # tricarboxylic acid cycle
    "GO:0015031",  # protein transport
    "GO:0006886",  # intracellular protein transport
    "GO:0000715",  # nucleotide-excision repair
    "GO:0006281",  # DNA repair
]

# =============================================================================
# CONFIDENCE SCORE WEIGHTS
# =============================================================================

# Weights for calculating confidence scores (0-20 range)
EVIDENCE_WEIGHTS = {
    "virulence_model": 5,       # Tested in mouse/Galleria
    "tier1_phenotype": 4,       # Direct virulence phenotype
    "tier2_phenotype": 3,       # Host interaction phenotype
    "virulence_go": 3,          # Pathogenesis/host GO terms
    "disease_literature": 2,    # Disease literature topic
    "gene_pattern": 1,          # Gene name pattern match
    "keyword_match": 1,         # Headline keyword
    "housekeeping_penalty": -3,  # Housekeeping gene penalty
}

# =============================================================================
# VIRULENCE CATEGORY DEFINITIONS
# =============================================================================

# Category rules define how genes are mapped to virulence categories
VIRULENCE_CATEGORIES = {
    "adhesins": {
        "name": "Adhesins",
        "description": "Cell surface adhesion proteins including ALS family, Hwp1",
        "rules": {
            "gene_patterns": ["ALS%", "HWP%", "EAP%", "PGA%"],
            "phenotype_observables": ["adhesion%", "cell wall%"],
            "go_terms": ["GO:0007155", "GO:0044406"],  # cell adhesion, adhesion to host
            "headlines": ["%adhesin%", "%cell surface%"],
        }
    },
    "secreted_enzymes": {
        "name": "Secreted Enzymes",
        "description": "Secreted aspartyl proteases (SAPs), lipases, phospholipases",
        "rules": {
            "gene_patterns": ["SAP%", "LIP%", "PLC%", "PLB%"],
            "go_terms": ["GO:0008233", "GO:0016298", "GO:0008970"],  # protease, lipase, phospholipase
        }
    },
    "morphogenesis": {
        "name": "Morphogenesis",
        "description": "Genes involved in yeast-hyphal transition and morphological switching",
        "rules": {
            "gene_patterns": ["ECE%", "HGC%", "TUP%", "NRG%", "EFG%"],
            "phenotype_observables": ["filamentous growth%", "hyphal%", "cell morphology%"],
            "go_terms": ["GO:0001403"],  # invasive filamentous growth
        }
    },
    "host_interaction": {
        "name": "Host Interaction",
        "description": "Factors mediating host-pathogen interactions",
        "rules": {
            "phenotype_has_virulence_model": True,  # genes tested in animal models
            "literature_topics": ["Disease"],
            "go_terms": ["GO:0009405", "GO:0044419"],  # pathogenesis, host-pathogen interaction
        }
    },
    "biofilm": {
        "name": "Biofilm Formation",
        "description": "Genes required for biofilm development and maintenance",
        "rules": {
            "phenotype_observables": ["biofilm%"],
            "go_terms": ["GO:0044010", "GO:0043709"],  # biofilm formation, biofilm matrix
        }
    },
    "immune_evasion": {
        "name": "Immune Evasion",
        "description": "Genes involved in evading host immune responses",
        "rules": {
            "go_terms": ["GO:0042832", "GO:0009615"],  # defense response to protozoan, defense evasion
            "phenotype_observables": ["%immune%", "%phagocyt%"],
        }
    },
    "drug_resistance": {
        "name": "Drug Resistance",
        "description": "Genes conferring antifungal drug resistance",
        "rules": {
            "gene_patterns": ["CDR%", "MDR%", "ERG%", "FKS%"],
            "phenotype_observables": ["%resistance%", "%susceptibility%"],
            "go_terms": ["GO:0042493", "GO:0046677"],  # response to drug, response to antibiotic
        }
    }
}


# =============================================================================
# API RESPONSE SCHEMAS
# =============================================================================

class VirulenceCategory(BaseModel):
    """Single virulence category with metadata and gene count."""
    key: str
    name: str
    description: str
    count: int = 0


class VirulenceCategoriesResponse(BaseModel):
    """Response from categories endpoint."""
    categories: list[VirulenceCategory]
    total_genes: int = 0


class VirulenceFactor(BaseModel):
    """Single virulence factor (gene) with its category mappings."""
    feature_no: int
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism: str
    organism_abbrev: str
    headline: typing.Optional[str] = None
    description: typing.Optional[str] = None
    categories: list[str] = []  # List of category names this gene belongs to
    match_reasons: list[str] = []  # Why this gene matched (e.g., "gene pattern: ALS1", "GO: pathogenesis")

    # Evidence quality fields
    evidence_tier: int = 4                          # 1=best, 4=weakest
    evidence_tier_name: str = "Indirect"
    confidence_score: int = 0                       # 0-20 range
    is_housekeeping: bool = False
    housekeeping_reason: typing.Optional[str] = None
    ortholog_count: int = 0                         # Candida species with orthologs


class VirulenceFactorsResponse(BaseModel):
    """Response from factors search endpoint."""
    items: list[VirulenceFactor]
    total_count: int
    page: int
    page_size: int
    categories_searched: list[str] = []


class VirulenceCategoryMatch(BaseModel):
    """Details about why a gene matched a category."""
    category_key: str
    category_name: str
    match_type: str  # "gene_pattern", "phenotype", "go_term", "literature", "virulence_model"
    match_value: str  # The specific value that matched (e.g., "ALS1", "biofilm formation", "GO:0044010")


class VirulenceFactorDetail(BaseModel):
    """Detailed virulence information for a specific gene."""
    feature_no: int
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism: str
    organism_abbrev: str
    headline: typing.Optional[str] = None
    description: typing.Optional[str] = None
    categories: list[VirulenceCategoryMatch] = []


class VirulenceCategoryStats(BaseModel):
    """Stats for a single category."""
    key: str
    name: str
    count: int


class VirulenceOrganismStats(BaseModel):
    """Stats for a single organism."""
    organism_abbrev: str
    organism_name: str
    count: int


class VirulenceStats(BaseModel):
    """Summary statistics for virulence factors."""
    total_genes: int
    categories: list[VirulenceCategoryStats]
    organisms: list[VirulenceOrganismStats]


class VirulenceDownloadRequest(BaseModel):
    """Request parameters for download endpoint."""
    categories: list[str] = []
    organisms: list[str] = []
    search_term: typing.Optional[str] = None
    format: str = "tsv"  # "tsv" or "csv"


# Forward reference resolution
VirulenceFactorDetail.model_rebuild()
