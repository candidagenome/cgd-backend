"""
Virulence Factor Browser Schemas - Pydantic models for virulence factor API.
"""
from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


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
