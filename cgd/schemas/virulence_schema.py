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
# DIRECT VS INDIRECT EVIDENCE CLASSIFICATION
# =============================================================================

# GO terms that indicate direct virulence evidence
DIRECT_VIRULENCE_GO_TERMS = [
    "pathogenesis", "virulence", "host", "invasion", "adhesion",
    "biofilm", "filament", "hyphal", "morphogenesis",
]

# Phenotype patterns that indicate direct evidence (Tier 1 & 2)
DIRECT_PHENOTYPE_PATTERNS = [
    # Tier 1 - Direct Virulence
    "virulence", "pathogenesis", "host killing", "infection",
    "lethality", "colonization",
    # Tier 2 - Host Interaction
    "host cell", "phagocytosis", "macrophage", "epithelial",
    "endothelial", "invasion", "galleria", "mouse", "animal model",
]


def split_evidence(match_reasons: list[str]) -> tuple[list[str], list[str]]:
    """
    Split match reasons into direct and indirect evidence.

    Direct evidence includes:
    - Virulence model evidence
    - Tier 1 & 2 phenotypes (Direct Virulence, Host Interaction)
    - Pathogenesis/host-related GO terms

    Indirect evidence includes:
    - Tier 3 & 4 phenotypes (Stress Response, Indirect)
    - Gene pattern matches
    - Headline matches
    - Literature topic matches
    - Non-virulence GO terms

    Args:
        match_reasons: List of match reason strings

    Returns:
        Tuple of (direct_evidence, indirect_evidence) lists
    """
    direct = []
    indirect = []

    for reason in match_reasons:
        reason_lower = reason.lower()

        # Virulence model is always direct
        if reason_lower.startswith("virulence model:"):
            direct.append(reason)
            continue

        # Check phenotype evidence
        if reason_lower.startswith("phenotype:"):
            phenotype_text = reason_lower.replace("phenotype:", "").strip()
            is_direct = any(
                pattern in phenotype_text
                for pattern in DIRECT_PHENOTYPE_PATTERNS
            )
            if is_direct:
                direct.append(reason)
            else:
                indirect.append(reason)
            continue

        # Check GO evidence
        if reason_lower.startswith("go:"):
            is_direct = any(
                term in reason_lower
                for term in DIRECT_VIRULENCE_GO_TERMS
            )
            if is_direct:
                direct.append(reason)
            else:
                indirect.append(reason)
            continue

        # Everything else is indirect (gene pattern, headline, literature)
        indirect.append(reason)

    return direct, indirect


# =============================================================================
# CONFIDENCE TIER DEFINITIONS
# =============================================================================

# Simple confidence tiers mapped from 0-20 score
CONFIDENCE_TIERS = {
    "High": {"min_score": 10, "description": "Strong direct virulence evidence"},
    "Medium": {"min_score": 5, "description": "Moderate evidence with host interaction"},
    "Low": {"min_score": 0, "description": "Indirect or weak evidence"},
}


def get_confidence_tier(score: int) -> str:
    """Map a 0-20 confidence score to High/Medium/Low tier."""
    if score >= 10:
        return "High"
    elif score >= 5:
        return "Medium"
    else:
        return "Low"


# =============================================================================
# EVIDENCE TYPE DEFINITIONS
# =============================================================================

# Evidence types for filtering
EVIDENCE_TYPES = {
    "GO": {
        "name": "GO Annotation",
        "description": "Gene Ontology annotation evidence",
        "match_prefixes": ["GO:"],
    },
    "PHE": {
        "name": "Phenotype",
        "description": "Phenotype and virulence model evidence",
        "match_prefixes": ["phenotype:", "virulence model:"],
    },
    "KW": {
        "name": "Keyword",
        "description": "Gene pattern, headline, or literature matches",
        "match_prefixes": ["gene pattern:", "headline:", "literature topic:"],
    },
}


def extract_evidence_types(match_reasons: list[str]) -> list[str]:
    """Extract evidence type codes (GO/PHE/KW) from match reasons."""
    types = set()
    for reason in match_reasons:
        reason_lower = reason.lower()
        for type_code, config in EVIDENCE_TYPES.items():
            for prefix in config["match_prefixes"]:
                if reason_lower.startswith(prefix.lower()):
                    types.add(type_code)
                    break
    return sorted(types)


def generate_inclusion_reason(match_reasons: list[str], categories: list[str]) -> str:
    """
    Generate a short human-readable reason for inclusion.

    Examples:
    - "Virulence model + GO (pathogenesis)"
    - "Gene pattern (ALS family) + Phenotype (adhesion)"
    - "GO (host interaction) | Adhesins, Host Interaction"
    """
    parts = []

    # Group reasons by type
    has_virulence_model = False
    go_terms = []
    phenotypes = []
    gene_patterns = []
    headlines = []
    literature = []

    for reason in match_reasons:
        reason_lower = reason.lower()
        if reason_lower.startswith("virulence model:"):
            has_virulence_model = True
        elif reason_lower.startswith("go:"):
            # Extract just the term name
            term = reason.split("(")[0].replace("GO:", "").strip()
            if term and term not in go_terms:
                go_terms.append(term[:20])  # Truncate long terms
        elif reason_lower.startswith("phenotype:"):
            pheno = reason.replace("phenotype:", "").strip()[:20]
            if pheno and pheno not in phenotypes:
                phenotypes.append(pheno)
        elif reason_lower.startswith("gene pattern:"):
            pattern = reason.replace("gene pattern:", "").strip()
            # Extract family name (e.g., ALS1 -> ALS)
            import re
            family = re.sub(r'\d+$', '', pattern)
            if family and f"{family} family" not in gene_patterns:
                gene_patterns.append(f"{family} family")
        elif reason_lower.startswith("headline:"):
            headlines.append("headline match")
        elif reason_lower.startswith("literature topic:"):
            topic = reason.replace("literature topic:", "").strip()
            if topic and topic not in literature:
                literature.append(topic)

    # Build the reason string
    if has_virulence_model:
        parts.append("Virulence model")
    if gene_patterns:
        parts.append(f"Gene pattern ({', '.join(gene_patterns[:2])})")
    if go_terms:
        parts.append(f"GO ({', '.join(go_terms[:2])})")
    if phenotypes:
        parts.append(f"Phenotype ({', '.join(phenotypes[:2])})")
    if literature:
        parts.append(f"Literature ({', '.join(literature[:2])})")
    if headlines and not parts:
        parts.append("Headline match")

    if not parts:
        parts.append("Category match")

    # Combine with categories if space allows
    reason_str = " + ".join(parts[:3])
    if len(reason_str) < 60 and categories:
        reason_str += f" | {', '.join(categories[:2])}"

    return reason_str[:100]  # Max 100 chars


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
    confidence_tier: str = "Low"                    # High/Medium/Low
    is_housekeeping: bool = False
    housekeeping_reason: typing.Optional[str] = None
    ortholog_count: int = 0                         # Candida species with orthologs

    # Quick win fields
    inclusion_reason: str = ""                      # Short human-readable reason
    evidence_types: list[str] = []                  # List of GO/PHE/KW codes

    # Paper/reference fields
    paper_count: int = 0                            # Number of associated papers
    pmids: list[int] = []                           # List of PubMed IDs (top 10)

    # Split evidence fields
    direct_evidence: list[str] = []                 # Direct virulence evidence
    indirect_evidence: list[str] = []               # Indirect/supporting evidence


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
