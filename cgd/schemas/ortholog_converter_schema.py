"""Pydantic schemas for ortholog converter endpoint."""
from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, model_validator


class TargetOrganism(str, Enum):
    """Available target organisms for ortholog conversion."""
    # CGD species
    C_ALBICANS = "C_albicans_SC5314"
    C_DUBLINIENSIS = "C_dubliniensis_CD36"
    C_TROPICALIS = "C_tropicalis_MYA3404"
    C_PARAPSILOSIS = "C_parapsilosis_CDC317"
    C_AURIS = "C_auris_B8441"
    C_GLABRATA = "C_glabrata_CBS138"
    # External species (only S. cerevisiae has ortholog data)
    S_CEREVISIAE = "S_cerevisiae"


class SourceOrganism(str, Enum):
    """Available source organisms for ortholog conversion."""
    # CGD species (default - any CGD species)
    CGD = "CGD"
    # External species that can be used as source
    S_CEREVISIAE = "S_cerevisiae"


# Mapping from enum to display names used in the database
TARGET_ORGANISM_DISPLAY_NAMES = {
    TargetOrganism.C_ALBICANS: "Candida albicans SC5314",
    TargetOrganism.C_DUBLINIENSIS: "Candida dubliniensis CD36",
    TargetOrganism.C_TROPICALIS: "Candida tropicalis MYA-3404",
    TargetOrganism.C_PARAPSILOSIS: "Candida parapsilosis CDC317",
    TargetOrganism.C_AURIS: "Candida auris B8441",
    TargetOrganism.C_GLABRATA: "Candida glabrata CBS138",
    TargetOrganism.S_CEREVISIAE: "Saccharomyces cerevisiae",
}

SOURCE_ORGANISM_DISPLAY_NAMES = {
    SourceOrganism.CGD: "CGD Species",
    SourceOrganism.S_CEREVISIAE: "Saccharomyces cerevisiae (SGD)",
}

# External organism sources (stored in DbxrefHomology)
EXTERNAL_ORGANISM_SOURCES = {
    TargetOrganism.S_CEREVISIAE: "SGD",
}


class OrthologConvertRequest(BaseModel):
    """Request body for ortholog conversion.

    Provide either a single target_organism (legacy) or target_organisms
    (one to all species) — exactly one of the two. With multiple targets the
    response holds one result row per input gene per target, gene-major, with
    relationship 'no_ortholog' (or 'same_organism') where appropriate.
    """
    gene_ids: list[str] = Field(
        ...,
        description="List of gene identifiers to convert",
        min_length=1,
        max_length=5000,
    )
    target_organism: Optional[TargetOrganism] = Field(
        None,
        description="Single target organism to convert orthologs to",
    )
    target_organisms: Optional[list[TargetOrganism]] = Field(
        None,
        description="Multiple target organisms (use instead of target_organism)",
        min_length=1,
    )
    source_organism: Optional[SourceOrganism] = Field(
        default=SourceOrganism.CGD,
        description="Source organism of input genes (default: CGD species)",
    )

    @model_validator(mode="after")
    def _exactly_one_target_form(self):
        if (self.target_organism is None) == (self.target_organisms is None):
            raise ValueError(
                "provide exactly one of target_organism or target_organisms")
        return self

    @property
    def targets(self) -> list[TargetOrganism]:
        """Requested targets as a de-duplicated, order-preserving list."""
        if self.target_organisms:
            return list(dict.fromkeys(self.target_organisms))
        return [self.target_organism]


class OrthologResult(BaseModel):
    """Result for a single gene's ortholog conversion."""
    input_id: str = Field(..., description="Original input gene ID")
    input_gene_name: Optional[str] = Field(None, description="Standard gene name of input")
    input_feature_name: Optional[str] = Field(None, description="Systematic/ORF name of input")
    input_sgdid: Optional[str] = Field(None, description="SGDID of input (S. cerevisiae source only)")
    input_organism: Optional[str] = Field(None, description="Organism of the input gene")
    found: bool = Field(..., description="Whether the input gene was found in CGD")
    ortholog_id: Optional[str] = Field(None, description="Target ortholog ID")
    ortholog_gene_name: Optional[str] = Field(None, description="Target ortholog gene name")
    ortholog_feature_name: Optional[str] = Field(None, description="Target ortholog systematic name")
    ortholog_description: Optional[str] = Field(None, description="Description/headline of the ortholog")
    target_organism: Optional[str] = Field(None, description="Target organism name")
    relationship: Optional[str] = Field(
        None,
        description="Relationship type: '1:1', '1:many', 'many:1', 'many:many', or 'no_ortholog'",
    )
    cluster_id: Optional[str] = Field(None, description="CGOB cluster ID for traceability")
    ortholog_url: Optional[str] = Field(None, description="URL to ortholog page")
    notes: Optional[str] = Field(None, description="Additional notes (e.g., multiple orthologs)")


class OrthologConvertResponse(BaseModel):
    """Response for ortholog conversion."""
    source_organism: Optional[str] = Field(None, description="Source organism display name")
    target_organism: str = Field(..., description="Target organism display name(s)")
    target_organisms: list[str] = Field(
        default_factory=list,
        description="All target organism display names (multi-target requests)",
    )
    total_input: int = Field(..., description="Total number of input genes")
    found_count: int = Field(..., description="Number of input genes found")
    converted_count: int = Field(
        ..., description="Number of (gene, target) pairs with an ortholog")
    results: list[OrthologResult] = Field(..., description="Conversion results")


class TargetOrganismInfo(BaseModel):
    """Information about an available target organism."""
    id: str = Field(..., description="Organism ID for API calls")
    name: str = Field(..., description="Display name")
    source: str = Field(..., description="Data source (CGD, SGD, etc.)")
    is_external: bool = Field(..., description="Whether this is an external organism")


class SourceOrganismInfo(BaseModel):
    """Information about an available source organism."""
    id: str = Field(..., description="Organism ID for API calls")
    name: str = Field(..., description="Display name")
    description: str = Field(..., description="Description of what genes to enter")


class AvailableTargetsResponse(BaseModel):
    """Response listing available target organisms."""
    targets: list[TargetOrganismInfo] = Field(..., description="Available target organisms")
    sources: list[SourceOrganismInfo] = Field(
        default=[],
        description="Available source organisms",
    )
