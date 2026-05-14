"""Pydantic schemas for ortholog converter endpoint."""
from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class TargetOrganism(str, Enum):
    """Available target organisms for ortholog conversion."""
    # CGD species
    C_ALBICANS = "C_albicans_SC5314"
    C_DUBLINIENSIS = "C_dubliniensis_CD36"
    C_TROPICALIS = "C_tropicalis_MYA3404"
    C_PARAPSILOSIS = "C_parapsilosis_CDC317"
    C_AURIS = "C_auris_B8441"
    C_GLABRATA = "C_glabrata_CBS138"
    # External species
    S_CEREVISIAE = "S_cerevisiae"
    S_POMBE = "S_pombe"
    A_NIDULANS = "A_nidulans"
    N_CRASSA = "N_crassa"


# Mapping from enum to display names used in the database
TARGET_ORGANISM_DISPLAY_NAMES = {
    TargetOrganism.C_ALBICANS: "Candida albicans SC5314",
    TargetOrganism.C_DUBLINIENSIS: "Candida dubliniensis CD36",
    TargetOrganism.C_TROPICALIS: "Candida tropicalis MYA-3404",
    TargetOrganism.C_PARAPSILOSIS: "Candida parapsilosis CDC317",
    TargetOrganism.C_AURIS: "Candida auris B8441",
    TargetOrganism.C_GLABRATA: "Candida glabrata CBS138",
    TargetOrganism.S_CEREVISIAE: "Saccharomyces cerevisiae",
    TargetOrganism.S_POMBE: "Schizosaccharomyces pombe",
    TargetOrganism.A_NIDULANS: "Aspergillus nidulans",
    TargetOrganism.N_CRASSA: "Neurospora crassa",
}

# External organism sources (stored in DbxrefHomology)
EXTERNAL_ORGANISM_SOURCES = {
    TargetOrganism.S_CEREVISIAE: "SGD",
    TargetOrganism.S_POMBE: "POMBASE",
    TargetOrganism.A_NIDULANS: "AspGD",
    TargetOrganism.N_CRASSA: "BROAD_NEUROSPORA",
}


class OrthologConvertRequest(BaseModel):
    """Request body for ortholog conversion."""
    gene_ids: list[str] = Field(
        ...,
        description="List of gene identifiers to convert",
        min_length=1,
        max_length=5000,
    )
    target_organism: TargetOrganism = Field(
        ...,
        description="Target organism to convert orthologs to",
    )


class OrthologResult(BaseModel):
    """Result for a single gene's ortholog conversion."""
    input_id: str = Field(..., description="Original input gene ID")
    input_gene_name: Optional[str] = Field(None, description="Standard gene name of input")
    input_feature_name: Optional[str] = Field(None, description="Systematic name of input")
    input_organism: Optional[str] = Field(None, description="Organism of the input gene")
    found: bool = Field(..., description="Whether the input gene was found in CGD")
    ortholog_id: Optional[str] = Field(None, description="Target ortholog ID")
    ortholog_gene_name: Optional[str] = Field(None, description="Target ortholog gene name")
    ortholog_feature_name: Optional[str] = Field(None, description="Target ortholog systematic name")
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
    target_organism: str = Field(..., description="Target organism display name")
    total_input: int = Field(..., description="Total number of input genes")
    found_count: int = Field(..., description="Number of input genes found in CGD")
    converted_count: int = Field(..., description="Number of genes with orthologs in target")
    results: list[OrthologResult] = Field(..., description="Conversion results")


class TargetOrganismInfo(BaseModel):
    """Information about an available target organism."""
    id: str = Field(..., description="Organism ID for API calls")
    name: str = Field(..., description="Display name")
    source: str = Field(..., description="Data source (CGD, SGD, etc.)")
    is_external: bool = Field(..., description="Whether this is an external organism")


class AvailableTargetsResponse(BaseModel):
    """Response listing available target organisms."""
    targets: list[TargetOrganismInfo] = Field(..., description="Available target organisms")
