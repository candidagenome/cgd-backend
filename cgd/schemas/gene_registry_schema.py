"""
Gene Registry Schemas.
"""
from __future__ import annotations

from typing import Optional, List
from pydantic import BaseModel, Field


class GeneRegistrySearchRequest(BaseModel):
    """Request for gene registry search/validation."""
    last_name: str = Field(..., min_length=1, description="Colleague last name")
    gene_name: str = Field(..., min_length=1, description="Proposed gene name")
    orf_name: Optional[str] = Field(None, description="ORF name (optional)")
    organism: str = Field(..., description="Organism abbreviation")


class GeneValidationResult(BaseModel):
    """Result of gene name validation."""
    is_valid: bool = Field(..., description="Whether gene name is valid")
    gene_exists: bool = Field(False, description="Gene already in database")
    gene_is_alias: bool = Field(False, description="Gene name is an alias")
    alias_for: Optional[str] = Field(None, description="Standard name if alias")
    orf_exists: bool = Field(False, description="ORF exists in database")
    orf_is_deleted: bool = Field(False, description="ORF is deleted/merged")
    orf_is_dubious: bool = Field(False, description="ORF is dubious")
    orf_has_gene: bool = Field(False, description="ORF already has gene name")
    orf_gene_name: Optional[str] = Field(None, description="Existing gene for ORF")
    format_valid: bool = Field(True, description="Gene name format valid")
    warnings: List[str] = Field(default_factory=list, description="Warning messages")
    errors: List[str] = Field(default_factory=list, description="Error messages")


class ColleagueMatch(BaseModel):
    """Colleague match for registry."""
    colleague_no: int = Field(..., description="Colleague ID")
    full_name: str = Field(..., description="Full name")
    institution: Optional[str] = Field(None, description="Organization")
    email: Optional[str] = Field(None, description="Email (partially masked)")
    work_phone: Optional[str] = Field(None, description="Work phone")
    urls: List[str] = Field(default_factory=list, description="Web URLs")


class GeneRegistrySearchResponse(BaseModel):
    """Response for gene registry search."""
    success: bool = Field(True, description="Whether search succeeded")
    validation: GeneValidationResult = Field(..., description="Gene validation result")
    can_proceed: bool = Field(False, description="Can proceed with registration")
    wildcard_appended: bool = Field(False, description="Wildcard added to search")
    search_term: str = Field(..., description="Search term used")
    colleagues: List[ColleagueMatch] = Field(
        default_factory=list, description="Matching colleagues"
    )
    organism_name: str = Field(..., description="Display name for organism")
    error: Optional[str] = Field(None, description="Error message")


# ==================== Species List ====================

class SpeciesOption(BaseModel):
    """Species option for dropdown."""
    abbrev: str = Field(..., description="Organism abbreviation")
    name: str = Field(..., description="Species display name")


class GeneRegistryConfigResponse(BaseModel):
    """Configuration for gene registry form."""
    species: List[SpeciesOption] = Field(..., description="Available species")
    default_species: str = Field(..., description="Default species abbreviation")
    gene_name_pattern: str = Field(
        "^[a-zA-Z]{3}[0-9]+$",
        description="Regex pattern for valid gene names"
    )
    nomenclature_url: str = Field(..., description="URL to naming guidelines")


# ==================== Submission ====================

class GeneRegistrySubmission(BaseModel):
    """Gene registry submission data."""
    # Colleague info (if new)
    colleague_no: Optional[int] = Field(None, description="Existing colleague ID")
    last_name: Optional[str] = Field(None, description="Last name (new colleague)")
    first_name: Optional[str] = Field(None, description="First name (new colleague)")
    email: Optional[str] = Field(None, description="Email")
    institution: Optional[str] = Field(None, description="Organization")

    # Gene info
    gene_name: str = Field(..., min_length=1, description="Proposed gene name")
    orf_name: Optional[str] = Field(None, description="ORF name")
    organism: str = Field(..., description="Organism abbreviation")

    # Additional info
    description: Optional[str] = Field(None, description="Gene description")
    reference: Optional[str] = Field(None, description="Publication reference")
    comments: Optional[str] = Field(None, description="Additional comments")


class GeneRegistrySubmissionRequest(BaseModel):
    """Request for gene registry submission."""
    data: GeneRegistrySubmission = Field(..., description="Submission data")


class GeneRegistrySubmissionResponse(BaseModel):
    """Response for gene registry submission."""
    success: bool = Field(True, description="Whether submission succeeded")
    message: Optional[str] = Field(None, description="Success message")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
