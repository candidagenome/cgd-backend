"""
Feature Search (Advanced Search) Schemas.
"""
from __future__ import annotations

from typing import Optional, List, Dict
from pydantic import BaseModel, Field


class PaginationInfo(BaseModel):
    """Pagination metadata."""
    page: int = Field(..., description="Current page (1-indexed)")
    page_size: int = Field(..., description="Items per page")
    total_items: int = Field(..., description="Total number of items")
    total_pages: int = Field(..., description="Total number of pages")
    has_next: bool = Field(..., description="Whether there's a next page")
    has_prev: bool = Field(..., description="Whether there's a previous page")


class FeatureSearchRequest(BaseModel):
    """Request for feature search."""
    organism: str = Field(
        ...,
        description="Organism/strain abbreviation (required)"
    )
    feature_types: List[str] = Field(
        default_factory=list,
        description="Feature types to include (ORF, tRNA, etc.)"
    )
    include_all_types: bool = Field(
        False,
        description="Include all feature types"
    )
    qualifiers: List[str] = Field(
        default_factory=list,
        description="Feature qualifiers (Verified, Uncharacterized, Dubious, Deleted)"
    )
    has_introns: Optional[bool] = Field(
        None,
        description="Filter by intron presence (None = no filter)"
    )
    chromosomes: List[str] = Field(
        default_factory=list,
        description="Chromosomes to include (empty = all)"
    )
    # GO Slim filters
    process_goids: List[int] = Field(
        default_factory=list,
        description="GO Slim Process term GOIDs"
    )
    function_goids: List[int] = Field(
        default_factory=list,
        description="GO Slim Function term GOIDs"
    )
    component_goids: List[int] = Field(
        default_factory=list,
        description="GO Slim Component term GOIDs"
    )
    additional_goids: List[int] = Field(
        default_factory=list,
        description="Additional GOIDs (free-text input)"
    )
    annotation_methods: List[str] = Field(
        default_factory=list,
        description="GO annotation methods (manually curated, high-throughput, computational)"
    )
    evidence_codes: List[str] = Field(
        default_factory=list,
        description="GO evidence codes (IDA, IEA, etc.)"
    )
    # Pagination and sorting
    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(30, ge=1, le=100, description="Results per page")
    sort_by: str = Field("orf", description="Sort field (orf, gene, feature_type)")


class GoTermBrief(BaseModel):
    """Brief GO term info for search results."""
    goid: str = Field(..., description="GO ID (GO:0000000 format)")
    term: str = Field(..., description="GO term name")


class FeatureSearchResult(BaseModel):
    """Single feature search result."""
    feature_name: str = Field(..., description="Systematic name")
    dbxref_id: str = Field(..., description="CGD ID")
    gene_name: Optional[str] = Field(None, description="Gene name if available")
    feature_type: str = Field(..., description="Feature type (ORF, tRNA, etc.)")
    qualifier: Optional[str] = Field(None, description="Qualifier (Verified, etc.)")
    headline: Optional[str] = Field(None, description="Brief description")
    # Position info (shown when position-related filters used)
    chromosome: Optional[str] = Field(None, description="Chromosome name")
    strand: Optional[str] = Field(None, description="Strand (W or C)")
    start_coord: Optional[int] = Field(None, description="Start coordinate")
    stop_coord: Optional[int] = Field(None, description="Stop coordinate")
    has_intron: Optional[bool] = Field(None, description="Whether feature has introns")
    # GO terms (shown when GO search performed)
    go_process_terms: List[GoTermBrief] = Field(
        default_factory=list,
        description="Relevant GO Process terms"
    )
    go_function_terms: List[GoTermBrief] = Field(
        default_factory=list,
        description="Relevant GO Function terms"
    )
    go_component_terms: List[GoTermBrief] = Field(
        default_factory=list,
        description="Relevant GO Component terms"
    )


class FilterCount(BaseModel):
    """Count for a filter criterion."""
    description: str = Field(..., description="Human-readable filter description")
    count: int = Field(..., description="Number of features matching this filter")


class QuerySummary(BaseModel):
    """Summary of the search query."""
    organism_name: str = Field(..., description="Full organism name")
    feature_types: List[str] = Field(..., description="Feature types searched")
    filter_counts: List[FilterCount] = Field(
        default_factory=list,
        description="Filter descriptions with counts"
    )
    total_results: int = Field(..., description="Total matching features")


class FeatureSearchResponse(BaseModel):
    """Response for feature search."""
    success: bool
    query_summary: Optional[QuerySummary] = None
    results: List[FeatureSearchResult] = Field(default_factory=list)
    pagination: Optional[PaginationInfo] = None
    show_position: bool = Field(False, description="Whether to show position columns")
    show_go_terms: bool = Field(False, description="Whether to show GO term columns")
    error: Optional[str] = None


# Configuration schemas for search form

class OrganismInfo(BaseModel):
    """Organism/strain info for dropdown."""
    organism_abbrev: str = Field(..., description="Organism abbreviation")
    organism_name: str = Field(..., description="Full organism name")


class GoSlimTerm(BaseModel):
    """GO Slim term for multi-select."""
    goid: int = Field(..., description="GO ID (numeric)")
    goid_formatted: str = Field(..., description="GO ID (GO:0000000 format)")
    term: str = Field(..., description="GO term name")


class GoSlimTerms(BaseModel):
    """GO Slim terms grouped by aspect."""
    process: List[GoSlimTerm] = Field(default_factory=list)
    function: List[GoSlimTerm] = Field(default_factory=list)
    component: List[GoSlimTerm] = Field(default_factory=list)


class FeatureSearchConfigResponse(BaseModel):
    """Configuration for feature search form."""
    organisms: List[OrganismInfo] = Field(..., description="Available organisms/strains")
    feature_types: List[str] = Field(..., description="Available feature types")
    qualifiers: List[str] = Field(..., description="Available qualifiers")
    chromosomes: Dict[str, List[str]] = Field(
        ...,
        description="Chromosomes by organism abbreviation"
    )
    go_slim_terms: GoSlimTerms = Field(..., description="GO Slim terms")
    evidence_codes: List[str] = Field(..., description="GO evidence codes")
    annotation_methods: List[str] = Field(..., description="GO annotation methods")
