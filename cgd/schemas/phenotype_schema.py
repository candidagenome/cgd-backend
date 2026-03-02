from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PhenotypeTerm(BaseModel):
    display_name: str
    link: typing.Optional[str] = None


class CitationLinkForPhenotype(BaseModel):
    """Link associated with a citation (PubMed, Full Text, etc.)"""
    name: str  # Display name: "CGD Paper", "PubMed", "Full Text", etc.
    url: str   # The actual URL
    link_type: str  # "internal" or "external"


class ReferenceForAnnotation(BaseModel):
    """Reference with full citation data for annotation display"""
    reference_no: typing.Optional[int] = None
    pubmed: typing.Optional[int] = None
    dbxref_id: typing.Optional[str] = None
    citation: typing.Optional[str] = None  # Full citation text
    journal_name: typing.Optional[str] = None
    year: typing.Optional[int] = None
    links: list[CitationLinkForPhenotype] = []  # Citation links (CGD Paper, PubMed, Full Text, etc.)


class ExperimentProperty(BaseModel):
    """Experiment property from expt_property table"""
    property_type: str
    property_value: str
    property_description: typing.Optional[str] = None


class PhenotypeAnnotationOut(BaseModel):
    phenotype: PhenotypeTerm
    qualifier: typing.Optional[str] = None
    experiment_type: typing.Optional[str] = None  # Raw experiment type from DB (e.g., "heterozygous diploid, competitive growth")
    experiment_comment: typing.Optional[str] = None  # Comment about the experiment
    mutant_type: typing.Optional[str] = None  # Raw mutant type from DB (e.g., "null", "overexpression")
    strain: typing.Optional[str] = None  # strain_background from expt_property
    alleles: list[ExperimentProperty] = []  # Allele properties
    chemicals: list[ExperimentProperty] = []  # Chemical_pending or chebi_ontology properties
    details: list[ExperimentProperty] = []  # Condition, Details, Reporter, Numerical_value properties
    references: list[ReferenceForAnnotation] = []


class PhenotypeDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    annotations: list[PhenotypeAnnotationOut]


class PhenotypeDetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "annotations": [...] },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, PhenotypeDetailsForOrganism]


# =============================================================================
# PHENOTYPE SEARCH SCHEMAS
# =============================================================================

class SearchResultDetail(BaseModel):
    """Detail item for phenotype search result (condition, chemical, details, etc.)"""
    property_type: str  # e.g., "Condition", "Chemical", "Details"
    property_value: str


class PhenotypeSearchResult(BaseModel):
    """Single result from phenotype search"""
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism: str
    observable: str
    qualifier: typing.Optional[str] = None
    experiment_type: typing.Optional[str] = None
    mutant_type: typing.Optional[str] = None
    experiment_comment: typing.Optional[str] = None
    strain: typing.Optional[str] = None
    details: list[SearchResultDetail] = []  # Condition, Chemical, Details, etc.
    references: list[ReferenceForAnnotation] = []


class PhenotypeSearchQuery(BaseModel):
    """Search parameters used in the query"""
    query: typing.Optional[str] = None
    observable: typing.Optional[str] = None
    qualifier: typing.Optional[str] = None
    experiment_type: typing.Optional[str] = None
    mutant_type: typing.Optional[str] = None
    property_value: typing.Optional[str] = None
    property_type: typing.Optional[str] = None
    pubmed: typing.Optional[str] = None
    organism: typing.Optional[str] = None


class PhenotypeSearchResponse(BaseModel):
    """Response from phenotype search endpoint"""
    query: PhenotypeSearchQuery
    total_results: int
    page: int
    limit: int
    results: list[PhenotypeSearchResult]


class PhenotypeMatchGroup(BaseModel):
    """Group of phenotype matches for a specific observable"""
    observable: str
    count: int
    is_direct_match: bool = True  # True if observable directly matches query


class PhenotypeSearchSummaryResponse(BaseModel):
    """Summary response from phenotype search grouped by observable"""
    query: str
    total_results: int
    direct_matches: list[PhenotypeMatchGroup]  # Observables that directly match the query
    related_matches: list[PhenotypeMatchGroup]  # Observables matched via qualifier/chemical/etc


# =============================================================================
# OBSERVABLE TERMS TREE SCHEMAS
# =============================================================================

class ObservableTerm(BaseModel):
    """Single node in the observable terms tree"""
    term: str
    count: int = 0  # Number of annotations with this observable
    children: list["ObservableTerm"] = []


class ObservableTreeResponse(BaseModel):
    """Response from observable tree endpoint"""
    tree: list[ObservableTerm]


# Forward reference resolution for recursive model
ObservableTerm.model_rebuild()
