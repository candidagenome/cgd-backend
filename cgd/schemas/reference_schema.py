from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- Citation Links ---

class CitationLink(BaseModel):
    """Represents a link associated with a citation (PubMed, Full Text, etc.)"""
    name: str  # Display name: "CGD Paper", "PubMed", "Access Full Text", etc.
    url: str   # The actual URL
    link_type: str  # "internal" or "external"


# --- Basic Reference Info ---

class AuthorOut(BaseModel):
    author_name: str
    author_type: str  # Author or Editor
    author_order: int


class ReferenceOut(BaseModel):
    reference_no: int
    dbxref_id: str
    pubmed: typing.Optional[int] = None
    citation: str
    title: typing.Optional[str] = None
    year: int
    status: str
    source: str
    journal_name: typing.Optional[str] = None
    journal_abbrev: typing.Optional[str] = None
    volume: typing.Optional[str] = None
    issue: typing.Optional[str] = None
    page: typing.Optional[str] = None
    authors: list[AuthorOut] = []
    abstract: typing.Optional[str] = None
    urls: list[str] = []
    links: list[CitationLink] = []  # Formatted citation links
    full_text_url: typing.Optional[str] = None  # URL for Reference full text
    supplement_url: typing.Optional[str] = None  # URL for Reference supplement


class ReferenceResponse(BaseModel):
    result: ReferenceOut


# --- Locus Details for Reference ---

class LocusForReference(BaseModel):
    feature_no: int
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
    taxon_id: int
    headline: typing.Optional[str] = None


class ReferenceLocusResponse(BaseModel):
    reference_no: int
    loci: list[LocusForReference]


# --- GO Details for Reference ---

class GOAnnotationForReference(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
    taxon_id: int
    goid: str
    go_term: str
    go_aspect: str
    evidence: str


class ReferenceGOResponse(BaseModel):
    reference_no: int
    annotations: list[GOAnnotationForReference]


# --- Phenotype Details for Reference ---

class PhenotypeForReference(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
    taxon_id: int
    observable: str
    qualifier: typing.Optional[str] = None
    experiment_type: str
    mutant_type: str


class ReferencePhenotypeResponse(BaseModel):
    reference_no: int
    annotations: list[PhenotypeForReference]


# --- Interaction Details for Reference ---

class InteractorForReference(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    action: typing.Optional[str] = None


class InteractionForReference(BaseModel):
    interaction_no: int
    experiment_type: str
    description: typing.Optional[str] = None
    interactors: list[InteractorForReference]


class ReferenceInteractionResponse(BaseModel):
    reference_no: int
    interactions: list[InteractionForReference]


# --- Literature Topics for Reference ---

class FeatureForTopic(BaseModel):
    """A feature/gene associated with a literature topic."""
    feature_no: int
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
    taxon_id: int


class LiteratureTopic(BaseModel):
    """A literature topic with its associated features."""
    topic: str
    features: list[FeatureForTopic] = []


class ReferenceLiteratureTopicsResponse(BaseModel):
    """Response containing all literature topics for a reference."""
    reference_no: int
    topics: list[LiteratureTopic]
    all_features: list[FeatureForTopic]  # All unique features for building the matrix


# --- Author Search Results ---

class ReferenceSearchResult(BaseModel):
    """A reference found in author search."""
    reference_no: int
    dbxref_id: str
    pubmed: typing.Optional[int] = None
    citation: str
    year: int
    author_list: str
    links: list[CitationLink] = []


class AuthorSearchResponse(BaseModel):
    """Response for author search."""
    author_query: str
    author_count: int
    reference_count: int
    references: list[ReferenceSearchResult]


# --- New Papers This Week ---

class NewPaperItem(BaseModel):
    """A reference item for new papers list."""
    reference_no: int
    dbxref_id: str
    pubmed: typing.Optional[int] = None
    citation: str
    title: typing.Optional[str] = None
    year: int
    date_created: str  # ISO format date string
    links: list[CitationLink] = []


class NewPapersThisWeekResponse(BaseModel):
    """Response for new papers added this week."""
    start_date: str  # ISO format date
    end_date: str    # ISO format date
    total_count: int
    references: list[NewPaperItem]
