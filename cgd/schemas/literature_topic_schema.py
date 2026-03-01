"""
Literature Topic Search Schemas.
"""
from __future__ import annotations

import typing
from pydantic import BaseModel


class LiteratureTopicTerm(BaseModel):
    """Single node in the literature topic tree."""
    cv_term_no: int
    term: str
    count: int = 0  # Number of references with this topic
    children: list["LiteratureTopicTerm"] = []


class LiteratureTopicTreeResponse(BaseModel):
    """Response from topic tree endpoint."""
    tree: list[LiteratureTopicTerm]


class CitationLinkForLitTopic(BaseModel):
    """Link associated with a citation."""
    name: str  # Display name: "CGD Paper", "PubMed", etc.
    url: str   # The actual URL
    link_type: str  # "internal" or "external"


class GeneForLitTopic(BaseModel):
    """Gene/feature associated with a literature topic."""
    feature_no: int
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism: typing.Optional[str] = None


class ReferenceForLitTopic(BaseModel):
    """Reference with topic-gene associations."""
    reference_no: int
    dbxref_id: typing.Optional[str] = None
    pubmed: typing.Optional[int] = None
    citation: typing.Optional[str] = None
    title: typing.Optional[str] = None
    year: typing.Optional[int] = None
    links: list[CitationLinkForLitTopic] = []


class TopicReferenceResult(BaseModel):
    """A topic with its associated references and genes."""
    topic: str
    cv_term_no: int
    references: list[ReferenceForLitTopic]
    genes: list[GeneForLitTopic]


class LiteratureTopicSearchQuery(BaseModel):
    """Search parameters used in the query."""
    topic_cv_term_nos: list[int]
    topic_names: list[str]


class LiteratureTopicSearchResponse(BaseModel):
    """Response from literature topic search endpoint."""
    query: LiteratureTopicSearchQuery
    total_references: int
    total_genes: int
    results: list[TopicReferenceResult]


# Forward reference resolution for recursive model
LiteratureTopicTerm.model_rebuild()
