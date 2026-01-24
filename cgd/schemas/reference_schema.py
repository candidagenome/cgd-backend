from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


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


class ReferenceResponse(BaseModel):
    result: ReferenceOut


# --- Locus Details for Reference ---

class LocusForReference(BaseModel):
    feature_no: int
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
    headline: typing.Optional[str] = None


class ReferenceLocusResponse(BaseModel):
    reference_no: int
    loci: list[LocusForReference]


# --- GO Details for Reference ---

class GOAnnotationForReference(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
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
