from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class GOTerm(BaseModel):
    goid: str                      # e.g. "GO:0008150"
    display_name: str
    aspect: typing.Optional[str] = None  # P/F/C
    link: typing.Optional[str] = None


class GOEvidence(BaseModel):
    code: typing.Optional[str] = None
    with_from: typing.Optional[str] = None


class ReferenceForAnnotation(BaseModel):
    """Reference with full citation data for annotation display"""
    reference_no: typing.Optional[int] = None
    pubmed: typing.Optional[int] = None
    dbxref_id: typing.Optional[str] = None
    citation: typing.Optional[str] = None  # Full citation text
    journal_name: typing.Optional[str] = None
    year: typing.Optional[int] = None


class GOAnnotationOut(BaseModel):
    term: GOTerm
    evidence: GOEvidence = GOEvidence()
    references: list[ReferenceForAnnotation] = []  # Full reference objects with citation
    qualifier: typing.Optional[str] = None  # contributes_to, NOT, etc.
    annotation_type: typing.Optional[str] = None  # manually_curated, computational, high-throughput
    source: typing.Optional[str] = None  # Assigned by (e.g., CGD)
    date_created: typing.Optional[str] = None  # When annotation was assigned


class GODetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    annotations: list[GOAnnotationOut]


class GODetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "annotations": [...] },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, GODetailsForOrganism]
