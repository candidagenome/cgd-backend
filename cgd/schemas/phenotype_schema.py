from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PhenotypeTerm(BaseModel):
    display_name: str
    link: typing.Optional[str] = None


class ReferenceForAnnotation(BaseModel):
    """Reference with full citation data for annotation display"""
    reference_no: typing.Optional[int] = None
    pubmed: typing.Optional[int] = None
    dbxref_id: typing.Optional[str] = None
    citation: typing.Optional[str] = None  # Full citation text
    journal_name: typing.Optional[str] = None
    year: typing.Optional[int] = None


class PhenotypeAnnotationOut(BaseModel):
    phenotype: PhenotypeTerm
    qualifier: typing.Optional[str] = None
    experiment_type: typing.Optional[str] = None  # Mapped to "Classical genetics" or "Large-scale survey"
    mutant_type: typing.Optional[str] = None  # e.g., "null", "overexpression", "homozygous null"
    strain: typing.Optional[str] = None
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
