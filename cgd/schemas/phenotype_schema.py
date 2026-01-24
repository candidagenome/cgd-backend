from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PhenotypeTerm(BaseModel):
    display_name: str
    link: typing.Optional[str] = None


class ReferenceStub(BaseModel):
    display_name: typing.Optional[str] = None
    link: typing.Optional[str] = None


class PhenotypeAnnotationOut(BaseModel):
    phenotype: PhenotypeTerm
    qualifier: typing.Optional[str] = None
    experiment: typing.Optional[str] = None
    strain: typing.Optional[str] = None
    references: list[ReferenceStub] = []


class PhenotypeDetailsForOrganism(BaseModel):
    locus_display_name: str
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
