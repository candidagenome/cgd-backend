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

class GOAnnotationOut(BaseModel):
    term: GOTerm
    evidence: GOEvidence = GOEvidence()
    references: list[str] = []     # or ReferenceStub if you prefer

class GODetailsResponse(BaseModel):
    locus_display_name: str
    annotations: list[GOAnnotationOut]
