from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class InteractorOut(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    action: str  # Bait, Hit, etc.


class InteractionReferenceOut(BaseModel):
    """Reference info for interaction citations."""
    dbxref_id: typing.Optional[str] = None
    pubmed: typing.Optional[int] = None
    citation: typing.Optional[str] = None  # "Author et al. (Year) Title. Journal"


class InteractionOut(BaseModel):
    interaction_no: int
    experiment_type: str
    description: typing.Optional[str] = None
    source: str
    interactors: list[InteractorOut] = []
    references: list[InteractionReferenceOut] = []


class InteractionDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    interactions: list[InteractionOut]


class InteractionDetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "interactions": [...] },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, InteractionDetailsForOrganism]
