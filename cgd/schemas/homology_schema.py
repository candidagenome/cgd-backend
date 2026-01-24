from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class HomologOut(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
    dbxref_id: str


class HomologyGroupOut(BaseModel):
    homology_group_type: str  # ortholog, paralog, etc.
    method: str  # InParanoid, etc.
    members: list[HomologOut] = []


class HomologyDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    homology_groups: list[HomologyGroupOut]


class HomologyDetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "homology_groups": [...] },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, HomologyDetailsForOrganism]
