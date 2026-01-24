from __future__ import annotations

import datetime
import typing as t
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class FeatureOut(ORMSchema):
    # --- Feature table columns (1:1) ---
    feature_no: int
    organism_no: int
    taxon_id: t.Optional[int] = None
    feature_name: str
    dbxref_id: str
    feature_type: str
    source: str
    date_created: datetime.datetime
    created_by: str

    gene_name: t.Optional[str] = None
    name_description: t.Optional[str] = None
    headline: t.Optional[str] = None


class LocusByOrganismResponse(BaseModel):
    """
    {
      "Candida albicans": { ...FeatureOut... },
      "Candida glabrata": { ...FeatureOut... },
      ...
    }
    """
    results: dict[str, FeatureOut]
