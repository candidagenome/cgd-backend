# cgd/schemas/locus_schema.py
from __future__ import annotations

from typing import List
from pydantic import BaseModel


class LocusHit(BaseModel):
    id: int
    name: str
    display_name: str


class LocusSearchResponse(BaseModel):
    results: List[LocusHit]
