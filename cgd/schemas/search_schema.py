"""Search schema definitions for quick search endpoint."""
from __future__ import annotations

from typing import Optional
from pydantic import BaseModel


class SearchResult(BaseModel):
    """Single search result item."""
    category: str  # "gene", "go_term", "phenotype", "reference"
    id: str
    name: str
    description: Optional[str] = None
    link: str
    organism: Optional[str] = None


class SearchResponse(BaseModel):
    """Response for /api/search/quick endpoint."""
    query: str
    total_results: int
    results_by_category: dict[str, list[SearchResult]]
    # e.g., {"genes": [...], "go_terms": [...], "phenotypes": [...], "references": [...]}
