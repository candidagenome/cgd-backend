"""Search schema definitions for quick search endpoint."""
from __future__ import annotations

from typing import Optional
from pydantic import BaseModel


class SearchResultLink(BaseModel):
    """Link for a search result (used for references)."""
    name: str
    url: str
    link_type: str  # "internal" or "external"


class SearchResult(BaseModel):
    """Single search result item."""
    category: str  # "gene", "go_term", "phenotype", "reference"
    id: str
    name: str
    description: Optional[str] = None
    link: str
    organism: Optional[str] = None
    links: Optional[list[SearchResultLink]] = None  # Citation links for references
    # Highlighted versions with <mark> tags around matching text
    highlighted_name: Optional[str] = None
    highlighted_description: Optional[str] = None


class PaginationInfo(BaseModel):
    """Pagination metadata."""
    page: int  # Current page (1-indexed)
    page_size: int  # Items per page
    total_items: int  # Total number of items
    total_pages: int  # Total number of pages
    has_next: bool  # Whether there's a next page
    has_prev: bool  # Whether there's a previous page


class SearchResponse(BaseModel):
    """Response for /api/search/quick endpoint."""
    query: str
    total_results: int
    results_by_category: dict[str, list[SearchResult]]
    # e.g., {"genes": [...], "go_terms": [...], "phenotypes": [...], "references": [...]}


class CategorySearchResponse(BaseModel):
    """Response for paginated category-specific search."""
    query: str
    category: str
    results: list[SearchResult]
    pagination: PaginationInfo


class ResolveResponse(BaseModel):
    """Response for /api/search/resolve endpoint - checks for exact identifier matches."""
    query: str
    resolved: bool  # True if exact match found
    redirect_url: Optional[str] = None  # URL to redirect to if resolved
    entity_type: Optional[str] = None  # "locus", "reference", "go_term"
    entity_name: Optional[str] = None  # Display name of the matched entity


# --- Autocomplete/Suggestions ---

class AutocompleteSuggestion(BaseModel):
    """Single autocomplete suggestion item."""
    text: str  # Display text for the suggestion
    category: str  # "gene", "go_term", "phenotype", "reference"
    link: str  # URL to navigate to
    description: Optional[str] = None  # Optional additional context
    # Highlighted versions with <mark> tags around matching text
    highlighted_text: Optional[str] = None
    highlighted_description: Optional[str] = None


class AutocompleteResponse(BaseModel):
    """Response for /api/search/autocomplete endpoint."""
    query: str
    suggestions: list[AutocompleteSuggestion]
