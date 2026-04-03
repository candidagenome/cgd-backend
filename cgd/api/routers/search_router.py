import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from cgd.core.settings import settings
from cgd.core.elasticsearch import get_es_client
from cgd.db.deps import get_db
from cgd.api.crud.search_crud import dispatch
from cgd.api.services import search_service
from cgd.api.services import text_search_service
from cgd.api.services import es_search_service
from cgd.schemas.search_schema import (
    SearchResponse,
    ResolveResponse,
    AutocompleteResponse,
    CategorySearchResponse,
    TextSearchResponse,
    TextSearchCategoryPagedResponse,
)

logger = logging.getLogger(__name__)


# Schema for legacy dispatch endpoint
class SearchDispatchData(BaseModel):
    kind: str
    target: str
    params: dict[str, str]


class SearchDispatchResponse(BaseModel):
    dispatch: SearchDispatchData


router = APIRouter(prefix="/api/search", tags=["search"])


@router.get("/resolve", response_model=ResolveResponse)
def resolve_identifier(
    query: str = Query(..., min_length=1, description="Identifier to resolve"),
    db: Session = Depends(get_db),
):
    """
    Resolve an exact identifier to a direct URL.

    Checks if query matches exactly:
    - Gene/locus name (gene_name or feature_name)
    - Gene/locus CGDID (dbxref_id like CAL0001571)
    - Reference CGDID (dbxref_id like CAL0080639)

    If resolved, returns redirect_url for direct navigation.
    If not resolved, returns resolved=False and frontend should show search results.
    """
    return search_service.resolve_identifier(db, query)


@router.get("/quick", response_model=SearchResponse)
def quick_search(
    query: str = Query(..., min_length=1, description="Search query string"),
    limit: int = Query(20, ge=1, le=100, description="Max results per category"),
    db: Session = Depends(get_db),
):
    """
    Quick search across all categories (genes, GO terms, phenotypes, references).

    Returns results grouped by category.
    Uses Elasticsearch when enabled and available, falls back to Oracle.
    """
    # Try Elasticsearch first if enabled
    if settings.use_elasticsearch:
        try:
            es = get_es_client()
            if es_search_service.check_es_available(es):
                logger.debug("Using Elasticsearch for quick search")
                return es_search_service.quick_search(es, query, limit)
            else:
                logger.warning("Elasticsearch index not available, falling back to Oracle")
        except Exception as e:
            logger.warning(f"Elasticsearch error, falling back to Oracle: {e}")

    # Fall back to Oracle-based search
    return search_service.quick_search(db, query, limit)


@router.get("/autocomplete", response_model=AutocompleteResponse)
def autocomplete(
    query: str = Query(..., min_length=1, description="Search query for suggestions"),
    limit: int = Query(10, ge=1, le=20, description="Max suggestions to return"),
    db: Session = Depends(get_db),
):
    """
    Get autocomplete suggestions for search input.

    Returns a flat list of suggestions optimized for dropdown display.
    Prioritizes genes, then GO terms, phenotypes, and references.
    Uses Elasticsearch when enabled for fast prefix matching.
    """
    # Try Elasticsearch first if enabled
    if settings.use_elasticsearch:
        try:
            es = get_es_client()
            if es_search_service.check_es_available(es):
                logger.debug("Using Elasticsearch for autocomplete")
                return es_search_service.get_autocomplete_suggestions(es, query, limit)
            else:
                logger.warning("Elasticsearch index not available, falling back to Oracle")
        except Exception as e:
            logger.warning(f"Elasticsearch error, falling back to Oracle: {e}")

    # Fall back to Oracle-based search
    return search_service.get_autocomplete_suggestions(db, query, limit)


@router.get("/category", response_model=CategorySearchResponse)
def search_category(
    query: str = Query(..., min_length=1, description="Search query string"),
    category: str = Query(
        ...,
        description="Category to search",
        pattern="^(genes|go_terms|phenotypes|references|orthologs)$"
    ),
    db: Session = Depends(get_db),
):
    """
    Search within a specific category.

    Returns all results for a single category.
    Uses Elasticsearch when enabled and available.
    """
    # Try Elasticsearch first if enabled
    if settings.use_elasticsearch:
        try:
            es = get_es_client()
            if es_search_service.check_es_available(es):
                logger.debug(f"Using Elasticsearch for category search: {category}")
                result = es_search_service.search_category(es, query, category)
                if result is not None:
                    return result
                logger.debug(f"Category {category} not supported by ES, falling back to Oracle")
        except Exception as e:
            logger.warning(f"Elasticsearch error, falling back to Oracle: {e}")

    # Fall back to Oracle-based search
    return search_service.search_category(db, query, category)


@router.get("/text", response_model=TextSearchResponse)
def text_search(
    query: str = Query(..., min_length=1, description="Search query string"),
    limit: int = Query(10, ge=1, le=50, description="Max results per category"),
    type: Optional[str] = Query(
        None,
        description="Filter: 'homolog' for orthologs only"
    ),
    search_field: str = Query(
        "both",
        description="For paper abstracts: 'title', 'abstract', or 'both' (default)",
        pattern="^(title|abstract|both)$"
    ),
    match_mode: str = Query(
        "all",
        description="For multi-term queries: 'all' (AND), 'any' (OR), or 'exact' (phrase)",
        pattern="^(all|any|exact)$"
    ),
    db: Session = Depends(get_db),
):
    """
    Full text search across all CGD categories.

    Searches 14 categories: genes, descriptions, go_terms, colleagues, authors,
    pathways, paragraphs, abstracts, name_descriptions, phenotypes, notes,
    external_ids, orthologs, literature_topics.

    Use type=homolog to search only orthologs/best hits.
    Use search_field to limit paper search to title, abstract, or both.
    Use match_mode to specify:
      - 'all': All words must appear (AND logic) - default
      - 'any': Any word can appear (OR logic)
      - 'exact': Search for the exact phrase

    Uses Elasticsearch when enabled for all categories, falls back to Oracle.
    """
    category_filter = "orthologs" if type == "homolog" else None

    # Use ES for full text search when enabled
    if settings.use_elasticsearch and category_filter is None:
        try:
            es = get_es_client()
            if es_search_service.check_es_available(es):
                logger.debug("Using Elasticsearch for text search")
                es_result = es_search_service.text_search(es, query, limit, match_mode)
                if es_result is not None:
                    return es_result
        except Exception as e:
            logger.warning(f"Elasticsearch error, falling back to Oracle: {e}")

    # Fall back to Oracle-based search for all categories
    try:
        return text_search_service.text_search(
            db, query, limit, category_filter,
            search_field=search_field, match_mode=match_mode
        )
    except Exception as e:
        logger.error(f"Oracle text search failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.get("/text/category", response_model=TextSearchCategoryPagedResponse)
def text_search_category(
    query: str = Query(..., min_length=1, description="Search query string"),
    category: str = Query(..., description="Category to search"),
    search_field: str = Query(
        "both",
        description="For paper abstracts: 'title', 'abstract', or 'both' (default)",
        pattern="^(title|abstract|both)$"
    ),
    match_mode: str = Query(
        "all",
        description="For multi-term queries: 'all' (AND), 'any' (OR), or 'exact' (phrase)",
        pattern="^(all|any|exact)$"
    ),
    db: Session = Depends(get_db),
):
    """
    Text search within a specific category.

    Returns all results for a single category.
    Use search_field to limit paper search to title, abstract, or both.
    Use match_mode to specify:
      - 'all': All words must appear (AND logic) - default
      - 'any': Any word can appear (OR logic)
      - 'exact': Search for the exact phrase
    Uses Elasticsearch when enabled for supported categories.
    """
    # Try Elasticsearch first if enabled and category is supported
    if settings.use_elasticsearch and category in es_search_service.get_es_supported_categories():
        try:
            es = get_es_client()
            if es_search_service.check_es_available(es):
                logger.debug(f"Using Elasticsearch for text search category: {category}")
                result = es_search_service.text_search_category(es, query, category, match_mode)
                if result is not None:
                    return result
        except Exception as e:
            logger.warning(f"Elasticsearch error, falling back to Oracle: {e}")

    # Fall back to Oracle-based search
    return text_search_service.text_search_category(
        db, query, category,
        search_field=search_field, match_mode=match_mode
    )


@router.get("", response_model=SearchDispatchResponse)
def legacy_search_dispatch(
    class_: str = Query(..., alias="class"),
    item: str = Query(...),
):
    """
    Legacy search dispatch endpoint for compatibility with old CGI URLs.
    """
    if not settings.allow_search_dispatch:
        raise HTTPException(status_code=403, detail="Search dispatch disabled")

    res = dispatch(class_, item)
    if not res:
        raise HTTPException(status_code=404, detail=f"Unknown class: {class_}")

    return {"dispatch": {"kind": res.kind, "target": res.target, "params": res.params}}


class ElasticsearchStatus(BaseModel):
    """Elasticsearch status response."""
    enabled: bool
    available: bool
    index_exists: bool
    document_count: int
    error: Optional[str] = None


@router.get("/es/status", response_model=ElasticsearchStatus)
def elasticsearch_status():
    """
    Check Elasticsearch status and index health.

    Returns whether ES is enabled, available, and index statistics.
    """
    status = ElasticsearchStatus(
        enabled=settings.use_elasticsearch,
        available=False,
        index_exists=False,
        document_count=0,
    )

    if not settings.use_elasticsearch:
        return status

    try:
        es = get_es_client()

        # Check if ES is reachable
        if not es.ping():
            status.error = "Cannot connect to Elasticsearch"
            return status

        status.available = True

        # Check if index exists
        from cgd.core.elasticsearch import INDEX_NAME
        if es.indices.exists(index=INDEX_NAME):
            status.index_exists = True

            # Get document count
            count_response = es.count(index=INDEX_NAME)
            status.document_count = count_response.get("count", 0)

    except Exception as e:
        status.error = str(e)
        logger.warning(f"Elasticsearch status check failed: {e}")

    return status
