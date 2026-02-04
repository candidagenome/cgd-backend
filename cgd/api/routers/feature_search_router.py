"""
Feature Search (Advanced Search) API Router.
"""
import logging
import traceback
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.feature_search_schema import (
    FeatureSearchRequest,
    FeatureSearchResponse,
    FeatureSearchConfigResponse,
)
from cgd.api.services.feature_search_service import (
    get_feature_search_config,
    search_features,
    generate_download_tsv,
    _get_chromosomes_for_organism,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/feature-search", tags=["feature-search"])


@router.get("/config", response_model=FeatureSearchConfigResponse)
def get_config(
    organism: Optional[str] = Query(None, description="Organism abbreviation"),
    db: Session = Depends(get_db),
):
    """
    Get feature search form configuration.

    Returns available organisms, feature types, qualifiers, chromosomes, and GO terms.
    """
    return get_feature_search_config(db, organism)


@router.post("/search", response_model=FeatureSearchResponse)
def search(
    request: FeatureSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Execute feature search with filters.

    **Required Fields:**
    - `organism`: Organism/strain abbreviation
    - `feature_types`: List of feature types (or set `include_all_types` to true)

    **Optional Filters:**
    - `qualifiers`: Filter by feature qualifiers (Verified, Uncharacterized, etc.)
    - `has_introns`: Filter by intron presence (true/false/null)
    - `chromosomes`: Filter by chromosome location
    - `process_goids`, `function_goids`, `component_goids`: Filter by GO Slim terms
    - `annotation_methods`: Filter by GO annotation method
    - `evidence_codes`: Filter by GO evidence codes

    **Pagination:**
    - `page`: Page number (1-indexed)
    - `page_size`: Results per page (max 100)
    - `sort_by`: Sort field (orf, gene, feature_type)

    Returns paginated results with optional position and GO term information.
    """
    try:
        return search_features(db, request)
    except Exception as e:
        logger.error(f"Feature search error: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chromosomes/{organism}", response_model=List[str])
def get_chromosomes(
    organism: str,
    db: Session = Depends(get_db),
):
    """
    Get chromosomes for a specific organism.

    Useful for dynamically updating the chromosome dropdown when organism changes.
    """
    return _get_chromosomes_for_organism(db, organism)


@router.post("/download", response_class=PlainTextResponse)
def download_results(
    request: FeatureSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Download search results as TSV.

    Returns a tab-separated file with all matching features.
    """
    tsv_content = generate_download_tsv(db, request)

    return PlainTextResponse(
        content=tsv_content,
        media_type="text/tab-separated-values",
        headers={
            "Content-Disposition": "attachment; filename=feature_search_results.tsv"
        }
    )
