"""
Database Search Router - Endpoints for searching phenotypes and related data.

Mirrors functionality from legacy SearchDB.pm for curators to search
phenotypes and get their database IDs for curation purposes.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.db_search_service import DbSearchService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/curation/db-search",
    tags=["curation-db-search"],
)


# ---------------------------
# Request/Response Schemas
# ---------------------------


class PhenotypeSearchResult(BaseModel):
    """Single phenotype in search results."""

    phenotype_no: int
    observable: Optional[str]
    qualifier: Optional[str]
    experiment_type: str
    mutant_type: str
    source: str
    display_text: str


class PhenotypeSearchResponse(BaseModel):
    """Response for phenotype search."""

    results: list[PhenotypeSearchResult]
    count: int
    query: str


class FeatureLink(BaseModel):
    """Feature linked to a phenotype."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    pheno_annotation_no: int


class PhenotypeDetailsResponse(BaseModel):
    """Full details for a phenotype."""

    phenotype_no: int
    observable: Optional[str]
    qualifier: Optional[str]
    experiment_type: str
    mutant_type: str
    source: str
    display_text: str
    features: list[FeatureLink]
    feature_count: int


class ValuesResponse(BaseModel):
    """Response for distinct values (for autocomplete)."""

    values: list[str]


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/phenotype/search", response_model=PhenotypeSearchResponse)
def search_phenotypes(
    query: str,
    limit: int = 100,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Search for phenotypes matching the query string.

    Searches in observable, qualifier, experiment_type, and mutant_type fields.
    Returns phenotype_no for use in curation.
    """
    if not query or len(query) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Query must be at least 2 characters",
        )

    service = DbSearchService(db)
    results = service.search_phenotypes(query, limit)

    return PhenotypeSearchResponse(
        results=[PhenotypeSearchResult(**r) for r in results],
        count=len(results),
        query=query,
    )


@router.get("/phenotype/{phenotype_no}", response_model=PhenotypeDetailsResponse)
def get_phenotype_details(
    phenotype_no: int,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Get details for a specific phenotype including associated features.

    Returns the phenotype information and list of features that have
    annotations with this phenotype.
    """
    service = DbSearchService(db)
    details = service.get_phenotype_details(phenotype_no)

    if not details:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Phenotype {phenotype_no} not found",
        )

    return PhenotypeDetailsResponse(**details)


@router.get("/phenotype/values/observable", response_model=ValuesResponse)
def get_observable_values(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """Get distinct observable values for autocomplete."""
    service = DbSearchService(db)
    values = service.get_observable_values()
    return ValuesResponse(values=values)


@router.get("/phenotype/values/qualifier", response_model=ValuesResponse)
def get_qualifier_values(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """Get distinct qualifier values for autocomplete."""
    service = DbSearchService(db)
    values = service.get_qualifier_values()
    return ValuesResponse(values=values)


@router.get("/phenotype/values/experiment-type", response_model=ValuesResponse)
def get_experiment_types(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """Get distinct experiment types for autocomplete."""
    service = DbSearchService(db)
    values = service.get_experiment_types()
    return ValuesResponse(values=values)


@router.get("/phenotype/values/mutant-type", response_model=ValuesResponse)
def get_mutant_types(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """Get distinct mutant types for autocomplete."""
    service = DbSearchService(db)
    values = service.get_mutant_types()
    return ValuesResponse(values=values)
