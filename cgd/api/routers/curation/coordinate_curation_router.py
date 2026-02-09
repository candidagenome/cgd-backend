"""
Coordinate and Relationship Curation Router.

Mirrors functionality from legacy UpdateCoordRelation.pm for curators to
adjust feature coordinates, update feature/subfeature relationships,
and manage sequences.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.coordinate_curation_service import (
    CoordinateCurationService,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/curation/coordinates",
    tags=["curation-coordinates"],
)


# ---------------------------
# Request/Response Schemas
# ---------------------------


class LocationInfo(BaseModel):
    """Feature location information."""

    feat_location_no: int
    start_coord: int
    stop_coord: int
    strand: str
    seq_source: str


class SubfeatureInfo(BaseModel):
    """Subfeature (child) information."""

    feature_no: int
    feature_name: str
    feature_type: str
    relationship_type: str
    rank: Optional[int]
    start_coord: Optional[int]
    stop_coord: Optional[int]
    strand: Optional[str]


class ParentInfo(BaseModel):
    """Parent feature information."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    feature_type: str
    relationship_type: str
    rank: Optional[int]
    start_coord: Optional[int]
    stop_coord: Optional[int]
    strand: Optional[str]


class FeatureInfoResponse(BaseModel):
    """Full feature information with coordinates and relationships."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    feature_type: str
    dbxref_id: str
    headline: Optional[str]
    location: Optional[LocationInfo]
    root_feature_name: Optional[str]
    subfeatures: list[SubfeatureInfo]
    parents: list[ParentInfo]


class SeqSourcesResponse(BaseModel):
    """Available sequence sources (assemblies)."""

    sources: list[str]


class FeatureTypesResponse(BaseModel):
    """Available feature types."""

    types: list[str]


class RelationshipTypesResponse(BaseModel):
    """Available relationship types."""

    types: list[str]


class FeatureQualifiersResponse(BaseModel):
    """Available feature qualifiers."""

    qualifiers: list[str]


class CoordinateChange(BaseModel):
    """A single coordinate change."""

    feature_no: int
    start_coord: Optional[int] = None
    stop_coord: Optional[int] = None
    strand: Optional[str] = None


class PreviewChangesRequest(BaseModel):
    """Request to preview coordinate changes."""

    feature_name: str
    seq_source: str
    changes: list[CoordinateChange]


class ChangeDetail(BaseModel):
    """Details of a coordinate change."""

    feature_no: int
    feature_name: str
    old_start: int
    old_stop: int
    old_strand: str
    new_start: int
    new_stop: int
    new_strand: str


class PreviewChangesResponse(BaseModel):
    """Response for coordinate change preview."""

    feature_name: str
    seq_source: str
    changes: list[ChangeDetail]
    change_count: int
    error: Optional[str] = None


class FeatureSearchResult(BaseModel):
    """Feature search result."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    feature_type: str


class FeatureSearchResponse(BaseModel):
    """Feature search response."""

    results: list[FeatureSearchResult]
    count: int


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/seq-sources", response_model=SeqSourcesResponse)
def get_seq_sources(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Get available sequence sources (assemblies/strains).

    Returns list of assemblies that can be selected for coordinate editing.
    """
    service = CoordinateCurationService(db)
    sources = service.get_seq_sources()
    return SeqSourcesResponse(sources=sources)


@router.get("/feature-types", response_model=FeatureTypesResponse)
def get_feature_types(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """Get available feature types for dropdown."""
    service = CoordinateCurationService(db)
    types = service.get_feature_types()
    return FeatureTypesResponse(types=types)


@router.get("/relationship-types", response_model=RelationshipTypesResponse)
def get_relationship_types(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """Get available relationship types for parent/child relationships."""
    service = CoordinateCurationService(db)
    types = service.get_relationship_types()
    return RelationshipTypesResponse(types=types)


@router.get("/feature-qualifiers", response_model=FeatureQualifiersResponse)
def get_feature_qualifiers(
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """Get available feature qualifiers."""
    service = CoordinateCurationService(db)
    qualifiers = service.get_feature_qualifiers()
    return FeatureQualifiersResponse(qualifiers=qualifiers)


@router.get("/feature/{feature_name}", response_model=FeatureInfoResponse)
def get_feature_info(
    feature_name: str,
    seq_source: Optional[str] = None,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Get feature information with coordinates and relationships.

    Args:
        feature_name: Feature or gene name
        seq_source: Optional assembly/strain to filter by
    """
    service = CoordinateCurationService(db)
    result = service.get_feature_info(feature_name, seq_source)

    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature {feature_name} not found",
        )

    return FeatureInfoResponse(**result)


@router.post("/preview", response_model=PreviewChangesResponse)
def preview_changes(
    request: PreviewChangesRequest,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Preview coordinate changes without committing.

    Shows what coordinates would change for the feature and its subfeatures.
    """
    service = CoordinateCurationService(db)
    changes_dicts = [c.model_dump() for c in request.changes]
    result = service.preview_coordinate_changes(
        request.feature_name,
        request.seq_source,
        changes_dicts,
    )

    if "error" in result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=result["error"],
        )

    return PreviewChangesResponse(**result)


@router.get("/search", response_model=FeatureSearchResponse)
def search_features(
    query: str,
    limit: int = 20,
    current_user: CurrentUser = None,
    db: Session = Depends(get_db),
):
    """
    Search for features by name.

    Useful for autocomplete when entering feature names.
    """
    if not query or len(query) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Query must be at least 2 characters",
        )

    service = CoordinateCurationService(db)
    results = service.search_features(query, limit)

    return FeatureSearchResponse(
        results=results,
        count=len(results),
    )
