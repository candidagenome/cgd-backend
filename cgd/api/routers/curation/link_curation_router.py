"""
Link Curation Router - Endpoints for managing feature links and pull-downs.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.link_curation_service import (
    LinkCurationService,
    LinkCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/links", tags=["curation-links"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class FeatureInfoOut(BaseModel):
    """Feature info for link management."""

    feature_no: int
    feature_name: str
    gene_name: Optional[str] = None
    feature_type: str
    dbxref_id: Optional[str] = None


class FeatureInfoResponse(BaseModel):
    """Response for feature lookup."""

    found: bool
    feature: Optional[FeatureInfoOut] = None


class AvailableLinkOut(BaseModel):
    """Available link definition."""

    url_no: int
    url: str
    label_name: str
    label_location: str
    label_type: str
    link_table: str
    usage_count: int
    is_common_to_all: bool


class AvailableLinksResponse(BaseModel):
    """Response for available links."""

    links: list[AvailableLinkOut]
    feature_type: str


class CurrentLinkOut(BaseModel):
    """Currently selected link."""

    url_no: int
    link_table: str


class CurrentLinksResponse(BaseModel):
    """Response for current feature links."""

    links: list[CurrentLinkOut]


class LinkSelection(BaseModel):
    """A single link selection."""

    url_no: int
    link_table: str = Field(default="FEAT_URL", description="FEAT_URL or DBXREF_URL")


class UpdateLinksRequest(BaseModel):
    """Request to update feature links."""

    feature_no: int
    selected_links: list[LinkSelection]


class UpdateLinksResponse(BaseModel):
    """Response for link update."""

    success: bool
    added: int
    removed: int
    total: int
    message: str


# ---------------------------
# Endpoints
# ---------------------------


@router.get("/feature/{organism_abbrev}/{feature_name}", response_model=FeatureInfoResponse)
def get_feature_info(
    organism_abbrev: str,
    feature_name: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Look up a feature for link management."""
    service = LinkCurationService(db)

    info = service.get_feature_info(feature_name, organism_abbrev)

    if info:
        return FeatureInfoResponse(
            found=True,
            feature=FeatureInfoOut(**info),
        )

    return FeatureInfoResponse(found=False)


@router.get("/available/{feature_type}", response_model=AvailableLinksResponse)
def get_available_links(
    feature_type: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get available link types for a feature type.

    Returns links that can be added to features of this type,
    with usage counts and "common to all" vs "common to some" classification.
    """
    service = LinkCurationService(db)

    links = service.get_available_links(feature_type)

    return AvailableLinksResponse(
        links=[AvailableLinkOut(**link) for link in links],
        feature_type=feature_type,
    )


@router.get("/current/{feature_no}", response_model=CurrentLinksResponse)
def get_current_links(
    feature_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get currently selected links for a feature."""
    service = LinkCurationService(db)

    links = service.get_feature_links(feature_no)

    return CurrentLinksResponse(
        links=[CurrentLinkOut(**link) for link in links]
    )


@router.post("/update", response_model=UpdateLinksResponse)
def update_links(
    request: UpdateLinksRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Update links for a feature.

    Adds new links and removes unselected links.
    """
    service = LinkCurationService(db)

    try:
        result = service.update_feature_links(
            feature_no=request.feature_no,
            selected_links=[link.model_dump() for link in request.selected_links],
            curator_userid=current_user.userid,
        )

        return UpdateLinksResponse(
            success=True,
            added=result["added"],
            removed=result["removed"],
            total=result["total"],
            message=f"Links updated: {result['added']} added, {result['removed']} removed",
        )

    except LinkCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
