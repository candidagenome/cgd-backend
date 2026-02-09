"""
Feature Curation Router - Endpoints for creating new features.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.feature_curation_service import (
    FeatureCurationService,
    FeatureCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/feature", tags=["curation-feature"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class OrganismOut(BaseModel):
    """Organism for dropdown."""

    organism_no: int
    organism_name: str
    organism_abbrev: str


class OrganismsResponse(BaseModel):
    """Response for organisms list."""

    organisms: list[OrganismOut]


class ChromosomeOut(BaseModel):
    """Chromosome for dropdown."""

    feature_no: int
    feature_name: str


class ChromosomesResponse(BaseModel):
    """Response for chromosomes list."""

    chromosomes: list[ChromosomeOut]


class FeatureTypesResponse(BaseModel):
    """Response for feature types list."""

    feature_types: list[str]


class FeatureQualifiersResponse(BaseModel):
    """Response for feature qualifiers list."""

    qualifiers: list[str]


class StrandsResponse(BaseModel):
    """Response for strand values."""

    strands: list[str]


class CheckFeatureRequest(BaseModel):
    """Request to check if feature exists."""

    feature_name: str = Field(..., description="Feature name to check")


class CheckFeatureResponse(BaseModel):
    """Response for feature existence check."""

    exists: bool
    feature_no: Optional[int] = None
    feature_name: Optional[str] = None
    gene_name: Optional[str] = None
    feature_type: Optional[str] = None


class CreateFeatureRequest(BaseModel):
    """Request to create a new feature."""

    feature_name: str = Field(..., description="Systematic name for the feature")
    feature_type: str = Field(..., description="Feature type (ORF, pseudogene, etc.)")
    organism_abbrev: str = Field(..., description="Organism abbreviation")
    chromosome_name: Optional[str] = Field(
        None, description="Chromosome name (required for mapped features)"
    )
    start_coord: Optional[int] = Field(None, description="Start coordinate")
    stop_coord: Optional[int] = Field(None, description="Stop coordinate")
    strand: Optional[str] = Field(
        None, description="Strand: W (Watson/forward) or C (Crick/reverse)"
    )
    qualifiers: Optional[list[str]] = Field(
        None, description="Feature qualifiers (Verified, Dubious, etc.)"
    )
    reference_no: Optional[int] = Field(
        None, description="Reference number for this feature"
    )


class CreateFeatureResponse(BaseModel):
    """Response for feature creation."""

    feature_no: int
    feature_name: str
    message: str


class DeleteFeatureResponse(BaseModel):
    """Response for feature deletion."""

    success: bool
    message: str


# ---------------------------
# Endpoints
# ---------------------------


# Non-parameterized routes MUST come before parameterized routes


@router.get("/organisms", response_model=OrganismsResponse)
def get_organisms(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get list of organisms for dropdown."""
    service = FeatureCurationService(db)
    organisms = service.get_organisms()

    return OrganismsResponse(
        organisms=[OrganismOut(**org) for org in organisms]
    )


@router.get("/chromosomes/{organism_abbrev}", response_model=ChromosomesResponse)
def get_chromosomes(
    organism_abbrev: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get chromosomes for an organism."""
    service = FeatureCurationService(db)
    chromosomes = service.get_chromosomes(organism_abbrev)

    return ChromosomesResponse(
        chromosomes=[ChromosomeOut(**chr) for chr in chromosomes]
    )


@router.get("/feature-types", response_model=FeatureTypesResponse)
def get_feature_types(current_user: CurrentUser):
    """Get list of valid feature types."""
    return FeatureTypesResponse(feature_types=FeatureCurationService.FEATURE_TYPES)


@router.get("/qualifiers", response_model=FeatureQualifiersResponse)
def get_feature_qualifiers(current_user: CurrentUser):
    """Get list of valid feature qualifiers."""
    return FeatureQualifiersResponse(
        qualifiers=FeatureCurationService.FEATURE_QUALIFIERS
    )


@router.get("/strands", response_model=StrandsResponse)
def get_strands(current_user: CurrentUser):
    """Get valid strand values."""
    return StrandsResponse(strands=FeatureCurationService.STRANDS)


@router.post("/check", response_model=CheckFeatureResponse)
def check_feature_exists(
    request: CheckFeatureRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Check if a feature name already exists in the database."""
    service = FeatureCurationService(db)

    existing = service.check_feature_exists(request.feature_name)

    if existing:
        return CheckFeatureResponse(
            exists=True,
            feature_no=existing["feature_no"],
            feature_name=existing["feature_name"],
            gene_name=existing["gene_name"],
            feature_type=existing["feature_type"],
        )

    return CheckFeatureResponse(exists=False)


@router.post("/", response_model=CreateFeatureResponse)
def create_feature(
    request: CreateFeatureRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Create a new feature.

    For mapped features, provide chromosome, coordinates, and strand.
    For unmapped features (not physically mapped, etc.), omit positional info.
    """
    service = FeatureCurationService(db)

    try:
        feature_no = service.create_feature(
            feature_name=request.feature_name,
            feature_type=request.feature_type,
            organism_abbrev=request.organism_abbrev,
            curator_userid=current_user.userid,
            chromosome_name=request.chromosome_name,
            start_coord=request.start_coord,
            stop_coord=request.stop_coord,
            strand=request.strand,
            qualifiers=request.qualifiers,
            reference_no=request.reference_no,
        )

        return CreateFeatureResponse(
            feature_no=feature_no,
            feature_name=request.feature_name,
            message=f"Feature '{request.feature_name}' created successfully",
        )

    except FeatureCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.delete("/{feature_no}", response_model=DeleteFeatureResponse)
def delete_feature(
    feature_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a feature.

    Warning: This will fail if the feature has linked annotations.
    """
    service = FeatureCurationService(db)

    try:
        service.delete_feature(feature_no, current_user.userid)

        return DeleteFeatureResponse(
            success=True,
            message=f"Feature {feature_no} deleted",
        )

    except FeatureCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
