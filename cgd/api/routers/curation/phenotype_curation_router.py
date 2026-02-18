"""
Phenotype Curation Router - Endpoints for phenotype annotation CRUD operations.

Requires curator authentication.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.api.services.curation.phenotype_curation_service import (
    PhenotypeCurationService,
    PhenotypeCurationError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/phenotype", tags=["curation-phenotype"])


# ---------------------------
# Request/Response Schemas
# ---------------------------


class PropertyOut(BaseModel):
    """Experiment property in annotation response."""

    property_type: str
    property_value: str
    property_description: Optional[str]


class ExperimentOut(BaseModel):
    """Experiment in annotation response."""

    experiment_no: int
    experiment_comment: Optional[str]


class ReferenceOut(BaseModel):
    """Reference in annotation response."""

    reference_no: int
    pubmed: Optional[int]
    citation: Optional[str]


class PhenotypeAnnotationOut(BaseModel):
    """Phenotype annotation response."""

    pheno_annotation_no: int
    feature_no: int
    feature_name: Optional[str] = None  # Added for multi-feature queries
    gene_name: Optional[str] = None  # Added for multi-feature queries
    phenotype_no: int
    experiment_type: Optional[str]
    mutant_type: Optional[str]
    observable: Optional[str]
    qualifier: Optional[str]
    experiment: Optional[ExperimentOut]
    properties: list[PropertyOut]
    references: list[ReferenceOut]
    date_created: Optional[str]
    created_by: str


class FeaturePhenotypeResponse(BaseModel):
    """Response for all phenotype annotations of a feature (or multiple features)."""

    feature_no: Optional[int] = None  # Primary feature (may be None for multi-feature)
    feature_name: str  # Search term or primary feature name
    gene_name: Optional[str] = None
    features_searched: Optional[int] = None  # Number of features searched (for all-species)
    annotations: list[PhenotypeAnnotationOut]


class PropertyInput(BaseModel):
    """Input for experiment property."""

    property_type: str
    property_value: str
    property_description: Optional[str] = None


class CreateAnnotationRequest(BaseModel):
    """Request to create a new phenotype annotation."""

    experiment_type: str = Field(..., description="Experiment type (CV term)")
    mutant_type: str = Field(..., description="Mutant type (CV term)")
    observable: str = Field(..., description="Observable (CV term)")
    qualifier: Optional[str] = Field(None, description="Qualifier (CV term)")
    reference_no: int = Field(..., description="Reference number")
    experiment_comment: Optional[str] = Field(
        None, description="Experiment comment/description"
    )
    properties: Optional[list[PropertyInput]] = Field(
        None,
        description="Experiment properties (strain_background, allele, etc.)",
    )


class CreateAnnotationResponse(BaseModel):
    """Response for annotation creation."""

    pheno_annotation_no: int
    message: str


class DeleteAnnotationResponse(BaseModel):
    """Response for annotation deletion."""

    success: bool
    message: str


class CVTermsResponse(BaseModel):
    """Response for CV terms lookup."""

    cv_name: str
    terms: list[str]


class PropertyTypesResponse(BaseModel):
    """Response for property types lookup."""

    property_types: list[str]


# ---------------------------
# Endpoints
# ---------------------------


# Non-parameterized routes MUST come before /{feature_name} and /{annotation_no}
# routes to prevent "cv" or "property-types" from matching as path parameters


@router.get("/cv/{cv_name}", response_model=CVTermsResponse)
def get_cv_terms(
    cv_name: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get CV terms for dropdowns.

    Args:
        cv_name: experiment_type, mutant_type, qualifier, or observable
    """
    service = PhenotypeCurationService(db)
    terms = service.get_cv_terms(cv_name)

    if not terms:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown CV name: {cv_name}",
        )

    return CVTermsResponse(cv_name=cv_name, terms=terms)


@router.get("/property-types", response_model=PropertyTypesResponse)
def get_property_types(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """Get valid property types for experiment properties."""
    service = PhenotypeCurationService(db)
    property_types = service.get_property_types()

    return PropertyTypesResponse(property_types=property_types)


@router.get("/debug/count/{feature_name}")
def debug_phenotype_count(
    feature_name: str,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Debug endpoint: Check raw phenotype annotation counts for a feature.

    Runs direct queries to diagnose why annotations might not be showing.
    """
    from sqlalchemy import text
    from cgd.models.models import Feature, PhenoAnnotation, Phenotype

    results = {}

    # Find feature
    feature = (
        db.query(Feature)
        .filter(
            or_(
                func.upper(Feature.feature_name) == feature_name.upper(),
                func.upper(Feature.gene_name) == feature_name.upper(),
            )
        )
        .first()
    )

    if not feature:
        return {"error": f"Feature '{feature_name}' not found"}

    results["feature"] = {
        "feature_no": feature.feature_no,
        "feature_name": feature.feature_name,
        "gene_name": feature.gene_name,
        "organism_no": feature.organism_no,
    }

    # Count pheno_annotations via ORM
    orm_count = (
        db.query(PhenoAnnotation)
        .filter(PhenoAnnotation.feature_no == feature.feature_no)
        .count()
    )
    results["orm_pheno_annotation_count"] = orm_count

    # Get some sample annotations if any
    sample_annotations = (
        db.query(PhenoAnnotation)
        .filter(PhenoAnnotation.feature_no == feature.feature_no)
        .limit(5)
        .all()
    )
    results["sample_annotations"] = [
        {
            "pheno_annotation_no": a.pheno_annotation_no,
            "phenotype_no": a.phenotype_no,
            "experiment_no": a.experiment_no,
        }
        for a in sample_annotations
    ]

    # Try raw SQL query to bypass ORM
    try:
        raw_sql = text("""
            SELECT COUNT(*) FROM MULTI.pheno_annotation WHERE feature_no = :fno
        """)
        raw_result = db.execute(raw_sql, {"fno": feature.feature_no}).scalar()
        results["raw_sql_count"] = raw_result
    except Exception as e:
        results["raw_sql_error"] = str(e)

    # Check total phenotypes in database
    total_phenotypes = db.query(Phenotype).count()
    results["total_phenotypes_in_db"] = total_phenotypes

    total_annotations = db.query(PhenoAnnotation).count()
    results["total_pheno_annotations_in_db"] = total_annotations

    return results


# Parameterized routes below


@router.get("/{feature_name}", response_model=FeaturePhenotypeResponse)
def get_phenotype_annotations(
    feature_name: str,
    current_user: CurrentUser,
    organism: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Get all phenotype annotations for a feature.

    Returns annotations with phenotype details, experiment info, properties,
    and references.

    - If 'organism' is specified, returns annotations only for that organism's feature.
    - If 'organism' is not specified (all species), returns annotations from ALL
      matching features across all organisms.
    """
    try:
        service = PhenotypeCurationService(db)

        # Always get ALL matching features (there can be multiple features
        # with the same gene_name even within a single organism)
        features = service.get_features_by_name(feature_name, organism)
        if not features:
            if organism:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Feature '{feature_name}' not found in organism '{organism}'",
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Feature '{feature_name}' not found",
                )

        feature_nos = [f.feature_no for f in features]
        annotations = service.get_annotations_for_features(feature_nos)

        # Use first feature's info for display
        primary_feature = features[0]

        return FeaturePhenotypeResponse(
            feature_no=primary_feature.feature_no if len(features) == 1 else None,
            feature_name=feature_name,  # Use search term
            gene_name=primary_feature.gene_name,
            features_searched=len(features),
            annotations=[
                PhenotypeAnnotationOut(
                    pheno_annotation_no=ann["pheno_annotation_no"],
                    feature_no=ann["feature_no"],
                    feature_name=ann.get("feature_name"),
                    gene_name=ann.get("gene_name"),
                    phenotype_no=ann["phenotype_no"],
                    experiment_type=ann["experiment_type"],
                    mutant_type=ann["mutant_type"],
                    observable=ann["observable"],
                    qualifier=ann["qualifier"],
                    experiment=ExperimentOut(**ann["experiment"]) if ann["experiment"] else None,
                    properties=[PropertyOut(**p) for p in ann["properties"]],
                    references=[ReferenceOut(**r) for r in ann["references"]],
                    date_created=ann["date_created"],
                    created_by=ann["created_by"],
                )
                for ann in annotations
            ],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error retrieving phenotype annotations for {feature_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )


@router.post("/{feature_name}", response_model=CreateAnnotationResponse)
def create_phenotype_annotation(
    feature_name: str,
    request: CreateAnnotationRequest,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Create a new phenotype annotation for a feature.

    Creates phenotype, experiment (if comment/properties provided), and links
    to reference.
    """
    try:
        service = PhenotypeCurationService(db)

        feature = service.get_feature_by_name(feature_name)
        if not feature:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feature '{feature_name}' not found",
            )

        properties = None
        if request.properties:
            properties = [
                {
                    "property_type": p.property_type,
                    "property_value": p.property_value,
                    "property_description": p.property_description,
                }
                for p in request.properties
            ]

        annotation_no = service.create_annotation(
            feature_no=feature.feature_no,
            experiment_type=request.experiment_type,
            mutant_type=request.mutant_type,
            observable=request.observable,
            qualifier=request.qualifier,
            reference_no=request.reference_no,
            curator_userid=current_user.userid,
            experiment_comment=request.experiment_comment,
            properties=properties,
        )

        return CreateAnnotationResponse(
            pheno_annotation_no=annotation_no,
            message="Phenotype annotation created successfully",
        )

    except HTTPException:
        raise
    except PhenotypeCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.exception(f"Error creating phenotype annotation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )


@router.delete("/{annotation_no}", response_model=DeleteAnnotationResponse)
def delete_phenotype_annotation(
    annotation_no: int,
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Delete a phenotype annotation.

    Removes the annotation and its reference links.
    """
    try:
        service = PhenotypeCurationService(db)
        service.delete_annotation(annotation_no, current_user.userid)

        return DeleteAnnotationResponse(
            success=True,
            message=f"Phenotype annotation {annotation_no} deleted",
        )

    except PhenotypeCurationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.exception(f"Error deleting phenotype annotation {annotation_no}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )
