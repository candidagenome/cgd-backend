"""
Todo List Router - Endpoints for curator todo lists.

Provides GO and Literature Guide todo lists filtered by year/status.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import distinct, extract, func, or_
from sqlalchemy.orm import Session

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.models.models import (
    Feature,
    Go,
    GoAnnotation,
    Organism,
    RefProperty,
    Reference,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/curation/todo", tags=["curation-todo"])


# ---------------------------
# Response Schemas
# ---------------------------


class GoTodoItem(BaseModel):
    """Single GO annotation todo item."""

    go_annotation_no: int
    feature_no: int
    feature_name: str
    gene_name: Optional[str]
    organism_name: str
    goid: int
    go_term: str
    go_aspect: str
    go_evidence: str
    date_last_reviewed: str


class GoTodoResponse(BaseModel):
    """Response for GO todo list."""

    year: int
    total_count: int
    items: list[GoTodoItem]


class LitGuideTodoItem(BaseModel):
    """Single literature guide todo item."""

    reference_no: int
    pubmed: Optional[int]
    citation: str
    year: int
    curation_status: str
    property_value: str
    date_last_reviewed: str


class LitGuideTodoResponse(BaseModel):
    """Response for literature guide todo list."""

    year: Optional[int]
    status: str
    total_count: int
    items: list[LitGuideTodoItem]


class ValidYearsResponse(BaseModel):
    """Response for valid years query."""

    years: list[int]


# ---------------------------
# GO Todo List Endpoints
# ---------------------------


@router.get("/years/go", response_model=ValidYearsResponse)
def get_go_todo_years(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get valid years for GO todo list.

    Returns distinct years from GO_ANNOTATION.date_last_reviewed.
    """
    years = (
        db.query(distinct(extract("year", GoAnnotation.date_last_reviewed)))
        .filter(GoAnnotation.date_last_reviewed.isnot(None))
        .order_by(extract("year", GoAnnotation.date_last_reviewed).desc())
        .all()
    )

    return ValidYearsResponse(years=[int(y[0]) for y in years if y[0]])


@router.get("/go", response_model=GoTodoResponse)
def get_go_todo_list(
    current_user: CurrentUser,
    year: int = Query(..., description="Year to filter by date_last_reviewed"),
    organism: Optional[str] = Query(None, description="Filter by organism abbreviation"),
    limit: int = Query(50, ge=1, le=500, description="Max results per organism"),
    db: Session = Depends(get_db),
):
    """
    Get GO annotations last reviewed in specified year.

    Returns GO annotations grouped by organism, limited to LIMIT per organism.
    Mirrors legacy curateGOtodo.pl behavior.
    """
    query = (
        db.query(
            GoAnnotation.go_annotation_no,
            GoAnnotation.feature_no,
            Feature.feature_name,
            Feature.gene_name,
            Organism.organism_name.label("organism_name"),
            Go.goid,
            Go.go_term,
            Go.go_aspect,
            GoAnnotation.go_evidence,
            GoAnnotation.date_last_reviewed,
        )
        .join(Feature, GoAnnotation.feature_no == Feature.feature_no)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .join(Go, GoAnnotation.go_no == Go.go_no)
        .filter(extract("year", GoAnnotation.date_last_reviewed) == year)
    )

    if organism:
        query = query.filter(
            or_(
                func.upper(Organism.abbreviation) == organism.upper(),
                func.upper(Organism.organism_name) == organism.upper(),
            )
        )

    # Order by organism, then feature name
    query = query.order_by(
        Organism.organism_name,
        Feature.feature_name,
        Go.go_aspect,
    )

    # Apply limit
    results = query.limit(limit * 10).all()  # Get more to account for grouping

    items = []
    for row in results:
        items.append(
            GoTodoItem(
                go_annotation_no=row.go_annotation_no,
                feature_no=row.feature_no,
                feature_name=row.feature_name,
                gene_name=row.gene_name,
                organism_name=row.organism_name,
                goid=row.goid,
                go_term=row.go_term,
                go_aspect=row.go_aspect,
                go_evidence=row.go_evidence,
                date_last_reviewed=row.date_last_reviewed.strftime("%Y-%m-%d")
                if row.date_last_reviewed
                else "",
            )
        )

    return GoTodoResponse(
        year=year,
        total_count=len(items),
        items=items[:limit],
    )


# ---------------------------
# Literature Guide Todo List
# ---------------------------

# Curation status values from database REF_PROPERTY table
LITGUIDE_STATUSES = [
    "Not yet curated",
    "High Priority",
    "Abstract curated, full text not curated",
    "Done:Abstract curated, full text not curated",
    "Basic, lit guide, GO, Pheno curation done",
    "Dataset to load",
    "Gene model",
    "Genomic sequence not identified",
    "Pathways",
    "Related species",
    "cell biology",
    "clinical",
    "multiple",
    "not gene specific",
    "other",
]


@router.get("/years/litguide", response_model=ValidYearsResponse)
def get_litguide_todo_years(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get valid years for Literature Guide todo list.

    Returns distinct years from REFERENCE.year.
    """
    years = (
        db.query(distinct(Reference.year))
        .filter(Reference.year.isnot(None))
        .order_by(Reference.year.desc())
        .all()
    )

    return ValidYearsResponse(years=[int(y[0]) for y in years if y[0]])


@router.get("/statuses/litguide")
def get_litguide_statuses(current_user: CurrentUser):
    """
    Get valid curation status values for Literature Guide todo list.
    """
    return {"statuses": LITGUIDE_STATUSES}


@router.get("/debug/ref-property-types")
def get_ref_property_types(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Debug endpoint: Get distinct property_type values from REF_PROPERTY table.
    """
    types = (
        db.query(RefProperty.property_type, func.count(RefProperty.ref_property_no))
        .group_by(RefProperty.property_type)
        .order_by(RefProperty.property_type)
        .all()
    )
    return {"property_types": [{"type": t[0], "count": t[1]} for t in types]}


@router.get("/debug/curation-status-values")
def get_curation_status_values(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Debug endpoint: Get distinct curation status values from REF_PROPERTY table.
    """
    # Try various potential property_type values
    results = (
        db.query(RefProperty.property_type, RefProperty.property_value, func.count(RefProperty.ref_property_no))
        .filter(
            or_(
                RefProperty.property_type.ilike("%curation%"),
                RefProperty.property_type.ilike("%status%"),
            )
        )
        .group_by(RefProperty.property_type, RefProperty.property_value)
        .order_by(RefProperty.property_type, RefProperty.property_value)
        .all()
    )
    return {"curation_statuses": [{"type": r[0], "value": r[1], "count": r[2]} for r in results]}


@router.get("/litguide", response_model=LitGuideTodoResponse)
def get_litguide_todo_list(
    current_user: CurrentUser,
    status: str = Query(
        "Not yet curated",
        description="Curation status to filter by",
    ),
    year: Optional[int] = Query(None, description="Year to filter by (optional)"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    db: Session = Depends(get_db),
):
    """
    Get literature references by curation status.

    Returns references with their curation status from REF_PROPERTY table.
    References without a curation status property are treated as "Not Yet Curated".
    Mirrors legacy curateLitTodo.pl behavior.
    """
    if status == "Not yet curated":
        # For "Not yet curated", find references that DON'T have a curation status property
        # Use a subquery to find references that DO have a status
        refs_with_status = (
            db.query(RefProperty.reference_no)
            .filter(RefProperty.property_type == "curation_status")
            .subquery()
        )

        query = (
            db.query(
                Reference.reference_no,
                Reference.pubmed,
                Reference.citation,
                Reference.year,
            )
            .filter(~Reference.reference_no.in_(db.query(refs_with_status.c.reference_no)))
        )

        if year:
            query = query.filter(Reference.year == year)

        # Order by year descending, then pubmed
        query = query.order_by(Reference.year.desc(), Reference.pubmed)

        results = query.limit(limit).all()

        items = []
        for row in results:
            items.append(
                LitGuideTodoItem(
                    reference_no=row.reference_no,
                    pubmed=row.pubmed,
                    citation=row.citation or "",
                    year=row.year or 0,
                    curation_status=status,
                    property_value=status,
                    date_last_reviewed="",
                )
            )
    else:
        # For other statuses, query references with matching curation status property
        query = (
            db.query(
                Reference.reference_no,
                Reference.pubmed,
                Reference.citation,
                Reference.year,
                RefProperty.property_value,
                RefProperty.date_last_reviewed,
            )
            .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
            .filter(RefProperty.property_type == "curation_status")
            .filter(RefProperty.property_value == status)
        )

        if year:
            query = query.filter(Reference.year == year)

        # Order by year descending, then pubmed
        query = query.order_by(Reference.year.desc(), Reference.pubmed)

        results = query.limit(limit).all()

        items = []
        for row in results:
            items.append(
                LitGuideTodoItem(
                    reference_no=row.reference_no,
                    pubmed=row.pubmed,
                    citation=row.citation or "",
                    year=row.year or 0,
                    curation_status=status,
                    property_value=row.property_value,
                    date_last_reviewed=row.date_last_reviewed.strftime("%Y-%m-%d")
                    if row.date_last_reviewed
                    else "",
                )
            )

    return LitGuideTodoResponse(
        year=year,
        status=status,
        total_count=len(items),
        items=items,
    )
