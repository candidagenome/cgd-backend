"""
Todo List Router - Endpoints for curator todo lists.

Provides GO and Literature Guide todo lists filtered by year/status.

Legacy Perl Implementation Notes (LitGuideTodoList.pm + Database::LiteratureGuide.pm):
- "Not yet curated": References with no literature_topic CV term properties
  (only status properties like "Not yet curated" or "High Priority")
- "Partially Curated": References with BOTH literature_topic properties AND
  "High Priority" or "Not yet curated" status
- "Curated Todo": References with topics under "Curation to-do" parent term
- Other statuses: Filter out references where Done:X exists for every X topic
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, distinct, exists, extract, func, not_, or_, select
from sqlalchemy.orm import Session, aliased

from cgd.auth.deps import CurrentUser
from cgd.db.deps import get_db
from cgd.models.models import (
    Cv,
    CvTerm,
    CvtermRelationship,
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
                func.upper(Organism.organism_abbrev) == organism.upper(),
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

# Constants matching legacy Perl implementation
NOT_YET_CURATED = "Not yet curated"
HIGH_PRIORITY = "High Priority"
PARTIALLY_CURATED = "Partially Curated"
CURATED_TODO = "Curated Todo"
LIT_TOPIC_CV_NAME = "literature_topic"
CURATION_TODO_PARENT = "Curation to-do"

# Curation status values from database REF_PROPERTY table
# Order matches legacy LitGuideTodoList.pm
LITGUIDE_STATUSES = [
    NOT_YET_CURATED,
    HIGH_PRIORITY,
    PARTIALLY_CURATED,
    CURATED_TODO,
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
def get_litguide_statuses(
    current_user: CurrentUser,
    db: Session = Depends(get_db),
):
    """
    Get valid curation status values for Literature Guide todo list.

    Returns the standard statuses plus "Done:" statuses dynamically loaded
    from the literature_topic CV terms in the database.
    Matches legacy LitGuideTodoList.pm behavior.
    """
    # Start with the standard statuses
    statuses = list(LITGUIDE_STATUSES)

    # Load "Done:" statuses from database - these are literature_topic CV terms
    # that have been completed (the legacy system shows them with "Done:" prefix removed)
    try:
        done_terms = (
            db.query(CvTerm.term_name)
            .join(Cv, CvTerm.cv_no == Cv.cv_no)
            .filter(Cv.cv_name == LIT_TOPIC_CV_NAME)
            .filter(CvTerm.term_name.like("Done:%"))
            .order_by(CvTerm.term_name)
            .all()
        )
        # Add Done: statuses (keeping the Done: prefix)
        for (term_name,) in done_terms:
            if term_name not in statuses:
                statuses.append(term_name)
    except Exception as e:
        logger.warning(f"Failed to load Done: statuses from database: {e}")

    return {"statuses": statuses}


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


def _get_literature_topic_terms(db: Session) -> set:
    """Get all term_names from the literature_topic CV."""
    terms = (
        db.query(CvTerm.term_name)
        .join(Cv, CvTerm.cv_no == Cv.cv_no)
        .filter(Cv.cv_name == LIT_TOPIC_CV_NAME)
        .all()
    )
    return {t[0] for t in terms}


def _get_curation_todo_child_terms(db: Session) -> set:
    """Get all child term_names under 'Curation to-do' parent."""
    # Find the parent term "Curation to-do"
    parent_term = aliased(CvTerm)
    child_term = aliased(CvTerm)

    terms = (
        db.query(child_term.term_name)
        .join(CvtermRelationship, child_term.cv_term_no == CvtermRelationship.child_cv_term_no)
        .join(parent_term, parent_term.cv_term_no == CvtermRelationship.parent_cv_term_no)
        .filter(parent_term.term_name == CURATION_TODO_PARENT)
        .all()
    )
    return {t[0] for t in terms}


def _filter_done_states(db: Session, reference_nos: list[int], lit_topic_terms: set) -> list[int]:
    """
    Filter out references where all todo topics have corresponding Done: topics.

    This implements the legacy filter_done_states logic:
    - For each reference, get all its topics (property_values that are lit_topic CV terms)
    - If a reference has "Done:X" for every "X" topic, exclude it
    """
    if not reference_nos:
        return []

    # Bulk query: get all topics for all references at once
    all_topics = (
        db.query(RefProperty.reference_no, RefProperty.property_value)
        .filter(RefProperty.reference_no.in_(reference_nos))
        .filter(RefProperty.property_value.in_(lit_topic_terms))
        .all()
    )

    # Group topics by reference_no
    ref_topics = {}
    for ref_no, prop_value in all_topics:
        if ref_no not in ref_topics:
            ref_topics[ref_no] = set()
        ref_topics[ref_no].add(prop_value)

    filtered_refs = []

    for ref_no in reference_nos:
        topic_set = ref_topics.get(ref_no, set())

        # Check if all todo topics have a corresponding Done: topic
        todo_topics = [t for t in topic_set if not t.startswith("Done:")]
        done_topics = {t.replace("Done:", "") for t in topic_set if t.startswith("Done:")}

        # If there are todo topics without corresponding Done: topics, include this reference
        has_uncompleted = any(t not in done_topics for t in todo_topics)

        if has_uncompleted or not todo_topics:
            filtered_refs.append(ref_no)

    return filtered_refs


@router.get("/litguide", response_model=LitGuideTodoResponse)
def get_litguide_todo_list(
    current_user: CurrentUser,
    status: str = Query(
        NOT_YET_CURATED,
        description="Curation status to filter by",
    ),
    year: Optional[int] = Query(None, description="Year to filter by (optional)"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    db: Session = Depends(get_db),
):
    """
    Get literature references by curation status.

    Implements legacy LitGuideTodoList.pm + Database::LiteratureGuide.pm logic:
    - "Not yet curated": References with no literature_topic CV term properties
    - "High Priority": References with High Priority status property
    - "Partially Curated": References with BOTH literature_topic AND status properties
    - "Curated Todo": References with topics under "Curation to-do" parent
    - "Done:X" statuses: References with that specific Done: topic, filtering out
      references where all todo topics have corresponding Done: topics
    """
    items = []
    total_count = 0

    if status == NOT_YET_CURATED:
        # Legacy curated='No' logic:
        # Find references that have NO literature_topic CV term properties
        # (they may have status properties like "Not yet curated" or "High Priority")
        # Note: Legacy system checks property_value against CV terms without requiring
        # property_type to match, so we do the same here.

        # Get all literature_topic terms
        lit_topic_terms = _get_literature_topic_terms(db)

        # Find references that have any property_value that's a literature_topic CV term
        refs_with_lit_topic = (
            db.query(RefProperty.reference_no)
            .filter(RefProperty.property_value.in_(lit_topic_terms))
            .distinct()
            .subquery()
        )

        # Get references WITHOUT any literature_topic properties
        base_query = db.query(Reference).filter(
            ~Reference.reference_no.in_(select(refs_with_lit_topic.c.reference_no))
        )

        if year:
            base_query = base_query.filter(Reference.year == year)

        total_count = base_query.count()

        results = (
            base_query.order_by(Reference.year.desc(), Reference.pubmed)
            .limit(limit)
            .all()
        )

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

    elif status == HIGH_PRIORITY:
        # References with High Priority status
        base_query = (
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
            .filter(RefProperty.property_value == HIGH_PRIORITY)
        )

        if year:
            base_query = base_query.filter(Reference.year == year)

        total_count = base_query.count()

        results = (
            base_query.order_by(Reference.year.desc(), Reference.pubmed)
            .limit(limit)
            .all()
        )

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

    elif status == PARTIALLY_CURATED:
        # Legacy curated='Partial' logic:
        # References that have BOTH:
        # 1. At least one property_value that's a literature_topic CV term
        # 2. AND "High Priority" or "Not yet curated" property_value
        # Note: Legacy system doesn't filter by property_type, just property_value

        lit_topic_terms = _get_literature_topic_terms(db)

        # References with property_value in literature_topic CV terms
        refs_with_lit_topic = (
            db.query(RefProperty.reference_no)
            .filter(RefProperty.property_value.in_(lit_topic_terms))
            .distinct()
            .subquery()
        )

        # References with HP or NYC property_value
        refs_with_status = (
            db.query(RefProperty.reference_no)
            .filter(RefProperty.property_value.in_([HIGH_PRIORITY, NOT_YET_CURATED]))
            .distinct()
            .subquery()
        )

        # Get references that match BOTH conditions
        base_query = (
            db.query(Reference)
            .filter(Reference.reference_no.in_(select(refs_with_lit_topic.c.reference_no)))
            .filter(Reference.reference_no.in_(select(refs_with_status.c.reference_no)))
        )

        if year:
            base_query = base_query.filter(Reference.year == year)

        # Apply filter_done_states
        all_refs = base_query.all()
        ref_nos = [r.reference_no for r in all_refs]
        filtered_ref_nos = _filter_done_states(db, ref_nos, lit_topic_terms)

        total_count = len(filtered_ref_nos)

        # Get final results with limit
        results = (
            db.query(Reference)
            .filter(Reference.reference_no.in_(filtered_ref_nos))
            .order_by(Reference.year.desc(), Reference.pubmed)
            .limit(limit)
            .all()
        )

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

    elif status == CURATED_TODO:
        # "Curated Todo" = ALL references that have ANY literature_topic CV term property
        # This is the total curation backlog - all papers with any topic assignment
        lit_topic_terms = _get_literature_topic_terms(db)

        # Get ALL references with any literature_topic property
        base_query = (
            db.query(Reference)
            .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
            .filter(RefProperty.property_value.in_(lit_topic_terms))
            .distinct()
        )

        if year:
            base_query = base_query.filter(Reference.year == year)

        total_count = base_query.count()

        results = (
            base_query.order_by(Reference.year.desc(), Reference.pubmed)
            .limit(limit)
            .all()
        )

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
        # For other statuses (typically "Done:X" statuses):
        # Query references with matching topic and apply filter_done_states
        lit_topic_terms = _get_literature_topic_terms(db)

        base_query = (
            db.query(
                Reference.reference_no,
                Reference.pubmed,
                Reference.citation,
                Reference.year,
                RefProperty.property_value,
                RefProperty.date_last_reviewed,
            )
            .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
            .filter(RefProperty.property_value == status)
        )

        if year:
            base_query = base_query.filter(Reference.year == year)

        # Get all matching references
        all_results = base_query.all()
        ref_nos = list({r.reference_no for r in all_results})

        # Apply filter_done_states to exclude fully completed references
        filtered_ref_nos = set(_filter_done_states(db, ref_nos, lit_topic_terms))

        # Filter results to only include those that passed the filter
        filtered_results = [r for r in all_results if r.reference_no in filtered_ref_nos]

        total_count = len(filtered_results)

        # Apply limit
        for row in filtered_results[:limit]:
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
        total_count=total_count,
        items=items,
    )
