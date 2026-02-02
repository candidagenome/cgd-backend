from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import reference_service
from cgd.schemas.reference_schema import (
    ReferenceResponse,
    ReferenceLocusResponse,
    ReferenceGOResponse,
    ReferencePhenotypeResponse,
    ReferenceInteractionResponse,
    ReferenceLiteratureTopicsResponse,
    AuthorSearchResponse,
    NewPapersThisWeekResponse,
)

router = APIRouter(prefix="/api/reference", tags=["reference"])


@router.get("/search/author", response_model=AuthorSearchResponse)
def search_references_by_author(
    author: str = Query(..., description="Author name to search for"),
    db: Session = Depends(get_db),
):
    """
    Search for references by author name.

    Args:
        author: Author name to search for (case-insensitive, wildcards supported)

    Returns list of references with matching authors, along with author counts.
    """
    return reference_service.search_references_by_author(db, author)


@router.get("/new-papers-this-week", response_model=NewPapersThisWeekResponse)
def get_new_papers_this_week(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    db: Session = Depends(get_db),
):
    """
    Get references added to CGD within the last N days.

    Args:
        days: Number of days to look back (default 7, max 90)

    Returns list of new papers with citation info and links.
    """
    return reference_service.get_new_papers_this_week(db, days)


@router.get("/{identifier}", response_model=ReferenceResponse)
def get_reference(identifier: str, db: Session = Depends(get_db)):
    """
    Get basic reference info by PubMed ID or DBXREF_ID.

    Args:
        identifier: Either a PubMed ID (numeric) or a DBXREF_ID (string like 'CGD_REF:xxx')

    Returns citation, title, year, authors, abstract, journal info, and URLs.
    """
    return reference_service.get_reference(db, identifier)


@router.get("/{identifier}/locus_details", response_model=ReferenceLocusResponse)
def get_reference_locus_details(identifier: str, db: Session = Depends(get_db)):
    """
    Get loci (genes/features) addressed in this paper.

    Args:
        identifier: Either a PubMed ID (numeric) or a DBXREF_ID (string like 'CGD_REF:xxx')

    Returns list of features linked to this reference via ref_property.
    """
    return reference_service.get_reference_locus_details(db, identifier)


@router.get("/{identifier}/go_details", response_model=ReferenceGOResponse)
def get_reference_go_details(identifier: str, db: Session = Depends(get_db)):
    """
    Get GO annotations citing this reference.

    Args:
        identifier: Either a PubMed ID (numeric) or a DBXREF_ID (string like 'CGD_REF:xxx')

    Returns list of GO annotations linked to this reference.
    """
    return reference_service.get_reference_go_details(db, identifier)


@router.get("/{identifier}/phenotype_details", response_model=ReferencePhenotypeResponse)
def get_reference_phenotype_details(identifier: str, db: Session = Depends(get_db)):
    """
    Get phenotype annotations citing this reference.

    Args:
        identifier: Either a PubMed ID (numeric) or a DBXREF_ID (string like 'CGD_REF:xxx')

    Returns list of phenotype annotations linked to this reference.
    """
    return reference_service.get_reference_phenotype_details(db, identifier)


@router.get("/{identifier}/interaction_details", response_model=ReferenceInteractionResponse)
def get_reference_interaction_details(identifier: str, db: Session = Depends(get_db)):
    """
    Get interactions citing this reference.

    Args:
        identifier: Either a PubMed ID (numeric) or a DBXREF_ID (string like 'CGD_REF:xxx')

    Returns list of interactions linked to this reference.
    """
    return reference_service.get_reference_interaction_details(db, identifier)


@router.get("/{identifier}/literature_topics", response_model=ReferenceLiteratureTopicsResponse)
def get_reference_literature_topics(identifier: str, db: Session = Depends(get_db)):
    """
    Get literature/curation topics for this reference.

    Args:
        identifier: Either a PubMed ID (numeric) or a DBXREF_ID (string like 'CGD_REF:xxx')

    Returns topics addressed in this paper, with associated genes/features for each topic.
    This data is used to build the topic matrix showing which topics are addressed
    for which genes.
    """
    return reference_service.get_reference_literature_topics(db, identifier)
