"""
Phenotype Router - API endpoints for phenotype search and observable terms.
"""
from __future__ import annotations

import logging
import traceback
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import phenotype_service
from cgd.schemas.phenotype_schema import PhenotypeSearchResponse, PhenotypeSearchSummaryResponse, ObservableTreeResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/phenotype", tags=["phenotype"])


@router.get("/search", response_model=PhenotypeSearchResponse)
def search_phenotypes(
    query: Optional[str] = Query(None, description="General keyword search across all fields"),
    observable: Optional[str] = Query(None, description="Observable term to search for (supports * wildcard)"),
    qualifier: Optional[str] = Query(None, description="Qualifier filter (e.g., abnormal, normal)"),
    experiment_type: Optional[str] = Query(None, description="Experiment type filter"),
    mutant_type: Optional[str] = Query(None, description="Mutant type filter (e.g., deletion, overexpression)"),
    property_value: Optional[str] = Query(None, description="Chemical/condition search (e.g., fluconazole)"),
    property_type: Optional[str] = Query(None, description="Property type filter (e.g., chemical, condition)"),
    pubmed: Optional[str] = Query(None, description="PubMed ID search"),
    organism: Optional[str] = Query(None, description="Organism filter (organism abbreviation)"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    limit: int = Query(25, ge=1, le=100, description="Results per page (max 100)"),
    db: Session = Depends(get_db),
):
    """
    Search phenotype annotations by criteria.

    Search for phenotype annotations matching the specified criteria.
    All filters are optional and support wildcard matching with *.

    Args:
        query: General keyword search across observables, qualifiers, chemicals, conditions
        observable: Observable term (e.g., "colony morphology", "drug resistance")
        qualifier: Qualifier describing the phenotype direction (e.g., "abnormal", "normal")
        experiment_type: Type of experiment (e.g., "classical genetics", "large-scale survey")
        mutant_type: Type of mutation (e.g., "deletion", "overexpression", "null")
        property_value: Chemical or condition value (e.g., "fluconazole", "37C")
        property_type: Type of property (e.g., "chemical", "condition")
        pubmed: PubMed ID to filter by
        organism: Organism abbreviation (e.g., "C_albicans_SC5314")
        page: Page number for pagination
        limit: Number of results per page

    Returns:
        Paginated list of phenotype annotations with gene info and references.
    """
    try:
        return phenotype_service.search_phenotypes(
            db=db,
            query=query,
            observable=observable,
            qualifier=qualifier,
            experiment_type=experiment_type,
            mutant_type=mutant_type,
            property_value=property_value,
            property_type=property_type,
            pubmed=pubmed,
            organism=organism,
            page=page,
            limit=limit,
        )
    except Exception as e:
        logger.error(f"Error in search_phenotypes: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/summary", response_model=PhenotypeSearchSummaryResponse)
def search_phenotypes_summary(
    query: Optional[str] = Query(None, description="General keyword search"),
    db: Session = Depends(get_db),
):
    """
    Get summary of phenotype search results grouped by observable.

    Returns counts of annotations for each observable matching the search query,
    similar to the Perl CGI summary page.

    Args:
        query: Keyword to search for in observables and qualifiers

    Returns:
        Summary with counts grouped by observable term.
    """
    try:
        return phenotype_service.search_phenotypes_summary(db=db, query=query)
    except Exception as e:
        logger.error(f"Error in search_phenotypes_summary: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/observables", response_model=ObservableTreeResponse)
def get_observable_tree(db: Session = Depends(get_db)):
    """
    Get hierarchical tree of observable terms.

    Returns a tree structure of observable terms used in phenotype annotations,
    with annotation counts for each term. The tree is organized hierarchically
    based on CV term relationships if available.

    Returns:
        Tree structure of observable terms with annotation counts.
    """
    try:
        return phenotype_service.get_observable_tree(db)
    except Exception as e:
        logger.error(f"Error in get_observable_tree: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
