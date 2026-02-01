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
from cgd.schemas.phenotype_schema import PhenotypeSearchResponse, ObservableTreeResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/phenotype", tags=["phenotype"])


@router.get("/search", response_model=PhenotypeSearchResponse)
def search_phenotypes(
    observable: Optional[str] = Query(None, description="Observable term to search for (supports * wildcard)"),
    qualifier: Optional[str] = Query(None, description="Qualifier filter (e.g., abnormal, normal)"),
    experiment_type: Optional[str] = Query(None, description="Experiment type filter"),
    mutant_type: Optional[str] = Query(None, description="Mutant type filter (e.g., deletion, overexpression)"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    limit: int = Query(25, ge=1, le=100, description="Results per page (max 100)"),
    db: Session = Depends(get_db),
):
    """
    Search phenotype annotations by criteria.

    Search for phenotype annotations matching the specified criteria.
    All filters are optional and support wildcard matching with *.

    Args:
        observable: Observable term (e.g., "colony morphology", "drug resistance")
        qualifier: Qualifier describing the phenotype direction (e.g., "abnormal", "normal")
        experiment_type: Type of experiment (e.g., "classical genetics", "large-scale survey")
        mutant_type: Type of mutation (e.g., "deletion", "overexpression", "null")
        page: Page number for pagination
        limit: Number of results per page

    Returns:
        Paginated list of phenotype annotations with gene info and references.
    """
    try:
        return phenotype_service.search_phenotypes(
            db=db,
            observable=observable,
            qualifier=qualifier,
            experiment_type=experiment_type,
            mutant_type=mutant_type,
            page=page,
            limit=limit,
        )
    except Exception as e:
        logger.error(f"Error in search_phenotypes: {e}")
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
