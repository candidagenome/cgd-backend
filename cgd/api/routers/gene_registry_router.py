"""
Gene Registry API Router.
"""
import logging
import traceback
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.gene_registry_schema import (
    GeneRegistrySearchResponse,
    GeneRegistryConfigResponse,
    GeneRegistrySubmissionRequest,
    GeneRegistrySubmissionResponse,
)
from cgd.api.services.gene_registry_service import (
    search_gene_registry,
    get_gene_registry_config,
    submit_gene_registry,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gene-registry", tags=["gene-registry"])


@router.get("/config", response_model=GeneRegistryConfigResponse)
def get_config(
    db: Session = Depends(get_db),
):
    """
    Get configuration for gene registry form.

    Returns list of species/strains and default species.
    """
    try:
        return get_gene_registry_config(db)
    except Exception as e:
        logger.error(f"Gene registry config error: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search", response_model=GeneRegistrySearchResponse)
def search(
    last_name: str = Query(..., min_length=1, description="Colleague last name"),
    gene_name: str = Query(..., min_length=1, description="Proposed gene name"),
    orf_name: str = Query(None, description="ORF name (optional)"),
    organism: str = Query(..., description="Organism abbreviation"),
    db: Session = Depends(get_db),
):
    """
    Validate gene name and search for colleagues.

    This is the first step of gene registration:
    1. Validates the proposed gene name format
    2. Checks if gene name already exists
    3. Validates ORF if provided
    4. Searches for colleague by last name

    Returns validation results and matching colleagues.
    """
    try:
        return search_gene_registry(
            db,
            last_name=last_name,
            gene_name=gene_name,
            orf_name=orf_name if orf_name else None,
            organism_abbrev=organism,
        )
    except Exception as e:
        logger.error(f"Gene registry search error: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/submit", response_model=GeneRegistrySubmissionResponse)
def submit(
    request: GeneRegistrySubmissionRequest,
    db: Session = Depends(get_db),
):
    """
    Submit gene registry request.

    This creates a submission for curator review.
    Either provide an existing colleague_no, or provide
    new colleague details (last_name, first_name, email, institution).
    """
    try:
        result = submit_gene_registry(db, request.data.model_dump())
        return GeneRegistrySubmissionResponse(**result)
    except Exception as e:
        logger.error(f"Gene registry submission error: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
