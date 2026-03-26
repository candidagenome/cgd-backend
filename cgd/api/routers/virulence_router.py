"""
Virulence Factor Router - API endpoints for virulence factor browser.
"""
from __future__ import annotations

import logging
import traceback
from typing import Optional
from io import StringIO

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import virulence_service
from cgd.schemas.virulence_schema import (
    VirulenceCategoriesResponse,
    VirulenceFactorsResponse,
    VirulenceFactorDetail,
    VirulenceStats,
    VirulenceDownloadRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/virulence", tags=["virulence"])


@router.get("/categories", response_model=VirulenceCategoriesResponse)
def get_virulence_categories(
    organism: Optional[str] = Query(None, description="Filter counts by organism abbreviation"),
    db: Session = Depends(get_db),
):
    """
    Get all virulence categories with gene counts.

    Returns the list of virulence categories (Adhesins, Secreted Enzymes, etc.)
    with the number of genes matching each category.

    Args:
        organism: Optional organism abbreviation to filter counts

    Returns:
        List of categories with gene counts
    """
    try:
        return virulence_service.get_virulence_categories(db=db, organism=organism)
    except Exception as e:
        logger.error(f"Error in get_virulence_categories: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors", response_model=VirulenceFactorsResponse)
def get_virulence_factors(
    categories: list[str] = Query(default=[], description="Category keys to filter by"),
    organisms: list[str] = Query(default=[], description="Organism abbreviations to filter by"),
    search_term: Optional[str] = Query(None, description="Search term for gene name or description"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(25, ge=1, le=1000, description="Results per page"),
    db: Session = Depends(get_db),
):
    """
    Search virulence factors by criteria.

    Returns genes matching the specified virulence categories, with optional
    filtering by organism and keyword search.

    Args:
        categories: List of category keys (e.g., ["adhesins", "biofilm"])
        organisms: List of organism abbreviations to filter
        search_term: Keyword to search gene names and headlines
        page: Page number (1-indexed)
        page_size: Number of results per page

    Returns:
        Paginated list of virulence factors with category mappings
    """
    try:
        return virulence_service.get_virulence_factors(
            db=db,
            categories=categories if categories else None,
            organisms=organisms if organisms else None,
            search_term=search_term,
            page=page,
            page_size=page_size,
        )
    except Exception as e:
        logger.error(f"Error in get_virulence_factors: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors/{gene_name}", response_model=VirulenceFactorDetail)
def get_virulence_factor_detail(
    gene_name: str,
    db: Session = Depends(get_db),
):
    """
    Get virulence annotations for a specific gene.

    Returns detailed information about which virulence categories a gene
    matches and why (which rules triggered the match).

    Args:
        gene_name: Gene name or systematic name

    Returns:
        Detailed virulence information for the gene
    """
    try:
        result = virulence_service.get_virulence_factor_detail(db=db, gene_name=gene_name)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Gene '{gene_name}' not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_virulence_factor_detail: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=VirulenceStats)
def get_virulence_stats(db: Session = Depends(get_db)):
    """
    Get summary statistics for virulence factors.

    Returns counts per category and per organism.

    Returns:
        Summary statistics
    """
    try:
        return virulence_service.get_virulence_stats(db=db)
    except Exception as e:
        logger.error(f"Error in get_virulence_stats: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factors/download")
def download_virulence_factors(
    request: VirulenceDownloadRequest,
    db: Session = Depends(get_db),
):
    """
    Download virulence factors as TSV or CSV.

    Args:
        request: Download parameters including categories, organisms, search_term, and format

    Returns:
        File download response
    """
    try:
        # Get all matching factors
        factors = virulence_service.get_virulence_factors(
            db=db,
            categories=request.categories if request.categories else None,
            organisms=request.organisms if request.organisms else None,
            search_term=request.search_term,
            page=1,
            page_size=10000,  # Get all results
        )

        # Build file content
        separator = "\t" if request.format == "tsv" else ","
        output = StringIO()

        # Header
        headers = ["Gene Name", "Systematic Name", "Organism", "Categories", "Matched By", "Description"]
        output.write(separator.join(headers) + "\n")

        # Data rows
        for factor in factors.items:
            def escape_field(value: str) -> str:
                if value is None:
                    return ""
                value = str(value)
                if request.format == "csv" and ("," in value or '"' in value or "\n" in value):
                    return f'"{value.replace(chr(34), chr(34)+chr(34))}"'
                return value

            row = [
                escape_field(factor.gene_name or ""),
                escape_field(factor.feature_name),
                escape_field(factor.organism),
                escape_field("; ".join(factor.categories)),
                escape_field("; ".join(factor.match_reasons)),
                escape_field(factor.description or ""),
            ]
            output.write(separator.join(row) + "\n")

        # Create response
        content = output.getvalue()
        media_type = "text/csv" if request.format == "csv" else "text/tab-separated-values"
        filename = f"virulence_factors.{request.format}"

        return StreamingResponse(
            iter([content]),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        logger.error(f"Error in download_virulence_factors: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
