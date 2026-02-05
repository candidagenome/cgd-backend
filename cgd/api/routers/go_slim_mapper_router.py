"""
GO Slim Mapper Router - API endpoints for mapping genes to GO Slim categories.
"""
from __future__ import annotations

import csv
import io
import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import go_slim_mapper_service
from cgd.schemas.go_slim_mapper_schema import (
    GoSlimMapperConfigResponse,
    GoSlimMapperRequest,
    GoSlimMapperResponse,
    GoSlimSet,
    GoSlimSetDetail,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/go-slim-mapper", tags=["go-slim-mapper"])


@router.get("/config", response_model=GoSlimMapperConfigResponse)
def get_config(db: Session = Depends(get_db)):
    """
    Get configuration options for GO Slim Mapper.

    Returns available organisms, GO Slim sets, and annotation types.
    """
    try:
        return go_slim_mapper_service.get_go_slim_mapper_config(db)
    except Exception as e:
        logger.error(f"Error in get_config: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/slim-sets", response_model=list[GoSlimSet])
def get_slim_sets(db: Session = Depends(get_db)):
    """
    Get all available GO Slim sets.

    Returns a list of GO Slim sets with their available aspects.
    """
    try:
        return go_slim_mapper_service.get_go_slim_sets(db)
    except Exception as e:
        logger.error(f"Error in get_slim_sets: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/slim-terms/{set_name}/{aspect}", response_model=GoSlimSetDetail)
def get_slim_terms(
    set_name: str,
    aspect: str,
    db: Session = Depends(get_db),
):
    """
    Get GO Slim terms for a specific set and aspect.

    Args:
        set_name: Name of the GO Slim set
        aspect: GO aspect code (P, F, or C)

    Returns:
        GoSlimSetDetail with the list of terms in the set.
    """
    if aspect.upper() not in ("P", "F", "C"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid aspect: {aspect}. Use 'P', 'F', or 'C'"
        )

    try:
        return go_slim_mapper_service.get_slim_terms_for_set(db, set_name, aspect.upper())
    except Exception as e:
        logger.error(f"Error in get_slim_terms: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze", response_model=GoSlimMapperResponse)
def run_analysis(
    request: GoSlimMapperRequest,
    db: Session = Depends(get_db),
):
    """
    Run GO Slim Mapper analysis.

    Maps genes to GO Slim terms via direct annotations or ancestors.

    Args:
        request: Analysis parameters including:
            - genes: List of gene names/IDs (required)
            - organism_no: Organism number (required)
            - go_set_name: Name of GO Slim set to use (required)
            - go_aspect: GO aspect code P/F/C (required)
            - selected_terms: Specific term IDs to include (optional)
            - annotation_types: Annotation types to include (optional)

    Returns:
        GoSlimMapperResponse with mapped terms, or error details if analysis fails.
    """
    try:
        return go_slim_mapper_service.run_go_slim_mapper(db, request)
    except Exception as e:
        logger.error(f"Error in run_analysis: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/download/{format}")
def download_results(
    format: str,
    request: GoSlimMapperRequest,
    db: Session = Depends(get_db),
):
    """
    Run analysis and download results in specified format.

    Args:
        format: Output format ('tsv' or 'csv')
        request: Same analysis parameters as /analyze endpoint

    Returns:
        StreamingResponse with tab-separated or comma-separated data.
    """
    if format not in ("tsv", "csv"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid format: {format}. Use 'tsv' or 'csv'"
        )

    try:
        result = go_slim_mapper_service.run_go_slim_mapper(db, request)

        if not result.success or not result.result:
            raise HTTPException(
                status_code=400,
                detail=result.error or "Analysis failed"
            )

        # Build output
        delimiter = "\t" if format == "tsv" else ","
        output = io.StringIO()
        writer = csv.writer(output, delimiter=delimiter)

        # Header
        writer.writerow([
            "GOID",
            "GO Term",
            "Gene Count",
            "Total Genes",
            "Frequency %",
            "Genes",
        ])

        # Write mapped terms
        for term in result.result.mapped_terms:
            gene_list = ", ".join(
                g.gene_name or g.systematic_name
                for g in term.genes
            )
            writer.writerow([
                term.goid,
                term.go_term,
                term.gene_count,
                term.total_genes,
                f"{term.frequency_percent:.2f}%",
                gene_list,
            ])

        # Write "other" row if there are genes
        if result.result.other_genes:
            gene_list = ", ".join(
                g.gene_name or g.systematic_name
                for g in result.result.other_genes
            )
            writer.writerow([
                "",
                "[Other GO annotations]",
                len(result.result.other_genes),
                result.result.query_genes_with_go,
                f"{len(result.result.other_genes) / result.result.query_genes_with_go * 100:.2f}%"
                if result.result.query_genes_with_go > 0 else "0.00%",
                gene_list,
            ])

        # Write "not annotated" row if there are genes
        if result.result.not_annotated_genes:
            gene_list = ", ".join(
                g.gene_name or g.systematic_name
                for g in result.result.not_annotated_genes
            )
            writer.writerow([
                "",
                "[No GO annotations]",
                len(result.result.not_annotated_genes),
                result.result.query_genes_found,
                f"{len(result.result.not_annotated_genes) / result.result.query_genes_found * 100:.2f}%"
                if result.result.query_genes_found > 0 else "0.00%",
                gene_list,
            ])

        output.seek(0)

        media_type = "text/tab-separated-values" if format == "tsv" else "text/csv"
        filename = f"go_slim_mapper_results.{format}"

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in download_results: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
