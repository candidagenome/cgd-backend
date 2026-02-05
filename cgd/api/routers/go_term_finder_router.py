"""
GO Term Finder Router - API endpoints for GO enrichment analysis.
"""
from __future__ import annotations

import csv
import io
import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import go_term_finder_service
from cgd.schemas.go_term_finder_schema import (
    GoEnrichmentGraphResponse,
    GoTermFinderConfigResponse,
    GoTermFinderRequest,
    GoTermFinderResponse,
    ValidateGenesRequest,
    ValidateGenesResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/go-term-finder", tags=["go-term-finder"])


@router.get("/config", response_model=GoTermFinderConfigResponse)
def get_config(db: Session = Depends(get_db)):
    """
    Get configuration options for GO Term Finder.

    Returns available organisms, evidence codes, annotation types, and default settings.
    """
    try:
        return go_term_finder_service.get_go_term_finder_config(db)
    except Exception as e:
        logger.error(f"Error in get_config: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate-genes", response_model=ValidateGenesResponse)
def validate_genes(
    request: ValidateGenesRequest,
    db: Session = Depends(get_db),
):
    """
    Validate a list of gene names/IDs against the database.

    Performs case-insensitive matching on systematic names, gene names, and aliases.
    Returns found genes with their GO annotation status.
    """
    try:
        return go_term_finder_service.validate_genes(db, request)
    except Exception as e:
        logger.error(f"Error in validate_genes: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze", response_model=GoTermFinderResponse)
def run_analysis(
    request: GoTermFinderRequest,
    db: Session = Depends(get_db),
):
    """
    Run GO Term Finder enrichment analysis.

    Performs hypergeometric test for GO term enrichment with optional
    multiple testing correction (Bonferroni or Benjamini-Hochberg FDR).

    Args:
        request: Analysis parameters including:
            - genes: List of gene names/IDs (required)
            - organism_no: Organism number (required)
            - ontology: Filter by aspect (P/F/C/all)
            - background_genes: Custom background set (optional)
            - evidence_codes: Evidence codes to include (optional)
            - annotation_types: Annotation types to include (optional)
            - p_value_cutoff: Significance cutoff (default 0.01)
            - correction_method: Multiple testing correction method

    Returns:
        GoTermFinderResponse with enriched terms grouped by aspect,
        or error details if analysis fails.
    """
    try:
        return go_term_finder_service.run_go_term_finder(db, request)
    except Exception as e:
        logger.error(f"Error in run_analysis: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/download/{format}")
def download_results(
    format: str,
    request: GoTermFinderRequest,
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
        result = go_term_finder_service.run_go_term_finder(db, request)

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
            "Aspect",
            "Query Count",
            "Query Total",
            "Query %",
            "Background Count",
            "Background Total",
            "Background %",
            "Fold Enrichment",
            "P-value",
            "FDR",
            "Genes",
        ])

        # Combine all terms
        all_terms = (
            result.result.process_terms +
            result.result.function_terms +
            result.result.component_terms
        )

        # Sort by p-value
        all_terms.sort(key=lambda x: x.p_value)

        for term in all_terms:
            gene_list = ", ".join(
                g.gene_name or g.systematic_name
                for g in term.genes
            )
            writer.writerow([
                term.goid,
                term.go_term,
                term.aspect_name,
                term.query_count,
                term.query_total,
                f"{term.query_frequency:.2f}%",
                term.background_count,
                term.background_total,
                f"{term.background_frequency:.4f}%",
                f"{term.fold_enrichment:.2f}",
                f"{term.p_value:.2e}",
                f"{term.fdr:.2e}" if term.fdr is not None else "N/A",
                gene_list,
            ])

        output.seek(0)

        media_type = "text/tab-separated-values" if format == "tsv" else "text/csv"
        filename = f"go_term_finder_results.{format}"

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


@router.post("/graph", response_model=GoEnrichmentGraphResponse)
def get_enrichment_graph(
    request: GoTermFinderRequest,
    max_nodes: int = Query(50, ge=5, le=200, description="Maximum nodes in graph"),
    db: Session = Depends(get_db),
):
    """
    Run analysis and return GO hierarchy graph for visualization.

    Args:
        request: Same analysis parameters as /analyze endpoint
        max_nodes: Maximum number of nodes to include (default 50)

    Returns:
        GoEnrichmentGraphResponse with nodes and edges for Cytoscape.js
    """
    try:
        result = go_term_finder_service.run_go_term_finder(db, request)

        if not result.success or not result.result:
            return GoEnrichmentGraphResponse(nodes=[], edges=[])

        # Combine all enriched terms
        all_terms = (
            result.result.process_terms +
            result.result.function_terms +
            result.result.component_terms
        )

        # Build graph
        return go_term_finder_service.build_enrichment_graph(
            db, all_terms, max_nodes=max_nodes
        )

    except Exception as e:
        logger.error(f"Error in get_enrichment_graph: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
