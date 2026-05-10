"""
Phenotype Enrichment Router - API endpoints for phenotype enrichment analysis.
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
from cgd.api.services import phenotype_enrichment_service
from cgd.schemas.phenotype_enrichment_schema import (
    PhenotypeEnrichmentConfigResponse,
    PhenotypeEnrichmentRequest,
    PhenotypeEnrichmentResponse,
    ValidateGenesRequest,
    ValidateGenesResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/phenotype-enrichment", tags=["phenotype-enrichment"])


@router.get("/config", response_model=PhenotypeEnrichmentConfigResponse)
def get_config(db: Session = Depends(get_db)):
    """
    Get configuration options for Phenotype Enrichment.

    Returns available organisms and default settings.
    """
    try:
        return phenotype_enrichment_service.get_phenotype_enrichment_config(db)
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
    Returns found genes with their phenotype annotation status.
    """
    try:
        return phenotype_enrichment_service.validate_genes(db, request)
    except Exception as e:
        logger.error(f"Error in validate_genes: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze", response_model=PhenotypeEnrichmentResponse)
def run_analysis(
    request: PhenotypeEnrichmentRequest,
    db: Session = Depends(get_db),
):
    """
    Run phenotype enrichment analysis.

    Performs hypergeometric test for phenotype term enrichment with optional
    multiple testing correction (Bonferroni or Benjamini-Hochberg FDR).

    Args:
        request: Analysis parameters including:
            - genes: List of gene names/IDs (required)
            - organism_no: Organism number (required)
            - background_genes: Custom background set (optional)
            - p_value_cutoff: Significance cutoff (default 0.01)
            - correction_method: Multiple testing correction method

    Returns:
        PhenotypeEnrichmentResponse with enriched phenotypes,
        or error details if analysis fails.
    """
    try:
        return phenotype_enrichment_service.run_phenotype_enrichment(db, request)
    except Exception as e:
        logger.error(f"Error in run_analysis: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/download/{format}")
def download_results(
    format: str,
    request: PhenotypeEnrichmentRequest,
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
        result = phenotype_enrichment_service.run_phenotype_enrichment(db, request)

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
            "Observable",
            "Mutant Type",
            "Qualifier",
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

        # Sort by p-value
        phenotypes = sorted(result.result.enriched_phenotypes, key=lambda x: x.p_value)

        for pheno in phenotypes:
            gene_list = ", ".join(
                g.gene_name or g.systematic_name
                for g in pheno.genes
            )
            writer.writerow([
                pheno.observable,
                pheno.mutant_type or "",
                pheno.qualifier or "",
                pheno.query_count,
                pheno.query_total,
                f"{pheno.query_frequency:.2f}%",
                pheno.background_count,
                pheno.background_total,
                f"{pheno.background_frequency:.4f}%",
                f"{pheno.fold_enrichment:.2f}",
                f"{pheno.p_value:.2e}",
                f"{pheno.fdr:.2e}" if pheno.fdr is not None else "N/A",
                gene_list,
            ])

        output.seek(0)

        media_type = "text/tab-separated-values" if format == "tsv" else "text/csv"
        filename = f"phenotype_enrichment_results.{format}"

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
