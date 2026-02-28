"""
GO Annotation Summary Router - API endpoints for GO annotation reports.
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
from cgd.api.services import go_annotation_summary_service
from cgd.schemas.go_annotation_summary_schema import (
    GoAnnotationSummaryRequest,
    GoAnnotationSummaryResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/go-annotation-summary", tags=["go-annotation-summary"])


@router.post("/analyze", response_model=GoAnnotationSummaryResponse)
def analyze(
    request: GoAnnotationSummaryRequest,
    db: Session = Depends(get_db),
):
    """
    Generate GO Annotation Summary for a list of genes.

    Returns frequency of GO term annotations comparing cluster vs genome.
    """
    try:
        return go_annotation_summary_service.run_go_annotation_summary(db, request)
    except Exception as e:
        logger.error(f"Error in analyze: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/download/{format}")
def download_results(
    request: GoAnnotationSummaryRequest,
    format: str = "tsv",
    db: Session = Depends(get_db),
):
    """
    Run analysis and download results as TSV or CSV.

    Args:
        request: Analysis request parameters
        format: Output format ('tsv' or 'csv')

    Returns:
        StreamingResponse with file download
    """
    try:
        result = go_annotation_summary_service.run_go_annotation_summary(db, request)

        if not result.success or not result.result:
            raise HTTPException(
                status_code=400,
                detail=result.error or "Analysis failed"
            )

        # Build output
        delimiter = '\t' if format == 'tsv' else ','
        output = io.StringIO()
        writer = csv.writer(output, delimiter=delimiter)

        # Write header
        writer.writerow([
            'Ontology',
            'GO ID',
            'GO Term',
            'Cluster Count',
            'Cluster Total',
            'Cluster Frequency (%)',
            'Genome Count',
            'Genome Total',
            'Genome Frequency (%)',
            'Genes',
        ])

        # Write data for each ontology
        ontology_data = [
            ('Biological Process', result.result.process_terms),
            ('Molecular Function', result.result.function_terms),
            ('Cellular Component', result.result.component_terms),
        ]

        for ontology_name, terms in ontology_data:
            for term in terms:
                gene_names = ', '.join(
                    g.gene_name or g.systematic_name for g in term.genes
                )
                writer.writerow([
                    ontology_name,
                    term.goid,
                    term.go_term,
                    term.cluster_count,
                    term.cluster_total,
                    f"{term.cluster_frequency:.2f}",
                    term.genome_count,
                    term.genome_total,
                    f"{term.genome_frequency:.2f}",
                    gene_names,
                ])

        output.seek(0)
        content_type = 'text/tab-separated-values' if format == 'tsv' else 'text/csv'
        filename = f"go_annotation_summary.{format}"

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type=content_type,
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in download_results: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
