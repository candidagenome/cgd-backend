"""Router for ortholog converter endpoints."""
from __future__ import annotations

import csv
import io
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from cgd.api.deps import get_db
from cgd.api.services import ortholog_converter_service
from cgd.schemas.ortholog_converter_schema import (
    TargetOrganism,
    OrthologConvertRequest,
    OrthologConvertResponse,
    AvailableTargetsResponse,
)

router = APIRouter(prefix="/api/orthologs", tags=["Ortholog Converter"])


@router.get(
    "/targets",
    response_model=AvailableTargetsResponse,
    summary="List available target organisms",
    description="Returns a list of organisms that can be used as conversion targets.",
)
def get_available_targets() -> AvailableTargetsResponse:
    """List available target organisms for ortholog conversion."""
    return ortholog_converter_service.get_available_targets()


@router.post(
    "/convert",
    response_model=OrthologConvertResponse,
    summary="Convert gene list to orthologs",
    description="""
    Convert a list of gene identifiers to their orthologs in a target organism.

    **Input formats accepted:**
    - Gene names (e.g., ACT1, ERG11)
    - Systematic names (e.g., C1_00010W_A, CAGL0A00110g)
    - CGD dbxref IDs (e.g., CAL0000184082)

    **Supported target organisms:**
    - CGD species: C. albicans, C. dubliniensis, C. tropicalis, C. parapsilosis, C. auris, C. glabrata
    - External: S. cerevisiae (SGD), S. pombe, A. nidulans, N. crassa

    **Relationship types:**
    - `1:1`: One-to-one ortholog relationship
    - `1:many`: One input gene maps to multiple orthologs
    - `many:1`: Multiple genes map to one ortholog
    - `many:many`: Complex many-to-many relationship
    - `no_ortholog`: No ortholog found in target organism
    - `same_organism`: Input gene is already in target organism
    - `not_found`: Input gene not found in CGD
    """,
)
def convert_orthologs(
    request: OrthologConvertRequest,
    db: Session = Depends(get_db),
) -> OrthologConvertResponse:
    """Convert a list of gene IDs to orthologs in the target organism."""
    return ortholog_converter_service.convert_orthologs(
        db=db,
        gene_ids=request.gene_ids,
        target_organism=request.target_organism,
    )


@router.post(
    "/convert/download",
    summary="Download ortholog conversion as CSV/TSV",
    description="Convert genes and download results as a file.",
    responses={
        200: {
            "content": {
                "text/csv": {},
                "text/tab-separated-values": {},
            },
            "description": "Ortholog conversion results as downloadable file",
        }
    },
)
def download_ortholog_conversion(
    request: OrthologConvertRequest,
    db: Session = Depends(get_db),
    format: Annotated[str, Query(description="Output format: csv or tsv")] = "csv",
) -> StreamingResponse:
    """Download ortholog conversion results as CSV or TSV."""
    result = ortholog_converter_service.convert_orthologs(
        db=db,
        gene_ids=request.gene_ids,
        target_organism=request.target_organism,
    )

    # Create output
    output = io.StringIO()
    delimiter = '\t' if format == 'tsv' else ','
    writer = csv.writer(output, delimiter=delimiter)

    # Header
    writer.writerow([
        'Input_ID',
        'Input_Gene_Name',
        'Input_Systematic_Name',
        'Input_Organism',
        'Found_In_CGD',
        'Ortholog_ID',
        'Ortholog_Gene_Name',
        'Ortholog_Systematic_Name',
        'Target_Organism',
        'Relationship',
        'Cluster_ID',
        'Notes',
    ])

    # Data rows
    for r in result.results:
        writer.writerow([
            r.input_id,
            r.input_gene_name or '',
            r.input_feature_name or '',
            r.input_organism or '',
            'Yes' if r.found else 'No',
            r.ortholog_id or '',
            r.ortholog_gene_name or '',
            r.ortholog_feature_name or '',
            r.target_organism or '',
            r.relationship or '',
            r.cluster_id or '',
            r.notes or '',
        ])

    output.seek(0)

    # Set filename
    media_type = 'text/tab-separated-values' if format == 'tsv' else 'text/csv'
    extension = 'tsv' if format == 'tsv' else 'csv'
    filename = f"ortholog_conversion_{request.target_organism.value}.{extension}"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type=media_type,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
        },
    )


@router.post(
    "/convert/ids-only",
    summary="Get just the ortholog IDs",
    description="Returns only the ortholog IDs as a newline-separated list (for pasting into other tools).",
    responses={
        200: {
            "content": {"text/plain": {}},
            "description": "Ortholog IDs as plain text, one per line",
        }
    },
)
def get_ortholog_ids_only(
    request: OrthologConvertRequest,
    db: Session = Depends(get_db),
    include_missing: Annotated[
        bool,
        Query(description="Include placeholder for genes without orthologs")
    ] = False,
) -> StreamingResponse:
    """Get just the ortholog IDs as a plain text list."""
    result = ortholog_converter_service.convert_orthologs(
        db=db,
        gene_ids=request.gene_ids,
        target_organism=request.target_organism,
    )

    lines = []
    for r in result.results:
        if r.ortholog_id:
            lines.append(r.ortholog_id)
        elif include_missing:
            lines.append(f"# {r.input_id}: {r.relationship or 'no_ortholog'}")

    output = '\n'.join(lines)

    return StreamingResponse(
        iter([output]),
        media_type="text/plain",
        headers={
            "Content-Disposition": f"attachment; filename=ortholog_ids_{request.target_organism.value}.txt",
        },
    )
