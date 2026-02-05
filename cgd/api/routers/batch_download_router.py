"""
Batch Download API Router.

Replaces the legacy Perl CGI batchDownload script.
"""
from typing import List
import zipfile
import io

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.batch_download_schema import (
    DataType,
    BatchDownloadRequest,
    BatchDownloadResponse,
    DownloadFile,
)
from cgd.api.services.batch_download_service import (
    process_batch_download,
)

router = APIRouter(prefix="/api/batch-download", tags=["batch-download"])


def _parse_gene_list(genes_str: str) -> List[str]:
    """Parse a comma or newline separated list of genes."""
    # Replace common separators
    genes_str = genes_str.replace('\r\n', '\n').replace('\r', '\n')
    genes_str = genes_str.replace(',', '\n')

    # Split and clean
    genes = [g.strip() for g in genes_str.split('\n')]
    return [g for g in genes if g]


@router.post("", response_class=Response)
def batch_download(
    request: BatchDownloadRequest,
    db: Session = Depends(get_db),
):
    """
    Download batch data for a list of genes.

    Submit a list of gene names and data types to download.
    Returns a ZIP file containing all requested data.

    **Input Options:**
    - `genes`: List of gene names, ORF names, feature names, or CGDIDs
    - `regions`: List of chromosomal regions (chromosome, start, end, strand)

    **Data Types:**
    - `genomic`: Genomic DNA sequence (FASTA)
    - `genomic_flanking`: Genomic + flanking regions (FASTA)
    - `coding`: Coding sequence / CDS (FASTA)
    - `protein`: Protein sequence (FASTA)
    - `coords`: Chromosomal coordinates (TSV)
    - `go`: GO annotations (GAF 2.2 format)
    - `phenotype`: Phenotype data (TSV)
    - `ortholog`: Ortholog data (TSV)

    **Options:**
    - `flank_left`: Upstream flanking bp (for genomic_flanking)
    - `flank_right`: Downstream flanking bp (for genomic_flanking)
    - `compress`: Gzip compress individual files (default: true)
    """
    if not request.genes and not request.regions:
        raise HTTPException(
            status_code=400,
            detail="Must provide either 'genes' or 'regions'"
        )

    results, features, not_found = process_batch_download(db, request)

    if not results:
        raise HTTPException(
            status_code=404,
            detail="No data found for the requested genes/regions"
        )

    # If only one data type and one file, return directly
    if len(results) == 1:
        data_type, (filename, content) = list(results.items())[0]
        if request.compress:
            media_type = "application/gzip"
        else:
            if data_type in (DataType.GENOMIC, DataType.GENOMIC_FLANKING,
                             DataType.CODING, DataType.PROTEIN):
                media_type = "text/plain"
            else:
                media_type = "text/tab-separated-values"

        return Response(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    # Multiple data types: create a ZIP file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for data_type, (filename, content) in results.items():
            zf.writestr(filename, content)

        # Add not_found.txt if there are missing genes
        if not_found:
            not_found_content = "# Genes not found\n"
            for nf in not_found:
                not_found_content += f"{nf.query}\t{nf.reason}\n"
            zf.writestr("not_found.txt", not_found_content)

    zip_buffer.seek(0)

    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={
            "Content-Disposition": "attachment; filename=batch_download.zip"
        }
    )


@router.get("", response_class=Response)
def batch_download_get(
    genes: str = Query(
        ...,
        description="Comma or newline-separated list of gene names"
    ),
    data_types: str = Query(
        "genomic",
        alias="types",
        description="Comma-separated data types: genomic, protein, coding, coords, go, phenotype, ortholog"
    ),
    flank_left: int = Query(0, alias="flankl", ge=0, le=100000),
    flank_right: int = Query(0, alias="flankr", ge=0, le=100000),
    compress: bool = Query(True, description="Gzip compress output"),
    db: Session = Depends(get_db),
):
    """
    Download batch data (GET endpoint for simple queries).

    Example:
    - /api/batch-download?genes=ACT1,TUB1&types=genomic,protein
    """
    gene_list = _parse_gene_list(genes)
    if not gene_list:
        raise HTTPException(status_code=400, detail="No genes provided")

    # Parse data types
    type_strs = [t.strip().lower() for t in data_types.split(',')]
    parsed_types = []
    for t in type_strs:
        try:
            parsed_types.append(DataType(t))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid data type: {t}. "
                       f"Valid types: {', '.join(dt.value for dt in DataType)}"
            )

    request = BatchDownloadRequest(
        genes=gene_list,
        data_types=parsed_types,
        flank_left=flank_left,
        flank_right=flank_right,
        compress=compress,
    )

    return batch_download(request, db)


@router.post("/upload", response_class=Response)
async def batch_download_upload(
    file: UploadFile = File(..., description="File containing gene names (one per line)"),
    data_types: str = Query(
        "genomic",
        alias="types",
        description="Comma-separated data types"
    ),
    flank_left: int = Query(0, alias="flankl", ge=0, le=100000),
    flank_right: int = Query(0, alias="flankr", ge=0, le=100000),
    compress: bool = Query(True),
    db: Session = Depends(get_db),
):
    """
    Upload a file of gene names and download batch data.

    The file should contain one gene name per line.
    """
    content = await file.read()
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        text = content.decode('latin-1')

    gene_list = _parse_gene_list(text)
    if not gene_list:
        raise HTTPException(status_code=400, detail="No genes found in uploaded file")

    # Parse data types
    type_strs = [t.strip().lower() for t in data_types.split(',')]
    parsed_types = []
    for t in type_strs:
        try:
            parsed_types.append(DataType(t))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid data type: {t}"
            )

    request = BatchDownloadRequest(
        genes=gene_list,
        data_types=parsed_types,
        flank_left=flank_left,
        flank_right=flank_right,
        compress=compress,
    )

    return batch_download(request, db)


@router.get("/metadata", response_model=BatchDownloadResponse)
def batch_download_metadata(
    genes: str = Query(
        ...,
        description="Comma or newline-separated list of gene names"
    ),
    data_types: str = Query(
        "genomic",
        alias="types",
        description="Comma-separated data types"
    ),
    flank_left: int = Query(0, alias="flankl", ge=0, le=100000),
    flank_right: int = Query(0, alias="flankr", ge=0, le=100000),
    compress: bool = Query(True),
    db: Session = Depends(get_db),
):
    """
    Get metadata about what would be downloaded without actually downloading.

    Returns information about the files that would be generated, including
    counts and any genes that were not found.
    """
    gene_list = _parse_gene_list(genes)
    if not gene_list:
        raise HTTPException(status_code=400, detail="No genes provided")

    # Parse data types
    type_strs = [t.strip().lower() for t in data_types.split(',')]
    parsed_types = []
    for t in type_strs:
        try:
            parsed_types.append(DataType(t))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid data type: {t}"
            )

    request = BatchDownloadRequest(
        genes=gene_list,
        data_types=parsed_types,
        flank_left=flank_left,
        flank_right=flank_right,
        compress=compress,
    )

    results, features, not_found = process_batch_download(db, request)

    # Build file metadata
    files = []
    for data_type, (filename, content) in results.items():
        if compress:
            content_type = "application/gzip"
        elif data_type in (DataType.GENOMIC, DataType.GENOMIC_FLANKING,
                           DataType.CODING, DataType.PROTEIN):
            content_type = "text/plain"
        elif data_type == DataType.GO:
            content_type = "text/plain"
        else:
            content_type = "text/tab-separated-values"

        # Count records (roughly - count newlines for text content)
        if compress:
            import gzip
            import io
            with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as f:
                text_content = f.read().decode('utf-8')
        else:
            text_content = content.decode('utf-8')

        if data_type in (DataType.GENOMIC, DataType.GENOMIC_FLANKING,
                         DataType.CODING, DataType.PROTEIN):
            record_count = text_content.count('>')
        else:
            record_count = max(0, text_content.count('\n') - 1)  # Subtract header

        files.append(DownloadFile(
            data_type=data_type,
            filename=filename,
            content_type=content_type,
            size=len(content),
            record_count=record_count,
        ))

    return BatchDownloadResponse(
        success=True,
        files=files,
        total_requested=len(gene_list),
        total_found=len(features),
        not_found=not_found,
    )


@router.get("/types")
def list_data_types():
    """
    List available data types for batch download.
    """
    return {
        "data_types": [
            {
                "value": dt.value,
                "description": {
                    DataType.GENOMIC: "Genomic DNA sequence (FASTA)",
                    DataType.GENOMIC_FLANKING: "Genomic + flanking regions (FASTA)",
                    DataType.CODING: "Coding sequence / CDS (FASTA)",
                    DataType.PROTEIN: "Protein sequence (FASTA)",
                    DataType.COORDS: "Chromosomal coordinates (TSV)",
                    DataType.GO: "GO annotations (GAF 2.2)",
                    DataType.PHENOTYPE: "Phenotype data (TSV)",
                    DataType.ORTHOLOG: "Ortholog data (TSV)",
                }.get(dt, ""),
                "format": {
                    DataType.GENOMIC: "fasta",
                    DataType.GENOMIC_FLANKING: "fasta",
                    DataType.CODING: "fasta",
                    DataType.PROTEIN: "fasta",
                    DataType.COORDS: "tsv",
                    DataType.GO: "gaf",
                    DataType.PHENOTYPE: "tsv",
                    DataType.ORTHOLOG: "tsv",
                }.get(dt, ""),
            }
            for dt in DataType
        ]
    }
