"""
CRISPR Guide RNA Designer API Router.
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse, Response
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.crispr_schema import (
    PAMType,
    TargetRegion,
    CrisprDesignRequest,
    CrisprDesignResponse,
    CrisprConfigResponse,
    CrisprDownloadRequest,
    GeneInfo,
)
from cgd.api.services.crispr_service import (
    get_crispr_config,
    design_guides,
    get_gene_sequence,
    generate_download,
)

router = APIRouter(prefix="/api/crispr", tags=["crispr"])


@router.get("/config", response_model=CrisprConfigResponse)
def get_config():
    """
    Get CRISPR tool configuration options.

    Returns available PAM sequences, organisms, and default settings.
    """
    return get_crispr_config()


@router.post("/design", response_model=CrisprDesignResponse)
def design(
    request: CrisprDesignRequest,
    db: Session = Depends(get_db),
):
    """
    Design CRISPR guide RNAs for a gene or sequence.

    **Input** (provide one):
    - `gene_name`: Gene name to design guides for (e.g., HOG1, EFG1)
    - `sequence`: Raw DNA sequence to design guides for

    **Key Parameters**:
    - `organism`: Target organism/genome assembly (default: C_albicans_SC5314_A22)
    - `pam`: PAM sequence for the CRISPR system (default: NGG for SpCas9)
    - `guide_length`: Length of guide RNA (default: 20)
    - `target_region`: Preferred region within gene (5_prime recommended for knockouts)

    **Off-target Settings**:
    - `check_offtargets`: Enable off-target analysis (default: true)
    - `max_offtarget_mismatches`: Maximum mismatches to consider (default: 3)

    **Output Options**:
    - `max_guides`: Maximum guides to return (default: 50, max: 100)
    - `include_homology_arms`: Design HDR homology arms (default: false)

    Returns ranked guide RNAs with efficiency scores, off-target counts,
    GC content, and cloning primers.
    """
    return design_guides(db, request)


@router.get("/design", response_model=CrisprDesignResponse)
def design_get(
    gene: Optional[str] = Query(None, description="Gene name"),
    sequence: Optional[str] = Query(None, alias="seq", description="DNA sequence"),
    organism: str = Query("C_albicans_SC5314_A22", description="Organism"),
    pam: PAMType = Query(PAMType.NGG, description="PAM sequence"),
    guide_length: int = Query(20, ge=17, le=25, description="Guide length"),
    target_region: TargetRegion = Query(TargetRegion.FIVE_PRIME, description="Target region"),
    max_guides: int = Query(50, ge=1, le=100, description="Max guides to return"),
    db: Session = Depends(get_db),
):
    """
    Design CRISPR guide RNAs (GET endpoint for simple queries).

    Supports the same core options as the POST endpoint via query parameters.
    Use POST for advanced options like off-target genome selection.
    """
    request = CrisprDesignRequest(
        gene_name=gene,
        sequence=sequence,
        organism=organism,
        pam=pam,
        guide_length=guide_length,
        target_region=target_region,
        max_guides=max_guides,
    )
    return design_guides(db, request)


@router.get("/gene/{gene_name}")
def get_gene(
    gene_name: str,
    organism: str = Query("C_albicans_SC5314_A22", description="Organism"),
    db: Session = Depends(get_db),
):
    """
    Get gene information and sequence.

    Returns gene details (name, description, coordinates) and coding sequence.
    Useful for previewing gene information before designing guides.
    """
    result = get_gene_sequence(db, gene_name, organism)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Gene '{gene_name}' not found in {organism}"
        )

    gene_info, sequence = result
    return {
        "gene_info": gene_info,
        "sequence": sequence,
        "sequence_length": len(sequence),
    }


@router.post("/download/{format}")
def download_results(
    format: str,
    request: CrisprDownloadRequest,
):
    """
    Download CRISPR design results.

    **Formats**:
    - `tsv`: Tab-separated values
    - `csv`: Comma-separated values
    - `fasta`: FASTA format (guide sequences only)

    Returns file as attachment for download.
    """
    if format not in ["tsv", "csv", "fasta"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid format: {format}. Supported: tsv, csv, fasta"
        )

    content = generate_download(
        guides=request.guides,
        gene_info=request.gene_info,
        format=format,
        include_offtargets=request.include_offtargets,
        include_primers=request.include_primers,
    )

    content_types = {
        "tsv": "text/tab-separated-values",
        "csv": "text/csv",
        "fasta": "text/plain",
    }

    gene_name = ""
    if request.gene_info:
        gene_name = f"_{request.gene_info.gene_name or request.gene_info.feature_name}"

    filename = f"crispr_guides{gene_name}.{format}"

    return Response(
        content=content,
        media_type=content_types[format],
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


@router.get("/organisms")
def list_organisms():
    """
    List available organisms for CRISPR guide design.

    Returns organism tags and full names that can be used in the
    `organism` parameter of the design endpoint.
    """
    config = get_crispr_config()
    return config.organisms


@router.get("/pam-options")
def list_pam_options():
    """
    List available PAM sequences.

    Returns supported PAM sequences with their associated CRISPR systems.
    """
    config = get_crispr_config()
    return config.pam_options
