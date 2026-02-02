"""
Sequence Tools API endpoints.

Replaces the legacy Perl CGI seqTools script.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import seq_tools_service
from cgd.schemas.seq_tools_schema import (
    SeqType,
    SeqToolsRequest,
    SeqToolsResponse,
    AssembliesResponse,
    ChromosomesResponse,
)

router = APIRouter(prefix="/api/seq-tools", tags=["sequence-tools"])


@router.post("/resolve", response_model=SeqToolsResponse)
def resolve_and_get_tools(
    request: SeqToolsRequest,
    db: Session = Depends(get_db),
):
    """
    Resolve gene/coordinates/sequence and return available tools.

    This endpoint accepts three types of input:

    1. **Gene query**: Provide `query` with a gene name, ORF name, or CGDID
    2. **Coordinates**: Provide `chromosome`, `start`, and `end`
    3. **Raw sequence**: Provide `sequence` and optionally `seq_type`

    Returns a list of categorized tool links appropriate for the input type.

    Examples:
    ```json
    // Gene query
    {"query": "ACT1"}

    // Coordinates
    {"chromosome": "Ca21chr1_C_albicans_SC5314", "start": 1000, "end": 2000}

    // Raw sequence
    {"sequence": "ATGCATGCATGC", "seq_type": "dna"}
    ```
    """
    result = seq_tools_service.resolve_and_get_tools(
        db=db,
        query=request.query,
        seq_source=request.seq_source,
        chromosome=request.chromosome,
        start=request.start,
        end=request.end,
        sequence=request.sequence,
        seq_type=request.seq_type,
        flank_left=request.flank_left,
        flank_right=request.flank_right,
        reverse_complement=request.reverse_complement,
    )

    if not result:
        # Determine what type of error message to return
        if request.query:
            raise HTTPException(
                status_code=404,
                detail=f"No feature found for: {request.query}"
            )
        elif request.chromosome:
            raise HTTPException(
                status_code=400,
                detail="Invalid coordinates provided"
            )
        elif request.sequence:
            raise HTTPException(
                status_code=400,
                detail="Invalid sequence provided"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="No valid input provided. Provide query, coordinates, or sequence."
            )

    return result


@router.get("/resolve", response_model=SeqToolsResponse)
def resolve_and_get_tools_get(
    # Gene query
    query: str = Query(
        None,
        description="Gene name, ORF name, feature name, or CGDID",
        alias="locus"
    ),
    seq_source: str = Query(None, description="Assembly/sequence source"),

    # Coordinates
    chromosome: str = Query(None, alias="chr", description="Chromosome name"),
    start: int = Query(None, alias="beg", description="Start coordinate"),
    end: int = Query(None, description="End coordinate"),

    # Raw sequence
    sequence: str = Query(None, alias="seq", description="Raw sequence"),
    seq_type: SeqType = Query(None, description="Sequence type (dna or protein)"),

    # Common options
    flankl: int = Query(0, ge=0, le=10000, description="Left flanking bp"),
    flankr: int = Query(0, ge=0, le=10000, description="Right flanking bp"),
    rev: bool = Query(False, description="Reverse complement"),

    db: Session = Depends(get_db),
):
    """
    GET endpoint for resolving gene/coordinates/sequence (backwards compatible).

    Same functionality as POST /resolve but with query parameters.

    Examples:
    - /api/seq-tools/resolve?query=ACT1
    - /api/seq-tools/resolve?locus=orf19.5007&flankl=500
    - /api/seq-tools/resolve?chr=Ca21chr1&beg=1000&end=2000
    """
    result = seq_tools_service.resolve_and_get_tools(
        db=db,
        query=query,
        seq_source=seq_source,
        chromosome=chromosome,
        start=start,
        end=end,
        sequence=sequence,
        seq_type=seq_type,
        flank_left=flankl,
        flank_right=flankr,
        reverse_complement=rev,
    )

    if not result:
        if query:
            raise HTTPException(
                status_code=404,
                detail=f"No feature found for: {query}"
            )
        elif chromosome:
            raise HTTPException(
                status_code=400,
                detail="Invalid coordinates provided"
            )
        elif sequence:
            raise HTTPException(
                status_code=400,
                detail="Invalid sequence provided"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="No valid input provided. Use query, chr+beg+end, or seq parameter."
            )

    return result


@router.get("/assemblies", response_model=AssembliesResponse)
def get_available_assemblies(db: Session = Depends(get_db)):
    """
    Get list of available assemblies/genome versions.

    Returns assemblies for the dropdown selector in the SeqTools form.
    """
    return seq_tools_service.get_available_assemblies(db)


@router.get("/chromosomes", response_model=ChromosomesResponse)
def get_chromosomes(
    seq_source: str = Query(None, description="Assembly/genome version name"),
    db: Session = Depends(get_db),
):
    """
    Get list of chromosomes for an assembly.

    Returns chromosomes for the dropdown selector in the SeqTools form.
    """
    return seq_tools_service.get_chromosomes(db, seq_source)
