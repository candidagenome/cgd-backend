"""
Sequence retrieval API endpoints.

Replaces the legacy Perl CGI getSeq script.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.api.services import sequence_service
from cgd.schemas.sequence_schema import SeqType, SeqFormat

router = APIRouter(prefix="/api/sequence", tags=["sequence"])


def _get_filename(gene_name: str, feature_name: str, seq_type: SeqType) -> str:
    """Generate filename like ACT1_protein.fsa or orf19.5007_dna.fsa"""
    name = gene_name or feature_name or "sequence"
    type_suffix = "protein" if seq_type == SeqType.PROTEIN else "coding" if seq_type == SeqType.CODING else "dna"
    return f"{name}_{type_suffix}.fsa"


@router.get("")
def get_sequence(
    # Query by identifier
    query: str = Query(
        None,
        description="Gene name, ORF name, feature name, or CGDID",
        alias="locus"
    ),
    # Alternative parameter names for backwards compatibility
    gene: str = Query(None, description="Gene name (alias for query)"),
    orf: str = Query(None, description="ORF name (alias for query)"),
    seq: str = Query(None, description="Sequence name (alias for query)"),
    seqname: str = Query(None, description="Sequence name (alias for query)"),
    sgdid: str = Query(None, description="CGDID (alias for query)"),

    # Sequence type
    seq_type: SeqType = Query(
        SeqType.GENOMIC,
        alias="seqtype",
        description="Type of sequence: genomic, protein, or coding"
    ),

    # Output format
    format: SeqFormat = Query(
        SeqFormat.FASTA,
        description="Output format: fasta, raw, or json"
    ),

    # Flanking regions
    flankl: int = Query(0, ge=0, le=10000, description="Left flanking bp"),
    flankr: int = Query(0, ge=0, le=10000, description="Right flanking bp"),

    # Reverse complement
    rev: bool = Query(False, description="Return reverse complement"),

    db: Session = Depends(get_db),
):
    """
    Retrieve DNA or protein sequence for a gene/feature.

    This endpoint replaces the legacy getSeq CGI script.

    Examples:
    - /api/sequence?query=ACT1&seqtype=genomic
    - /api/sequence?locus=orf19.5007&seqtype=protein
    - /api/sequence?gene=ACT1&flankl=500&flankr=500
    """
    # Resolve query from various parameter aliases
    identifier = query or gene or orf or seq or seqname or sgdid

    if not identifier:
        raise HTTPException(
            status_code=400,
            detail="Missing required parameter: query, locus, gene, orf, seq, seqname, or sgdid"
        )

    result = sequence_service.get_sequence_by_feature(
        db=db,
        query=identifier,
        seq_type=seq_type,
        flank_left=flankl,
        flank_right=flankr,
        reverse_complement=rev,
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No sequence found for: {identifier}"
        )

    # Handle different output formats
    if format == SeqFormat.FASTA:
        fasta_content = sequence_service.format_as_fasta(
            result.fasta_header,
            result.sequence
        )
        filename = _get_filename(result.info.gene_name, result.info.feature_name, seq_type)
        return Response(
            content=fasta_content,
            media_type="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    elif format == SeqFormat.RAW:
        return Response(
            content=result.sequence,
            media_type="text/plain"
        )
    else:
        # JSON format
        return result


@router.get("/region")
def get_sequence_by_region(
    chr: str = Query(..., description="Chromosome name"),
    beg: int = Query(..., alias="start", description="Start coordinate (1-based)"),
    end: int = Query(..., description="End coordinate (1-based)"),
    strand: str = Query("W", description="Strand: W (Watson/+) or C (Crick/-)"),
    format: SeqFormat = Query(SeqFormat.FASTA, description="Output format"),
    rev: bool = Query(False, description="Return reverse complement"),
    db: Session = Depends(get_db),
):
    """
    Retrieve sequence for a chromosomal region.

    Examples:
    - /api/sequence/region?chr=1&start=1000&end=2000
    - /api/sequence/region?chr=Ca21chr1&beg=5000&end=6000&strand=C
    """
    if beg > end:
        raise HTTPException(
            status_code=400,
            detail="Start coordinate must be less than or equal to end coordinate"
        )

    if strand not in ("W", "C", "+", "-"):
        raise HTTPException(
            status_code=400,
            detail="Strand must be W, C, +, or -"
        )

    # Normalize strand
    normalized_strand = "W" if strand in ("W", "+") else "C"

    result = sequence_service.get_sequence_by_coordinates(
        db=db,
        chromosome=chr,
        start=beg,
        end=end,
        strand=normalized_strand,
        reverse_complement=rev,
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Chromosome not found: {chr}"
        )

    # Handle different output formats
    if format == SeqFormat.FASTA:
        fasta_content = sequence_service.format_as_fasta(
            result.fasta_header,
            result.sequence
        )
        return Response(
            content=fasta_content,
            media_type="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={chr}_{beg}_{end}.fsa"
            }
        )
    elif format == SeqFormat.RAW:
        return Response(
            content=result.sequence,
            media_type="text/plain"
        )
    else:
        # JSON format
        return result


@router.get("/fasta/{identifier}")
def get_fasta(
    identifier: str,
    seq_type: SeqType = Query(SeqType.GENOMIC, alias="type"),
    flankl: int = Query(0, ge=0, le=10000),
    flankr: int = Query(0, ge=0, le=10000),
    db: Session = Depends(get_db),
):
    """
    Convenience endpoint to get FASTA sequence directly.

    Examples:
    - /api/sequence/fasta/ACT1
    - /api/sequence/fasta/orf19.5007?type=protein
    """
    result = sequence_service.get_sequence_by_feature(
        db=db,
        query=identifier,
        seq_type=seq_type,
        flank_left=flankl,
        flank_right=flankr,
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No sequence found for: {identifier}"
        )

    fasta_content = sequence_service.format_as_fasta(
        result.fasta_header,
        result.sequence
    )
    filename = _get_filename(result.info.gene_name, result.info.feature_name, seq_type)

    return Response(
        content=fasta_content,
        media_type="text/plain",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )
