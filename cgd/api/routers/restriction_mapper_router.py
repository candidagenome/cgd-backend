"""
Restriction Mapper API Router.
"""
from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.restriction_mapper_schema import (
    EnzymeFilterType,
    RestrictionMapperRequest,
    RestrictionMapperResponse,
    RestrictionMapperConfigResponse,
)
from cgd.api.services.restriction_mapper_service import (
    get_restriction_mapper_config,
    run_restriction_mapping,
    format_results_tsv,
    format_non_cutting_tsv,
)

router = APIRouter(prefix="/api/restriction-mapper", tags=["restriction-mapper"])


@router.get("/config", response_model=RestrictionMapperConfigResponse)
def get_config():
    """
    Get restriction mapper configuration options.

    Returns available enzyme filters and total number of enzymes in the database.
    """
    return get_restriction_mapper_config()


@router.post("/search", response_model=RestrictionMapperResponse)
def search(
    request: RestrictionMapperRequest,
    db: Session = Depends(get_db),
):
    """
    Run restriction enzyme mapping on a DNA sequence.

    **Input Options** (provide one):
    - `locus`: Gene name, ORF name, or CGDID
    - `sequence`: Raw DNA sequence (with optional `sequence_name`)

    **Enzyme Filters**:
    - `all`: All cutting enzymes
    - `5_overhang`: Enzymes producing 5' overhangs
    - `3_overhang`: Enzymes producing 3' overhangs
    - `blunt`: Enzymes producing blunt ends
    - `cut_once`: Enzymes that cut exactly once
    - `cut_twice`: Enzymes that cut exactly twice
    - `six_base`: Six-base recognition sequence cutters
    - `no_cut`: Non-cutting enzymes only

    Returns enzyme cut sites, fragment sizes, and non-cutting enzymes.
    """
    return run_restriction_mapping(
        db=db,
        locus=request.locus,
        sequence=request.sequence,
        sequence_name=request.sequence_name,
        enzyme_filter=request.enzyme_filter,
    )


@router.get("/search", response_model=RestrictionMapperResponse)
def search_get(
    locus: str = Query(None, description="Gene name, ORF, or CGDID"),
    sequence: str = Query(None, alias="seq", description="Raw DNA sequence"),
    sequence_name: str = Query(None, alias="name", description="Name for the sequence"),
    enzyme_filter: EnzymeFilterType = Query(
        EnzymeFilterType.ALL,
        alias="filter",
        description="Enzyme filter type"
    ),
    db: Session = Depends(get_db),
):
    """
    Run restriction enzyme mapping (GET endpoint).

    Supports the same options as the POST endpoint but via query parameters.
    """
    return run_restriction_mapping(
        db=db,
        locus=locus,
        sequence=sequence,
        sequence_name=sequence_name,
        enzyme_filter=enzyme_filter,
    )


@router.post("/download", response_class=PlainTextResponse)
def download_results(
    request: RestrictionMapperRequest,
    db: Session = Depends(get_db),
):
    """
    Download restriction mapping results as TSV.

    Returns a tab-separated file with enzyme cut sites and fragment sizes.
    """
    result = run_restriction_mapping(
        db=db,
        locus=request.locus,
        sequence=request.sequence,
        sequence_name=request.sequence_name,
        enzyme_filter=request.enzyme_filter,
    )

    if not result.success or not result.result:
        return PlainTextResponse(
            content=f"# Error: {result.error or 'Unknown error'}",
            media_type="text/plain",
        )

    tsv_content = format_results_tsv(result.result)

    return PlainTextResponse(
        content=tsv_content,
        media_type="text/tab-separated-values",
        headers={
            "Content-Disposition": f"attachment; filename=restriction_map_{result.result.seq_name}.tsv"
        }
    )


@router.post("/download/no-cut", response_class=PlainTextResponse)
def download_non_cutting(
    request: RestrictionMapperRequest,
    db: Session = Depends(get_db),
):
    """
    Download non-cutting enzymes list as TSV.

    Returns a tab-separated file with enzymes that do not cut the sequence.
    """
    result = run_restriction_mapping(
        db=db,
        locus=request.locus,
        sequence=request.sequence,
        sequence_name=request.sequence_name,
        enzyme_filter=EnzymeFilterType.ALL,  # Always get all to find non-cutters
    )

    if not result.success or not result.result:
        return PlainTextResponse(
            content=f"# Error: {result.error or 'Unknown error'}",
            media_type="text/plain",
        )

    tsv_content = format_non_cutting_tsv(result.result)

    return PlainTextResponse(
        content=tsv_content,
        media_type="text/tab-separated-values",
        headers={
            "Content-Disposition": f"attachment; filename=non_cutting_{result.result.seq_name}.tsv"
        }
    )
