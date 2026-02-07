"""
Pattern Match Search API Router.
"""
from typing import List
from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.patmatch_schema import (
    PatternType,
    PatmatchSearchRequest,
    PatmatchSearchResponse,
    PatmatchConfigResponse,
    DatasetInfo,
)
from cgd.api.services.patmatch_service import (
    get_patmatch_config,
    get_datasets_for_type,
    run_patmatch_search,
    format_results_tsv,
)

router = APIRouter(prefix="/api/patmatch", tags=["pattern-match"])


@router.get("/config", response_model=PatmatchConfigResponse)
def get_config():
    """
    Get pattern match configuration options.

    Returns available datasets, maximum pattern length, and other limits.
    """
    return get_patmatch_config()


@router.get("/datasets", response_model=List[DatasetInfo])
def list_datasets(
    pattern_type: PatternType = Query(
        None,
        description="Filter datasets by pattern type (dna or protein)"
    ),
):
    """
    List available sequence datasets.

    Optionally filter by pattern type (DNA or protein).
    """
    if pattern_type:
        return get_datasets_for_type(pattern_type)
    config = get_patmatch_config()
    return config.datasets


@router.post("/search", response_model=PatmatchSearchResponse)
def search(
    request: PatmatchSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Run a pattern match search.

    Search for a DNA or protein pattern/motif across selected sequence datasets.

    **Pattern Input**:
    - DNA patterns: A, C, G, T, and IUPAC ambiguity codes (R, Y, S, W, K, M, B, D, H, V, N)
    - Protein patterns: Standard amino acids and ambiguity codes (B, Z, X)

    **Datasets**:
    - DNA: chromosomes, orf_genomic, orf_coding, intergenic, noncoding
    - Protein: orf_protein

    **Options**:
    - `strand`: Which strand(s) to search (both, watson, crick) - DNA only
    - `max_mismatches`: Allow 0-3 mismatches
    - `max_insertions`: Allow 0-3 insertions
    - `max_deletions`: Allow 0-3 deletions
    - `max_results`: Limit number of results (default 100)

    Returns matching sequences with coordinates and context.
    """
    return run_patmatch_search(db, request)


@router.get("/search", response_model=PatmatchSearchResponse)
def search_get(
    pattern: str = Query(..., description="Pattern to search for"),
    pattern_type: PatternType = Query(PatternType.DNA, alias="type"),
    dataset: str = Query(..., alias="ds", description="Dataset name from /api/patmatch/config"),
    strand: str = Query("both"),
    max_mismatches: int = Query(0, alias="mm", ge=0, le=3),
    max_insertions: int = Query(0, alias="ins", ge=0, le=3),
    max_deletions: int = Query(0, alias="del", ge=0, le=3),
    max_results: int = Query(100, alias="max", ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    Run a pattern match search (GET endpoint for simple queries).

    Supports the same options as the POST endpoint but via query parameters.
    """
    from cgd.schemas.patmatch_schema import StrandOption

    # Convert strand string to enum
    try:
        strand_enum = StrandOption(strand)
    except ValueError:
        strand_enum = StrandOption.BOTH

    request = PatmatchSearchRequest(
        pattern=pattern,
        pattern_type=pattern_type,
        dataset=dataset,
        strand=strand_enum,
        max_mismatches=max_mismatches,
        max_insertions=max_insertions,
        max_deletions=max_deletions,
        max_results=max_results,
    )
    return run_patmatch_search(db, request)


@router.post("/download", response_class=PlainTextResponse)
def download_results(
    request: PatmatchSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Download pattern match results as TSV.

    Returns a tab-separated file with all matching sequences.
    """
    result = run_patmatch_search(db, request)

    if not result.success or not result.result:
        return PlainTextResponse(
            content=f"# Error: {result.error or 'Unknown error'}",
            media_type="text/plain",
        )

    tsv_content = format_results_tsv(result.result)
    filename = f"patmatch_{result.result.pattern[:20]}_{result.result.dataset}.tsv"
    # Sanitize filename
    filename = "".join(c if c.isalnum() or c in "._-" else "_" for c in filename)

    return PlainTextResponse(
        content=tsv_content,
        media_type="text/tab-separated-values",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )
