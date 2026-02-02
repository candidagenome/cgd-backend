"""
BLAST Search API Router.
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from cgd.db.deps import get_db
from cgd.schemas.blast_schema import (
    BlastProgram,
    BlastDatabase,
    BlastSearchRequest,
    BlastSearchResponse,
    BlastConfigResponse,
    BlastDatabaseInfo,
    BlastProgramInfo,
)
from cgd.api.services.blast_service import (
    get_blast_config,
    get_compatible_databases,
    get_compatible_programs,
    run_blast_search,
    format_blast_results_text,
)

router = APIRouter(prefix="/api/blast", tags=["blast"])


@router.get("/config", response_model=BlastConfigResponse)
def get_config():
    """
    Get BLAST configuration options.

    Returns available programs, databases, matrices, and default values.
    """
    return get_blast_config()


@router.get("/programs", response_model=List[BlastProgramInfo])
def list_programs():
    """List all available BLAST programs."""
    config = get_blast_config()
    return config.programs


@router.get("/databases", response_model=List[BlastDatabaseInfo])
def list_databases(
    program: Optional[BlastProgram] = Query(
        None,
        description="Filter databases compatible with this program"
    ),
):
    """
    List available BLAST databases.

    Optionally filter by compatible program.
    """
    if program:
        return get_compatible_databases(program)
    config = get_blast_config()
    return config.databases


@router.get("/databases/{program}", response_model=List[BlastDatabaseInfo])
def get_databases_for_program(program: BlastProgram):
    """Get databases compatible with a specific BLAST program."""
    databases = get_compatible_databases(program)
    if not databases:
        raise HTTPException(status_code=404, detail=f"Unknown program: {program}")
    return databases


@router.get("/programs/{database}", response_model=List[BlastProgramInfo])
def get_programs_for_database(database: BlastDatabase):
    """Get BLAST programs compatible with a specific database."""
    programs = get_compatible_programs(database)
    if not programs:
        raise HTTPException(status_code=404, detail=f"Unknown database: {database}")
    return programs


@router.post("/search", response_model=BlastSearchResponse)
def search(
    request: BlastSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Run a BLAST search.

    Submit a query sequence or locus name to search against a BLAST database.

    **Query Input** (provide one):
    - `sequence`: Raw sequence or FASTA format
    - `locus`: Locus name to fetch sequence from database

    **Required Parameters**:
    - `program`: BLAST program (blastn, blastp, blastx, tblastn, tblastx)
    - `database`: Target database

    **Optional Parameters**:
    - `evalue`: E-value threshold (default: 10)
    - `max_hits`: Maximum hits to return (default: 50)
    - `word_size`: Word size for initial matches
    - `gap_open`: Gap opening penalty
    - `gap_extend`: Gap extension penalty
    - `low_complexity_filter`: Filter low complexity regions (default: true)
    - `matrix`: Scoring matrix for protein BLAST
    - `strand`: Query strand for nucleotide BLAST

    Returns search results with hits and alignments.
    """
    response = run_blast_search(db, request)
    return response


@router.get("/search", response_model=BlastSearchResponse)
def search_get(
    sequence: Optional[str] = Query(None, alias="seq", description="Query sequence"),
    locus: Optional[str] = Query(None, description="Locus name"),
    program: BlastProgram = Query(BlastProgram.BLASTN, description="BLAST program"),
    database: BlastDatabase = Query(BlastDatabase.CA22_GENOME, alias="db", description="Target database"),
    evalue: float = Query(10.0, description="E-value threshold"),
    max_hits: int = Query(50, alias="hits", description="Maximum hits"),
    word_size: Optional[int] = Query(None, alias="word", description="Word size"),
    low_complexity_filter: bool = Query(True, alias="filter", description="Filter low complexity"),
    matrix: Optional[str] = Query(None, description="Scoring matrix"),
    strand: Optional[str] = Query(None, description="Query strand"),
    db: Session = Depends(get_db),
):
    """
    Run a BLAST search (GET endpoint for simple queries).

    Supports the same options as the POST endpoint but via query parameters.
    """
    request = BlastSearchRequest(
        sequence=sequence,
        locus=locus,
        program=program,
        database=database,
        evalue=evalue,
        max_hits=max_hits,
        word_size=word_size,
        low_complexity_filter=low_complexity_filter,
        matrix=matrix,
        strand=strand,
    )
    return run_blast_search(db, request)


@router.post("/search/text", response_class=PlainTextResponse)
def search_text(
    request: BlastSearchRequest,
    db: Session = Depends(get_db),
):
    """
    Run a BLAST search and return results as plain text.

    Same parameters as /search but returns text format instead of JSON.
    """
    response = run_blast_search(db, request)

    if not response.success:
        return PlainTextResponse(
            content=f"Error: {response.error}",
            status_code=400,
        )

    if not response.result:
        return PlainTextResponse(content="No results found")

    text_output = format_blast_results_text(response.result)
    return PlainTextResponse(content=text_output)
