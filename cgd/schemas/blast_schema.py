"""
BLAST Search Schemas.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field


class BlastProgram(str, Enum):
    """BLAST program types."""
    BLASTN = "blastn"      # Nucleotide query vs nucleotide database
    BLASTP = "blastp"      # Protein query vs protein database
    BLASTX = "blastx"      # Translated nucleotide query vs protein database
    TBLASTN = "tblastn"    # Protein query vs translated nucleotide database
    TBLASTX = "tblastx"    # Translated nucleotide vs translated nucleotide


class BlastDatabase(str, Enum):
    """Available BLAST databases."""
    # C. albicans Assembly 22
    CA22_GENOME = "C_albicans_SC5314_A22_genome"
    CA22_ORFS = "C_albicans_SC5314_A22_ORFs"
    CA22_CODING = "C_albicans_SC5314_A22_coding"
    CA22_PROTEIN = "C_albicans_SC5314_A22_protein"
    # C. albicans Assembly 21
    CA21_GENOME = "C_albicans_SC5314_A21_genome"
    CA21_ORFS = "C_albicans_SC5314_A21_ORFs"
    CA21_CODING = "C_albicans_SC5314_A21_coding"
    CA21_PROTEIN = "C_albicans_SC5314_A21_protein"
    # C. glabrata
    CG_GENOME = "C_glabrata_CBS138_genome"
    CG_ORFS = "C_glabrata_CBS138_ORFs"
    CG_CODING = "C_glabrata_CBS138_coding"
    CG_PROTEIN = "C_glabrata_CBS138_protein"
    # All Candida
    ALL_CANDIDA_GENOME = "all_candida_genome"
    ALL_CANDIDA_ORFS = "all_candida_ORFs"
    ALL_CANDIDA_CODING = "all_candida_coding"
    ALL_CANDIDA_PROTEIN = "all_candida_protein"


class DatabaseType(str, Enum):
    """Database type (nucleotide or protein)."""
    NUCLEOTIDE = "nucleotide"
    PROTEIN = "protein"


class BlastSearchRequest(BaseModel):
    """Request for BLAST search."""
    # Query input
    sequence: Optional[str] = Field(
        None,
        description="Query sequence (FASTA format or raw sequence)"
    )
    locus: Optional[str] = Field(
        None,
        description="Locus name to use as query (will fetch sequence)"
    )
    # BLAST settings
    program: BlastProgram = Field(
        BlastProgram.BLASTN,
        description="BLAST program to use"
    )
    database: BlastDatabase = Field(
        BlastDatabase.CA22_GENOME,
        description="Target database"
    )
    # Advanced parameters
    evalue: float = Field(
        10.0,
        ge=0,
        le=1000,
        description="E-value threshold"
    )
    word_size: Optional[int] = Field(
        None,
        ge=2,
        le=48,
        description="Word size for initial matches"
    )
    max_hits: int = Field(
        50,
        ge=1,
        le=500,
        description="Maximum number of hits to return"
    )
    gap_open: Optional[int] = Field(
        None,
        description="Cost to open a gap"
    )
    gap_extend: Optional[int] = Field(
        None,
        description="Cost to extend a gap"
    )
    # Filter options
    low_complexity_filter: bool = Field(
        True,
        description="Filter low complexity regions"
    )
    # Protein-specific options
    matrix: Optional[str] = Field(
        None,
        description="Scoring matrix (BLOSUM62, BLOSUM45, PAM30, etc.)"
    )
    # Nucleotide-specific options
    strand: Optional[str] = Field(
        None,
        description="Query strand to search (both, plus, minus)"
    )
    # Output format
    output_format: str = Field(
        "html",
        description="Output format: html, text, xml, json"
    )


class BlastHsp(BaseModel):
    """High-scoring Segment Pair (alignment)."""
    hsp_num: int
    bit_score: float
    score: int
    evalue: float
    query_start: int
    query_end: int
    hit_start: int
    hit_end: int
    query_frame: Optional[int] = None
    hit_frame: Optional[int] = None
    identity: int
    positive: Optional[int] = None
    gaps: int
    align_len: int
    query_seq: str
    hit_seq: str
    midline: str
    percent_identity: float
    percent_positive: Optional[float] = None


class BlastHit(BaseModel):
    """Single BLAST hit (subject sequence)."""
    num: int
    id: str
    accession: str
    description: str
    length: int
    hsps: List[BlastHsp]
    # Computed fields
    best_evalue: float
    best_bit_score: float
    total_score: int
    query_cover: float
    # Links
    locus_link: Optional[str] = None


class BlastSearchResult(BaseModel):
    """Complete BLAST search result."""
    # Query info
    query_id: str
    query_length: int
    query_def: Optional[str] = None
    # Database info
    database: str
    database_length: int
    database_sequences: int
    # Search parameters
    program: str
    version: str
    parameters: dict
    # Results
    hits: List[BlastHit]
    # Statistics
    search_time: float
    effective_search_space: Optional[int] = None
    kappa: Optional[float] = None
    lambda_val: Optional[float] = None
    entropy: Optional[float] = None
    # Messages
    warnings: List[str] = []


class BlastSearchResponse(BaseModel):
    """API response for BLAST search."""
    success: bool
    job_id: Optional[str] = None
    result: Optional[BlastSearchResult] = None
    error: Optional[str] = None
    # For async jobs
    status: Optional[str] = None  # pending, running, complete, error


class BlastDatabaseInfo(BaseModel):
    """Information about a BLAST database."""
    name: str
    display_name: str
    description: str
    type: DatabaseType
    organism: str
    assembly: Optional[str] = None
    num_sequences: Optional[int] = None
    total_length: Optional[int] = None


class BlastProgramInfo(BaseModel):
    """Information about a BLAST program."""
    name: str
    display_name: str
    description: str
    query_type: DatabaseType
    database_type: DatabaseType


class BlastConfigResponse(BaseModel):
    """Response with BLAST configuration options."""
    programs: List[BlastProgramInfo]
    databases: List[BlastDatabaseInfo]
    matrices: List[str]
    default_evalue: float = 10.0
    default_max_hits: int = 50
