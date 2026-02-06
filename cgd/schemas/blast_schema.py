"""
BLAST Search Schemas.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class BlastProgram(str, Enum):
    """BLAST program types."""
    BLASTN = "blastn"      # Nucleotide query vs nucleotide database
    BLASTP = "blastp"      # Protein query vs protein database
    BLASTX = "blastx"      # Translated nucleotide query vs protein database
    TBLASTN = "tblastn"    # Protein query vs translated nucleotide database
    TBLASTX = "tblastx"    # Translated nucleotide vs translated nucleotide


class BlastTask(str, Enum):
    """BLAST task variants for specific search types."""
    # BLASTN tasks
    MEGABLAST = "megablast"              # Highly similar sequences (default for blastn)
    DC_MEGABLAST = "dc-megablast"        # Discontiguous megablast
    BLASTN = "blastn"                    # Traditional blastn
    BLASTN_SHORT = "blastn-short"        # Short query sequences (<50 bp)
    # BLASTP tasks
    BLASTP = "blastp"                    # Traditional blastp (default)
    BLASTP_FAST = "blastp-fast"          # Faster, less sensitive
    BLASTP_SHORT = "blastp-short"        # Short query sequences (<30 aa)


class GeneticCode(int, Enum):
    """NCBI genetic code tables for translation."""
    STANDARD = 1
    VERTEBRATE_MITOCHONDRIAL = 2
    YEAST_MITOCHONDRIAL = 3
    MOLD_MITOCHONDRIAL = 4
    INVERTEBRATE_MITOCHONDRIAL = 5
    CILIATE = 6
    ECHINODERM_MITOCHONDRIAL = 9
    EUPLOTID = 10
    BACTERIAL = 11
    YEAST_NUCLEAR = 12      # CTG clade (alternative yeast nuclear code)
    ASCIDIAN_MITOCHONDRIAL = 13
    FLATWORM_MITOCHONDRIAL = 14
    BLEPHARISMA = 15
    CHLOROPHYCEAN_MITOCHONDRIAL = 16
    TREMATODE_MITOCHONDRIAL = 21
    SCENEDESMUS_MITOCHONDRIAL = 22
    THRAUSTOCHYTRIUM_MITOCHONDRIAL = 23


class NtMatchScore(str, Enum):
    """Nucleotide match/mismatch scoring schemes (reward, penalty)."""
    SCORE_1_N4 = "1,-4"    # Default for megablast
    SCORE_1_N3 = "1,-3"
    SCORE_1_N2 = "1,-2"
    SCORE_2_N3 = "2,-3"    # Default for blastn
    SCORE_4_N5 = "4,-5"
    SCORE_1_N1 = "1,-1"


class DownloadFormat(str, Enum):
    """Available download formats for BLAST results."""
    FASTA = "fasta"         # FASTA sequences of hits
    TAB = "tab"             # Tab-delimited table
    RAW = "raw"             # Raw BLAST output


class BlastDatabase(str, Enum):
    """Available BLAST databases."""
    # C. albicans Assembly 22 (default/current)
    CA22_GENOME = "default_genomic_C_albicans_SC5314_A22"
    CA22_CODING = "default_coding_C_albicans_SC5314_A22"
    CA22_PROTEIN = "default_protein_C_albicans_SC5314_A22"
    # C. albicans Assembly 21
    CA21_GENOME = "genomic_C_albicans_SC5314_A21"
    # C. albicans Assembly 19
    CA19_GENOME = "genomic_C_albicans_SC5314_A19"


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
    database: Optional[BlastDatabase] = Field(
        BlastDatabase.CA22_GENOME,
        description="Target database (use this OR databases, not both)"
    )
    # Multi-database support
    databases: Optional[List[str]] = Field(
        None,
        description="Multiple databases to search (use this OR database, not both)"
    )
    # Task variant
    task: Optional[BlastTask] = Field(
        None,
        description="BLAST task variant (auto-selected if not specified)"
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
    # Genetic code options (for translated searches)
    query_gencode: Optional[int] = Field(
        None,
        ge=1,
        le=23,
        description="Genetic code for query translation (BLASTX, TBLASTX)"
    )
    db_gencode: Optional[int] = Field(
        None,
        ge=1,
        le=23,
        description="Genetic code for database translation (TBLASTN, TBLASTX)"
    )
    # Nucleotide match/mismatch scoring
    reward: Optional[int] = Field(
        None,
        ge=1,
        le=10,
        description="Reward for nucleotide match (BLASTN only)"
    )
    penalty: Optional[int] = Field(
        None,
        le=-1,
        ge=-10,
        description="Penalty for nucleotide mismatch (BLASTN only, must be negative)"
    )
    # Gapped/ungapped alignment
    ungapped: bool = Field(
        False,
        description="Perform ungapped alignment only"
    )
    # Output format
    output_format: str = Field(
        "html",
        description="Output format: html, text, xml, json"
    )
    # Query comment/name
    query_comment: Optional[str] = Field(
        None,
        description="Optional name/comment to identify the query"
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
    jbrowse_url: Optional[str] = None
    # Organism info
    organism_name: Optional[str] = None
    organism_tag: Optional[str] = None
    # Assembly 21 mapping
    orf19_id: Optional[str] = None


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


class BlastOrganismConfig(BaseModel):
    """Configuration for an organism's BLAST databases."""
    tag: str = Field(description="Short organism identifier tag")
    full_name: str = Field(description="Full organism name")
    trans_table: int = Field(
        default=1,
        description="NCBI translation table number"
    )
    seq_sets: List[str] = Field(
        default_factory=lambda: ["genomic", "gene", "coding", "protein"],
        description="Available sequence sets"
    )
    jbrowse_data: Optional[str] = Field(
        None,
        description="JBrowse data path for this organism"
    )
    is_cgd: bool = Field(
        default=True,
        description="Whether this is a CGD internal organism"
    )


class BlastTaskInfo(BaseModel):
    """Information about a BLAST task variant."""
    name: str
    display_name: str
    description: str
    programs: List[str] = Field(description="Programs this task applies to")


class GeneticCodeInfo(BaseModel):
    """Information about a genetic code table."""
    code: int
    name: str
    description: str


class BlastDownloadResponse(BaseModel):
    """Response for downloadable BLAST results."""
    format: DownloadFormat
    content: str
    filename: str
    content_type: str


class BlastMultiSearchRequest(BaseModel):
    """Request for multi-database BLAST search."""
    sequence: Optional[str] = Field(
        None,
        description="Query sequence (FASTA format or raw sequence)"
    )
    locus: Optional[str] = Field(
        None,
        description="Locus name to use as query"
    )
    program: BlastProgram = Field(
        BlastProgram.BLASTN,
        description="BLAST program to use"
    )
    databases: List[str] = Field(
        description="List of database names to search"
    )
    task: Optional[BlastTask] = Field(
        None,
        description="BLAST task variant"
    )
    evalue: float = Field(10.0, ge=0, le=1000)
    max_hits: int = Field(50, ge=1, le=500)
    word_size: Optional[int] = Field(None, ge=2, le=48)
    gap_open: Optional[int] = None
    gap_extend: Optional[int] = None
    low_complexity_filter: bool = True
    matrix: Optional[str] = None
    strand: Optional[str] = None
    query_gencode: Optional[int] = Field(None, ge=1, le=23)
    db_gencode: Optional[int] = Field(None, ge=1, le=23)
    reward: Optional[int] = Field(None, ge=1, le=10)
    penalty: Optional[int] = Field(None, le=-1, ge=-10)
    ungapped: bool = False
    query_comment: Optional[str] = Field(
        None,
        description="Optional name/comment to identify the query"
    )
