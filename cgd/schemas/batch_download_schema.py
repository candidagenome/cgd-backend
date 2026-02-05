"""Batch download schema definitions."""
from __future__ import annotations

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class DataType(str, Enum):
    """Downloadable data types."""
    GENOMIC = "genomic"
    GENOMIC_FLANKING = "genomic_flanking"
    CODING = "coding"
    PROTEIN = "protein"
    COORDS = "coords"
    GO = "go"
    PHENOTYPE = "phenotype"
    ORTHOLOG = "ortholog"


class ChromosomalRegion(BaseModel):
    """A chromosomal region specification."""
    chromosome: str = Field(..., description="Chromosome name")
    start: int = Field(..., ge=1, description="Start coordinate (1-based)")
    end: int = Field(..., ge=1, description="End coordinate (1-based)")
    strand: str = Field("W", description="Strand: W (Watson/+) or C (Crick/-)")


class BatchDownloadRequest(BaseModel):
    """Request parameters for batch download."""
    genes: Optional[List[str]] = Field(
        None,
        description="List of gene names, ORF names, feature names, or CGDIDs"
    )
    regions: Optional[List[ChromosomalRegion]] = Field(
        None,
        description="List of chromosomal regions"
    )
    data_types: List[DataType] = Field(
        ...,
        min_length=1,
        description="Data types to download"
    )
    flank_left: int = Field(
        0, ge=0, le=100000,
        description="Upstream flanking bp (for genomic_flanking)"
    )
    flank_right: int = Field(
        0, ge=0, le=100000,
        description="Downstream flanking bp (for genomic_flanking)"
    )
    compress: bool = Field(
        True,
        description="Gzip compress the output"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "genes": ["ACT1", "TUB1", "CDC28"],
                "data_types": ["genomic", "protein"],
                "flank_left": 500,
                "flank_right": 500,
                "compress": True
            }
        }


class FeatureNotFound(BaseModel):
    """Information about a feature that was not found."""
    query: str = Field(..., description="Original query string")
    reason: str = Field("not found", description="Reason for failure")


class DownloadFile(BaseModel):
    """Information about a generated download file."""
    data_type: DataType = Field(..., description="Type of data in the file")
    filename: str = Field(..., description="Suggested filename")
    content_type: str = Field(..., description="MIME content type")
    size: int = Field(..., description="File size in bytes")
    record_count: int = Field(..., description="Number of records in the file")


class BatchDownloadResponse(BaseModel):
    """Response for batch download request (metadata only)."""
    success: bool = Field(..., description="Whether the request was successful")
    files: List[DownloadFile] = Field(
        default_factory=list,
        description="Generated download files"
    )
    total_requested: int = Field(..., description="Total features requested")
    total_found: int = Field(..., description="Total features found")
    not_found: List[FeatureNotFound] = Field(
        default_factory=list,
        description="Features that were not found"
    )
    error: Optional[str] = Field(None, description="Error message if failed")


class ResolvedFeature(BaseModel):
    """A feature resolved from a query."""
    feature_no: int
    feature_name: str
    gene_name: Optional[str] = None
    dbxref_id: str
    feature_type: str
    organism_name: Optional[str] = None
    chromosome: Optional[str] = None
    start: Optional[int] = None
    end: Optional[int] = None
    strand: Optional[str] = None
