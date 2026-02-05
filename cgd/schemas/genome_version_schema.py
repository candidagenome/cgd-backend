"""
Genome Version History Schemas.
"""
from __future__ import annotations

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


class GenomeVersionEntry(BaseModel):
    """Single genome version entry."""
    genome_version: str = Field(..., description="Version string (e.g., s01-m02-r03)")
    strain_name: str = Field(..., description="Display name of the strain")
    is_current: bool = Field(..., description="Whether this is the current version")
    date_created: datetime = Field(..., description="Date version was created")
    description: Optional[str] = Field(None, description="Description of changes")
    is_major_version: bool = Field(
        False,
        description="Whether this is a major version (ends with r01)"
    )


class SeqSourceInfo(BaseModel):
    """Sequence source (strain/assembly) info for dropdown."""
    seq_source: str = Field(..., description="Sequence source identifier")
    organism_abbrev: str = Field(..., description="Organism abbreviation")
    organism_name: str = Field(..., description="Full organism name")
    display_name: str = Field(..., description="Display name for dropdown")


class GenomeVersionConfigResponse(BaseModel):
    """Configuration response for genome version page."""
    seq_sources: List[SeqSourceInfo] = Field(
        ...,
        description="Available sequence sources"
    )
    default_seq_source: str = Field(
        ...,
        description="Default sequence source to display"
    )
    version_format_explanation: str = Field(
        ...,
        description="Explanation of version format"
    )


class GenomeVersionHistoryRequest(BaseModel):
    """Request for genome version history."""
    seq_source: str = Field(..., description="Sequence source to get history for")
    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(20, ge=1, le=100, description="Results per page")


class GenomeVersionHistoryResponse(BaseModel):
    """Response with genome version history."""
    success: bool = Field(True, description="Whether request was successful")
    seq_source: str = Field(..., description="Sequence source queried")
    strain_display_name: str = Field(..., description="Display name for the strain")
    versions: List[GenomeVersionEntry] = Field(
        default_factory=list,
        description="List of genome versions"
    )
    total_count: int = Field(0, description="Total number of versions")
    page: int = Field(1, description="Current page")
    page_size: int = Field(20, description="Items per page")
    total_pages: int = Field(0, description="Total number of pages")
    error: Optional[str] = Field(None, description="Error message if any")
