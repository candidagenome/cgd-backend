"""Sequence Tools (seqTools) schema definitions."""
from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class SeqType(str, Enum):
    """Sequence type for raw sequence input."""
    DNA = "dna"
    PROTEIN = "protein"


class InputType(str, Enum):
    """Type of input provided to seqTools."""
    GENE = "gene"
    COORDINATES = "coordinates"
    SEQUENCE = "sequence"


class ToolLink(BaseModel):
    """A single tool link with metadata."""
    name: str = Field(..., description="Display name for the tool")
    url: str = Field(..., description="URL to the tool")
    description: Optional[str] = Field(None, description="Brief description of what the tool does")
    external: bool = Field(False, description="Whether the link opens an external site")


class ToolCategory(BaseModel):
    """A category grouping related tools."""
    name: str = Field(..., description="Category name (e.g., 'Biology/Literature')")
    tools: list[ToolLink] = Field(default_factory=list, description="Tools in this category")


class FeatureInfo(BaseModel):
    """Information about a resolved feature/gene."""
    feature_name: str
    gene_name: Optional[str] = None
    dbxref_id: str
    organism: str
    chromosome: Optional[str] = None
    start: Optional[int] = None
    end: Optional[int] = None
    strand: Optional[str] = None


class SeqToolsRequest(BaseModel):
    """Request for resolving gene/coordinates/sequence and getting tool links."""
    # Gene query
    query: Optional[str] = Field(
        None,
        description="Gene name, ORF name, feature name, or CGDID"
    )
    seq_source: Optional[str] = Field(
        None,
        description="Assembly/sequence source to use"
    )

    # Coordinate query
    chromosome: Optional[str] = Field(None, description="Chromosome name")
    start: Optional[int] = Field(None, description="Start coordinate (1-based)")
    end: Optional[int] = Field(None, description="End coordinate (1-based)")

    # Raw sequence input
    sequence: Optional[str] = Field(None, description="Raw DNA or protein sequence")
    seq_type: Optional[SeqType] = Field(None, description="Type of raw sequence")

    # Common options
    flank_left: int = Field(0, ge=0, le=10000, description="Left flanking bp")
    flank_right: int = Field(0, ge=0, le=10000, description="Right flanking bp")
    reverse_complement: bool = Field(False, description="Return reverse complement")


class SeqToolsResponse(BaseModel):
    """Response containing resolved feature info and available tool links."""
    input_type: InputType = Field(..., description="Type of input that was resolved")
    feature: Optional[FeatureInfo] = Field(
        None,
        description="Resolved feature info (for gene queries)"
    )
    categories: list[ToolCategory] = Field(
        default_factory=list,
        description="Tool categories with links"
    )
    sequence_length: Optional[int] = Field(
        None,
        description="Length of the sequence (for raw sequence input)"
    )


class AssemblyInfo(BaseModel):
    """Information about an available assembly/sequence source."""
    name: str = Field(..., description="Assembly identifier")
    display_name: str = Field(..., description="Human-readable name")
    organism: str = Field(..., description="Organism name")
    is_default: bool = Field(False, description="Whether this is the default assembly")


class AssembliesResponse(BaseModel):
    """Response listing available assemblies."""
    assemblies: list[AssemblyInfo] = Field(default_factory=list)


class ChromosomeInfo(BaseModel):
    """Information about a chromosome."""
    name: str = Field(..., description="Chromosome identifier")
    display_name: str = Field(..., description="Human-readable name")
    length: Optional[int] = Field(None, description="Chromosome length in bp")


class ChromosomesResponse(BaseModel):
    """Response listing available chromosomes."""
    chromosomes: list[ChromosomeInfo] = Field(default_factory=list)
