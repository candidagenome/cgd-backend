"""
GO Annotation Summary schemas for request/response models.
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class GoAnnotationSummaryRequest(BaseModel):
    """Request model for GO Annotation Summary."""
    genes: list[str] = Field(..., min_length=1, description="List of gene names/IDs")
    organism_no: Optional[int] = Field(default=None, description="Organism number (optional)")


class AnnotatedGene(BaseModel):
    """A gene annotated to a GO term."""
    feature_no: int
    systematic_name: str  # feature_name
    gene_name: Optional[str] = None
    organism: Optional[str] = None


class GoTermAnnotation(BaseModel):
    """A GO term with annotation statistics."""
    go_no: int
    goid: str  # GO:XXXXXXX format
    go_term: str
    go_aspect: str  # P, F, or C

    # Cluster frequency (genes in input list annotated to this term)
    cluster_count: int  # Number of genes in input annotated to this term
    cluster_total: int  # Total genes in input
    cluster_frequency: float  # (cluster_count / cluster_total) * 100

    # Genome frequency (all genes in genome annotated to this term)
    genome_count: int  # Number of genes in genome annotated to this term
    genome_total: int  # Total annotated genes in genome
    genome_frequency: float  # (genome_count / genome_total) * 100

    # Genes annotated to this term
    genes: list[AnnotatedGene] = []


class GoAnnotationSummaryResult(BaseModel):
    """Result of GO Annotation Summary analysis."""
    # Query summary
    query_genes_submitted: int  # Total genes submitted
    query_genes_found: int  # Genes found in database
    query_genes_not_found: list[str] = []  # Genes not found

    # Genome totals
    genome_annotated_genes: int  # Total annotated genes in genome

    # Organism info
    organism_no: Optional[int] = None
    organism_name: Optional[str] = None

    # Results by ontology
    process_terms: list[GoTermAnnotation] = []
    function_terms: list[GoTermAnnotation] = []
    component_terms: list[GoTermAnnotation] = []


class GoAnnotationSummaryResponse(BaseModel):
    """Response wrapper for GO Annotation Summary."""
    success: bool
    error: Optional[str] = None
    warnings: list[str] = []
    result: Optional[GoAnnotationSummaryResult] = None
