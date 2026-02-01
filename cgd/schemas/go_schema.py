from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class GOTerm(BaseModel):
    goid: str                      # e.g. "GO:0008150"
    display_name: str
    aspect: typing.Optional[str] = None  # P/F/C
    link: typing.Optional[str] = None


class GOEvidence(BaseModel):
    code: typing.Optional[str] = None
    with_from: typing.Optional[str] = None


class CitationLinkForGO(BaseModel):
    """Link associated with a citation (PubMed, Full Text, etc.)"""
    name: str  # Display name: "CGD Paper", "PubMed", "Full Text", etc.
    url: str   # The actual URL
    link_type: str  # "internal" or "external"


class ReferenceForAnnotation(BaseModel):
    """Reference with full citation data for annotation display"""
    reference_no: typing.Optional[int] = None
    pubmed: typing.Optional[int] = None
    dbxref_id: typing.Optional[str] = None
    citation: typing.Optional[str] = None  # Full citation text
    journal_name: typing.Optional[str] = None
    year: typing.Optional[int] = None
    links: list[CitationLinkForGO] = []  # Citation links (CGD Paper, PubMed, Full Text, etc.)


class GOAnnotationOut(BaseModel):
    term: GOTerm
    evidence: GOEvidence = GOEvidence()
    references: list[ReferenceForAnnotation] = []  # Full reference objects with citation
    qualifier: typing.Optional[str] = None  # contributes_to, NOT, etc.
    annotation_type: typing.Optional[str] = None  # manually_curated, computational, high-throughput
    source: typing.Optional[str] = None  # Assigned by (e.g., CGD)
    date_created: typing.Optional[str] = None  # When annotation was assigned


class GODetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    annotations: list[GOAnnotationOut]
    last_reviewed_date: typing.Optional[str] = None  # Max date_last_reviewed for manually curated annotations


class GODetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "annotations": [...] },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, GODetailsForOrganism]


# ============================================================
# GO Term Page Schemas (for /api/go/{goid} endpoint)
# ============================================================

class GoTermOut(BaseModel):
    """GO term basic information"""
    goid: str  # Formatted as GO:XXXXXXX
    go_term: str
    go_definition: typing.Optional[str] = None
    go_aspect: str  # C, F, or P
    aspect_name: str  # Cellular Component, Molecular Function, Biological Process
    synonyms: list[str] = []


class ReferenceEvidence(BaseModel):
    """Reference with evidence codes for GO term page"""
    citation: typing.Optional[str] = None
    pubmed: typing.Optional[int] = None
    dbxref_id: typing.Optional[str] = None  # CGD reference ID for internal links
    evidence_codes: list[str] = []
    qualifiers: list[str] = []
    links: list[CitationLinkForGO] = []  # Citation links (CGD Paper, PubMed, Full Text, etc.)


class AnnotatedGene(BaseModel):
    """Gene annotated to a GO term"""
    locus_name: typing.Optional[str] = None  # gene_name if available
    systematic_name: str  # feature_name
    species: str  # organism name
    references: list[ReferenceEvidence] = []


class SpeciesCount(BaseModel):
    """Count of genes for a species within a qualifier group"""
    species: str  # e.g., "C. albicans"
    count: int


class QualifierGroup(BaseModel):
    """Genes grouped by qualifier (e.g., direct, contributes_to, NOT)"""
    qualifier: typing.Optional[str] = None  # None for direct annotations
    display_name: str  # e.g., "histone H4 acetyltransferase activity" or "contributes_to histone H4 acetyltransferase activity"
    species_counts: list[SpeciesCount] = []
    genes: list[AnnotatedGene] = []


class AnnotationSummary(BaseModel):
    """Annotations grouped by type"""
    annotation_type: str  # manually_curated, high_throughput, computational
    gene_count: int
    qualifier_groups: list[QualifierGroup] = []  # Genes grouped by qualifier


class GoTermResponse(BaseModel):
    """Response for /api/go/{goid} endpoint"""
    term: GoTermOut
    total_genes: int
    annotations: list[AnnotationSummary] = []
