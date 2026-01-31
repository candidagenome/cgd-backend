from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PhenotypeTerm(BaseModel):
    display_name: str
    link: typing.Optional[str] = None


class CitationLinkForPhenotype(BaseModel):
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
    links: list[CitationLinkForPhenotype] = []  # Citation links (CGD Paper, PubMed, Full Text, etc.)


class ExperimentProperty(BaseModel):
    """Experiment property from expt_property table"""
    property_type: str
    property_value: str
    property_description: typing.Optional[str] = None


class PhenotypeAnnotationOut(BaseModel):
    phenotype: PhenotypeTerm
    qualifier: typing.Optional[str] = None
    experiment_type: typing.Optional[str] = None  # Raw experiment type from DB (e.g., "heterozygous diploid, competitive growth")
    experiment_comment: typing.Optional[str] = None  # Comment about the experiment
    mutant_type: typing.Optional[str] = None  # Raw mutant type from DB (e.g., "null", "overexpression")
    strain: typing.Optional[str] = None  # strain_background from expt_property
    alleles: list[ExperimentProperty] = []  # Allele properties
    chemicals: list[ExperimentProperty] = []  # Chemical_pending or chebi_ontology properties
    details: list[ExperimentProperty] = []  # Condition, Details, Reporter, Numerical_value properties
    references: list[ReferenceForAnnotation] = []


class PhenotypeDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    annotations: list[PhenotypeAnnotationOut]


class PhenotypeDetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "annotations": [...] },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, PhenotypeDetailsForOrganism]
