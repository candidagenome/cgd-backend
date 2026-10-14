"""Schemas for the locus Regulation tab (PathoYeastract regulatory associations)."""
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class RegulationGene(BaseModel):
    """A TF or target gene, resolved to its CGD feature when possible."""
    name: str = Field(description="Name as PathoYeastract reports it (protein or ORF name)")
    feature_name: Optional[str] = Field(None, description="CGD systematic name, if the gene is in CGD")
    gene_name: Optional[str] = Field(None, description="CGD standard gene name")
    display_name: str = Field(description="Gene name, else systematic name, else the PathoYeastract name")


class RegulationReference(BaseModel):
    pubmed: Optional[int] = None
    citation: Optional[str] = Field(None, description="CGD citation when the paper is in CGD, else PathoYeastract's")
    dbxref_id: Optional[str] = Field(None, description="CGD reference ID (links to /reference/<id>)")


class RegulationEvidence(BaseModel):
    """One curated observation supporting a documented TF -> target association."""
    reference: RegulationReference
    evidence_code: Optional[str] = Field(None, description="Direct (DNA binding) or Indirect (expression)")
    experiment: Optional[str] = None
    association_type: Optional[str] = Field(None, description="Positive (activation), Negative (repression), N/A")
    strain: Optional[str] = None
    environmental_group: Optional[str] = None
    environmental_condition: Optional[str] = None
    log2fc: Optional[float] = None


class BindingSite(BaseModel):
    """A consensus match in the promoter, in coordinates relative to the start codon (-1000..-1)."""
    strand: str = Field(description="'+' or '-'")
    start: int
    end: int


class Regulator(BaseModel):
    tf: RegulationGene
    documented: bool = False
    binding_evidence: bool = Field(False, description="At least one Direct (DNA binding) evidence row")
    expression_evidence: bool = Field(False, description="At least one Indirect (expression) evidence row")
    activator: bool = False
    repressor: bool = False
    reference_count: int = 0
    evidence: List[RegulationEvidence] = Field(default_factory=list)
    potential: bool = Field(False, description="TF consensus found in the target's promoter")
    consensus: List[str] = Field(default_factory=list)
    sites: List[BindingSite] = Field(default_factory=list)
    pathoyeastract_url: Optional[str] = Field(None, description="PathoYeastract evidence page for this association")


class Promoter(BaseModel):
    start: int
    end: int
    sequence: str


class RegulationTarget(BaseModel):
    gene: RegulationGene
    documented: bool = False
    binding: bool = False
    expression: bool = False
    activated: bool = False
    repressed: bool = False
    potential: bool = False


class TargetCounts(BaseModel):
    documented: int = 0
    binding: int = 0
    expression: int = 0
    activated: int = 0
    repressed: int = 0
    potential: int = 0
    documented_and_potential: int = 0


class TfTargets(BaseModel):
    """Genome-wide targets of this gene when it is a transcription factor."""
    tf_protein: str
    consensus: List[str] = Field(default_factory=list)
    counts: TargetCounts
    targets: List[RegulationTarget] = Field(default_factory=list)


class RegulationSource(BaseModel):
    name: str = "PathoYeastract"
    url: str
    gene_url: Optional[str] = Field(None, description="This gene's page at PathoYeastract")
    citation_doi: Optional[str] = None
    retrieved: Optional[str] = None


class RegulationForOrganism(BaseModel):
    locus_display_name: str
    feature_name: str
    gene_name: Optional[str] = None
    covered: bool = Field(description="Whether PathoYeastract covers this organism at all")
    has_data: bool = Field(False, description="Whether regulation data is loaded for this gene")
    source: Optional[RegulationSource] = None
    regulators: List[Regulator] = Field(default_factory=list)
    promoter: Optional[Promoter] = None
    tf_targets: Optional[TfTargets] = None


class RegulationDetailsResponse(BaseModel):
    results: dict[str, RegulationForOrganism] = Field(
        default_factory=dict,
        description="Regulation data keyed by organism name",
    )
