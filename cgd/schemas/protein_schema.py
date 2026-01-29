from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- Reference for Protein Page ---

class ReferenceForProtein(BaseModel):
    reference_no: int
    pubmed: typing.Optional[int] = None
    citation: str
    title: typing.Optional[str] = None
    year: typing.Optional[int] = None


# --- Alias (Protein-specific) ---

class ProteinAliasOut(BaseModel):
    alias_name: str
    alias_type: str


# --- External Link (Protein-specific) ---

class ProteinExternalLinkOut(BaseModel):
    label: str
    url: str
    source: typing.Optional[str] = None
    url_type: typing.Optional[str] = None


# --- Conserved Domain ---

class ConservedDomainOut(BaseModel):
    domain_name: str  # protein_detail_value
    domain_type: str  # protein_detail_type (e.g., "Pfam", "SMART")
    domain_group: str  # protein_detail_group
    start_coord: typing.Optional[int] = None
    stop_coord: typing.Optional[int] = None
    interpro_id: typing.Optional[str] = None
    member_db_id: typing.Optional[str] = None


# --- Structural Information ---

class StructuralInfoOut(BaseModel):
    info_type: str  # protein_detail_type
    info_value: str  # protein_detail_value
    info_unit: typing.Optional[str] = None
    start_coord: typing.Optional[int] = None
    stop_coord: typing.Optional[int] = None


# --- Experimental Observation ---

class ExperimentalObservationOut(BaseModel):
    observation_type: str
    observation_value: str
    reference: typing.Optional[ReferenceForProtein] = None


# --- Homolog (Protein-specific) ---

class ProteinHomologOut(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
    dbxref_id: str
    source: typing.Optional[str] = None  # InParanoid, SGD, etc.
    url: typing.Optional[str] = None


# --- Sequence Detail ---

class SequenceDetailOut(BaseModel):
    protein_length: typing.Optional[int] = None
    protein_sequence: typing.Optional[str] = None
    n_term_seq: typing.Optional[str] = None
    c_term_seq: typing.Optional[str] = None
    cds_length: typing.Optional[int] = None


# --- Protein Info (basic properties) ---

class ProteinInfoOut(BaseModel):
    protein_length: typing.Optional[int] = None
    molecular_weight: typing.Optional[int] = None
    pi: typing.Optional[float] = None  # Isoelectric point
    cai: typing.Optional[float] = None  # Codon Adaptation Index
    codon_bias: typing.Optional[float] = None
    fop_score: typing.Optional[float] = None  # Frequency of optimal codons
    n_term_seq: typing.Optional[str] = None
    c_term_seq: typing.Optional[str] = None
    gravy_score: typing.Optional[float] = None
    aromaticity_score: typing.Optional[float] = None
    amino_acids: typing.Optional[dict[str, int]] = None  # {"ala": 10, "arg": 5, ...}


# --- Full Protein Details for Organism (matching Perl format) ---

class ProteinDetailsForOrganism(BaseModel):
    # Basic identification
    locus_display_name: str  # Stanford Name (gene_name) or Systematic Name (feature_name)
    taxon_id: int

    # Section 1: Stanford Name
    stanford_name: typing.Optional[str] = None  # gene_name

    # Section 2: Systematic Name
    systematic_name: str  # feature_name

    # Section 3: Alias Names
    aliases: list[ProteinAliasOut] = []

    # Section 4: Description
    description: typing.Optional[str] = None  # headline

    # Section 5: Experimental Observations
    experimental_observations: list[ExperimentalObservationOut] = []

    # Section 6: Structural Information
    structural_info: list[StructuralInfoOut] = []
    protein_info: typing.Optional[ProteinInfoOut] = None  # MW, pI, CAI, etc.

    # Section 7: Conserved Domains
    conserved_domains: list[ConservedDomainOut] = []

    # Section 8: Sequence Detail
    sequence_detail: typing.Optional[SequenceDetailOut] = None

    # Section 9: Homologs
    homologs: list[ProteinHomologOut] = []

    # Section 10: External Sequence Database
    external_links: list[ProteinExternalLinkOut] = []

    # Section 11: References Cited on This Page
    cited_references: list[ReferenceForProtein] = []
    literature_guide_url: typing.Optional[str] = None


class ProteinDetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "protein_info": {...} },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, ProteinDetailsForOrganism]
