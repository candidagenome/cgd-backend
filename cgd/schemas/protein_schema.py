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


# --- Allele Name (Protein-specific) ---

class ProteinAlleleNameOut(BaseModel):
    allele_name: str  # Original allele name (e.g., C1_13700W_B)
    protein_allele_name: str  # Protein format (e.g., C1_13700wp_b)
    allele_name_with_refs: typing.Optional[str] = None  # HTML with reference superscripts


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


# --- AlphaFold Info ---

class AlphaFoldInfo(BaseModel):
    uniprot_id: typing.Optional[str] = None
    alphafold_url: typing.Optional[str] = None
    structure_available: bool = False


# --- Experimental Observation ---

class ExperimentalObservationOut(BaseModel):
    observation_type: str
    observation_value: str
    reference: typing.Optional[ReferenceForProtein] = None


# --- Homolog (Protein-specific) ---

class ProteinHomologOut(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    protein_name: typing.Optional[str] = None  # Protein format (e.g., Act1p)
    organism_name: str
    dbxref_id: str
    source: typing.Optional[str] = None  # InParanoid, SGD, etc.
    url: typing.Optional[str] = None


# --- Sequence Detail ---

class SequenceDetailOut(BaseModel):
    protein_length: typing.Optional[int] = None
    protein_sequence: typing.Optional[str] = None  # Raw sequence
    protein_sequence_gcg: typing.Optional[str] = None  # GCG format
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

    # Section 1: Protein Standard Name (e.g., Act1p)
    stanford_name: typing.Optional[str] = None  # gene_name (ACT1)
    protein_standard_name: typing.Optional[str] = None  # protein format (Act1p)
    protein_standard_name_with_refs: typing.Optional[str] = None  # HTML with ref superscripts

    # Section 2: Protein Systematic Name (e.g., C1_13700wp_a)
    systematic_name: str  # feature_name (C1_13700W_A)
    protein_systematic_name: typing.Optional[str] = None  # protein format (C1_13700wp_a)

    # Section 3: Allele Names (protein format only, e.g., C1_13700wp_b)
    allele_names: list[ProteinAlleleNameOut] = []

    # Section 4: Description
    description: typing.Optional[str] = None  # headline
    description_with_refs: typing.Optional[str] = None  # HTML with ref superscripts

    # Section 5: Experimental Observations
    experimental_observations: list[ExperimentalObservationOut] = []

    # Section 6: Structural Information
    structural_info: list[StructuralInfoOut] = []
    protein_info: typing.Optional[ProteinInfoOut] = None  # MW, pI, CAI, etc.
    alphafold_info: typing.Optional[AlphaFoldInfo] = None  # AlphaFold structure

    # Section 7: Conserved Domains
    conserved_domains: list[ConservedDomainOut] = []

    # Section 8: Sequence Detail
    sequence_detail: typing.Optional[SequenceDetailOut] = None

    # Section 9: Homologs
    homologs: list[ProteinHomologOut] = []
    blast_url: typing.Optional[str] = None  # BLAST against Candida sequences

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
