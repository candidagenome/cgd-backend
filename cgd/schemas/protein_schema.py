from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- Reference for Protein Page ---

class CitationLinkForProtein(BaseModel):
    """Link associated with a citation (PubMed, Full Text, etc.)"""
    name: str  # Display name: "CGD Paper", "PubMed", etc.
    url: str   # The actual URL
    link_type: str  # "internal" or "external"


class ReferenceForProtein(BaseModel):
    reference_no: int
    pubmed: typing.Optional[int] = None
    dbxref_id: typing.Optional[str] = None
    citation: str
    title: typing.Optional[str] = None
    year: typing.Optional[int] = None
    links: list[CitationLinkForProtein] = []  # Citation links


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

    # Section 4b: Name Description
    name_description: typing.Optional[str] = None
    name_description_with_refs: typing.Optional[str] = None  # HTML with ref superscripts

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

    # PBrowse URL for domain visualization
    pbrowse_url: typing.Optional[str] = None


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


# =====================================================
# Protein Physico-chemical Properties Page Schemas
# =====================================================

class AminoAcidComposition(BaseModel):
    """Amino acid composition with counts and percentages."""
    amino_acid: str  # e.g., "A (Ala)"
    count: int
    percentage: float  # e.g., 5.2


class BulkPropertyItem(BaseModel):
    """A single bulk property with label and value."""
    label: str  # e.g., "Isoelectric Point (pI)"
    value: str  # Formatted value with unit if applicable
    note: typing.Optional[str] = None  # e.g., "(stable)" or "(unstable)"


class ExtinctionCoefficient(BaseModel):
    """Extinction coefficient value."""
    condition: str  # e.g., "Assuming all Cys residues exist as cysteine"
    value: float
    unit: str  # e.g., "M⁻¹ cm⁻¹"


class AtomicCompositionItem(BaseModel):
    """Atomic composition entry."""
    atom: str  # e.g., "Carbon"
    count: int


class CodonUsageItem(BaseModel):
    """Codon usage statistics."""
    label: str  # e.g., "Codon Bias Index"
    value: float


class ProteinPropertiesForOrganism(BaseModel):
    """Full protein properties for a single organism."""
    # Identification
    locus_display_name: str
    protein_name: str  # e.g., "Act1p/C1_13700wp_a"
    taxon_id: int
    organism_name: str

    # Section 1: Amino Acid Composition
    amino_acid_composition: list[AminoAcidComposition] = []
    protein_length: int = 0

    # Section 2: Bulk Protein Properties
    bulk_properties: list[BulkPropertyItem] = []

    # Section 3: Extinction Coefficients
    extinction_coefficients: list[ExtinctionCoefficient] = []

    # Section 4: Codon Usage Statistics
    codon_usage: list[CodonUsageItem] = []

    # Section 5: Atomic Composition
    atomic_composition: list[AtomicCompositionItem] = []

    # Has ambiguous residues (if true, properties couldn't be calculated)
    has_ambiguous_residues: bool = False

    # Link back to protein page
    protein_page_url: typing.Optional[str] = None


class ProteinPropertiesResponse(BaseModel):
    """Response for protein properties endpoint."""
    results: dict[str, ProteinPropertiesForOrganism]


# =====================================================
# Protein Domain/Motif Page Schemas
# =====================================================

class DomainHit(BaseModel):
    """A single domain hit with coordinates."""
    start_coord: typing.Optional[int] = None
    stop_coord: typing.Optional[int] = None
    evalue: typing.Optional[str] = None


class DomainEntry(BaseModel):
    """A domain entry with member database info."""
    member_db: str  # e.g., "Pfam", "SMART"
    member_id: str  # e.g., "PF00022"
    description: str  # Domain description
    hits: list[DomainHit] = []  # Can have multiple hits per domain
    member_url: typing.Optional[str] = None  # URL to member database


class InterProDomain(BaseModel):
    """An InterPro domain grouping member domains."""
    interpro_id: typing.Optional[str] = None  # e.g., "IPR001023" or None if unintegrated
    interpro_description: typing.Optional[str] = None
    interpro_url: typing.Optional[str] = None
    member_domains: list[DomainEntry] = []


class TransmembraneDomain(BaseModel):
    """Transmembrane domain prediction (TMHMM)."""
    type: str  # e.g., "transmembrane helix", "inside", "outside"
    start_coord: int
    stop_coord: int


class SignalPeptide(BaseModel):
    """Signal peptide prediction (SignalP)."""
    type: str  # e.g., "signal peptide"
    start_coord: int
    stop_coord: typing.Optional[int] = None


class DomainExternalLink(BaseModel):
    """External link to domain search databases."""
    name: str  # e.g., "NCBI DART", "SMART", "Pfam"
    url: str
    description: typing.Optional[str] = None


class ProteinDomainForOrganism(BaseModel):
    """Full domain/motif information for a single organism."""
    # Identification
    locus_display_name: str
    protein_name: str  # e.g., "Act1p/C1_13700wp_a"
    taxon_id: int
    organism_name: str
    protein_length: typing.Optional[int] = None

    # Section 1: Conserved Domains (grouped by InterPro)
    interpro_domains: list[InterProDomain] = []

    # Section 2: Transmembrane Domains (TMHMM)
    transmembrane_domains: list[TransmembraneDomain] = []

    # Section 3: Signal Peptides (SignalP)
    signal_peptides: list[SignalPeptide] = []

    # Section 4: External Links
    external_links: list[DomainExternalLink] = []

    # PBrowse URL for visualization
    pbrowse_url: typing.Optional[str] = None

    # Link back to protein page
    protein_page_url: typing.Optional[str] = None

    # Last update date (if available)
    last_update: typing.Optional[str] = None


class ProteinDomainResponse(BaseModel):
    """Response for protein domain endpoint."""
    results: dict[str, ProteinDomainForOrganism]
