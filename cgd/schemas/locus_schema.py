from __future__ import annotations

import datetime
import typing as t
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- Alias ---

class AliasOut(BaseModel):
    alias_name: str
    alias_type: str


# --- External Link ---

class ExternalLinkOut(BaseModel):
    source: str
    url_type: str
    url: str


# --- Allele ---

class AlleleOut(BaseModel):
    feature_no: int
    feature_name: str
    gene_name: t.Optional[str] = None
    dbxref_id: str


class AlleleSubfeatureOut(BaseModel):
    """Subfeature details for an allele (intron, exon, CDS, UTR, etc.)"""
    feature_type: str
    start_coord: int
    stop_coord: int
    relative_start: t.Optional[int] = None
    relative_stop: t.Optional[int] = None
    coord_version: t.Optional[datetime.datetime] = None
    seq_version: t.Optional[datetime.datetime] = None


class AlleleLocationOut(BaseModel):
    """Location information for an allele"""
    feature_no: int
    feature_name: str
    gene_name: t.Optional[str] = None
    chromosome: t.Optional[str] = None
    start_coord: t.Optional[int] = None
    stop_coord: t.Optional[int] = None
    strand: t.Optional[str] = None
    coord_version: t.Optional[datetime.datetime] = None
    seq_version: t.Optional[datetime.datetime] = None
    subfeatures: list[AlleleSubfeatureOut] = []


# --- Candida Ortholog (internal CGD species) ---

class CandidaOrthologOut(BaseModel):
    feature_name: str
    gene_name: t.Optional[str] = None
    organism_name: str
    dbxref_id: str


# --- Other Strain Name ---

class OtherStrainNameOut(BaseModel):
    alias_name: str
    strain_name: t.Optional[str] = None


# --- External Ortholog (non-CGD species) ---

class ExternalOrthologOut(BaseModel):
    dbxref_id: str
    description: t.Optional[str] = None
    source: str
    url: t.Optional[str] = None
    species_name: t.Optional[str] = None  # Display name like "S. cerevisiae"


# --- Basic Feature Info (updated with aliases and links) ---

class FeatureOut(ORMSchema):
    # --- Feature table columns (1:1) ---
    feature_no: int
    organism_no: int
    taxon_id: t.Optional[int] = None
    feature_name: str
    dbxref_id: str
    feature_type: str
    source: str
    date_created: datetime.datetime
    created_by: str

    gene_name: t.Optional[str] = None
    name_description: t.Optional[str] = None
    headline: t.Optional[str] = None

    # Extended info
    aliases: list[AliasOut] = []
    external_links: list[ExternalLinkOut] = []

    # New fields for Summary tab enhancements
    assembly_21_identifier: t.Optional[str] = None  # Assembly 19/21 ORF name
    feature_qualifier: t.Optional[str] = None  # e.g., "Verified", "Uncharacterized"
    alleles: list[AlleleOut] = []
    other_strain_names: list[OtherStrainNameOut] = []  # Systematic names in other strains
    ortholog_cluster_url: t.Optional[str] = None  # URL to CGOB ortholog cluster viewer
    candida_orthologs: list[CandidaOrthologOut] = []
    external_orthologs: list[ExternalOrthologOut] = []
    cug_codons: t.Optional[int] = None  # Number of CUG codons
    allelic_variation: t.Optional[str] = None  # Allelic variation info


class LocusByOrganismResponse(BaseModel):
    """
    {
      "Candida albicans": { ...FeatureOut... },
      "Candida glabrata": { ...FeatureOut... },
      ...
    }
    """
    results: dict[str, FeatureOut]


# --- Sequence Info ---

class SubfeatureOut(BaseModel):
    """Subfeature details (intron, exon, CDS, UTR, etc.)"""
    feature_type: str  # e.g., "Intron", "CDS", "five_prime_UTR"
    start_coord: int  # Chromosomal start
    stop_coord: int  # Chromosomal stop
    relative_start: t.Optional[int] = None  # Relative to gene start
    relative_stop: t.Optional[int] = None  # Relative to gene start
    coord_version: t.Optional[datetime.datetime] = None
    seq_version: t.Optional[datetime.datetime] = None


class SequenceLocationOut(BaseModel):
    chromosome: t.Optional[str] = None
    start_coord: int
    stop_coord: int
    strand: str
    is_current: bool
    coord_version: t.Optional[datetime.datetime] = None
    seq_version: t.Optional[datetime.datetime] = None
    source: t.Optional[str] = None  # Assembly source


class SequenceOut(BaseModel):
    seq_type: str
    seq_length: int
    source: str
    seq_version: datetime.datetime
    is_current: bool
    residues: t.Optional[str] = None  # Can be omitted for large sequences


class SequenceResourceItem(BaseModel):
    """Single item in a sequence resource pulldown menu"""
    label: str
    url: str


class SequenceResources(BaseModel):
    """Resource pulldown menus for sequence tools"""
    retrieve_sequences: list[SequenceResourceItem] = []
    sequence_analysis_tools: list[SequenceResourceItem] = []
    maps_displays: list[SequenceResourceItem] = []


class JBrowseInfo(BaseModel):
    """JBrowse embedded viewer information"""
    embed_url: str  # URL for embedded iframe
    full_url: str  # URL for full JBrowse view
    feature_name: str
    chromosome: str
    start_coord: int
    stop_coord: int


class SequenceDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    locations: list[SequenceLocationOut] = []
    sequences: list[SequenceOut] = []
    subfeatures: list[SubfeatureOut] = []  # Introns, exons, CDS, etc.
    sequence_resources: t.Optional[SequenceResources] = None  # Pulldown menus
    allele_locations: list[AlleleLocationOut] = []  # Location info for alleles
    jbrowse_info: t.Optional[JBrowseInfo] = None  # JBrowse embedded viewer info


class SequenceDetailsResponse(BaseModel):
    results: dict[str, SequenceDetailsForOrganism]


# --- References ---

class ReferenceForLocus(BaseModel):
    reference_no: int
    pubmed: t.Optional[int] = None
    citation: str
    title: t.Optional[str] = None
    year: int


class ReferencesForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    references: list[ReferenceForLocus] = []


class LocusReferencesResponse(BaseModel):
    results: dict[str, ReferencesForOrganism]


# --- Summary Notes ---

class SummaryNoteOut(BaseModel):
    paragraph_no: int
    paragraph_text: str
    paragraph_order: int
    date_edited: datetime.datetime


class SummaryNotesForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    summary_notes: list[SummaryNoteOut] = []


class LocusSummaryNotesResponse(BaseModel):
    results: dict[str, SummaryNotesForOrganism]


# --- Locus History ---

class HistoryEventOut(BaseModel):
    event_type: str
    date: datetime.datetime
    note: t.Optional[str] = None


class LocusHistoryForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    history: list[HistoryEventOut] = []


class LocusHistoryResponse(BaseModel):
    results: dict[str, LocusHistoryForOrganism]
