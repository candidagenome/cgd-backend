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


# --- Candida Ortholog (internal CGD species) ---

class CandidaOrthologOut(BaseModel):
    feature_name: str
    gene_name: t.Optional[str] = None
    organism_name: str
    dbxref_id: str


# --- External Ortholog (non-CGD species) ---

class ExternalOrthologOut(BaseModel):
    dbxref_id: str
    description: t.Optional[str] = None
    source: str
    url: t.Optional[str] = None


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
    other_strain_names: list[str] = []  # Systematic names in other strains
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

class SequenceLocationOut(BaseModel):
    chromosome: t.Optional[str] = None
    start_coord: int
    stop_coord: int
    strand: str
    is_current: bool


class SequenceOut(BaseModel):
    seq_type: str
    seq_length: int
    source: str
    seq_version: datetime.datetime
    is_current: bool
    residues: t.Optional[str] = None  # Can be omitted for large sequences


class SequenceDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    locations: list[SequenceLocationOut] = []
    sequences: list[SequenceOut] = []


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
