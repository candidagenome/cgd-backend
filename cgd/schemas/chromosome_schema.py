from __future__ import annotations

import datetime
import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- Basic Chromosome Info ---

class AliasOut(BaseModel):
    alias_name: str
    alias_type: str  # Uniform, Non-uniform, Protein name


class HistorySummary(BaseModel):
    sequence_updates: int
    sequence_last_update: typing.Optional[str] = None
    annotation_updates: int
    annotation_last_update: typing.Optional[str] = None
    curatorial_notes: int


class ChromosomeOut(BaseModel):
    feature_no: int
    feature_name: str
    feature_type: str
    dbxref_id: str
    organism_name: str
    taxon_id: int
    headline: typing.Optional[str] = None
    start_coord: typing.Optional[int] = None
    stop_coord: typing.Optional[int] = None
    seq_source: typing.Optional[str] = None
    aliases: list[AliasOut] = []
    history_summary: typing.Optional[HistorySummary] = None


class ChromosomeResponse(BaseModel):
    result: ChromosomeOut


# --- Chromosome History ---

class SequenceChangeOut(BaseModel):
    date: str
    affected_features: list[str]
    start_coord: int
    stop_coord: int
    change_type: str
    old_seq: typing.Optional[str] = None
    new_seq: typing.Optional[str] = None
    note: typing.Optional[str] = None


class AnnotationChangeOut(BaseModel):
    date: str
    affected_features: list[str]
    note: typing.Optional[str] = None


class CuratorialNoteOut(BaseModel):
    date: str
    note: str


class ChromosomeHistoryOut(BaseModel):
    reference_no: int
    feature_name: str
    sequence_changes: list[SequenceChangeOut] = []
    annotation_changes: list[AnnotationChangeOut] = []
    curatorial_notes: list[CuratorialNoteOut] = []


class ChromosomeHistoryResponse(BaseModel):
    result: ChromosomeHistoryOut


# --- Chromosome References ---

class ReferenceForChromosome(BaseModel):
    reference_no: int
    pubmed: typing.Optional[int] = None
    citation: str
    title: typing.Optional[str] = None
    year: int


class ChromosomeReferencesResponse(BaseModel):
    reference_no: int
    feature_name: str
    references: list[ReferenceForChromosome]


# --- Chromosome Summary Notes ---

class SummaryNoteOut(BaseModel):
    paragraph_no: int
    paragraph_text: str
    paragraph_order: int
    date_edited: datetime.datetime


class ChromosomeSummaryNotesResponse(BaseModel):
    reference_no: int
    feature_name: str
    summary_notes: list[SummaryNoteOut]
