# Auto-generated from schemas_generated.py; do not edit by hand.
# Generated: 2026-01-23T20:07:21
# Contents: 1:1 table schemas for reference/literature/author related tables

from __future__ import annotations

import datetime
import decimal
import typing

try:
    # Pydantic v2
    from pydantic import BaseModel, ConfigDict
    _CFG = ConfigDict(from_attributes=True)
except Exception:  # pragma: no cover
    # Pydantic v1 fallback
    from pydantic import BaseModel
    _CFG = None


class ORMBaseSchema(BaseModel):
    if _CFG is not None:  # Pydantic v2
        model_config = _CFG
    else:  # Pydantic v1
        class Config:
            orm_mode = True

class AbstractSchema(ORMBaseSchema):
    reference_no: int
    abstract: str

class AuthorEditorSchema(ORMBaseSchema):
    author_editor_no: int
    author_no: int
    reference_no: int
    author_order: int
    author_type: str

class AuthorSchema(ORMBaseSchema):
    author_no: int
    author_name: str
    date_created: datetime.datetime
    created_by: str

class Author_Schema(ORMBaseSchema):
    author_no: int
    author_name: str
    date_created: datetime.datetime
    created_by: str

class BookSchema(ORMBaseSchema):
    book_no: int
    title: str
    date_created: datetime.datetime
    created_by: str
    volume_title: typing.Optional[str] = None
    isbn: typing.Optional[str] = None
    total_pages: typing.Optional[int] = None
    publisher: typing.Optional[str] = None
    publisher_location: typing.Optional[str] = None

class Book_Schema(ORMBaseSchema):
    book_no: int
    title: str
    date_created: datetime.datetime
    created_by: str
    volume_title: typing.Optional[str] = None
    isbn: typing.Optional[str] = None
    total_pages: typing.Optional[int] = None
    publisher: typing.Optional[str] = None
    publisher_location: typing.Optional[str] = None

class DbxrefRefSchema(ORMBaseSchema):
    dbxref_ref_no: int
    dbxref_no: int
    reference_no: int

class JournalSchema(ORMBaseSchema):
    journal_no: int
    date_created: datetime.datetime
    created_by: str
    full_name: typing.Optional[str] = None
    abbreviation: typing.Optional[str] = None
    issn: typing.Optional[str] = None
    essn: typing.Optional[str] = None
    publisher: typing.Optional[str] = None

class Journal_Schema(ORMBaseSchema):
    journal_no: int
    date_created: datetime.datetime
    created_by: str
    full_name: typing.Optional[str] = None
    abbreviation: typing.Optional[str] = None
    issn: typing.Optional[str] = None
    essn: typing.Optional[str] = None
    publisher: typing.Optional[str] = None

class RefBadSchema(ORMBaseSchema):
    pubmed: int
    date_created: datetime.datetime
    created_by: str

class RefLinkSchema(ORMBaseSchema):
    ref_link_no: int
    reference_no: int
    tab_name: str
    primary_key: int
    col_name: str
    date_created: datetime.datetime
    created_by: str

class RefPropertySchema(ORMBaseSchema):
    ref_property_no: int
    reference_no: int
    source: str
    property_type: str
    property_value: str
    date_last_reviewed: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class RefProperty_Schema(ORMBaseSchema):
    ref_property_no: int
    reference_no: int
    source: str
    property_type: str
    property_value: str
    date_last_reviewed: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class RefReftypeSchema(ORMBaseSchema):
    ref_reftype_no: int
    reference_no: int
    ref_type_no: int

class RefRelationshipSchema(ORMBaseSchema):
    ref_relationship_no: int
    reference_no: int
    related_ref_no: int
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class RefTempSchema(ORMBaseSchema):
    ref_temp_no: int
    pubmed: int
    citation: str
    date_created: datetime.datetime
    created_by: str
    fulltext_url: typing.Optional[str] = None
    abstract: typing.Optional[str] = None

class RefTypeSchema(ORMBaseSchema):
    ref_type_no: int
    source: str
    ref_type: str
    date_created: datetime.datetime
    created_by: str

class RefType_Schema(ORMBaseSchema):
    ref_type_no: int
    source: str
    ref_type: str
    date_created: datetime.datetime
    created_by: str

class RefUnlinkSchema(ORMBaseSchema):
    ref_unlink_no: int
    pubmed: int
    tab_name: str
    primary_key: int
    date_created: datetime.datetime
    created_by: str

class RefUrlSchema(ORMBaseSchema):
    ref_url_no: int
    reference_no: int
    url_no: int

class ReferenceSchema(ORMBaseSchema):
    reference_no: int
    source: str
    status: str
    pdf_status: str
    dbxref_id: str
    citation: str
    year: int
    date_created: datetime.datetime
    created_by: str
    curation_status: typing.Optional[str] = None
    pubmed: typing.Optional[int] = None
    date_published: typing.Optional[str] = None
    date_revised: typing.Optional[int] = None
    issue: typing.Optional[str] = None
    page: typing.Optional[str] = None
    volume: typing.Optional[str] = None
    title: typing.Optional[str] = None
    journal_no: typing.Optional[int] = None
    book_no: typing.Optional[int] = None

class Reference_Schema(ORMBaseSchema):
    reference_no: int
    source: str
    status: str
    pdf_status: str
    dbxref_id: str
    citation: str
    year: int
    date_created: datetime.datetime
    created_by: str
    curation_status: typing.Optional[str] = None
    pubmed: typing.Optional[int] = None
    date_published: typing.Optional[str] = None
    date_revised: typing.Optional[int] = None
    issue: typing.Optional[str] = None
    page: typing.Optional[str] = None
    volume: typing.Optional[str] = None
    title: typing.Optional[str] = None
    journal_no: typing.Optional[int] = None
    book_no: typing.Optional[int] = None

class RefpropFeatSchema(ORMBaseSchema):
    refprop_feat_no: int
    feature_no: int
    ref_property_no: int
    date_created: datetime.datetime
    created_by: str
