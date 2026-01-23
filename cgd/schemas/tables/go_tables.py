# Auto-generated from schemas_generated.py; do not edit by hand.
# Generated: 2026-01-23T20:07:21
# Contents: 1:1 table schemas for GO related tables

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

class GoAnnotationSchema(ORMBaseSchema):
    go_annotation_no: int
    go_no: int
    feature_no: int
    go_evidence: str
    annotation_type: str
    source: str
    date_last_reviewed: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class GoAnnotation_Schema(ORMBaseSchema):
    go_annotation_no: int
    go_no: int
    feature_no: int
    go_evidence: str
    annotation_type: str
    source: str
    date_last_reviewed: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class GoGosynSchema(ORMBaseSchema):
    go_gosyn_no: int
    go_no: int
    go_synonym_no: int

class GoPathSchema(ORMBaseSchema):
    go_path_no: int
    ancestor_go_no: int
    child_go_no: int
    generation: int
    ancestor_path: str
    relationship_type: typing.Optional[str] = None

class GoQualifierSchema(ORMBaseSchema):
    go_qualifier_no: int
    go_ref_no: int
    qualifier: str

class GoRefSchema(ORMBaseSchema):
    go_ref_no: int
    reference_no: int
    go_annotation_no: int
    has_qualifier: str
    has_supporting_evidence: str
    date_created: datetime.datetime
    created_by: str

class GoRef_Schema(ORMBaseSchema):
    go_ref_no: int
    reference_no: int
    go_annotation_no: int
    has_qualifier: str
    has_supporting_evidence: str
    date_created: datetime.datetime
    created_by: str

class GoSchema(ORMBaseSchema):
    go_no: int
    goid: int
    go_term: str
    go_aspect: str
    date_created: datetime.datetime
    created_by: str
    go_definition: typing.Optional[str] = None

class GoSetSchema(ORMBaseSchema):
    go_set_no: int
    go_no: int
    go_set_name: str
    date_created: datetime.datetime
    created_by: str

class GoSynonymSchema(ORMBaseSchema):
    go_synonym_no: int
    go_synonym: str
    date_created: datetime.datetime
    created_by: str

class GoSynonym_Schema(ORMBaseSchema):
    go_synonym_no: int
    go_synonym: str
    date_created: datetime.datetime
    created_by: str

class Go_Schema(ORMBaseSchema):
    go_no: int
    goid: int
    go_term: str
    go_aspect: str
    date_created: datetime.datetime
    created_by: str
    go_definition: typing.Optional[str] = None

class GorefDbxrefSchema(ORMBaseSchema):
    goref_dbxref_no: int
    go_ref_no: int
    dbxref_no: int
    support_type: str
