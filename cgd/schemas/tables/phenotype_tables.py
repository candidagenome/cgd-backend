# Auto-generated from schemas_generated.py; do not edit by hand.
# Generated: 2026-01-23T20:07:21
# Contents: 1:1 table schemas for phenotype related tables

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

class InteractPhenoSchema(ORMBaseSchema):
    interact_pheno_no: int
    interaction_no: int
    phenotype_no: int

class PhenoAnnotationSchema(ORMBaseSchema):
    pheno_annotation_no: int
    feature_no: int
    phenotype_no: int
    date_created: datetime.datetime
    created_by: str
    experiment_no: typing.Optional[int] = None

class PhenotypeSchema(ORMBaseSchema):
    phenotype_no: int
    source: str
    experiment_type: str
    mutant_type: str
    observable: str
    date_created: datetime.datetime
    created_by: str
    qualifier: typing.Optional[str] = None

class Phenotype_Schema(ORMBaseSchema):
    phenotype_no: int
    source: str
    experiment_type: str
    mutant_type: str
    observable: str
    date_created: datetime.datetime
    created_by: str
    qualifier: typing.Optional[str] = None
