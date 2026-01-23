# Auto-generated from schemas_generated.py; do not edit by hand.
# Generated: 2026-01-23T20:07:21
# Contents: 1:1 table schemas for locus/feature/sequence/protein related tables

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

class CollFeatSchema(ORMBaseSchema):
    coll_feat_no: int
    colleague_no: int
    feature_no: int

class DbxrefFeatSchema(ORMBaseSchema):
    dbxref_feat_no: int
    dbxref_no: int
    feature_no: int

class FeatAliasSchema(ORMBaseSchema):
    feat_alias_no: int
    feature_no: int
    alias_no: int

class FeatHomologySchema(ORMBaseSchema):
    feat_homology_no: int
    feature_no: int
    homology_group_no: int
    date_created: datetime.datetime
    created_by: str

class FeatInteractSchema(ORMBaseSchema):
    feat_interact_no: int
    feature_no: int
    interaction_no: int
    action: str
    date_created: datetime.datetime
    created_by: str

class FeatLocationSchema(ORMBaseSchema):
    feat_location_no: int
    feature_no: int
    root_seq_no: int
    coord_version: datetime.datetime
    start_coord: int
    stop_coord: int
    strand: str
    is_loc_current: str
    date_created: datetime.datetime
    created_by: str
    seq_no: typing.Optional[int] = None

class FeatParaSchema(ORMBaseSchema):
    feat_para_no: int
    feature_no: int
    paragraph_no: int
    paragraph_order: int

class FeatPropertySchema(ORMBaseSchema):
    feat_property_no: int
    feature_no: int
    source: str
    property_type: str
    property_value: str
    date_created: datetime.datetime
    created_by: str

class FeatRelationshipSchema(ORMBaseSchema):
    feat_relationship_no: int
    parent_feature_no: int
    child_feature_no: int
    relationship_type: str
    date_created: datetime.datetime
    created_by: str
    rank: typing.Optional[int] = None

class FeatUrlSchema(ORMBaseSchema):
    feat_url_no: int
    feature_no: int
    url_no: int

class FeatureSchema(ORMBaseSchema):
    feature_no: int
    organism_no: int
    feature_name: str
    dbxref_id: str
    feature_type: str
    source: str
    date_created: datetime.datetime
    created_by: str
    gene_name: typing.Optional[str] = None
    name_description: typing.Optional[str] = None
    headline: typing.Optional[str] = None

class Feature_Schema(ORMBaseSchema):
    feature_no: int
    organism_no: int
    feature_name: str
    dbxref_id: str
    feature_type: str
    source: str
    date_created: datetime.datetime
    created_by: str
    gene_name: typing.Optional[str] = None
    name_description: typing.Optional[str] = None
    headline: typing.Optional[str] = None

class GenomeVersionSchema(ORMBaseSchema):
    genome_version_no: int
    genome_version: str
    organism_no: int
    is_ver_current: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class GenomeVersion_Schema(ORMBaseSchema):
    genome_version_no: int
    genome_version: str
    organism_no: int
    is_ver_current: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class PdbAlignmentSequenceSchema(ORMBaseSchema):
    pdb_alignment_sequence_no: int
    pdb_alignment_no: int
    date_created: datetime.datetime
    created_by: str
    query_seq: str
    target_seq: str
    alignment_symbol: str

class PdbSequenceSchema(ORMBaseSchema):
    pdb_sequence_no: int
    sequence_name: str
    source: str
    sequence_length: int
    date_created: datetime.datetime
    created_by: str
    taxon_id: typing.Optional[int] = None
    note: typing.Optional[str] = None

class PdbSequence_Schema(ORMBaseSchema):
    pdb_sequence_no: int
    sequence_name: str
    source: str
    sequence_length: int
    date_created: datetime.datetime
    created_by: str
    taxon_id: typing.Optional[int] = None
    note: typing.Optional[str] = None

class ProteinDetailSchema(ORMBaseSchema):
    protein_detail_no: int
    protein_info_no: int
    protein_detail_group: str
    protein_detail_type: str
    protein_detail_value: str
    date_created: datetime.datetime
    created_by: str
    protein_detail_unit: typing.Optional[str] = None
    start_coord: typing.Optional[int] = None
    stop_coord: typing.Optional[int] = None
    interpro_dbxref_id: typing.Optional[str] = None
    member_dbxref_id: typing.Optional[str] = None

class ProteinInfoSchema(ORMBaseSchema):
    protein_info_no: int
    feature_no: int
    date_created: datetime.datetime
    created_by: str
    molecular_weight: typing.Optional[int] = None
    pi: typing.Optional[decimal.Decimal] = None
    cai: typing.Optional[decimal.Decimal] = None
    protein_length: typing.Optional[int] = None
    n_term_seq: typing.Optional[str] = None
    c_term_seq: typing.Optional[str] = None
    codon_bias: typing.Optional[decimal.Decimal] = None
    fop_score: typing.Optional[decimal.Decimal] = None
    gravy_score: typing.Optional[decimal.Decimal] = None
    aromaticity_score: typing.Optional[decimal.Decimal] = None
    ala: typing.Optional[int] = None
    arg: typing.Optional[int] = None
    asn: typing.Optional[int] = None
    asp: typing.Optional[int] = None
    cys: typing.Optional[int] = None
    gln: typing.Optional[int] = None
    glu: typing.Optional[int] = None
    gly: typing.Optional[int] = None
    his: typing.Optional[int] = None
    ile: typing.Optional[int] = None
    leu: typing.Optional[int] = None
    lys: typing.Optional[int] = None
    met: typing.Optional[int] = None
    phe: typing.Optional[int] = None
    pro: typing.Optional[int] = None
    thr: typing.Optional[int] = None
    ser: typing.Optional[int] = None
    trp: typing.Optional[int] = None
    tyr: typing.Optional[int] = None
    val: typing.Optional[int] = None

class ProteinInfo_Schema(ORMBaseSchema):
    protein_info_no: int
    feature_no: int
    date_created: datetime.datetime
    created_by: str
    molecular_weight: typing.Optional[int] = None
    pi: typing.Optional[decimal.Decimal] = None
    cai: typing.Optional[decimal.Decimal] = None
    protein_length: typing.Optional[int] = None
    n_term_seq: typing.Optional[str] = None
    c_term_seq: typing.Optional[str] = None
    codon_bias: typing.Optional[decimal.Decimal] = None
    fop_score: typing.Optional[decimal.Decimal] = None
    gravy_score: typing.Optional[decimal.Decimal] = None
    aromaticity_score: typing.Optional[decimal.Decimal] = None
    ala: typing.Optional[int] = None
    arg: typing.Optional[int] = None
    asn: typing.Optional[int] = None
    asp: typing.Optional[int] = None
    cys: typing.Optional[int] = None
    gln: typing.Optional[int] = None
    glu: typing.Optional[int] = None
    gly: typing.Optional[int] = None
    his: typing.Optional[int] = None
    ile: typing.Optional[int] = None
    leu: typing.Optional[int] = None
    lys: typing.Optional[int] = None
    met: typing.Optional[int] = None
    phe: typing.Optional[int] = None
    pro: typing.Optional[int] = None
    thr: typing.Optional[int] = None
    ser: typing.Optional[int] = None
    trp: typing.Optional[int] = None
    tyr: typing.Optional[int] = None
    val: typing.Optional[int] = None

class SeqChangeArchiveSchema(ORMBaseSchema):
    seq_change_archive_no: float
    seq_no: int
    seq_change_type: str
    change_start_coord: float
    change_stop_coord: float
    date_created: datetime.datetime
    created_by: str
    old_seq: typing.Optional[str] = None
    new_seq: typing.Optional[str] = None

class SeqSchema(ORMBaseSchema):
    seq_no: int
    feature_no: int
    genome_version_no: int
    seq_version: datetime.datetime
    seq_type: str
    source: str
    is_seq_current: str
    date_created: datetime.datetime
    created_by: str
    seq_length: int
    residues: str
    ftp_file: typing.Optional[str] = None

class Seq_Schema(ORMBaseSchema):
    seq_no: int
    feature_no: int
    genome_version_no: int
    seq_version: datetime.datetime
    seq_type: str
    source: str
    is_seq_current: str
    date_created: datetime.datetime
    created_by: str
    seq_length: int
    residues: str
    ftp_file: typing.Optional[str] = None
