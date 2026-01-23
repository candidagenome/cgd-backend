# Auto-generated from schemas_generated.py; do not edit by hand.
# Generated: 2026-01-23T20:07:21
# Contents: 1:1 table schemas for remaining tables (CV, taxonomy, colleague, admin, etc.)

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

class AliasSchema(ORMBaseSchema):
    alias_no: int
    alias_name: str
    alias_type: str
    date_created: datetime.datetime
    created_by: str

class Alias_Schema(ORMBaseSchema):
    alias_no: int
    alias_name: str
    alias_type: str
    date_created: datetime.datetime
    created_by: str

class BlastAlignmentSchema(ORMBaseSchema):
    blast_alignment_no: int
    query_no: int
    target_no: int
    method: str
    query_start_coord: int
    query_stop_coord: int
    target_start_coord: int
    target_stop_coord: int
    score: decimal.Decimal
    score_type: str
    date_created: datetime.datetime
    created_by: str
    pct_aligned: typing.Optional[decimal.Decimal] = None
    pct_identical: typing.Optional[decimal.Decimal] = None
    pct_similar: typing.Optional[decimal.Decimal] = None

class BlastHitSchema(ORMBaseSchema):
    blast_hit_no: int
    identifier: str
    source: str
    length: int
    date_created: datetime.datetime
    created_by: str
    taxon_id: typing.Optional[int] = None
    description: typing.Optional[str] = None

class BlastHit_Schema(ORMBaseSchema):
    blast_hit_no: int
    identifier: str
    source: str
    length: int
    date_created: datetime.datetime
    created_by: str
    taxon_id: typing.Optional[int] = None
    description: typing.Optional[str] = None

class CodeSchema(ORMBaseSchema):
    code_no: int
    tab_name: str
    col_name: str
    code_value: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class ColRuleSchema(ORMBaseSchema):
    col_rule_no: int
    tab_name: str
    col_name: str
    col_order: float
    col_rule: typing.Optional[str] = None
    col_sequence_name: typing.Optional[str] = None

class CollGeneresSchema(ORMBaseSchema):
    coll_generes_no: int
    colleague_no: int
    gene_reservation_no: int

class CollKwSchema(ORMBaseSchema):
    coll_kw_no: int
    colleague_no: int
    keyword_no: int

class CollRelationshipSchema(ORMBaseSchema):
    coll_relationship_no: int
    colleague_no: int
    associate_no: int
    relationship_type: str
    date_created: datetime.datetime
    created_by: str

class CollUrlSchema(ORMBaseSchema):
    coll_url_no: int
    colleague_no: int
    url_no: int

class ColleagueRemarkSchema(ORMBaseSchema):
    colleague_remark_no: int
    colleague_no: int
    remark: str
    remark_type: str
    remark_date: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class ColleagueSchema(ORMBaseSchema):
    colleague_no: int
    last_name: str
    first_name: str
    source: str
    is_pi: str
    is_contact: str
    date_modified: datetime.datetime
    date_created: datetime.datetime
    created_by: str
    suffix: typing.Optional[str] = None
    other_last_name: typing.Optional[str] = None
    profession: typing.Optional[str] = None
    job_title: typing.Optional[str] = None
    institution: typing.Optional[str] = None
    address1: typing.Optional[str] = None
    address2: typing.Optional[str] = None
    address3: typing.Optional[str] = None
    address4: typing.Optional[str] = None
    address5: typing.Optional[str] = None
    city: typing.Optional[str] = None
    state: typing.Optional[str] = None
    region: typing.Optional[str] = None
    country: typing.Optional[str] = None
    postal_code: typing.Optional[str] = None
    work_phone: typing.Optional[str] = None
    other_phone: typing.Optional[str] = None
    fax: typing.Optional[str] = None
    email: typing.Optional[str] = None

class Colleague_Schema(ORMBaseSchema):
    colleague_no: int
    last_name: str
    first_name: str
    source: str
    is_pi: str
    is_contact: str
    date_modified: datetime.datetime
    date_created: datetime.datetime
    created_by: str
    suffix: typing.Optional[str] = None
    other_last_name: typing.Optional[str] = None
    profession: typing.Optional[str] = None
    job_title: typing.Optional[str] = None
    institution: typing.Optional[str] = None
    address1: typing.Optional[str] = None
    address2: typing.Optional[str] = None
    address3: typing.Optional[str] = None
    address4: typing.Optional[str] = None
    address5: typing.Optional[str] = None
    city: typing.Optional[str] = None
    state: typing.Optional[str] = None
    region: typing.Optional[str] = None
    country: typing.Optional[str] = None
    postal_code: typing.Optional[str] = None
    work_phone: typing.Optional[str] = None
    other_phone: typing.Optional[str] = None
    fax: typing.Optional[str] = None
    email: typing.Optional[str] = None

class CvSchema(ORMBaseSchema):
    cv_no: int
    cv_name: str
    date_created: datetime.datetime
    created_by: str
    url_no: typing.Optional[int] = None
    description: typing.Optional[str] = None

class CvTermSchema(ORMBaseSchema):
    cv_term_no: int
    cv_no: int
    term_name: str
    date_created: datetime.datetime
    created_by: str
    dbxref_id: typing.Optional[str] = None
    cvterm_definition: typing.Optional[str] = None

class CvTerm_Schema(ORMBaseSchema):
    cv_term_no: int
    cv_no: int
    term_name: str
    date_created: datetime.datetime
    created_by: str
    dbxref_id: typing.Optional[str] = None
    cvterm_definition: typing.Optional[str] = None

class Cv_Schema(ORMBaseSchema):
    cv_no: int
    cv_name: str
    date_created: datetime.datetime
    created_by: str
    url_no: typing.Optional[int] = None
    description: typing.Optional[str] = None

class CvtermDbxrefSchema(ORMBaseSchema):
    cvterm_dbxref_no: int
    cv_term_no: int
    dbxref_no: int

class CvtermGroupSchema(ORMBaseSchema):
    cvterm_group_no: int
    group_name: str
    cv_term_no: int
    date_created: datetime.datetime
    created_by: str

class CvtermPathSchema(ORMBaseSchema):
    cvterm_path_no: int
    child_cv_term_no: int
    ancestor_cv_term_no: int
    generation: int
    full_path: str
    relationship_type: typing.Optional[str] = None

class CvtermRelationshipSchema(ORMBaseSchema):
    cvterm_relationship_no: int
    child_cv_term_no: int
    parent_cv_term_no: int
    relationship_type: str
    date_created: datetime.datetime
    created_by: str

class CvtermSynonymSchema(ORMBaseSchema):
    cvterm_synonym_no: int
    cv_term_no: int
    term_synonym: str
    date_created: datetime.datetime
    created_by: str
    synonym_type: typing.Optional[str] = None

class DbuserSchema(ORMBaseSchema):
    dbuser_no: int
    userid: str
    first_name: str
    last_name: str
    status: str
    email: str
    date_created: datetime.datetime

class DbxrefHomologySchema(ORMBaseSchema):
    dbxref_homology_no: int
    dbxref_no: int
    homology_group_no: int
    name: str
    date_created: datetime.datetime
    created_by: str

class DbxrefSchema(ORMBaseSchema):
    dbxref_no: int
    source: str
    dbxref_type: str
    dbxref_id: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class DbxrefUrlSchema(ORMBaseSchema):
    dbxref_url_no: int
    dbxref_no: int
    url_no: int

class Dbxref_Schema(ORMBaseSchema):
    dbxref_no: int
    source: str
    dbxref_type: str
    dbxref_id: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class DeleteLogSchema(ORMBaseSchema):
    delete_log_no: int
    tab_name: str
    primary_key: int
    deleted_row: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class ExperimentSchema(ORMBaseSchema):
    experiment_no: int
    source: str
    date_created: datetime.datetime
    created_by: str
    experiment_comment: typing.Optional[str] = None

class Experiment_Schema(ORMBaseSchema):
    experiment_no: int
    source: str
    date_created: datetime.datetime
    created_by: str
    experiment_comment: typing.Optional[str] = None

class ExptExptpropSchema(ORMBaseSchema):
    expt_exptprop_no: int
    expt_property_no: int
    experiment_no: int

class ExptPropertySchema(ORMBaseSchema):
    expt_property_no: int
    property_type: str
    property_value: str
    date_created: datetime.datetime
    created_by: str
    property_description: typing.Optional[str] = None

class ExptProperty_Schema(ORMBaseSchema):
    expt_property_no: int
    property_type: str
    property_value: str
    date_created: datetime.datetime
    created_by: str
    property_description: typing.Optional[str] = None

class GeneReservationSchema(ORMBaseSchema):
    gene_reservation_no: int
    feature_no: int
    reservation_date: datetime.datetime
    expiration_date: datetime.datetime
    date_created: datetime.datetime
    created_by: str
    date_standardized: typing.Optional[datetime.datetime] = None

class GeneReservation_Schema(ORMBaseSchema):
    gene_reservation_no: int
    feature_no: int
    reservation_date: datetime.datetime
    expiration_date: datetime.datetime
    date_created: datetime.datetime
    created_by: str
    date_standardized: typing.Optional[datetime.datetime] = None

class HomologyGroupSchema(ORMBaseSchema):
    homology_group_no: int
    homology_group_type: str
    method: str
    date_created: datetime.datetime
    created_by: str
    homology_group_id: typing.Optional[str] = None

class HomologyGroup_Schema(ORMBaseSchema):
    homology_group_no: int
    homology_group_type: str
    method: str
    date_created: datetime.datetime
    created_by: str
    homology_group_id: typing.Optional[str] = None

class InteractionSchema(ORMBaseSchema):
    interaction_no: int
    experiment_type: str
    source: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class Interaction_Schema(ORMBaseSchema):
    interaction_no: int
    experiment_type: str
    source: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class KeywordSchema(ORMBaseSchema):
    keyword_no: int
    keyword: str
    source: str
    date_created: datetime.datetime
    created_by: str

class Keyword_Schema(ORMBaseSchema):
    keyword_no: int
    keyword: str
    source: str
    date_created: datetime.datetime
    created_by: str

class NoteLinkSchema(ORMBaseSchema):
    note_link_no: int
    note_no: int
    tab_name: str
    primary_key: int
    date_created: datetime.datetime
    created_by: str

class NoteSchema(ORMBaseSchema):
    note_no: int
    note: str
    note_type: str
    date_created: datetime.datetime
    created_by: str

class Note_Schema(ORMBaseSchema):
    note_no: int
    note: str
    note_type: str
    date_created: datetime.datetime
    created_by: str

class OrganismSchema(ORMBaseSchema):
    organism_no: int
    organism_name: str
    organism_abbrev: str
    taxon_id: int
    taxonomic_rank: str
    organism_order: int
    date_created: datetime.datetime
    created_by: str
    parent_organism_no: typing.Optional[int] = None
    common_name: typing.Optional[str] = None

class Organism_Schema(ORMBaseSchema):
    organism_no: int
    organism_name: str
    organism_abbrev: str
    taxon_id: int
    taxonomic_rank: str
    organism_order: int
    date_created: datetime.datetime
    created_by: str
    parent_organism_no: typing.Optional[int] = None
    common_name: typing.Optional[str] = None

class ParagraphSchema(ORMBaseSchema):
    paragraph_no: int
    paragraph_text: str
    date_edited: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class Paragraph_Schema(ORMBaseSchema):
    paragraph_no: int
    paragraph_text: str
    date_edited: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class PdbAlignmentSchema(ORMBaseSchema):
    pdb_alignment_no: int
    query_seq_no: int
    target_seq_no: int
    method: str
    matrix: str
    query_align_start_coord: int
    query_align_stop_coord: int
    target_align_start_coord: int
    target_align_stop_coord: int
    pct_aligned: decimal.Decimal
    pct_identical: decimal.Decimal
    pct_similar: decimal.Decimal
    score: decimal.Decimal
    date_created: datetime.datetime
    created_by: str

class PdbAlignment_Schema(ORMBaseSchema):
    pdb_alignment_no: int
    query_seq_no: int
    target_seq_no: int
    method: str
    matrix: str
    query_align_start_coord: int
    query_align_stop_coord: int
    target_align_start_coord: int
    target_align_stop_coord: int
    pct_aligned: decimal.Decimal
    pct_identical: decimal.Decimal
    pct_similar: decimal.Decimal
    score: decimal.Decimal
    date_created: datetime.datetime
    created_by: str

class TabRuleSchema(ORMBaseSchema):
    tab_rule_no: int
    group_name: str
    tab_name: str
    diagram_name: str
    complex_rule: typing.Optional[str] = None

class TabRule_Schema(ORMBaseSchema):
    tab_rule_no: int
    group_name: str
    tab_name: str
    diagram_name: str
    complex_rule: typing.Optional[str] = None

class TaxRelationshipSchema(ORMBaseSchema):
    tax_relationship_no: int
    parent_taxon_id: int
    child_taxon_id: int
    generation: int

class TaxSynonymSchema(ORMBaseSchema):
    tax_synonym_no: int
    taxon_id: int
    tax_synonym: str

class TaxonomySchema(ORMBaseSchema):
    taxon_id: int
    tax_term: str
    is_default_display: str
    date_created: datetime.datetime
    created_by: str
    common_name: typing.Optional[str] = None
    rank: typing.Optional[str] = None

class Taxonomy_Schema(ORMBaseSchema):
    taxon_id: int
    tax_term: str
    is_default_display: str
    date_created: datetime.datetime
    created_by: str
    common_name: typing.Optional[str] = None
    rank: typing.Optional[str] = None

class UpdateLogSchema(ORMBaseSchema):
    update_log_no: int
    tab_name: str
    col_name: str
    primary_key: int
    date_created: datetime.datetime
    created_by: str
    old_value: typing.Optional[str] = None
    new_value: typing.Optional[str] = None
    description: typing.Optional[str] = None

class UrlHomologySchema(ORMBaseSchema):
    url_homology_no: int
    url_no: int
    homology_group_no: int
    date_created: datetime.datetime
    created_by: str

class UrlSchema(ORMBaseSchema):
    url_no: int
    source: str
    url_type: str
    url: str
    date_created: datetime.datetime
    created_by: str
    substitution_value: typing.Optional[str] = None

class Url_Schema(ORMBaseSchema):
    url_no: int
    source: str
    url_type: str
    url: str
    date_created: datetime.datetime
    created_by: str
    substitution_value: typing.Optional[str] = None

class WebDisplaySchema(ORMBaseSchema):
    web_display_no: int
    url_no: int
    web_page_name: str
    label_location: str
    label_type: str
    label_name: str
    is_default: str
    date_created: datetime.datetime
    created_by: str

class WebMetadataSchema(ORMBaseSchema):
    web_metadata_no: int
    application_name: str
    tab_name: str
    col_name: str
    date_created: datetime.datetime
    created_by: str
    col_value: typing.Optional[str] = None
