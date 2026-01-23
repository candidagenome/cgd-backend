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

class AuthorSchema(ORMBaseSchema):
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

class CodeSchema(ORMBaseSchema):
    code_no: int
    tab_name: str
    col_name: str
    code_value: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

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

class DbuserSchema(ORMBaseSchema):
    dbuser_no: int
    userid: str
    first_name: str
    last_name: str
    status: str
    email: str
    date_created: datetime.datetime

class DbxrefSchema(ORMBaseSchema):
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

class ExptPropertySchema(ORMBaseSchema):
    expt_property_no: int
    property_type: str
    property_value: str
    date_created: datetime.datetime
    created_by: str
    property_description: typing.Optional[str] = None

class GoSchema(ORMBaseSchema):
    go_no: int
    goid: int
    go_term: str
    go_aspect: str
    date_created: datetime.datetime
    created_by: str
    go_definition: typing.Optional[str] = None

class GoSynonymSchema(ORMBaseSchema):
    go_synonym_no: int
    go_synonym: str
    date_created: datetime.datetime
    created_by: str

class HomologyGroupSchema(ORMBaseSchema):
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

class JournalSchema(ORMBaseSchema):
    journal_no: int
    date_created: datetime.datetime
    created_by: str
    full_name: typing.Optional[str] = None
    abbreviation: typing.Optional[str] = None
    issn: typing.Optional[str] = None
    essn: typing.Optional[str] = None
    publisher: typing.Optional[str] = None

class KeywordSchema(ORMBaseSchema):
    keyword_no: int
    keyword: str
    source: str
    date_created: datetime.datetime
    created_by: str

class NoteSchema(ORMBaseSchema):
    note_no: int
    note: str
    note_type: str
    date_created: datetime.datetime
    created_by: str

class ParagraphSchema(ORMBaseSchema):
    paragraph_no: int
    paragraph_text: str
    date_edited: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class PhenotypeSchema(ORMBaseSchema):
    phenotype_no: int
    source: str
    experiment_type: str
    mutant_type: str
    observable: str
    date_created: datetime.datetime
    created_by: str
    qualifier: typing.Optional[str] = None

class RefBadSchema(ORMBaseSchema):
    pubmed: int
    date_created: datetime.datetime
    created_by: str

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

class RefUnlinkSchema(ORMBaseSchema):
    ref_unlink_no: int
    pubmed: int
    tab_name: str
    primary_key: int
    date_created: datetime.datetime
    created_by: str

class TabRuleSchema(ORMBaseSchema):
    tab_rule_no: int
    group_name: str
    tab_name: str
    diagram_name: str
    complex_rule: typing.Optional[str] = None

class TaxonomySchema(ORMBaseSchema):
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

class UrlSchema(ORMBaseSchema):
    url_no: int
    source: str
    url_type: str
    url: str
    date_created: datetime.datetime
    created_by: str
    substitution_value: typing.Optional[str] = None

class WebMetadataSchema(ORMBaseSchema):
    web_metadata_no: int
    application_name: str
    tab_name: str
    col_name: str
    date_created: datetime.datetime
    created_by: str
    col_value: typing.Optional[str] = None

class Alias_Schema(ORMBaseSchema):
    alias_no: int
    alias_name: str
    alias_type: str
    date_created: datetime.datetime
    created_by: str

class Author_Schema(ORMBaseSchema):
    author_no: int
    author_name: str
    date_created: datetime.datetime
    created_by: str

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

class Dbxref_Schema(ORMBaseSchema):
    dbxref_no: int
    source: str
    dbxref_type: str
    dbxref_id: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class Experiment_Schema(ORMBaseSchema):
    experiment_no: int
    source: str
    date_created: datetime.datetime
    created_by: str
    experiment_comment: typing.Optional[str] = None

class ExptProperty_Schema(ORMBaseSchema):
    expt_property_no: int
    property_type: str
    property_value: str
    date_created: datetime.datetime
    created_by: str
    property_description: typing.Optional[str] = None

class Go_Schema(ORMBaseSchema):
    go_no: int
    goid: int
    go_term: str
    go_aspect: str
    date_created: datetime.datetime
    created_by: str
    go_definition: typing.Optional[str] = None

class GoSynonym_Schema(ORMBaseSchema):
    go_synonym_no: int
    go_synonym: str
    date_created: datetime.datetime
    created_by: str

class HomologyGroup_Schema(ORMBaseSchema):
    homology_group_no: int
    homology_group_type: str
    method: str
    date_created: datetime.datetime
    created_by: str
    homology_group_id: typing.Optional[str] = None

class Interaction_Schema(ORMBaseSchema):
    interaction_no: int
    experiment_type: str
    source: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

class Journal_Schema(ORMBaseSchema):
    journal_no: int
    date_created: datetime.datetime
    created_by: str
    full_name: typing.Optional[str] = None
    abbreviation: typing.Optional[str] = None
    issn: typing.Optional[str] = None
    essn: typing.Optional[str] = None
    publisher: typing.Optional[str] = None

class Keyword_Schema(ORMBaseSchema):
    keyword_no: int
    keyword: str
    source: str
    date_created: datetime.datetime
    created_by: str

class Note_Schema(ORMBaseSchema):
    note_no: int
    note: str
    note_type: str
    date_created: datetime.datetime
    created_by: str

class Paragraph_Schema(ORMBaseSchema):
    paragraph_no: int
    paragraph_text: str
    date_edited: datetime.datetime
    date_created: datetime.datetime
    created_by: str

class Phenotype_Schema(ORMBaseSchema):
    phenotype_no: int
    source: str
    experiment_type: str
    mutant_type: str
    observable: str
    date_created: datetime.datetime
    created_by: str
    qualifier: typing.Optional[str] = None

class RefType_Schema(ORMBaseSchema):
    ref_type_no: int
    source: str
    ref_type: str
    date_created: datetime.datetime
    created_by: str

class TabRule_Schema(ORMBaseSchema):
    tab_rule_no: int
    group_name: str
    tab_name: str
    diagram_name: str
    complex_rule: typing.Optional[str] = None

class Taxonomy_Schema(ORMBaseSchema):
    taxon_id: int
    tax_term: str
    is_default_display: str
    date_created: datetime.datetime
    created_by: str
    common_name: typing.Optional[str] = None
    rank: typing.Optional[str] = None

class Url_Schema(ORMBaseSchema):
    url_no: int
    source: str
    url_type: str
    url: str
    date_created: datetime.datetime
    created_by: str
    substitution_value: typing.Optional[str] = None

class BlastHitSchema(ORMBaseSchema):
    blast_hit_no: int
    identifier: str
    source: str
    length: int
    date_created: datetime.datetime
    created_by: str
    taxon_id: typing.Optional[int] = None
    description: typing.Optional[str] = None

class ColRuleSchema(ORMBaseSchema):
    col_rule_no: int
    tab_name: str
    col_name: str
    col_order: float
    col_rule: typing.Optional[str] = None
    col_sequence_name: typing.Optional[str] = None

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

class CvSchema(ORMBaseSchema):
    cv_no: int
    cv_name: str
    date_created: datetime.datetime
    created_by: str
    url_no: typing.Optional[int] = None
    description: typing.Optional[str] = None

class DbxrefHomologySchema(ORMBaseSchema):
    dbxref_homology_no: int
    dbxref_no: int
    homology_group_no: int
    name: str
    date_created: datetime.datetime
    created_by: str

class DbxrefUrlSchema(ORMBaseSchema):
    dbxref_url_no: int
    dbxref_no: int
    url_no: int

class ExptExptpropSchema(ORMBaseSchema):
    expt_exptprop_no: int
    expt_property_no: int
    experiment_no: int

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

class GoSetSchema(ORMBaseSchema):
    go_set_no: int
    go_no: int
    go_set_name: str
    date_created: datetime.datetime
    created_by: str

class InteractPhenoSchema(ORMBaseSchema):
    interact_pheno_no: int
    interaction_no: int
    phenotype_no: int

class NoteLinkSchema(ORMBaseSchema):
    note_link_no: int
    note_no: int
    tab_name: str
    primary_key: int
    date_created: datetime.datetime
    created_by: str

class PdbSequenceSchema(ORMBaseSchema):
    pdb_sequence_no: int
    sequence_name: str
    source: str
    sequence_length: int
    date_created: datetime.datetime
    created_by: str
    taxon_id: typing.Optional[int] = None
    note: typing.Optional[str] = None

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

class TaxRelationshipSchema(ORMBaseSchema):
    tax_relationship_no: int
    parent_taxon_id: int
    child_taxon_id: int
    generation: int

class TaxSynonymSchema(ORMBaseSchema):
    tax_synonym_no: int
    taxon_id: int
    tax_synonym: str

class UrlHomologySchema(ORMBaseSchema):
    url_homology_no: int
    url_no: int
    homology_group_no: int
    date_created: datetime.datetime
    created_by: str

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

class BlastHit_Schema(ORMBaseSchema):
    blast_hit_no: int
    identifier: str
    source: str
    length: int
    date_created: datetime.datetime
    created_by: str
    taxon_id: typing.Optional[int] = None
    description: typing.Optional[str] = None

class Cv_Schema(ORMBaseSchema):
    cv_no: int
    cv_name: str
    date_created: datetime.datetime
    created_by: str
    url_no: typing.Optional[int] = None
    description: typing.Optional[str] = None

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

class PdbSequence_Schema(ORMBaseSchema):
    pdb_sequence_no: int
    sequence_name: str
    source: str
    sequence_length: int
    date_created: datetime.datetime
    created_by: str
    taxon_id: typing.Optional[int] = None
    note: typing.Optional[str] = None

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

class AbstractSchema(ORMBaseSchema):
    reference_no: int
    abstract: str

class AuthorEditorSchema(ORMBaseSchema):
    author_editor_no: int
    author_no: int
    reference_no: int
    author_order: int
    author_type: str

class CvTermSchema(ORMBaseSchema):
    cv_term_no: int
    cv_no: int
    term_name: str
    date_created: datetime.datetime
    created_by: str
    dbxref_id: typing.Optional[str] = None
    cvterm_definition: typing.Optional[str] = None

class DbxrefRefSchema(ORMBaseSchema):
    dbxref_ref_no: int
    dbxref_no: int
    reference_no: int

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

class GenomeVersionSchema(ORMBaseSchema):
    genome_version_no: int
    genome_version: str
    organism_no: int
    is_ver_current: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

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

class RefUrlSchema(ORMBaseSchema):
    ref_url_no: int
    reference_no: int
    url_no: int

class CvTerm_Schema(ORMBaseSchema):
    cv_term_no: int
    cv_no: int
    term_name: str
    date_created: datetime.datetime
    created_by: str
    dbxref_id: typing.Optional[str] = None
    cvterm_definition: typing.Optional[str] = None

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

class GenomeVersion_Schema(ORMBaseSchema):
    genome_version_no: int
    genome_version: str
    organism_no: int
    is_ver_current: str
    date_created: datetime.datetime
    created_by: str
    description: typing.Optional[str] = None

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

class RefProperty_Schema(ORMBaseSchema):
    ref_property_no: int
    reference_no: int
    source: str
    property_type: str
    property_value: str
    date_last_reviewed: datetime.datetime
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

class CollFeatSchema(ORMBaseSchema):
    coll_feat_no: int
    colleague_no: int
    feature_no: int

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

class GeneReservationSchema(ORMBaseSchema):
    gene_reservation_no: int
    feature_no: int
    reservation_date: datetime.datetime
    expiration_date: datetime.datetime
    date_created: datetime.datetime
    created_by: str
    date_standardized: typing.Optional[datetime.datetime] = None

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

class PdbAlignmentSequenceSchema(ORMBaseSchema):
    pdb_alignment_sequence_no: int
    pdb_alignment_no: int
    date_created: datetime.datetime
    created_by: str
    query_seq: str
    target_seq: str
    alignment_symbol: str

class PhenoAnnotationSchema(ORMBaseSchema):
    pheno_annotation_no: int
    feature_no: int
    phenotype_no: int
    date_created: datetime.datetime
    created_by: str
    experiment_no: typing.Optional[int] = None

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

class RefpropFeatSchema(ORMBaseSchema):
    refprop_feat_no: int
    feature_no: int
    ref_property_no: int
    date_created: datetime.datetime
    created_by: str

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

class GeneReservation_Schema(ORMBaseSchema):
    gene_reservation_no: int
    feature_no: int
    reservation_date: datetime.datetime
    expiration_date: datetime.datetime
    date_created: datetime.datetime
    created_by: str
    date_standardized: typing.Optional[datetime.datetime] = None

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

class CollGeneresSchema(ORMBaseSchema):
    coll_generes_no: int
    colleague_no: int
    gene_reservation_no: int

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

class GoRefSchema(ORMBaseSchema):
    go_ref_no: int
    reference_no: int
    go_annotation_no: int
    has_qualifier: str
    has_supporting_evidence: str
    date_created: datetime.datetime
    created_by: str

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

class GoRef_Schema(ORMBaseSchema):
    go_ref_no: int
    reference_no: int
    go_annotation_no: int
    has_qualifier: str
    has_supporting_evidence: str
    date_created: datetime.datetime
    created_by: str

class GoQualifierSchema(ORMBaseSchema):
    go_qualifier_no: int
    go_ref_no: int
    qualifier: str

class GorefDbxrefSchema(ORMBaseSchema):
    goref_dbxref_no: int
    go_ref_no: int
    dbxref_no: int
    support_type: str
