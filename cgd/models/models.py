from typing import Optional
import datetime
import decimal

from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKeyConstraint, Index, Integer, PrimaryKeyConstraint, Table, Text, VARCHAR, text
from sqlalchemy.dialects.oracle import NUMBER
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class Alias(Base):
    __tablename__ = 'alias'
    __table_args__ = (
        PrimaryKeyConstraint('alias_no', name='alias_pk'),
        Index('alias_uk', 'alias_name', 'alias_type', unique=True),
        Index('upper_alias_name_i'),
        {'comment': 'Contains other names or aliases for the standard name used to '
                'describe a feature or gene.',
     'schema': 'MULTI'}
    )

    alias_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an alias. Oracle sequence generated number.')
    alias_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Alternative name for a feature or gene.')
    alias_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of alias for the gene or feature (Coded: Standard or Non-standard).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')

    feat_alias: Mapped[list['FeatAlias']] = relationship('FeatAlias', back_populates='alias')


class Author(Base):
    __tablename__ = 'author'
    __table_args__ = (
        PrimaryKeyConstraint('author_no', name='author_pk'),
        Index('author_uk', 'author_name', unique=True),
        Index('upper_author_name_i'),
        {'comment': 'Contains names of authors for a reference.', 'schema': 'MULTI'}
    )

    author_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an author. Oracle sequence generated number.')
    author_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Author name, usually in PubMed format.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')

    author_editor: Mapped[list['AuthorEditor']] = relationship('AuthorEditor', back_populates='author')


class Book(Base):
    __tablename__ = 'book'
    __table_args__ = (
        PrimaryKeyConstraint('book_no', name='book_pk'),
        {'comment': 'Contains information for a book reference.', 'schema': 'MULTI'}
    )

    book_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a book. Oracle sequence generated number.')
    title: Mapped[str] = mapped_column(VARCHAR(400), nullable=False, comment='Title of the book.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    volume_title: Mapped[Optional[str]] = mapped_column(VARCHAR(400), comment='Title of the book volume.')
    isbn: Mapped[Optional[str]] = mapped_column(VARCHAR(20), comment='Interantional Standard Book Number.')
    total_pages: Mapped[Optional[float]] = mapped_column(NUMBER(5, 0, False), comment='Total number of pages in the book.')
    publisher: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='Publisher of the book.')
    publisher_location: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='Location of the book publisher.')

    reference: Mapped[list['Reference']] = relationship('Reference', back_populates='book')


class Code(Base):
    __tablename__ = 'code'
    __table_args__ = (
        PrimaryKeyConstraint('code_no', name='code_pk'),
        Index('code_uk', 'tab_name', 'col_name', 'code_value', unique=True),
        Index('upper_code_value_i'),
        {'comment': 'Contains values for coded columns used in the database.',
     'schema': 'MULTI'}
    )

    code_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a code.  Oracle sequence generated number.')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Table name associated with the coded value.')
    col_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Column name associated with the coded value.')
    code_value: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The actual code value.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='A description or explanation of the code value.')


class Colleague(Base):
    __tablename__ = 'colleague'
    __table_args__ = (
        CheckConstraint("is_contact in ('Y','N')", name='coll_is_contact_ck'),
        CheckConstraint("is_pi in ('Y','N')", name='coll_is_pi_ck'),
        PrimaryKeyConstraint('colleague_no', name='colleague_pk'),
        Index('upper_first_name_i'),
        Index('upper_last_name_i'),
        Index('upper_other_last_name_i'),
        {'comment': 'An individual who has submitted personal information to the '
                'database.',
     'schema': 'MULTI'}
    )

    colleague_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a colleague. Oracle sequence generated number.')
    last_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Colleague last name.')
    first_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Colleague first name.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source from which which the database obtained this colleague information. Coded.')
    is_pi: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Whether the colleague is a PI (Coded: Y/N).')
    is_contact: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Whether the colleague is a contact for SGD.')
    date_modified: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the colleague entry was created')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    suffix: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Colleague suffix (Coded: Jr., Sr., II, etc.)')
    other_last_name: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Other last name for the colleague (e.g., maiden name, etc.).')
    profession: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='Colleague profession, such as "yeast molecular biologist."')
    job_title: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='Colleague postition, such as post-doc, staff scientist, principal investigator, etc.  This is a user-defined entry without a controlled vocabulary.')
    institution: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='Organization at which the colleague is employed.  This is user defined and can be a department, institute, company, etc.')
    address1: Mapped[Optional[str]] = mapped_column(VARCHAR(60), comment='First line of an address.')
    address2: Mapped[Optional[str]] = mapped_column(VARCHAR(60), comment='Second line of an address.')
    address3: Mapped[Optional[str]] = mapped_column(VARCHAR(60), comment='Third line of an address.')
    address4: Mapped[Optional[str]] = mapped_column(VARCHAR(60), comment='Fourth line of an address.')
    address5: Mapped[Optional[str]] = mapped_column(VARCHAR(60), comment='Fifth line of an address.')
    city: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='City where colleague can be contacted')
    state: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='US State or Canadian Province, chosen by the colleague from a coded list.')
    region: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Region or province for non-US colleagues')
    country: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Colleague country.')
    postal_code: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Colleague postal code.')
    work_phone: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Colleague work phone number.')
    other_phone: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Additional phone number.')
    fax: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Fax number.')
    email: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='A fully qualified email address: name@domain.')

    coll_kw: Mapped[list['CollKw']] = relationship('CollKw', back_populates='colleague')
    coll_relationship: Mapped[list['CollRelationship']] = relationship('CollRelationship', foreign_keys='[CollRelationship.associate_no]', back_populates='colleague')
    coll_relationship: Mapped[list['CollRelationship']] = relationship('CollRelationship', foreign_keys='[CollRelationship.colleague_no]', back_populates='colleague')
    coll_url: Mapped[list['CollUrl']] = relationship('CollUrl', back_populates='colleague')
    colleague_remark: Mapped[list['ColleagueRemark']] = relationship('ColleagueRemark', back_populates='colleague')
    coll_feat: Mapped[list['CollFeat']] = relationship('CollFeat', back_populates='colleague')
    coll_generes: Mapped[list['CollGeneres']] = relationship('CollGeneres', back_populates='colleague')


class Dbuser(Base):
    __tablename__ = 'dbuser'
    __table_args__ = (
        PrimaryKeyConstraint('dbuser_no', name='dbuser_pk'),
        Index('dbuser_uk', 'userid', unique=True),
        {'comment': 'Contains information about database users, who are either '
                'curators or programers.',
     'schema': 'MULTI'}
    )

    dbuser_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a database user.  Oracle sequence generated number.')
    userid: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, comment='Assigned unique identifier for non-public user. Usually  their UNIX login name.')
    first_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='First name of the database user.')
    last_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Last name of the database user.')
    status: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Status of the user (Coded: current, former).')
    email: Mapped[str] = mapped_column(VARCHAR(100), nullable=False, comment='E-mail address of the database user.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')


class Dbxref(Base):
    __tablename__ = 'dbxref'
    __table_args__ = (
        PrimaryKeyConstraint('dbxref_no', name='dbxref_pk'),
        Index('dbxref_id_i', 'dbxref_id'),
        Index('dbxref_uk', 'source', 'dbxref_type', 'dbxref_id', unique=True),
        Index('upper_dbxref_description_i'),
        Index('upper_dbxref_id_i'),
        {'comment': 'Contains all external database IDs (eg., PIR, Swiss-Prot) for '
                'various components in the database.',
     'schema': 'MULTI'}
    )

    dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Unique identifier for an database cross reference. Oracle sequence generated number.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the database identifier (Coded: SwissProt, NCBI, etc.).')
    dbxref_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Type of database identifier (Coded: GenBank GI, RefSeq GI, etc.).')
    dbxref_id: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Database identifier assigned by another database.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person wno entered the record into the database.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='Long name or description of the other database identifier.')

    dbxref_homology: Mapped[list['DbxrefHomology']] = relationship('DbxrefHomology', back_populates='dbxref')
    dbxref_url: Mapped[list['DbxrefUrl']] = relationship('DbxrefUrl', back_populates='dbxref')
    dbxref_ref: Mapped[list['DbxrefRef']] = relationship('DbxrefRef', back_populates='dbxref')
    cvterm_dbxref: Mapped[list['CvtermDbxref']] = relationship('CvtermDbxref', back_populates='dbxref')
    dbxref_feat: Mapped[list['DbxrefFeat']] = relationship('DbxrefFeat', back_populates='dbxref')
    goref_dbxref: Mapped[list['GorefDbxref']] = relationship('GorefDbxref', back_populates='dbxref')


class DeleteLog(Base):
    __tablename__ = 'delete_log'
    __table_args__ = (
        PrimaryKeyConstraint('delete_log_no', name='delete_log_pk'),
        Index('delete_log_i', 'tab_name', 'primary_key'),
        {'comment': 'Contains an entry for deleted rows in the database. This table is '
                'populated by triggers.',
     'schema': 'MULTI'}
    )

    delete_log_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a deleted row.  Oracle sequence generated number.')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Table name.')
    primary_key: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Primary key of the row deleted.')
    deleted_row: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='Concatenation of all columns in the row deleted.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='Reason why the row was deleted.')


class Experiment(Base):
    __tablename__ = 'experiment'
    __table_args__ = (
        PrimaryKeyConstraint('experiment_no', name='experiment_pk'),
        Index('experiment_i', 'source', 'experiment_comment'),
        {'schema': 'MULTI'}
    )

    experiment_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an experiment.  Oracle sequence generated number.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The source of the experiment.  Coded.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    experiment_comment: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='A description or note about the experiment.')

    expt_exptprop: Mapped[list['ExptExptprop']] = relationship('ExptExptprop', back_populates='experiment')
    pheno_annotation: Mapped[list['PhenoAnnotation']] = relationship('PhenoAnnotation', back_populates='experiment')


class ExptProperty(Base):
    __tablename__ = 'expt_property'
    __table_args__ = (
        PrimaryKeyConstraint('expt_property_no', name='expt_property_pk'),
        Index('expt_property_uk', 'property_type', 'property_value', 'property_description', unique=True),
        {'comment': 'Properties or attributes associated with an experiment.',
     'schema': 'MULTI'}
    )

    expt_property_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an experiment property. Oracle sequence generated number.')
    property_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of experiment attribute or property.  Coded value.')
    property_value: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='The actual experiment attribute or property value.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    property_description: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='Description associated with an experiment property.')

    expt_exptprop: Mapped[list['ExptExptprop']] = relationship('ExptExptprop', back_populates='expt_property')


class Go(Base):
    __tablename__ = 'go'
    __table_args__ = (
        PrimaryKeyConstraint('go_no', name='go_pk'),
        Index('go_goid_uk', 'goid', unique=True),
        Index('go_term_uk', 'go_term', 'go_aspect', unique=True),
        Index('upper_go_term_i'),
        {'comment': 'Contains terms that comprise the Gene Ontology (GO), not the '
                'relationships between them.',
     'schema': 'MULTI'}
    )

    go_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier assigned to a goid. Oracle sequence generated number.')
    goid: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='GO number assigned by GO software.')
    go_term: Mapped[str] = mapped_column(VARCHAR(240), nullable=False, comment='Term or word in the Gene Ontology.')
    go_aspect: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Aspect of the GO term (coded: function, process, component).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    go_definition: Mapped[Optional[str]] = mapped_column(VARCHAR(2000), comment='Definition for the GO term.')

    go_gosyn: Mapped[list['GoGosyn']] = relationship('GoGosyn', back_populates='go')
    go_path: Mapped[list['GoPath']] = relationship('GoPath', foreign_keys='[GoPath.ancestor_go_no]', back_populates='go')
    go_path: Mapped[list['GoPath']] = relationship('GoPath', foreign_keys='[GoPath.child_go_no]', back_populates='go')
    go_set: Mapped[list['GoSet']] = relationship('GoSet', back_populates='go')
    go_annotation: Mapped[list['GoAnnotation']] = relationship('GoAnnotation', back_populates='go')


class GoSynonym(Base):
    __tablename__ = 'go_synonym'
    __table_args__ = (
        PrimaryKeyConstraint('go_synonym_no', name='go_synonym_pk'),
        Index('go_synonym_uk', 'go_synonym', unique=True),
        {'comment': 'Contains synonyms for GO terms.', 'schema': 'MULTI'}
    )

    go_synonym_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a GO synonym. Oracle sequence generated number.')
    go_synonym: Mapped[str] = mapped_column(VARCHAR(966), nullable=False, comment='Description of the GO synonym.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')

    go_gosyn: Mapped[list['GoGosyn']] = relationship('GoGosyn', back_populates='go_synonym')


class HomologyGroup(Base):
    __tablename__ = 'homology_group'
    __table_args__ = (
        PrimaryKeyConstraint('homology_group_no', name='homology_group_pk'),
        {'comment': 'Contains the type and analysis method for determining homology.',
     'schema': 'MULTI'}
    )

    homology_group_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a homology group.  Oracle generated sequence number.')
    homology_group_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of homology group. Coded: ortholog, paralog, etc.')
    method: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The method used to determine homology. Coded.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    homology_group_id: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Name for an homology group.')

    dbxref_homology: Mapped[list['DbxrefHomology']] = relationship('DbxrefHomology', back_populates='homology_group')
    url_homology: Mapped[list['UrlHomology']] = relationship('UrlHomology', back_populates='homology_group')
    feat_homology: Mapped[list['FeatHomology']] = relationship('FeatHomology', back_populates='homology_group')


class Interaction(Base):
    __tablename__ = 'interaction'
    __table_args__ = (
        PrimaryKeyConstraint('interaction_no', name='interaction_pk'),
        Index('interaction_i', 'experiment_type', 'source', 'description'),
        {'comment': 'Stores interaction data.', 'schema': 'MULTI'}
    )

    interaction_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an interaction. Oracle generated sequence number.')
    experiment_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of experiment conducted that produced the interaction (Coded).')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the interaction. Coded value.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='Description of the interaction.')

    interact_pheno: Mapped[list['InteractPheno']] = relationship('InteractPheno', back_populates='interaction')
    feat_interact: Mapped[list['FeatInteract']] = relationship('FeatInteract', back_populates='interaction')


class Journal(Base):
    __tablename__ = 'journal'
    __table_args__ = (
        PrimaryKeyConstraint('journal_no', name='journal_pk'),
        {'comment': 'Contains information about journals.', 'schema': 'MULTI'}
    )

    journal_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a journal. Oracle sequence generated number.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was first entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who first entered the record into the database.')
    full_name: Mapped[Optional[str]] = mapped_column(VARCHAR(200), comment='Full name of the journal.')
    abbreviation: Mapped[Optional[str]] = mapped_column(VARCHAR(140), comment='Journal abbreviation.')
    issn: Mapped[Optional[str]] = mapped_column(VARCHAR(20), comment='International Standard Serial Number.')
    essn: Mapped[Optional[str]] = mapped_column(VARCHAR(20), comment='Electronic Standard Serial Number')
    publisher: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='Publisher of the journal.')

    reference: Mapped[list['Reference']] = relationship('Reference', back_populates='journal')


class Keyword(Base):
    __tablename__ = 'keyword'
    __table_args__ = (
        PrimaryKeyConstraint('keyword_no', name='keyword_pk'),
        Index('keyword_uk', 'keyword', unique=True),
        Index('upper_keyword_i'),
        {'comment': 'Contains information about keywords or vocabulary terms defined '
                'by colleagues.',
     'schema': 'MULTI'}
    )

    keyword_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a keyword. Oracle sequence generated number.')
    keyword: Mapped[str] = mapped_column(VARCHAR(100), nullable=False, comment='Keyword.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the keyword. Coded.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    coll_kw: Mapped[list['CollKw']] = relationship('CollKw', back_populates='keyword')


class Note(Base):
    __tablename__ = 'note'
    __table_args__ = (
        PrimaryKeyConstraint('note_no', name='note_pk'),
        Index('note_uk', 'note_type', 'note', unique=True),
        {'comment': 'Contains notes about items in the database.', 'schema': 'MULTI'}
    )

    note_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a note. Oracle generated sequence number.')
    note: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='The note or description.')
    note_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of note (Coded).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    note_link: Mapped[list['NoteLink']] = relationship('NoteLink', back_populates='note')


class Paragraph(Base):
    __tablename__ = 'paragraph'
    __table_args__ = (
        PrimaryKeyConstraint('paragraph_no', name='paragraph_pk'),
        Index('paragraph_uk', 'paragraph_text', unique=True),
        {'comment': 'Contains paragraphs that summarize the literature for a '
                'particular feature.',
     'schema': 'MULTI'}
    )

    paragraph_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a paragraph. Oracle sequence generated number.')
    paragraph_text: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='Assembled paragraph text.')
    date_edited: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the paragraph was last significantly edited.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')

    feat_para: Mapped[list['FeatPara']] = relationship('FeatPara', back_populates='paragraph')


class Phenotype(Base):
    __tablename__ = 'phenotype'
    __table_args__ = (
        PrimaryKeyConstraint('phenotype_no', name='phenotype_pk'),
        Index('phenotype_uk', 'source', 'experiment_type', 'mutant_type', 'observable', 'qualifier', unique=True),
        {'comment': 'Contains categorized phenotypes associated with a feature.',
     'schema': 'MULTI'}
    )

    phenotype_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a phenotype. Oracle sequence generated number.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The source of the phenotype. Coded value.')
    experiment_type: Mapped[str] = mapped_column(VARCHAR(100), nullable=False, comment='The experimental methodology that produced the phenotype (e.g.,Systematic deletion, Classical genetics, etc.).')
    mutant_type: Mapped[str] = mapped_column(VARCHAR(100), nullable=False, comment='The mutation effect on the gene product function (e.g., Null, Overexpression, Conditional, etc.).')
    observable: Mapped[str] = mapped_column(VARCHAR(240), nullable=False, comment='Indicates what feature is changed in the mutant (e.g., colony size, drug resistance, etc.).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    qualifier: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='The direction of the change relative to wild type (e.g., Abnormal, Normal, etc.).')

    interact_pheno: Mapped[list['InteractPheno']] = relationship('InteractPheno', back_populates='phenotype')
    pheno_annotation: Mapped[list['PhenoAnnotation']] = relationship('PhenoAnnotation', back_populates='phenotype')


class RefBad(Base):
    __tablename__ = 'ref_bad'
    __table_args__ = (
        PrimaryKeyConstraint('pubmed', name='ref_bad_pk'),
        {'comment': 'Contains PubMed IDs for references that should not be associated '
                'with the database.',
     'schema': 'MULTI'}
    )

    pubmed: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='PubMed identifier for a reference that should not be associated with the database or a given table record.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was first entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')


class RefTemp(Base):
    __tablename__ = 'ref_temp'
    __table_args__ = (
        PrimaryKeyConstraint('ref_temp_no', name='ref_temp_pk'),
        Index('ref_temp_cit_uk', 'citation', unique=True),
        Index('ref_temp_pubmed_uk', 'pubmed', unique=True),
        {'comment': 'Contains all downloaded references before curator triage.',
     'schema': 'MULTI'}
    )

    ref_temp_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a temporary reference. Oracle sequence generated number.')
    pubmed: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='PMID of the reference from NCBI.')
    citation: Mapped[str] = mapped_column(VARCHAR(480), nullable=False, comment='Full citation, including authors, journal, title, etc.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    fulltext_url: Mapped[Optional[str]] = mapped_column(VARCHAR(480), comment='The URL to the full text paper, if available.')
    abstract: Mapped[Optional[str]] = mapped_column(VARCHAR(4000), comment='Abstract of the paper.')


class RefType(Base):
    __tablename__ = 'ref_type'
    __table_args__ = (
        PrimaryKeyConstraint('ref_type_no', name='ref_type_pk'),
        Index('ref_type_uk', 'source', 'ref_type', unique=True),
        {'comment': 'Contains NCBI and SGD codes for publication/reference types.',
     'schema': 'MULTI'}
    )

    ref_type_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a reference type. Oracle generated sequence number.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the reference type (Coded: NCBI, SGD).')
    ref_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Type of publication (NCBI PT) or SGD defined.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    ref_reftype: Mapped[list['RefReftype']] = relationship('RefReftype', back_populates='ref_type')


class RefUnlink(Base):
    __tablename__ = 'ref_unlink'
    __table_args__ = (
        PrimaryKeyConstraint('ref_unlink_no', name='ref_unlink_pk'),
        Index('ref_unlink_uk', 'pubmed', 'tab_name', 'primary_key', unique=True),
        {'comment': 'Contains references which should not be associated with a '
                'specific feature, but which should not be deleted from the '
                'database.',
     'schema': 'MULTI'}
    )

    ref_unlink_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an unlinked reference. Oracle sequence generated number.')
    pubmed: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='PubMed identifier for a reference that should not be associated with a given row in the database.')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Table name for which the reference needs to be unlinked.')
    primary_key: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Primary key of the row that should be unlinked from the reference')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')


class TabRule(Base):
    __tablename__ = 'tab_rule'
    __table_args__ = (
        PrimaryKeyConstraint('tab_rule_no', name='tab_rule_pk'),
        Index('tab_rule_uk', 'tab_name', unique=True),
        {'comment': 'Contains table-based business rules for the database.',
     'schema': 'MULTI'}
    )

    tab_rule_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a table rule. Oracle generated sequential number.')
    group_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The name or category for the table.')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Table name.')
    diagram_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Name of the schema diagram.')
    complex_rule: Mapped[Optional[str]] = mapped_column(VARCHAR(2000), comment='Any complex rules for the table, if any.')

    col_rule: Mapped[list['ColRule']] = relationship('ColRule', back_populates='tab_rule')


class Taxonomy(Base):
    __tablename__ = 'taxonomy'
    __table_args__ = (
        CheckConstraint("is_default_display in ('Y','N')", name='tax_is_default_display_ck'),
        PrimaryKeyConstraint('taxon_id', name='taxonomy_pk'),
        Index('taxonomy_uk', 'tax_term', unique=True),
        {'comment': 'This table stores taxonomy information from the NCBI.',
     'schema': 'MULTI'}
    )

    taxon_id: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a taxonomy term from NCBI.')
    tax_term: Mapped[str] = mapped_column(VARCHAR(240), nullable=False, comment='The taxonomy term itself (eg. Saccharomyces cerevisiae).')
    is_default_display: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Allowable values are Y or N.  Y indicates that the taxonomy term is used for default displays on SGD pages (eg. Homolog pull-down on a protein page).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    common_name: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment="The preferred common name field from NCBI. Multiple common names are separated by a '|'.")
    rank: Mapped[Optional[str]] = mapped_column(VARCHAR(20), comment='The rank of the term (NCBI rank field); for example, for Saccharomyces cerevisiae the rank = species.')

    blast_hit: Mapped[list['BlastHit']] = relationship('BlastHit', back_populates='taxon')
    pdb_sequence: Mapped[list['PdbSequence']] = relationship('PdbSequence', back_populates='taxon')
    tax_relationship: Mapped[list['TaxRelationship']] = relationship('TaxRelationship', foreign_keys='[TaxRelationship.child_taxon_id]', back_populates='child_taxon')
    tax_relationship: Mapped[list['TaxRelationship']] = relationship('TaxRelationship', foreign_keys='[TaxRelationship.parent_taxon_id]', back_populates='parent_taxon')
    tax_synonym: Mapped[list['TaxSynonym']] = relationship('TaxSynonym', back_populates='taxon')
    organism: Mapped[list['Organism']] = relationship('Organism', back_populates='taxon')
    

t_tmp10054 = Table(
    'tmp10054', Base.metadata,
    Column('feature_no', Integer),
    Column('go_annotation_no', Integer),
    Column('feature_name', VARCHAR(100)),
    Column('go_term', VARCHAR(1000)),
    Column('go_no', Integer),
    Column('go_id', VARCHAR(20)),
    Column('go_evidence', VARCHAR(10)),
    Column('subterms', Text),
    Column('ref_citation', VARCHAR(2000)),
    Column('ref_no', Integer),
    schema='MULTI'
)


t_tmp14015 = Table(
    'tmp14015', Base.metadata,
    Column('feature_no', Integer),
    Column('feature_name', VARCHAR(100)),
    Column('go_term', VARCHAR(1000)),
    Column('go_no', Integer),
    Column('go_id', VARCHAR(20)),
    Column('go_evidence', VARCHAR(10)),
    Column('subterms', Text),
    Column('ref_citation', VARCHAR(2000)),
    Column('ref_no', Integer),
    schema='MULTI'
)


t_tmp23704 = Table(
    'tmp23704', Base.metadata,
    Column('feature_no', Integer),
    Column('go_annotation_no', Integer),
    Column('feature_name', VARCHAR(100)),
    Column('go_term', VARCHAR(1000)),
    Column('go_no', Integer),
    Column('go_id', VARCHAR(20)),
    Column('go_evidence', VARCHAR(10)),
    Column('subterms', Text),
    Column('ref_citation', VARCHAR(2000)),
    Column('ref_no', Integer),
    schema='MULTI'
)


t_tmp6095 = Table(
    'tmp6095', Base.metadata,
    Column('feature_no', Integer),
    Column('go_annotation_no', Integer),
    Column('feature_name', VARCHAR(100)),
    Column('go_term', VARCHAR(1000)),
    Column('go_no', Integer),
    Column('go_id', VARCHAR(20)),
    Column('go_evidence', VARCHAR(10)),
    Column('subterms', Text),
    Column('ref_citation', VARCHAR(2000)),
    Column('ref_no', Integer),
    schema='MULTI'
)


class UpdateLog(Base):
    __tablename__ = 'update_log'
    __table_args__ = (
        PrimaryKeyConstraint('update_log_no', name='update_log_pk'),
        Index('update_log_i', 'tab_name', 'col_name', 'primary_key'),
        {'comment': 'Contains a row for each updated column in the database.',
     'schema': 'MULTI'}
    )

    update_log_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an entry in the update log. Oracle sequence generated number.')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Name of the table being updated.')
    col_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Name of the column being updated.')
    primary_key: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Primary key of the row that was updated.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    old_value: Mapped[Optional[str]] = mapped_column(VARCHAR(4000), comment='Old value.')
    new_value: Mapped[Optional[str]] = mapped_column(VARCHAR(4000), comment='New value.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='Reason why the data was updated.')


class Url(Base):
    __tablename__ = 'url'
    __table_args__ = (
        PrimaryKeyConstraint('url_no', name='url_pk'),
        Index('url_source_type_i', 'source', 'url_type'),
        Index('url_substitution_i', 'substitution_value'),
        Index('url_uk', 'url', unique=True),
        {'comment': 'Contains information about URLs linked to information in the '
                'database.',
     'schema': 'MULTI'}
    )

    url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier assigned to each URL.  Oracle sequence generated number')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Database or Institution providing the URL (Coded).')
    url_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Type of URL (Coded).')
    url: Mapped[str] = mapped_column(VARCHAR(480), nullable=False, comment='Actual URL of the particular site.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    substitution_value: Mapped[Optional[str]] = mapped_column(VARCHAR(30), comment='Table which contains the value that is substituted in the template URL.')

    coll_url: Mapped[list['CollUrl']] = relationship('CollUrl', back_populates='url')
    cv: Mapped[list['Cv']] = relationship('Cv', back_populates='url')
    dbxref_url: Mapped[list['DbxrefUrl']] = relationship('DbxrefUrl', back_populates='url')
    url_homology: Mapped[list['UrlHomology']] = relationship('UrlHomology', back_populates='url')
    web_display: Mapped[list['WebDisplay']] = relationship('WebDisplay', back_populates='url')
    ref_url: Mapped[list['RefUrl']] = relationship('RefUrl', back_populates='url')
    feat_url: Mapped[list['FeatUrl']] = relationship('FeatUrl', back_populates='url')


class WebMetadata(Base):
    __tablename__ = 'web_metadata'
    __table_args__ = (
        PrimaryKeyConstraint('web_metadata_no', name='web_metadata_pk'),
        Index('web_metedata_uk', 'application_name', 'tab_name', 'col_name', 'col_value', unique=True),
        {'comment': 'Contains information about what is displayed or searched on a web '
                'page.  Used in conjunction with the web_display table.',
     'schema': 'MULTI'}
    )

    web_metadata_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the web_metadata table. Oracle sequence generated number.')
    application_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Name of the web page or application program (Coded: Locus page, etc.).')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Table name from which the data is retrieved.')
    col_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Column name from which the data is retrieved.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who first entered the row into the database.')
    col_value: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Column value (usually a coded value) to which the web page display is restricted.')


class BlastHit(Base):
    __tablename__ = 'blast_hit'
    __table_args__ = (
        ForeignKeyConstraint(['taxon_id'], ['MULTI.taxonomy.taxon_id'], name='bh_tax_fk'),
        PrimaryKeyConstraint('blast_hit_no', name='blast_hit_pk'),
        Index('bh_tax_fk_i', 'taxon_id'),
        Index('blast_hit_uk', 'identifier', 'source', unique=True),
        {'comment': 'This table stores information about Blast hits.',
     'schema': 'MULTI'}
    )

    blast_hit_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a blast hit. Oracle generated sequence number.')
    identifier: Mapped[str] = mapped_column(VARCHAR(100), nullable=False, comment='The idenitifer for the blast hit; for the nr data, will be the nr title line, for example, gi|17945344|gb|AAL48728.1|')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The source of the data (Coded: nr, etc.).')
    length: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='The length of the sequence in amino acids.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    taxon_id: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='Unique identifier for a taxonomy term assigned by NCBI. Foreign key to the taxonomy table.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(2000), comment='The description of the blast hit.')

    taxon: Mapped[Optional['Taxonomy']] = relationship('Taxonomy', back_populates='blast_hit')
    blast_alignment: Mapped[list['BlastAlignment']] = relationship('BlastAlignment', back_populates='blast_hit')


class ColRule(Base):
    __tablename__ = 'col_rule'
    __table_args__ = (
        ForeignKeyConstraint(['tab_name'], ['MULTI.tab_rule.tab_name'], ondelete='CASCADE', name='colrule_tabrule_fk'),
        PrimaryKeyConstraint('col_rule_no', name='col_rule_pk'),
        Index('col_rule_uk', 'tab_name', 'col_name', unique=True),
        {'comment': 'Contains column-based rules for the database.', 'schema': 'MULTI'}
    )

    col_rule_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a column rule. Oracle generated sequential number.')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Table to which the column belongs.')
    col_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Column name.')
    col_order: Mapped[float] = mapped_column(NUMBER(asdecimal=False), nullable=False, comment='Order in which the column should be listed.')
    col_rule: Mapped[Optional[str]] = mapped_column(VARCHAR(4000), comment='Rules associated with this column.')
    col_sequence_name: Mapped[Optional[str]] = mapped_column(VARCHAR(30), comment='Name of the Oracle sequence, if any, associated with this column.')

    tab_rule: Mapped['TabRule'] = relationship('TabRule', back_populates='col_rule')


class CollKw(Base):
    __tablename__ = 'coll_kw'
    __table_args__ = (
        ForeignKeyConstraint(['colleague_no'], ['MULTI.colleague.colleague_no'], ondelete='CASCADE', name='coll_kw_coll_fk'),
        ForeignKeyConstraint(['keyword_no'], ['MULTI.keyword.keyword_no'], ondelete='CASCADE', name='coll_kw_kw_fk'),
        PrimaryKeyConstraint('coll_kw_no', name='coll_kw_pk'),
        Index('coll_kw_kw_fk_i', 'keyword_no'),
        Index('coll_kw_uk', 'colleague_no', 'keyword_no', unique=True),
        {'comment': 'Linking table between the colleague and keyword tables.',
     'schema': 'MULTI'}
    )

    coll_kw_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the coll_kw table. Oracle sequence generated number.')
    colleague_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a colleague. Foreign key to the colleague table.')
    keyword_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a keyword. Foreign key to the keyword table.')

    colleague: Mapped['Colleague'] = relationship('Colleague', back_populates='coll_kw')
    keyword: Mapped['Keyword'] = relationship('Keyword', back_populates='coll_kw')


class CollRelationship(Base):
    __tablename__ = 'coll_relationship'
    __table_args__ = (
        ForeignKeyConstraint(['associate_no'], ['MULTI.colleague.colleague_no'], ondelete='CASCADE', name='collrel_assoc_fk'),
        ForeignKeyConstraint(['colleague_no'], ['MULTI.colleague.colleague_no'], ondelete='CASCADE', name='collrel_coll_fk'),
        PrimaryKeyConstraint('coll_relationship_no', name='coll_relationship_pk'),
        Index('coll_relationship_uk', 'colleague_no', 'associate_no', 'relationship_type', unique=True),
        Index('collrel_assoc_fk_i', 'associate_no'),
        {'comment': 'Contains relationships between colleagues.', 'schema': 'MULTI'}
    )

    coll_relationship_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a colleague relationship. Oracle sequence generated number.')
    colleague_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a colleague who is an associate of another colleague. Foreign key to the colleague table.')
    associate_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a colleague who is an associate of another colleague. Foreign key to the colleague table.')
    relationship_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Type of colleague relationship (Coded: Lab member, Associate).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')

    colleague: Mapped['Colleague'] = relationship('Colleague', foreign_keys=[associate_no], back_populates='coll_relationship')
    colleague: Mapped['Colleague'] = relationship('Colleague', foreign_keys=[colleague_no], back_populates='coll_relationship')


class CollUrl(Base):
    __tablename__ = 'coll_url'
    __table_args__ = (
        ForeignKeyConstraint(['colleague_no'], ['MULTI.colleague.colleague_no'], ondelete='CASCADE', name='coll_url_coll_fk'),
        ForeignKeyConstraint(['url_no'], ['MULTI.url.url_no'], ondelete='CASCADE', name='coll_url_url_fk'),
        PrimaryKeyConstraint('coll_url_no', name='coll_url_pk'),
        Index('coll_url_uk', 'colleague_no', 'url_no', unique=True),
        Index('coll_url_url_fk_i', 'url_no'),
        {'comment': 'Linking table between the colleague and url tables.',
     'schema': 'MULTI'}
    )

    coll_url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the coll_url table. Oracles sequence generated number.')
    colleague_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a colleague. Foreign key to the colleague table.')
    url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Unique identifier assigned to each URL.  Foreign key to the url table.')

    colleague: Mapped['Colleague'] = relationship('Colleague', back_populates='coll_url')
    url: Mapped['Url'] = relationship('Url', back_populates='coll_url')


class ColleagueRemark(Base):
    __tablename__ = 'colleague_remark'
    __table_args__ = (
        ForeignKeyConstraint(['colleague_no'], ['MULTI.colleague.colleague_no'], ondelete='CASCADE', name='collrem_coll_fk'),
        PrimaryKeyConstraint('colleague_remark_no', name='colleague_remark_pk'),
        Index('colleague_remark_uk', 'remark_type', 'remark', 'colleague_no', unique=True),
        Index('collrem_coll_fk_i', 'colleague_no'),
        {'comment': 'Contains remarks or notes submitted by colleagues.',
     'schema': 'MULTI'}
    )

    colleague_remark_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a colleague remark. Oracle sequence generated number.')
    colleague_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a colleague. Foreign key to the colleague table.')
    remark: Mapped[str] = mapped_column(VARCHAR(1500), nullable=False, comment='Text of public remarks, research interests supplied by a colleague.')
    remark_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Type of colleague remark (coded: Announcement, Research Interest).')
    remark_date: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Date the remark was supplied to SGD by a colleague.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')

    colleague: Mapped['Colleague'] = relationship('Colleague', back_populates='colleague_remark')


class Cv(Base):
    __tablename__ = 'cv'
    __table_args__ = (
        ForeignKeyConstraint(['url_no'], ['MULTI.url.url_no'], name='cv_url_fk'),
        PrimaryKeyConstraint('cv_no', name='cv_pk'),
        Index('cv_uk', 'cv_name', unique=True),
        Index('cv_url_fk_i', 'url_no'),
        {'comment': 'Contains the name and description of a whole CV (e.g., ChEBI) or '
                'individual namespaces that comprise a CV (e.g., experiment_type, '
                'mutant_type, qualifier, and observable namespaces that comprise '
                'the Yeast Phenotype Ontology).',
     'schema': 'MULTI'}
    )

    cv_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a controlled vocabulary. Oracle sequence generated number.')
    cv_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Unique name of the controlled vocabulary.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    url_no: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='Assigned unique identifier assigned to each URL.  Foreign key to the URL table.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='Full description of the controlled vocabulary.')

    url: Mapped[Optional['Url']] = relationship('Url', back_populates='cv')
    cv_term: Mapped[list['CvTerm']] = relationship('CvTerm', back_populates='cv')


class DbxrefHomology(Base):
    __tablename__ = 'dbxref_homology'
    __table_args__ = (
        ForeignKeyConstraint(['dbxref_no'], ['MULTI.dbxref.dbxref_no'], ondelete='CASCADE', name='dbxref_homology_dbxref_fk'),
        ForeignKeyConstraint(['homology_group_no'], ['MULTI.homology_group.homology_group_no'], ondelete='CASCADE', name='dbxref_homology_hg_fk'),
        PrimaryKeyConstraint('dbxref_homology_no', name='dbxref_homology_pk'),
        Index('dbxref_homology_hg_fk_i', 'homology_group_no'),
        Index('dbxref_homology_uk', 'dbxref_no', 'homology_group_no', unique=True),
        {'comment': 'Contains data about external orthologs.', 'schema': 'MULTI'}
    )

    dbxref_homology_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an external ortholog. Oracle generated sequence number.')
    dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a dbxref. FK to the DBXREF table.')
    homology_group_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a homology group. FK to the HOMOLOGY_GROUP table.')
    name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Name associated with the ortholog, e.g., dbxref_id or feature_name.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the records was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    dbxref: Mapped['Dbxref'] = relationship('Dbxref', back_populates='dbxref_homology')
    homology_group: Mapped['HomologyGroup'] = relationship('HomologyGroup', back_populates='dbxref_homology')


class DbxrefUrl(Base):
    __tablename__ = 'dbxref_url'
    __table_args__ = (
        ForeignKeyConstraint(['dbxref_no'], ['MULTI.dbxref.dbxref_no'], ondelete='CASCADE', name='dbxref_url_dbxref_fk'),
        ForeignKeyConstraint(['url_no'], ['MULTI.url.url_no'], ondelete='CASCADE', name='dbxref_url_url_fk'),
        PrimaryKeyConstraint('dbxref_url_no', name='dbxref_url_pk'),
        Index('dbxref_url_uk', 'dbxref_no', 'url_no', unique=True),
        Index('dbxref_url_url_fk_i', 'url_no'),
        {'comment': 'Linking table between the dbxref and url tables.',
     'schema': 'MULTI'}
    )

    dbxref_url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the dbxref_url table. Oracle sequence generated number.')
    dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an database cross reference. Foreign key to the dbxref table.')
    url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier assigned for a URL.  Foreign key to the url table.')

    dbxref: Mapped['Dbxref'] = relationship('Dbxref', back_populates='dbxref_url')
    url: Mapped['Url'] = relationship('Url', back_populates='dbxref_url')


class ExptExptprop(Base):
    __tablename__ = 'expt_exptprop'
    __table_args__ = (
        ForeignKeyConstraint(['experiment_no'], ['MULTI.experiment.experiment_no'], ondelete='CASCADE', name='expt_exptprop_expt_fk'),
        ForeignKeyConstraint(['expt_property_no'], ['MULTI.expt_property.expt_property_no'], ondelete='CASCADE', name='expt_exptprop_exptprop_fk'),
        PrimaryKeyConstraint('expt_exptprop_no', name='expt_exptprop_pk'),
        Index('expt_exptprop_expt_fk_i', 'experiment_no'),
        Index('expt_exptprop_uk', 'expt_property_no', 'experiment_no', unique=True),
        {'comment': 'Linking table between the experiment and experiment_property '
                'tables.',
     'schema': 'MULTI'}
    )

    expt_exptprop_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the expt_exptprop table. Linking table between the experiment and expt_property tables.')
    expt_property_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an experiment property. FK to the expt_property table.')
    experiment_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an experiment.  FK to the experiment table.')

    experiment: Mapped['Experiment'] = relationship('Experiment', back_populates='expt_exptprop')
    expt_property: Mapped['ExptProperty'] = relationship('ExptProperty', back_populates='expt_exptprop')


class GoGosyn(Base):
    __tablename__ = 'go_gosyn'
    __table_args__ = (
        ForeignKeyConstraint(['go_no'], ['MULTI.go.go_no'], ondelete='CASCADE', name='go_gosyn_go_fk'),
        ForeignKeyConstraint(['go_synonym_no'], ['MULTI.go_synonym.go_synonym_no'], ondelete='CASCADE', name='go_gosyn_gosyn_fk'),
        PrimaryKeyConstraint('go_gosyn_no', name='go_gosyn_pk'),
        Index('go_gosyn_gosyn_fk_i', 'go_synonym_no'),
        Index('go_gosyn_uk', 'go_no', 'go_synonym_no', unique=True),
        {'comment': 'Linking table between the go and go_synonym tables.',
     'schema': 'MULTI'}
    )

    go_gosyn_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the go_gosyn table. Oracle sequence generated number.')
    go_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier assigned to a goid. Foreign key to the go table.')
    go_synonym_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a GO synonym. Foreign key to the go_synonym table.')

    go: Mapped['Go'] = relationship('Go', back_populates='go_gosyn')
    go_synonym: Mapped['GoSynonym'] = relationship('GoSynonym', back_populates='go_gosyn')


class GoPath(Base):
    __tablename__ = 'go_path'
    __table_args__ = (
        ForeignKeyConstraint(['ancestor_go_no'], ['MULTI.go.go_no'], name='ancestor_gopath_fk'),
        ForeignKeyConstraint(['child_go_no'], ['MULTI.go.go_no'], name='child_gopath_fk'),
        PrimaryKeyConstraint('go_path_no', name='go_path_pk'),
        Index('ancestor_child_gono_i', 'ancestor_go_no', 'child_go_no'),
        Index('child_gopath_fk_i', 'child_go_no'),
        Index('go_path_uk', 'ancestor_path', 'child_go_no', 'generation', unique=True),
        {'comment': 'Contains the structure of the Gene Onotology, the parent:child '
                'relationships between the terms.',
     'schema': 'MULTI'}
    )

    go_path_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a go path. Oracle generated sequence number.')
    ancestor_go_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier assigned to the ancestor goid. Foreign key to the go table.')
    child_go_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier assigned for a child goid. Foreign key to the go table.')
    generation: Mapped[float] = mapped_column(NUMBER(2, 0, False), nullable=False, comment='The number of generations between the parent and child term; for example, for a  grandchild:grandparent relationship, the generation = 2.')
    ancestor_path: Mapped[str] = mapped_column(VARCHAR(240), nullable=False, comment='A list of all GOIDs corresponding to all the GO terms in between the ancestor and the child, separated by ::.')
    relationship_type: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='A coded value to describe the type of relationship between the parent and child; valid values are "part of" and "is a".')

    go: Mapped['Go'] = relationship('Go', foreign_keys=[ancestor_go_no], back_populates='go_path')
    go: Mapped['Go'] = relationship('Go', foreign_keys=[child_go_no], back_populates='go_path')


class GoSet(Base):
    __tablename__ = 'go_set'
    __table_args__ = (
        ForeignKeyConstraint(['go_no'], ['MULTI.go.go_no'], name='goset_go_fk'),
        PrimaryKeyConstraint('go_set_no', name='go_set_pk'),
        Index('go_set_uk', 'go_no', 'go_set_name', unique=True),
        {'comment': 'Stores information about groups of GOIDs.  Used for defining GO '
                'Slims.',
     'schema': 'MULTI'}
    )

    go_set_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a go set. Oracle sequence generated number.')
    go_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier assigned to a goid. Foreign key to the go table.')
    go_set_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The name of the go set (Coded).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    go: Mapped['Go'] = relationship('Go', back_populates='go_set')


class InteractPheno(Base):
    __tablename__ = 'interact_pheno'
    __table_args__ = (
        ForeignKeyConstraint(['interaction_no'], ['MULTI.interaction.interaction_no'], ondelete='CASCADE', name='int_pheno_int_fk'),
        ForeignKeyConstraint(['phenotype_no'], ['MULTI.phenotype.phenotype_no'], ondelete='CASCADE', name='int_pheno_pheno_fk'),
        PrimaryKeyConstraint('interact_pheno_no', name='interact_pheno_pk'),
        Index('int_pheno_pheno_fk_i', 'phenotype_no'),
        Index('interact_pheno_uk', 'interaction_no', 'phenotype_no', unique=True),
        {'comment': 'Linking table between the interaction and phenotype tables.',
     'schema': 'MULTI'}
    )

    interact_pheno_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the interact_pheno. Oracle sequence generated number.')
    interaction_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an interaction. Foreign key to the interaction table.')
    phenotype_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Unique identifier for a phenotype.  Foreign key to the phenotype table.')

    interaction: Mapped['Interaction'] = relationship('Interaction', back_populates='interact_pheno')
    phenotype: Mapped['Phenotype'] = relationship('Phenotype', back_populates='interact_pheno')


class NoteLink(Base):
    __tablename__ = 'note_link'
    __table_args__ = (
        ForeignKeyConstraint(['note_no'], ['MULTI.note.note_no'], ondelete='CASCADE', name='nl_note_fk'),
        PrimaryKeyConstraint('note_link_no', name='note_link_pk'),
        Index('nl_note_fk_i', 'note_no'),
        Index('note_link_uk', 'tab_name', 'primary_key', 'note_no', unique=True),
        {'comment': 'This table is used to link a note with any row in the database.',
     'schema': 'MULTI'}
    )

    note_link_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a note link. Oracle generated sequence number.')
    note_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a note. Foreign key to the note table.')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='The table name of the row to which the note refers.')
    primary_key: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='The primary key of the row to which the note refers.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    note: Mapped['Note'] = relationship('Note', back_populates='note_link')


class PdbSequence(Base):
    __tablename__ = 'pdb_sequence'
    __table_args__ = (
        ForeignKeyConstraint(['taxon_id'], ['MULTI.taxonomy.taxon_id'], name='pdbseq_tax_fk'),
        PrimaryKeyConstraint('pdb_sequence_no', name='pdb_sequence_pk'),
        Index('pdb_sequence_uk', 'sequence_name', unique=True),
        Index('pdbseq_tax_fk_i', 'taxon_id'),
        Index('upper_pdb_sequence_name_i'),
        {'comment': 'Contains information about a biological sequence used for finding '
                'PDB homologs.',
     'schema': 'MULTI'}
    )

    pdb_sequence_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a PDB sequence. Oracle sequence generated number.')
    sequence_name: Mapped[str] = mapped_column(VARCHAR(50), nullable=False, comment='Name of the PDB sequence.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the PDB sequence. Coded.')
    sequence_length: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Length of the sequence.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    taxon_id: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='Unique identifier for a taxonomy term assigned by NCBI. Foreign key to the taxonomy table.')
    note: Mapped[Optional[str]] = mapped_column(VARCHAR(960), comment='Comment about the PDB sequence.')

    taxon: Mapped[Optional['Taxonomy']] = relationship('Taxonomy', back_populates='pdb_sequence')
    pdb_alignment: Mapped[list['PdbAlignment']] = relationship('PdbAlignment', foreign_keys='[PdbAlignment.query_seq_no]', back_populates='pdb_sequence')
    pdb_alignment: Mapped[list['PdbAlignment']] = relationship('PdbAlignment', foreign_keys='[PdbAlignment.target_seq_no]', back_populates='pdb_sequence')


class Reference(Base):
    __tablename__ = 'reference'
    __table_args__ = (
        ForeignKeyConstraint(['book_no'], ['MULTI.book.book_no'], name='ref_book_fk'),
        ForeignKeyConstraint(['journal_no'], ['MULTI.journal.journal_no'], name='ref_jour_fk'),
        PrimaryKeyConstraint('reference_no', name='reference_pk'),
        Index('ref_book_fk_i', 'book_no'),
        Index('ref_dbxref_id_uk', 'dbxref_id', unique=True),
        Index('ref_jour_fk_i', 'journal_no'),
        Index('ref_pubmed_i', 'pubmed'),
        Index('reference_uk', 'citation', unique=True),
        Index('upper_ref_dbxref_id_i'),
        {'comment': 'Contains references for data in the database, which can be from a '
                'book, journal, personal communication, etc.',
     'schema': 'MULTI'}
    )

    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a reference. Oracle sequence generated number.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the reference (Coded: PubMed, Curator, etc.)')
    status: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Status of the reference (coded: published, in press, etc.).')
    pdf_status: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Indicates whether there is a full-text paper for that reference. Coded value: Y(yes, has PDF), YT(yes, PDF text conversion), YF(yes PDF, failed text conversion), N(no PDF), NAA(no PDF automatically), NAM(no PDF manually), NAP(not applicable).')
    dbxref_id: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Primary DBXREF_ID for the reference.')
    citation: Mapped[str] = mapped_column(VARCHAR(480), nullable=False, comment='Full citation of the reference.')
    year: Mapped[float] = mapped_column(NUMBER(4, 0, False), nullable=False, comment='Year of publication or communication.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    curation_status: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Status of the reference in the curation process.  Coded value.')
    pubmed: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='PubMed ID from NCBI.')
    date_published: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Date the reference was published.')
    date_revised: Mapped[Optional[float]] = mapped_column(NUMBER(8, 0, False), comment='Date the reference was revised.')
    issue: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Issue of the publication.')
    page: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Pagination of the reference (characters separated by a dash).')
    volume: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Volume of the  publication.')
    title: Mapped[Optional[str]] = mapped_column(VARCHAR(400), comment='Title of the publication or communication.')
    journal_no: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='Assigned unique identifier for a journal. Foreign key to the journal table.')
    book_no: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='Assigned unique identifier for a book. Foreign key to the book table.')

    book: Mapped[Optional['Book']] = relationship('Book', back_populates='reference')
    journal: Mapped[Optional['Journal']] = relationship('Journal', back_populates='reference')
    author_editor: Mapped[list['AuthorEditor']] = relationship('AuthorEditor', back_populates='reference')
    dbxref_ref: Mapped[list['DbxrefRef']] = relationship('DbxrefRef', back_populates='reference')
    ref_link: Mapped[list['RefLink']] = relationship('RefLink', back_populates='reference')
    ref_property: Mapped[list['RefProperty']] = relationship('RefProperty', back_populates='reference')
    ref_reftype: Mapped[list['RefReftype']] = relationship('RefReftype', back_populates='reference')
    ref_relationship: Mapped[list['RefRelationship']] = relationship('RefRelationship', foreign_keys='[RefRelationship.reference_no]', back_populates='reference')
    ref_relationship: Mapped[list['RefRelationship']] = relationship('RefRelationship', foreign_keys='[RefRelationship.related_ref_no]', back_populates='reference')
    ref_url: Mapped[list['RefUrl']] = relationship('RefUrl', back_populates='reference')
    go_ref: Mapped[list['GoRef']] = relationship('GoRef', back_populates='reference')


class TaxRelationship(Base):
    __tablename__ = 'tax_relationship'
    __table_args__ = (
        ForeignKeyConstraint(['child_taxon_id'], ['MULTI.taxonomy.taxon_id'], ondelete='CASCADE', name='taxrel_child_tax_fk'),
        ForeignKeyConstraint(['parent_taxon_id'], ['MULTI.taxonomy.taxon_id'], ondelete='CASCADE', name='taxrel_parent_tax_fk'),
        PrimaryKeyConstraint('tax_relationship_no', name='tax_relationship_pk'),
        Index('tax_relationship_uk', 'parent_taxon_id', 'child_taxon_id', unique=True),
        Index('taxrel_child_tax_fk_i', 'child_taxon_id'),
        {'comment': 'This table stores the structure of taxonomy classifications '
                'provided by NCBI.',
     'schema': 'MULTI'}
    )

    tax_relationship_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a taxonomy relationship. Oracle sequence generated number.')
    parent_taxon_id: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Primary key, unique identifier for a taxonomy term; ID from NCBI, not an Oracle-generated sequence number.')
    child_taxon_id: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Primary key, unique identifier for a taxonomy term; ID from NCBI, not an Oracle-generated sequence number.')
    generation: Mapped[float] = mapped_column(NUMBER(2, 0, False), nullable=False, comment='The generation between the parent and the child term.  For example, a direct parent:child relationship would be generation = 1, while a grandparent:grandchild relationship would be generation = 2.')

    child_taxon: Mapped['Taxonomy'] = relationship('Taxonomy', foreign_keys=[child_taxon_id], back_populates='tax_relationship')
    parent_taxon: Mapped['Taxonomy'] = relationship('Taxonomy', foreign_keys=[parent_taxon_id], back_populates='tax_relationship')


class TaxSynonym(Base):
    __tablename__ = 'tax_synonym'
    __table_args__ = (
        ForeignKeyConstraint(['taxon_id'], ['MULTI.taxonomy.taxon_id'], ondelete='CASCADE', name='taxsyn_tax_fk'),
        PrimaryKeyConstraint('tax_synonym_no', name='tax_synonym_pk'),
        Index('tax_synonym_uk', 'taxon_id', 'tax_synonym', unique=True),
        {'comment': 'This table store the synonyms of the taxonomy terms provided by '
                'NCBI.',
     'schema': 'MULTI'}
    )

    tax_synonym_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a taxonomy synonym.  Oracle sequence generated number.')
    taxon_id: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a taxonomy term from NCBI. Foreign key to the taxonomy table.')
    tax_synonym: Mapped[str] = mapped_column(VARCHAR(240), nullable=False, comment='Synonym of the taxonomy term; note that it may not be unique (eg. Yeast can be a synonym for Saccharomyce cerevisiae and pombe).  The Other Name field at NCBI.')

    taxon: Mapped['Taxonomy'] = relationship('Taxonomy', back_populates='tax_synonym')


class UrlHomology(Base):
    __tablename__ = 'url_homology'
    __table_args__ = (
        ForeignKeyConstraint(['homology_group_no'], ['MULTI.homology_group.homology_group_no'], ondelete='CASCADE', name='url_homology_hg_fk'),
        ForeignKeyConstraint(['url_no'], ['MULTI.url.url_no'], ondelete='CASCADE', name='url_homology_url_fk'),
        PrimaryKeyConstraint('url_homology_no', name='url_homology_pk'),
        Index('url_homology_hg_fk_i', 'homology_group_no'),
        Index('url_homology_uk', 'url_no', 'homology_group_no', unique=True),
        {'comment': 'Linking table between URL and HOMOLOGY_GROUP tables.',
     'schema': 'MULTI'}
    )

    url_homology_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the URL_HOMOLOGY table. Oracle generated sequence number.')
    url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a URL. FK to the URL table.')
    homology_group_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a homology group. FK to the HOMOLOGY_GROUP table.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    homology_group: Mapped['HomologyGroup'] = relationship('HomologyGroup', back_populates='url_homology')
    url: Mapped['Url'] = relationship('Url', back_populates='url_homology')


class WebDisplay(Base):
    __tablename__ = 'web_display'
    __table_args__ = (
        CheckConstraint("is_default in ('Y','N')", name='webd_is_default_ck'),
        ForeignKeyConstraint(['url_no'], ['MULTI.url.url_no'], ondelete='CASCADE', name='webd_url_fk'),
        PrimaryKeyConstraint('web_display_no', name='web_display_pk'),
        Index('web_display_label_i', 'label_name', 'label_type', 'is_default'),
        Index('web_display_uk', 'web_page_name', 'label_location', 'url_no', unique=True),
        Index('webd_url_fk_i', 'url_no'),
        {'comment': 'Contains information about how URL links are displayed on web '
                'pages.',
     'schema': 'MULTI'}
    )

    web_display_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a web display location. Oracle sequence generated number.')
    url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier assigned to each URL.  Foreign key to the url table.')
    web_page_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Name of the web page containing the URL link (Coded: locus, protein, etc.)')
    label_location: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Location on the web page of the URL link.')
    label_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Type of URL link (Coded: Pull-down, Text, Gif)')
    label_name: Mapped[str] = mapped_column(VARCHAR(100), nullable=False, comment='Name used for the URL link.')
    is_default: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Whether this URL link is the default link in a pull down menu.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    url: Mapped['Url'] = relationship('Url', back_populates='web_display')


class CvTerm(Base):
    __tablename__ = 'cv_term'
    __table_args__ = (
        ForeignKeyConstraint(['cv_no'], ['MULTI.cv.cv_no'], name='cvterm_cv_fk'),
        PrimaryKeyConstraint('cv_term_no', name='cv_term_pk'),
        Index('cv_term_uk', 'term_name', 'cv_no', unique=True),
        Index('cvterm_cv_fk_i', 'cv_no'),
        {'comment': 'Contains the individual terms for a specific controlled '
                'vocabulary.  The same term name can be used in multiple '
                'controlled vocabularies.',
     'schema': 'MULTI'}
    )

    cv_term_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a controlled vocabulary term. Oracle sequence generated number.')
    cv_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a controlled vocabulary. Oracle sequence generated number.')
    term_name: Mapped[str] = mapped_column(VARCHAR(1024), nullable=False, comment='The name of the controlled vocabulary term (e.g., go term, phenotype observable term, etc.)')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    dbxref_id: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Identifier assigned by another source, e.g., GOID.')
    cvterm_definition: Mapped[Optional[str]] = mapped_column(VARCHAR(2900), comment='Definition of the controlled vocabulary term.')

    cv: Mapped['Cv'] = relationship('Cv', back_populates='cv_term')
    cvterm_dbxref: Mapped[list['CvtermDbxref']] = relationship('CvtermDbxref', back_populates='cv_term')
    cvterm_group: Mapped[list['CvtermGroup']] = relationship('CvtermGroup', back_populates='cv_term')
    cvterm_path: Mapped[list['CvtermPath']] = relationship('CvtermPath', foreign_keys='[CvtermPath.ancestor_cv_term_no]', back_populates='cv_term')
    cvterm_path: Mapped[list['CvtermPath']] = relationship('CvtermPath', foreign_keys='[CvtermPath.child_cv_term_no]', back_populates='cv_term')
    cvterm_relationship: Mapped[list['CvtermRelationship']] = relationship('CvtermRelationship', foreign_keys='[CvtermRelationship.child_cv_term_no]', back_populates='cv_term')
    cvterm_relationship: Mapped[list['CvtermRelationship']] = relationship('CvtermRelationship', foreign_keys='[CvtermRelationship.parent_cv_term_no]', back_populates='cv_term')
    cvterm_synonym: Mapped[list['CvtermSynonym']] = relationship('CvtermSynonym', back_populates='cv_term')


class Organism(Base):
    __tablename__ = 'organism'
    __table_args__ = (
        ForeignKeyConstraint(['parent_organism_no'], ['MULTI.organism.organism_no'], name='parent_organism_fk'),
        ForeignKeyConstraint(['taxon_id'], ['MULTI.taxonomy.taxon_id'], name='organism_tax_fk'),
        PrimaryKeyConstraint('organism_no', name='organism_pk'),
        Index('organism_abbrev_uk', 'organism_abbrev', unique=True),
        Index('organism_name_uk', 'organism_name', unique=True),
        Index('organism_tax_fk_i', 'taxon_id'),
        Index('parent_organism_fk_i', 'parent_organism_no'),
        {'comment': 'Contains information about organisms contained in the database.',
     'schema': 'MULTI'}
    )

    organism_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an organism. Oracle generated sequence number.')
    organism_name: Mapped[str] = mapped_column(VARCHAR(240), nullable=False, comment='Full name of the organism.')
    organism_abbrev: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Standard abbreviation for the organism used for file names and other applications.')
    taxon_id: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='The NCBI taxon_id if available for this organism. Link to the TAXONOMY table.')
    taxonomic_rank: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Taxonomic rank of the organism, e.g., Species, Genus, etc. Coded.')
    organism_order: Mapped[float] = mapped_column(NUMBER(3, 0, False), nullable=False, comment='Display order of the organism within species and/or within strains.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    parent_organism_no: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='Parent organism_no for this organism (e.g., parent_organism_no for a species is the genus organism_no). FK to the ORGANISM table.')
    common_name: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='The common name for the organism.')

    organism: Mapped[Optional['Organism']] = relationship('Organism', remote_side=[organism_no], back_populates='organism_reverse')
    organism_reverse: Mapped[list['Organism']] = relationship('Organism', remote_side=[parent_organism_no], back_populates='organism')
    taxon: Mapped['Taxonomy'] = relationship('Taxonomy', back_populates='organism')
    feature: Mapped[list['Feature']] = relationship('Feature', back_populates='organism')
    genome_version: Mapped[list['GenomeVersion']] = relationship('GenomeVersion', back_populates='organism')


class PdbAlignment(Base):
    __tablename__ = 'pdb_alignment'
    __table_args__ = (
        ForeignKeyConstraint(['query_seq_no'], ['MULTI.pdb_sequence.pdb_sequence_no'], name='pdbalign_query_seq_fk'),
        ForeignKeyConstraint(['target_seq_no'], ['MULTI.pdb_sequence.pdb_sequence_no'], name='pdbalign_target_seq_fk'),
        PrimaryKeyConstraint('pdb_alignment_no', name='pdb_alignment_pk'),
        Index('pdb_alignment_uk', 'query_seq_no', 'target_seq_no', unique=True),
        Index('pdbalign_target_seq_fk_i', 'target_seq_no'),
        {'comment': 'Contains information about how two sequences (yeast and a PDB '
                'homolog) are aligned.',
     'schema': 'MULTI'}
    )

    pdb_alignment_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a PDB alignment. Oracle sequence generated number.')
    query_seq_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for the query sequence. Foreign key to the pdb_sequence table.')
    target_seq_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a target sequence. Foreign key to the pdb_sequence table.')
    method: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Method used to align the sequences. Coded.')
    matrix: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Matrix used to align the sequences. Coded.')
    query_align_start_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Query alignment start basepair coordinate.')
    query_align_stop_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Query alignment stop basepair coordinate.')
    target_align_start_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Target alignment start basepair coordinate.')
    target_align_stop_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Target alignment stop basepair coordinate.')
    pct_aligned: Mapped[decimal.Decimal] = mapped_column(NUMBER(5, 2, True), nullable=False, comment='Percent alignment.')
    pct_identical: Mapped[decimal.Decimal] = mapped_column(NUMBER(5, 2, True), nullable=False, comment='Percent identity.')
    pct_similar: Mapped[decimal.Decimal] = mapped_column(NUMBER(5, 2, True), nullable=False, comment='Percent similarity.')
    score: Mapped[decimal.Decimal] = mapped_column(NUMBER(8, 3, True), nullable=False, comment='Alignment score.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    pdb_sequence: Mapped['PdbSequence'] = relationship('PdbSequence', foreign_keys=[query_seq_no], back_populates='pdb_alignment')
    pdb_sequence: Mapped['PdbSequence'] = relationship('PdbSequence', foreign_keys=[target_seq_no], back_populates='pdb_alignment')
    pdb_alignment_sequence: Mapped[list['PdbAlignmentSequence']] = relationship('PdbAlignmentSequence', back_populates='pdb_alignment')


class Abstract(Reference):
    __tablename__ = 'abstract'
    __table_args__ = (
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='abstract_ref_fk'),
        PrimaryKeyConstraint('reference_no', name='abstract_pk'),
        {'comment': 'Contains reference abstracts from published articles or meetings.',
     'schema': 'MULTI'}
    )

    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a reference. Foreign key to the reference table.')
    abstract: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='Abstract for a reference.')


class AuthorEditor(Base):
    __tablename__ = 'author_editor'
    __table_args__ = (
        ForeignKeyConstraint(['author_no'], ['MULTI.author.author_no'], ondelete='CASCADE', name='auth_ed_auth_fk'),
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='auth_ed_ref_fk'),
        PrimaryKeyConstraint('author_editor_no', name='author_editor_pk'),
        Index('auth_ed_auth_fk_i', 'author_no'),
        Index('author_editor_uk', 'reference_no', 'author_no', 'author_order', unique=True),
        {'comment': 'Contains information about the type and order of authors for a '
                'given reference.',
     'schema': 'MULTI'}
    )

    author_editor_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for each author or editor for a reference. Oracle sequence generated number.')
    author_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an author. Foreign key to the author table.')
    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a reference. Foreign key to the reference table.')
    author_order: Mapped[float] = mapped_column(NUMBER(4, 0, False), nullable=False, comment='Order in which an author is in the list of authors for a particular reference.')
    author_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Type of author (coded: Author, Editor).')

    author: Mapped['Author'] = relationship('Author', back_populates='author_editor')
    reference: Mapped['Reference'] = relationship('Reference', back_populates='author_editor')


class DbxrefRef(Base):
    __tablename__ = 'dbxref_ref'
    __table_args__ = (
        ForeignKeyConstraint(['dbxref_no'], ['MULTI.dbxref.dbxref_no'], ondelete='CASCADE', name='dbxref_ref_dbxref_fk'),
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='dbxref_ref_ref_fk'),
        PrimaryKeyConstraint('dbxref_ref_no', name='dbxref_ref_pk'),
        Index('dbxref_ref_ref_fk_i', 'reference_no'),
        Index('dbxref_ref_uk', 'dbxref_no', 'reference_no', unique=True),
        {'comment': 'Linking table between the dbxref and reference tables.',
     'schema': 'MULTI'}
    )

    dbxref_ref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the dbxref_ref table. Oracle sequence generated number.')
    dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an database cross reference. Foreign key to the dbxref table.')
    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a reference. Foreign key to the reference table.')

    dbxref: Mapped['Dbxref'] = relationship('Dbxref', back_populates='dbxref_ref')
    reference: Mapped['Reference'] = relationship('Reference', back_populates='dbxref_ref')


class Feature(Base):
    __tablename__ = 'feature'
    __table_args__ = (
        ForeignKeyConstraint(['organism_no'], ['MULTI.organism.organism_no'], name='feat_organism_fk'),
        PrimaryKeyConstraint('feature_no', name='feature_pk'),
        Index('feat_dbxref_id_uk', 'dbxref_id', unique=True),
        Index('feat_organism_fk_i', 'organism_no'),
        Index('feat_type_source_i', 'feature_type', 'source'),
        Index('feature_uk', 'feature_name', unique=True),
        Index('upper_feat_dbxref_id_i'),
        Index('upper_feature_name_i'),
        Index('upper_gene_name_i'),
        Index('upper_headline_i'),
        {'comment': 'Consists of features that are found in regions of sequences.',
     'schema': 'MULTI'}
    )

    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a feature. Oracle sequence generated number.')
    organism_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an organism. FK to the ORGANISM table.')
    feature_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Name of the feature, such as ORF name, tRNA name, etc.')
    dbxref_id: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Primary DBXREF_ID for the feature.')
    feature_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of the feature, based on SO. Coded.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the feature (Coded: SGD, ATCC, etc.).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    gene_name: Mapped[Optional[str]] = mapped_column(VARCHAR(20), comment='Gene name if the feature has been characterized.')
    name_description: Mapped[Optional[str]] = mapped_column(VARCHAR(100), comment='The description of the gene name acronym.')
    headline: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='Headline or description of the feature.')

    organism: Mapped['Organism'] = relationship('Organism', back_populates='feature')
    blast_alignment: Mapped[list['BlastAlignment']] = relationship('BlastAlignment', back_populates='feature')
    coll_feat: Mapped[list['CollFeat']] = relationship('CollFeat', back_populates='feature')
    dbxref_feat: Mapped[list['DbxrefFeat']] = relationship('DbxrefFeat', back_populates='feature')
    feat_alias: Mapped[list['FeatAlias']] = relationship('FeatAlias', back_populates='feature')
    feat_homology: Mapped[list['FeatHomology']] = relationship('FeatHomology', back_populates='feature')
    feat_interact: Mapped[list['FeatInteract']] = relationship('FeatInteract', back_populates='feature')
    feat_para: Mapped[list['FeatPara']] = relationship('FeatPara', back_populates='feature')
    feat_property: Mapped[list['FeatProperty']] = relationship('FeatProperty', back_populates='feature')
    feat_relationship: Mapped[list['FeatRelationship']] = relationship('FeatRelationship', foreign_keys='[FeatRelationship.child_feature_no]', back_populates='feature')
    feat_relationship: Mapped[list['FeatRelationship']] = relationship('FeatRelationship', foreign_keys='[FeatRelationship.parent_feature_no]', back_populates='feature')
    feat_url: Mapped[list['FeatUrl']] = relationship('FeatUrl', back_populates='feature')
    gene_reservation: Mapped[list['GeneReservation']] = relationship('GeneReservation', back_populates='feature')
    go_annotation: Mapped[list['GoAnnotation']] = relationship('GoAnnotation', back_populates='feature')
    pheno_annotation: Mapped[list['PhenoAnnotation']] = relationship('PhenoAnnotation', back_populates='feature')
    protein_info: Mapped[list['ProteinInfo']] = relationship('ProteinInfo', back_populates='feature')
    refprop_feat: Mapped[list['RefpropFeat']] = relationship('RefpropFeat', back_populates='feature')
    seq: Mapped[list['Seq']] = relationship('Seq', back_populates='feature')
    feat_location: Mapped[list['FeatLocation']] = relationship('FeatLocation', back_populates='feature')


class GenomeVersion(Base):
    __tablename__ = 'genome_version'
    __table_args__ = (
        CheckConstraint("is_ver_current in ('Y', 'N')", name='gv_is_current_ck'),
        ForeignKeyConstraint(['organism_no'], ['MULTI.organism.organism_no'], name='gv_organism_fk'),
        PrimaryKeyConstraint('genome_version_no', name='genome_version_pk'),
        Index('genome_version_uk', 'genome_version', 'organism_no', unique=True),
        Index('gv_is_current_i'),
        Index('gv_organism_fk_i', 'organism_no'),
        {'comment': 'Contains genome versioning information.', 'schema': 'MULTI'}
    )

    genome_version_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a genome version. Oracle sequence generated number.')
    genome_version: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The version of the genome.')
    organism_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Organism associated with this genome version. FK to the ORGANISM table.')
    is_ver_current: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Whether the version is current (Coded: Y/N).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the row was first entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(1000), comment='A description of the genome version.')

    organism: Mapped['Organism'] = relationship('Organism', back_populates='genome_version')
    seq: Mapped[list['Seq']] = relationship('Seq', back_populates='genome_version')


class PdbAlignmentSequence(Base):
    __tablename__ = 'pdb_alignment_sequence'
    __table_args__ = (
        ForeignKeyConstraint(['pdb_alignment_no'], ['MULTI.pdb_alignment.pdb_alignment_no'], ondelete='CASCADE', name='pdbalignseq_pdbalign_fk'),
        PrimaryKeyConstraint('pdb_alignment_sequence_no', name='pdb_alignment_sequence_pk'),
        Index('pdbalignseq_pdbalign_fk_i', 'pdb_alignment_no'),
        {'comment': 'Raw sequence used in a PDB alignment.', 'schema': 'MULTI'}
    )

    pdb_alignment_sequence_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a PDB aligned sequence. Oracle sequence generated number.')
    pdb_alignment_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a PDB alignment.  Foreign key to the PDB_ALIGNMENT table.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    query_seq: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='Raw query sequence.')
    target_seq: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='Raw target sequence.')
    alignment_symbol: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='Symbols used to indicate the identity or similarity between two aligned sequences in a sequence alignment display.')

    pdb_alignment: Mapped['PdbAlignment'] = relationship('PdbAlignment', back_populates='pdb_alignment_sequence')


class RefLink(Base):
    __tablename__ = 'ref_link'
    __table_args__ = (
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='rl_ref_fk'),
        PrimaryKeyConstraint('ref_link_no', name='ref_link_pk'),
        Index('ref_link_uk', 'tab_name', 'primary_key', 'reference_no', 'col_name', unique=True),
        Index('rl_ref_no_i', 'reference_no'),
        {'comment': 'Associates any piece of data (row or column) with a given '
                'reference.',
     'schema': 'MULTI'}
    )

    ref_link_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for an associated reference. Oracle generated sequence number.')
    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a reference. Foreign key to the reference table.')
    tab_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Name of table linked to a reference.')
    primary_key: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Primary key for the row linked to a reference.')
    col_name: Mapped[str] = mapped_column(VARCHAR(30), nullable=False, comment='Name of the column linked to a reference.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')

    reference: Mapped['Reference'] = relationship('Reference', back_populates='ref_link')


class RefProperty(Base):
    __tablename__ = 'ref_property'
    __table_args__ = (
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='refprop_ref_fk'),
        PrimaryKeyConstraint('ref_property_no', name='ref_property_pk'),
        Index('ref_property_uk', 'reference_no', 'property_type', 'source', 'property_value', unique=True),
        {'comment': 'Contains information about a reference in tag-attribute pairs.  '
                'Used for gene and non-gene literature curation.',
     'schema': 'MULTI'}
    )

    ref_property_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a reference property. Oracle sequence generated number.')
    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a reference. Foreign key to the reference table.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the reference attribute or property. Coded value.')
    property_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of the reference attribute or property. Coded value.')
    property_value: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='The value associated with the reference attribute or property.')
    date_last_reviewed: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was last reviewed.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    reference: Mapped['Reference'] = relationship('Reference', back_populates='ref_property')
    refprop_feat: Mapped[list['RefpropFeat']] = relationship('RefpropFeat', back_populates='ref_property')


class RefReftype(Base):
    __tablename__ = 'ref_reftype'
    __table_args__ = (
        ForeignKeyConstraint(['ref_type_no'], ['MULTI.ref_type.ref_type_no'], ondelete='CASCADE', name='rrt_reftype_fk'),
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='rrt_ref_fk'),
        PrimaryKeyConstraint('ref_reftype_no', name='ref_reftype_pk'),
        Index('ref_reftype_uk', 'reference_no', 'ref_type_no', unique=True),
        Index('rrt_reftype_fk_i', 'ref_type_no'),
        {'comment': 'Linking table between the reference and ref_type tables.',
     'schema': 'MULTI'}
    )

    ref_reftype_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the ref_reftype table. Oracle sequence generated number.')
    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a reference. Foreign key to the reference table.')
    ref_type_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a ref_type. Foreign key to the ref_type table.')

    ref_type: Mapped['RefType'] = relationship('RefType', back_populates='ref_reftype')
    reference: Mapped['Reference'] = relationship('Reference', back_populates='ref_reftype')


class RefRelationship(Base):
    __tablename__ = 'ref_relationship'
    __table_args__ = (
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='rr_ref_fk'),
        ForeignKeyConstraint(['related_ref_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='rr_relref_fk'),
        PrimaryKeyConstraint('ref_relationship_no', name='ref_relationship_pk'),
        Index('ref_relationship_uk', 'reference_no', 'related_ref_no', unique=True),
        Index('rr_relref_fk_i', 'related_ref_no'),
        {'comment': 'Contains information about published errata and comments about '
                'reference from NCBI.',
     'schema': 'MULTI'}
    )

    ref_relationship_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a reference relationship. Oracle sequence generated number.')
    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a reference. Foreign key to the reference table.')
    related_ref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for the related reference. Foreign key to the reference table.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was first entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who first entered the record into the database.')
    description: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='Text of the comment or erratum.')

    reference: Mapped['Reference'] = relationship('Reference', foreign_keys=[reference_no], back_populates='ref_relationship')
    reference: Mapped['Reference'] = relationship('Reference', foreign_keys=[related_ref_no], back_populates='ref_relationship')


class RefUrl(Base):
    __tablename__ = 'ref_url'
    __table_args__ = (
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='ref_url_ref_fk'),
        ForeignKeyConstraint(['url_no'], ['MULTI.url.url_no'], ondelete='CASCADE', name='ref_url_url_fk'),
        PrimaryKeyConstraint('ref_url_no', name='ref_url_pk'),
        Index('ref_url_uk', 'reference_no', 'url_no', unique=True),
        Index('ref_url_url_fk_i', 'url_no'),
        {'comment': 'Linking table between the reference and url tables.',
     'schema': 'MULTI'}
    )

    ref_url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the ref_url table. Oracle sequence generated number.')
    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a reference. Foreign key to the reference table.')
    url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for each URL.  Foreign key to the url table.')

    reference: Mapped['Reference'] = relationship('Reference', back_populates='ref_url')
    url: Mapped['Url'] = relationship('Url', back_populates='ref_url')


class BlastAlignment(Base):
    __tablename__ = 'blast_alignment'
    __table_args__ = (
        ForeignKeyConstraint(['query_no'], ['MULTI.feature.feature_no'], name='blast_align_feat_fk'),
        ForeignKeyConstraint(['target_no'], ['MULTI.blast_hit.blast_hit_no'], name='blast_align_bh_fk'),
        PrimaryKeyConstraint('blast_alignment_no', name='blast_alignment_pk'),
        Index('blast_align_bh_fk_i', 'target_no'),
        Index('blast_alignment_uk', 'query_no', 'target_no', 'method', 'query_start_coord', 'query_stop_coord', 'target_start_coord', 'target_stop_coord', unique=True),
        {'comment': 'This table stores blast hit alignments.', 'schema': 'MULTI'}
    )

    blast_alignment_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a blast alignment. Oracle generated sequence number.')
    query_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier of the query sequence. Foreign key to the feature table.')
    target_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a target sequence. Foreign key to homolog table.')
    method: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Method used to generate the alignment data (Coded: BLASTp, etc.)')
    query_start_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Start coordinate of where the alignment begins for the query sequence.')
    query_stop_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Stop coordinate of where the alignment begins for the query sequence.')
    target_start_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Start coordinate of where the alignment begins for the target sequence.')
    target_stop_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Stop coordinate of where the alignment begins for the target sequence.')
    score: Mapped[decimal.Decimal] = mapped_column(NUMBER(8, 3, True), nullable=False, comment='The significance score.')
    score_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of score (Coded:. p value, e value).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    pct_aligned: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(5, 2, True), comment='Percent alignment.')
    pct_identical: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(5, 2, True), comment='Percent identity.')
    pct_similar: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(5, 2, True), comment='Percent similarity.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='blast_alignment')
    blast_hit: Mapped['BlastHit'] = relationship('BlastHit', back_populates='blast_alignment')


class CollFeat(Base):
    __tablename__ = 'coll_feat'
    __table_args__ = (
        ForeignKeyConstraint(['colleague_no'], ['MULTI.colleague.colleague_no'], ondelete='CASCADE', name='coll_feat_coll_fk'),
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='coll_feat_feat_fk'),
        PrimaryKeyConstraint('coll_feat_no', name='coll_feat_pk'),
        Index('coll_feat_feat_fk_i', 'feature_no'),
        Index('coll_feat_uk', 'colleague_no', 'feature_no', unique=True),
        {'comment': 'Linking table between the colleague and feature tables.',
     'schema': 'MULTI'}
    )

    coll_feat_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a colleague-feature relationship.  Oracle sequence generated number.')
    colleague_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a colleague. Foreign key to the colleague table.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')

    colleague: Mapped['Colleague'] = relationship('Colleague', back_populates='coll_feat')
    feature: Mapped['Feature'] = relationship('Feature', back_populates='coll_feat')


class CvtermDbxref(Base):
    __tablename__ = 'cvterm_dbxref'
    __table_args__ = (
        ForeignKeyConstraint(['cv_term_no'], ['MULTI.cv_term.cv_term_no'], ondelete='CASCADE', name='cvtdbxref_cvterm_fk'),
        ForeignKeyConstraint(['dbxref_no'], ['MULTI.dbxref.dbxref_no'], ondelete='CASCADE', name='cvtdbxref_dbxref_fk'),
        PrimaryKeyConstraint('cvterm_dbxref_no', name='cvterm_dbxref_pk'),
        Index('cvtdbxref_dbxref_fk_i', 'dbxref_no'),
        Index('cvterm_dbxref_uk', 'cv_term_no', 'dbxref_no', unique=True),
        {'comment': 'Linking table between cv_term and dbxref tables.',
     'schema': 'MULTI'}
    )

    cvterm_dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier to link the cvterm and dbxref tables. Oracle sequence generated number.')
    cv_term_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a controlled vocabulary term. Foreign key to the cv_term table.')
    dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Unique identifier for an database cross reference. Foreign key to the dbxref table.')

    cv_term: Mapped['CvTerm'] = relationship('CvTerm', back_populates='cvterm_dbxref')
    dbxref: Mapped['Dbxref'] = relationship('Dbxref', back_populates='cvterm_dbxref')


class CvtermGroup(Base):
    __tablename__ = 'cvterm_group'
    __table_args__ = (
        ForeignKeyConstraint(['cv_term_no'], ['MULTI.cv_term.cv_term_no'], name='cvtgroup_cvterm_fk'),
        PrimaryKeyConstraint('cvterm_group_no', name='cvterm_group_pk'),
        Index('cvterm_group_uk', 'group_name', 'cv_term_no', unique=True),
        Index('cvtgroup_cvterm_fk_i', 'cv_term_no'),
        {'comment': 'Contains sub-sets or groups of terms that are not part of the '
                'definition of the controlled vocabulary (e.g., Yeast GO slim, '
                'terms not valid for annotation).',
     'schema': 'MULTI'}
    )

    cvterm_group_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a group of controlled vocabulary terms.  Oracle sequence generated number.')
    group_name: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The name of the controlled vocabulary group. Coded value.')
    cv_term_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a controlled vocabulary term. Foreign key to the cv_term table.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    cv_term: Mapped['CvTerm'] = relationship('CvTerm', back_populates='cvterm_group')


class CvtermPath(Base):
    __tablename__ = 'cvterm_path'
    __table_args__ = (
        ForeignKeyConstraint(['ancestor_cv_term_no'], ['MULTI.cv_term.cv_term_no'], name='cvtpath_ancestor_cvterm_fk'),
        ForeignKeyConstraint(['child_cv_term_no'], ['MULTI.cv_term.cv_term_no'], name='cvtpath_child_cvterm_fk'),
        PrimaryKeyConstraint('cvterm_path_no', name='cvterm_path_pk'),
        Index('cvterm_path_uk', 'full_path', 'child_cv_term_no', unique=True),
        Index('cvtpath_ancestor_cvterm_fk_i', 'ancestor_cv_term_no'),
        Index('cvtpath_child_cvterm_fk_i', 'child_cv_term_no'),
        {'comment': 'Contains a the full path information from any two nodes in a '
                'controlled vocabularly hierarchy.',
     'schema': 'MULTI'}
    )

    cvterm_path_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a cv term path.  Oracle sequence generated number.')
    child_cv_term_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a child controlled vocabulary term. Foreign key to the cv_term table.')
    ancestor_cv_term_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for the ancestor controlled vocabulary term. Foreign key to the cv_term table.')
    generation: Mapped[float] = mapped_column(NUMBER(2, 0, False), nullable=False, comment='The number of generations between the parent and child term; for example, for a grandchild:grandparent relationship, the generation = 2.')
    full_path: Mapped[str] = mapped_column(VARCHAR(240), nullable=False, comment='A list of all terms corresponding to all the terms in between the ancestor and the child, separated by ::.')
    relationship_type: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='The type of relationship between the parent and child term. Coded value.')

    cv_term: Mapped['CvTerm'] = relationship('CvTerm', foreign_keys=[ancestor_cv_term_no], back_populates='cvterm_path')
    cv_term: Mapped['CvTerm'] = relationship('CvTerm', foreign_keys=[child_cv_term_no], back_populates='cvterm_path')


class CvtermRelationship(Base):
    __tablename__ = 'cvterm_relationship'
    __table_args__ = (
        ForeignKeyConstraint(['child_cv_term_no'], ['MULTI.cv_term.cv_term_no'], ondelete='CASCADE', name='cvtrel_child_cvterm_fk'),
        ForeignKeyConstraint(['parent_cv_term_no'], ['MULTI.cv_term.cv_term_no'], ondelete='CASCADE', name='cvtrel_parent_cvterm_fk'),
        PrimaryKeyConstraint('cvterm_relationship_no', name='cvtermrel_pk'),
        Index('cvterm_relationship_uk', 'child_cv_term_no', 'parent_cv_term_no', 'relationship_type', unique=True),
        Index('cvtrel_parent_cvterm_fk_i', 'parent_cv_term_no'),
        {'comment': 'Stores the relationship between two controlled vocabulary terms: '
                'Parent (object) and Child (subject) terms.  The controlled '
                'vocabulary can be recreated entirely from the CVTERM_RELATIONSHIP '
                'table.',
     'schema': 'MULTI'}
    )

    cvterm_relationship_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier')
    child_cv_term_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a child/subject controlled vocabulary term. Foreign key to the cv_term table.')
    parent_cv_term_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a parent controlled vocabulary term. Foreign key to the cv_term table.')
    relationship_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of cv relationship.  Coded: is_a, part_of, etc.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    cv_term: Mapped['CvTerm'] = relationship('CvTerm', foreign_keys=[child_cv_term_no], back_populates='cvterm_relationship')
    cv_term: Mapped['CvTerm'] = relationship('CvTerm', foreign_keys=[parent_cv_term_no], back_populates='cvterm_relationship')


class CvtermSynonym(Base):
    __tablename__ = 'cvterm_synonym'
    __table_args__ = (
        ForeignKeyConstraint(['cv_term_no'], ['MULTI.cv_term.cv_term_no'], ondelete='CASCADE', name='cvtsyn_cvterm_fk'),
        PrimaryKeyConstraint('cvterm_synonym_no', name='cvterm_synonym_pk'),
        Index('cvterm_synonym_uk', 'term_synonym', 'cv_term_no', 'synonym_type', unique=True),
        Index('cvtsyn_cvterm_fk_i', 'cv_term_no'),
        {'comment': 'Contains synonyms or aliases for a controlled vocabulary term.',
     'schema': 'MULTI'}
    )

    cvterm_synonym_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a controlled vocabulary term synonym.  Oracle sequence generated number.')
    cv_term_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a controlled vocabulary term. Oracle sequence generated number.')
    term_synonym: Mapped[str] = mapped_column(VARCHAR(1024), nullable=False, comment='The cv term synonym.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    synonym_type: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='The type of synonym (Coded).')

    cv_term: Mapped['CvTerm'] = relationship('CvTerm', back_populates='cvterm_synonym')


class DbxrefFeat(Base):
    __tablename__ = 'dbxref_feat'
    __table_args__ = (
        ForeignKeyConstraint(['dbxref_no'], ['MULTI.dbxref.dbxref_no'], ondelete='CASCADE', name='dbxref_feat_dbxref_fk'),
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='dbxref_feat_feat_fk'),
        PrimaryKeyConstraint('dbxref_feat_no', name='dbxref_feat_pk'),
        Index('dbxref_feat_feat_fk_i', 'feature_no'),
        Index('dbxref_feat_uk', 'dbxref_no', 'feature_no', unique=True),
        {'comment': 'Linking table between the dbxref and feature tables.',
     'schema': 'MULTI'}
    )

    dbxref_feat_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the dbxref_feat table. Oracle sequence generated number.')
    dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an database cross reference. Foreign key to the dbxref table.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')

    dbxref: Mapped['Dbxref'] = relationship('Dbxref', back_populates='dbxref_feat')
    feature: Mapped['Feature'] = relationship('Feature', back_populates='dbxref_feat')


class FeatAlias(Base):
    __tablename__ = 'feat_alias'
    __table_args__ = (
        ForeignKeyConstraint(['alias_no'], ['MULTI.alias.alias_no'], ondelete='CASCADE', name='feat_alias_alias_fk'),
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='feat_alias_feat_fk'),
        PrimaryKeyConstraint('feat_alias_no', name='feat_alias_pk'),
        Index('feat_alias_alias_fk_i', 'alias_no'),
        Index('feat_alias_uk', 'feature_no', 'alias_no', unique=True),
        {'comment': 'Linking table between the alias and feature tables.',
     'schema': 'MULTI'}
    )

    feat_alias_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the feat_alias table. Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Oracle sequence generated number.')
    alias_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an alias. Oracle sequence generated number.')

    alias: Mapped['Alias'] = relationship('Alias', back_populates='feat_alias')
    feature: Mapped['Feature'] = relationship('Feature', back_populates='feat_alias')


class FeatHomology(Base):
    __tablename__ = 'feat_homology'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='feat_homology_feat_fk'),
        ForeignKeyConstraint(['homology_group_no'], ['MULTI.homology_group.homology_group_no'], ondelete='CASCADE', name='feat_homology_hg_fk'),
        PrimaryKeyConstraint('feat_homology_no', name='feat_homology_pk'),
        Index('feat_homology_hg_fk_i', 'homology_group_no'),
        Index('feat_homology_uk', 'feature_no', 'homology_group_no', unique=True),
        {'comment': 'Linking table between FEATURE and HOMOLOGY_GROUP tables.',
     'schema': 'MULTI'}
    )

    feat_homology_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the FEAT_HOMOLOGY table. Oracle generated sequence number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. FK to the FEATURE table.')
    homology_group_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a homology group. FK to the HOMOLOGY_GROUP table.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='feat_homology')
    homology_group: Mapped['HomologyGroup'] = relationship('HomologyGroup', back_populates='feat_homology')


class FeatInteract(Base):
    __tablename__ = 'feat_interact'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='featint_feat_fk'),
        ForeignKeyConstraint(['interaction_no'], ['MULTI.interaction.interaction_no'], ondelete='CASCADE', name='featint_int_fk'),
        PrimaryKeyConstraint('feat_interact_no', name='feat_interact_pk'),
        Index('feat_interact_uk', 'feature_no', 'interaction_no', 'action', unique=True),
        Index('featint_int_fk_i', 'interaction_no'),
        {'comment': 'Linking table between the feature and interaction tables.',
     'schema': 'MULTI'}
    )

    feat_interact_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a feature interaction. Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    interaction_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an feature interaction. Foreign key to the interaction table.')
    action: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Describes what action the feature is taking while in the interaction (e.g., Suppressor).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='feat_interact')
    interaction: Mapped['Interaction'] = relationship('Interaction', back_populates='feat_interact')


class FeatPara(Base):
    __tablename__ = 'feat_para'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='feat_para_feat_fk'),
        ForeignKeyConstraint(['paragraph_no'], ['MULTI.paragraph.paragraph_no'], ondelete='CASCADE', name='feat_para_para_fk'),
        PrimaryKeyConstraint('feat_para_no', name='feat_para_pk'),
        Index('feat_para_para_fk_i', 'paragraph_no'),
        Index('feat_para_uk', 'feature_no', 'paragraph_no', unique=True),
        {'comment': 'Linking table between the feature and paragraph tables.',
     'schema': 'MULTI'}
    )

    feat_para_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a feature-paragraph association. Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    paragraph_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a paragraph. Foreign key to the paragraph table.')
    paragraph_order: Mapped[float] = mapped_column(NUMBER(2, 0, False), nullable=False, comment='Order of the paragraphs associated with the feature.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='feat_para')
    paragraph: Mapped['Paragraph'] = relationship('Paragraph', back_populates='feat_para')


class FeatProperty(Base):
    __tablename__ = 'feat_property'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='featprop_feat_fk'),
        PrimaryKeyConstraint('feat_property_no', name='feat_property_pk'),
        Index('feat_property_uk', 'feature_no', 'property_type', 'property_value', unique=True),
        {'comment': 'Contains information about a feature in the form of tag-value '
                'pairs.',
     'schema': 'MULTI'}
    )

    feat_property_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the feat_property table. Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Unique identifier for a feature. Foreign key to the feature table.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the attribute or tag associated with the feature.  Coded value.')
    property_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The attribute or tag associated with the feature. Coded value.')
    property_value: Mapped[str] = mapped_column(VARCHAR(4000), nullable=False, comment='The value associated wwith the feature property or attribute.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who first entered the record into the database.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='feat_property')


class FeatRelationship(Base):
    __tablename__ = 'feat_relationship'
    __table_args__ = (
        ForeignKeyConstraint(['child_feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='child_feature_fk'),
        ForeignKeyConstraint(['parent_feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='parent_feature_fk'),
        PrimaryKeyConstraint('feat_relationship_no', name='feat_relationship_pk'),
        Index('child_feature_fk_i', 'child_feature_no'),
        Index('feat_relationship_uk', 'parent_feature_no', 'child_feature_no', 'relationship_type', 'rank', unique=True),
        {'comment': 'Describes the relationship between two features.',
     'schema': 'MULTI'}
    )

    feat_relationship_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a feature relationship. Oracle sequence generated number.')
    parent_feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for the parent feature. Foreign key to the feature table.')
    child_feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for the child feature. Foreign key to the feature table..')
    relationship_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of feature relationship. Coded.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    rank: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='The rank or order of the feature.')

    feature: Mapped['Feature'] = relationship('Feature', foreign_keys=[child_feature_no], back_populates='feat_relationship')
    feature: Mapped['Feature'] = relationship('Feature', foreign_keys=[parent_feature_no], back_populates='feat_relationship')


class FeatUrl(Base):
    __tablename__ = 'feat_url'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='feat_url_feat_fk'),
        ForeignKeyConstraint(['url_no'], ['MULTI.url.url_no'], ondelete='CASCADE', name='feat_url_url_fk'),
        PrimaryKeyConstraint('feat_url_no', name='feat_url_pk'),
        Index('feat_url_uk', 'feature_no', 'url_no', unique=True),
        Index('feat_url_url_fk_i', 'url_no'),
        {'comment': 'Linking table between the feature and url tables.',
     'schema': 'MULTI'}
    )

    feat_url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the feat_url table. Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    url_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier assigned for each URL.  Foreign key to the url table.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='feat_url')
    url: Mapped['Url'] = relationship('Url', back_populates='feat_url')


class GeneReservation(Base):
    __tablename__ = 'gene_reservation'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='gresv_feat_fk'),
        PrimaryKeyConstraint('gene_reservation_no', name='gene_reservation_pk'),
        Index('gene_reservation_uk', 'feature_no', unique=True),
        {'comment': 'Contains all gene name reservations.', 'schema': 'MULTI'}
    )

    gene_reservation_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a gene reservation. Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    reservation_date: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the gene reservation was made.')
    expiration_date: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE + 365 '), comment='Date the gene reservation expires.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    date_standardized: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, comment='Date the reserved gene name became standardized.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='gene_reservation')
    coll_generes: Mapped[list['CollGeneres']] = relationship('CollGeneres', back_populates='gene_reservation')


class GoAnnotation(Base):
    __tablename__ = 'go_annotation'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='goann_feat_fk'),
        ForeignKeyConstraint(['go_no'], ['MULTI.go.go_no'], name='goann_go_fk'),
        PrimaryKeyConstraint('go_annotation_no', name='go_annotation_pk'),
        Index('go_annotation_uk', 'go_no', 'feature_no', 'go_evidence', 'annotation_type', 'source', unique=True),
        Index('goann_feat_fk_i', 'feature_no'),
        {'comment': 'Contains information about go annotations.  Linking table between '
                'the go and feature tables.',
     'schema': 'MULTI'}
    )

    go_annotation_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a go annoation. Oracle sequence generated number.')
    go_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier assigned to a goid. Foreign key to the go table.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    go_evidence: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Evidence for the go annotation (Coded: IC, ISS, IDA, etc.).')
    annotation_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type or class of GO annotation. Coded value.')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The source of the GO annotation. Coded value.')
    date_last_reviewed: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date a curator last reviewed the GO annotation.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='go_annotation')
    go: Mapped['Go'] = relationship('Go', back_populates='go_annotation')
    go_ref: Mapped[list['GoRef']] = relationship('GoRef', back_populates='go_annotation')


class PhenoAnnotation(Base):
    __tablename__ = 'pheno_annotation'
    __table_args__ = (
        ForeignKeyConstraint(['experiment_no'], ['MULTI.experiment.experiment_no'], ondelete='CASCADE', name='pa_expt_fk'),
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='pa_feat_fk'),
        ForeignKeyConstraint(['phenotype_no'], ['MULTI.phenotype.phenotype_no'], name='pa_pheno_fk'),
        PrimaryKeyConstraint('pheno_annotation_no', name='pheno_annotation_pk'),
        Index('pa_expt_fk_i', 'experiment_no'),
        Index('pa_pheno_fk_i', 'phenotype_no'),
        Index('pheno_annotation_uk', 'feature_no', 'phenotype_no', 'experiment_no', unique=True),
        {'comment': 'Linking table between the feature and phenotype tables.',
     'schema': 'MULTI'}
    )

    pheno_annotation_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a feature-phenotype annotation. Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    phenotype_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a phenotype. Foreign key to the phenotype table.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Userid of the person who entered the record into the database.')
    experiment_no: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='Unique identifier for an experiment. Foreign key to the experiment table.')

    experiment: Mapped[Optional['Experiment']] = relationship('Experiment', back_populates='pheno_annotation')
    feature: Mapped['Feature'] = relationship('Feature', back_populates='pheno_annotation')
    phenotype: Mapped['Phenotype'] = relationship('Phenotype', back_populates='pheno_annotation')


class ProteinInfo(Base):
    __tablename__ = 'protein_info'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='pi_feat_fk'),
        PrimaryKeyConstraint('protein_info_no', name='protein_info_pk'),
        Index('protein_info_uk', 'feature_no', unique=True),
        {'comment': 'Contains protein information about features.', 'schema': 'MULTI'}
    )

    protein_info_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for protein information. Oracle generated sequence number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    molecular_weight: Mapped[Optional[float]] = mapped_column(NUMBER(7, 0, False), comment='Molecular weight of the protein.')
    pi: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(4, 2, True), comment='PI of the protein.')
    cai: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(4, 3, True), comment='Codon adaptation index.')
    protein_length: Mapped[Optional[float]] = mapped_column(NUMBER(5, 0, False), comment='Length of the protein.')
    n_term_seq: Mapped[Optional[str]] = mapped_column(VARCHAR(7), comment='N terminal sequence of protein.')
    c_term_seq: Mapped[Optional[str]] = mapped_column(VARCHAR(7), comment='C terminal sequence of the protein.')
    codon_bias: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(4, 3, True), comment='Codon bias of the protein.')
    fop_score: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(4, 3, True), comment='Frequency of optimal codons, which is the ratio of optimal codons to synonymous codons.')
    gravy_score: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(7, 6, True), comment='General average hydropathicity score for the hypothetical translated gene product.')
    aromaticity_score: Mapped[Optional[decimal.Decimal]] = mapped_column(NUMBER(7, 6, True), comment='Frequency of aromatic amino acids (Phe, Tyr, Trp) in the hypothetical translated gene product.')
    ala: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of alanines in the protein.')
    arg: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of arginines in the protein.')
    asn: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of asparagines in the protein.')
    asp: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of aspartic acids in the protein.')
    cys: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of cysteines in the protein.')
    gln: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of glutamines in the protein.')
    glu: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of glutamic acids in the protein.')
    gly: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of glycines in the protein.')
    his: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of histidines in the protein.')
    ile: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of isoleucines in the protein.')
    leu: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of leucines in the protein.')
    lys: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of lycines in the protein.')
    met: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of methionines in the protein.')
    phe: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of phenylalanines in the protein.')
    pro: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of prolines in the protein.')
    thr: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of threonines in the protein.')
    ser: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of serines in the protein.')
    trp: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of tryptophans in the protein.')
    tyr: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of tyrosines in the protein.')
    val: Mapped[Optional[float]] = mapped_column(NUMBER(4, 0, False), comment='Number of valines in the protein.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='protein_info')
    protein_detail: Mapped[list['ProteinDetail']] = relationship('ProteinDetail', back_populates='protein_info')


class RefpropFeat(Base):
    __tablename__ = 'refprop_feat'
    __table_args__ = (
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], ondelete='CASCADE', name='rpfeat_feat_fk'),
        ForeignKeyConstraint(['ref_property_no'], ['MULTI.ref_property.ref_property_no'], ondelete='CASCADE', name='rpfeat_refprop_fk'),
        PrimaryKeyConstraint('refprop_feat_no', name='refprop_feat_pk'),
        Index('refprop_feat_uk', 'feature_no', 'ref_property_no', unique=True),
        Index('rpfeat_refprop_fk_i', 'ref_property_no'),
        {'comment': 'Lnking table between the ref_property and feature tables.',
     'schema': 'MULTI'}
    )

    refprop_feat_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a row in the refprop_feat table.  Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    ref_property_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a reference property. Foreign key to the ref_property table.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was first entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who first entered the record into the database.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='refprop_feat')
    ref_property: Mapped['RefProperty'] = relationship('RefProperty', back_populates='refprop_feat')


class Seq(Base):
    __tablename__ = 'seq'
    __table_args__ = (
        CheckConstraint("is_seq_current in ('Y','N')", name='seq_is_current_ck'),
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], name='seq_feat_fk'),
        ForeignKeyConstraint(['genome_version_no'], ['MULTI.genome_version.genome_version_no'], name='seq_gv_fk'),
        PrimaryKeyConstraint('seq_no', name='seq_pk'),
        Index('seq_feat_fk_i', 'feature_no'),
        Index('seq_gv_fk_i', 'genome_version_no'),
        Index('seq_is_current_i'),
        Index('seq_type_current_i', 'seq_type', 'is_seq_current'),
        {'comment': 'Contains the old and new versions of a feature sequence.',
     'schema': 'MULTI'}
    )

    seq_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a feature sequence. Oracle generated sequence number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a feature. Foreign key to the feature table.')
    genome_version_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a genome version. FK to theGENOME_VERSION table.')
    seq_version: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='The version, expressed as a date (e.g., 2005-12-05), of the sequence.')
    seq_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Type of sequence (Coded: Genomic, Protein).')
    source: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='Source of the sequence (Coded: SGD, NCBI, ATCC, etc.).')
    is_seq_current: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Whether the sequence is the most current version (Coded: Y/N)')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    seq_length: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='The sequence length, in nucleotide or amino acid residues.')
    residues: Mapped[str] = mapped_column(Text, nullable=False, comment='The actual nucleotide or amino acid residues of the sequence.')
    ftp_file: Mapped[Optional[str]] = mapped_column(VARCHAR(240), comment='The full pathname to the most granular fasta file on the FTP site that contains this sequence.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='seq')
    genome_version: Mapped['GenomeVersion'] = relationship('GenomeVersion', back_populates='seq')
    feat_location: Mapped[list['FeatLocation']] = relationship('FeatLocation', foreign_keys='[FeatLocation.root_seq_no]', back_populates='seq')
    feat_location: Mapped[list['FeatLocation']] = relationship('FeatLocation', foreign_keys='[FeatLocation.seq_no]', back_populates='seq')
    seq_change_archive: Mapped[list['SeqChangeArchive']] = relationship('SeqChangeArchive', back_populates='seq')


class CollGeneres(Base):
    __tablename__ = 'coll_generes'
    __table_args__ = (
        ForeignKeyConstraint(['colleague_no'], ['MULTI.colleague.colleague_no'], name='coll_generes_coll_fk'),
        ForeignKeyConstraint(['gene_reservation_no'], ['MULTI.gene_reservation.gene_reservation_no'], ondelete='CASCADE', name='coll_generes_generes_fk'),
        PrimaryKeyConstraint('coll_generes_no', name='coll_generes_pk'),
        Index('coll_generes_generes_fk_i', 'gene_reservation_no'),
        Index('coll_generes_uk', 'colleague_no', 'gene_reservation_no', unique=True),
        {'comment': 'Linking table between the colleague and gene_reservation tables.',
     'schema': 'MULTI'}
    )

    coll_generes_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a colleague-gene_reservation relationship.  Oracle sequence generated number.')
    colleague_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a colleague.  Foreign key to the colleague table.')
    gene_reservation_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a gene reservation. Foreign key to the gene_reservation table.')

    colleague: Mapped['Colleague'] = relationship('Colleague', back_populates='coll_generes')
    gene_reservation: Mapped['GeneReservation'] = relationship('GeneReservation', back_populates='coll_generes')


class FeatLocation(Base):
    __tablename__ = 'feat_location'
    __table_args__ = (
        CheckConstraint("is_loc_current in ('Y','N')", name='fl_is_current_ck'),
        CheckConstraint("strand in ('C','W')", name='fl_strand_ck'),
        ForeignKeyConstraint(['feature_no'], ['MULTI.feature.feature_no'], name='fl_feat_fk'),
        ForeignKeyConstraint(['root_seq_no'], ['MULTI.seq.seq_no'], name='fl_root_seq_fk'),
        ForeignKeyConstraint(['seq_no'], ['MULTI.seq.seq_no'], name='fl_seq_fk'),
        PrimaryKeyConstraint('feat_location_no', name='feat_location_pk'),
        Index('feat_location_uk', 'feature_no', 'coord_version', unique=True),
        Index('fl_coord_i', 'feature_no', 'start_coord', 'stop_coord', 'strand'),
        Index('fl_is_current_i'),
        Index('fl_root_seq_fk_i', 'root_seq_no'),
        Index('fl_seq_fk_i', 'seq_no'),
        {'comment': 'Contains information for all feature locations (stop and start '
                'coordinates).  This table contains both current and archival '
                'coordinates.',
     'schema': 'MULTI'}
    )

    feat_location_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a feature sequence annotation. Oracle sequence generated number.')
    feature_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for this feature location. Foreign key to the feature table.')
    root_seq_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for the root sequence (e.g., chromosome) associated with this feature location. Foreign key to the seq table.')
    coord_version: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Date the feature was first identified at this location (stop and start coordinates).')
    start_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Start coordinate of the feature location.')
    stop_coord: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Stop coordinate of the feature location.')
    strand: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='DNA strand on which the feature is located (Coded: W = watson strand, C = crick strand).')
    is_loc_current: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Whether this feature location is the current version (Coded: Y/N).')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    seq_no: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='The sequence associated with this feature location.  Foreign key to the seq table.')

    feature: Mapped['Feature'] = relationship('Feature', back_populates='feat_location')
    seq: Mapped['Seq'] = relationship('Seq', foreign_keys=[root_seq_no], back_populates='feat_location')
    seq: Mapped[Optional['Seq']] = relationship('Seq', foreign_keys=[seq_no], back_populates='feat_location')


class GoRef(Base):
    __tablename__ = 'go_ref'
    __table_args__ = (
        CheckConstraint("has_qualifier in ('Y','N')", name='goref_has_qualifier_ck'),
        CheckConstraint("has_supporting_evidence in ('Y','N')", name='goref_has_suport_evidence_ck'),
        ForeignKeyConstraint(['go_annotation_no'], ['MULTI.go_annotation.go_annotation_no'], ondelete='CASCADE', name='goref_goann_fk'),
        ForeignKeyConstraint(['reference_no'], ['MULTI.reference.reference_no'], ondelete='CASCADE', name='goref_ref_fk'),
        PrimaryKeyConstraint('go_ref_no', name='go_ref_pk'),
        Index('go_ref_uk', 'reference_no', 'go_annotation_no', unique=True),
        Index('goref_goann_fk_i', 'go_annotation_no'),
        {'comment': 'Linking table between the go_annotation and reference tables.',
     'schema': 'MULTI'}
    )

    go_ref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a GO reference association. Oracle generated sequence number.')
    reference_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a  reference. Foreign key to the reference table.')
    go_annotation_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a go annoation. Foreign key to the go_annotation table.')
    has_qualifier: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Whether this GO reference has a qualifier.')
    has_supporting_evidence: Mapped[str] = mapped_column(VARCHAR(1), nullable=False, comment='Whether this GO reference has supporting evidence.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date this record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')

    go_annotation: Mapped['GoAnnotation'] = relationship('GoAnnotation', back_populates='go_ref')
    reference: Mapped['Reference'] = relationship('Reference', back_populates='go_ref')
    go_qualifier: Mapped[list['GoQualifier']] = relationship('GoQualifier', back_populates='go_ref')
    goref_dbxref: Mapped[list['GorefDbxref']] = relationship('GorefDbxref', back_populates='go_ref')


class ProteinDetail(Base):
    __tablename__ = 'protein_detail'
    __table_args__ = (
        ForeignKeyConstraint(['protein_info_no'], ['MULTI.protein_info.protein_info_no'], ondelete='CASCADE', name='pd_pi_fk'),
        PrimaryKeyConstraint('protein_detail_no', name='protein_detail_pk'),
        Index('protein_detail_uk', 'protein_info_no', 'protein_detail_type', 'protein_detail_value', 'start_coord', 'stop_coord', unique=True),
        {'comment': 'This table contains additional information about the protein '
                'encoded by a feature.  It is a tag/value type of table so that '
                'different types of protein information can be easily added.',
     'schema': 'MULTI'}
    )

    protein_detail_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a protein detail. Oracle generated sequence number.')
    protein_info_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for protein information. Oracle generated sequence number.')
    protein_detail_group: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='A category or group of associated protein details.')
    protein_detail_type: Mapped[str] = mapped_column(VARCHAR(50), nullable=False, comment='The type of information that is being recorded, eg. transmembrane domain.')
    protein_detail_value: Mapped[str] = mapped_column(VARCHAR(240), nullable=False, comment='The value of the type of information that is being stored.  This may be a number, Y or N, or some other type of data, depending on the protein_detail_type.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    protein_detail_unit: Mapped[Optional[str]] = mapped_column(VARCHAR(40), comment='Units, if any, of the protein_detail_value.')
    start_coord: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='The start coordinate in terms of the amino acid sequence, relevant to the additional information.')
    stop_coord: Mapped[Optional[float]] = mapped_column(NUMBER(10, 0, False), comment='The stop coordinate in terms of the amino acid sequence, relevant to the additional information.')
    interpro_dbxref_id: Mapped[Optional[str]] = mapped_column(VARCHAR(20))
    member_dbxref_id: Mapped[Optional[str]] = mapped_column(VARCHAR(20))

    protein_info: Mapped['ProteinInfo'] = relationship('ProteinInfo', back_populates='protein_detail')


class SeqChangeArchive(Base):
    __tablename__ = 'seq_change_archive'
    __table_args__ = (
        ForeignKeyConstraint(['seq_no'], ['MULTI.seq.seq_no'], name='sca_seq_fk'),
        PrimaryKeyConstraint('seq_change_archive_no', name='seq_change_archive_pk'),
        Index('sca_seq_fk_i', 'seq_no'),
        {'comment': 'Contains the changes made to root feature sequences (chromosome '
                'and contigs).  Each sequence in the seq table is associated with '
                'one or more individual sequence changes.',
     'schema': 'MULTI'}
    )

    seq_change_archive_no: Mapped[float] = mapped_column(NUMBER(asdecimal=False), primary_key=True, comment='Assigned unique identifier for a DNA sequence change. Oracle generated sequence number.')
    seq_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for the old DNA sequence. Foreign key to the seq table.')
    seq_change_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of sequence change (Coded: Deletion, Insertion, Substitution).')
    change_start_coord: Mapped[float] = mapped_column(NUMBER(asdecimal=False), nullable=False, comment='Coordinate where the sequence change starts.')
    change_stop_coord: Mapped[float] = mapped_column(NUMBER(asdecimal=False), nullable=False, comment='Coordinate where the sequence change stops.')
    date_created: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=text('SYSDATE '), comment='Date the record was entered into the database. Equals the seq_version of the new sequence.')
    created_by: Mapped[str] = mapped_column(VARCHAR(12), nullable=False, server_default=text('SUBSTR(USER,1,12) '), comment='Person who entered the record into the database.')
    old_seq: Mapped[Optional[str]] = mapped_column(Text)
    new_seq: Mapped[Optional[str]] = mapped_column(Text)

    seq: Mapped['Seq'] = relationship('Seq', back_populates='seq_change_archive')


class GoQualifier(Base):
    __tablename__ = 'go_qualifier'
    __table_args__ = (
        ForeignKeyConstraint(['go_ref_no'], ['MULTI.go_ref.go_ref_no'], ondelete='CASCADE', name='go_qual_goref_fk'),
        PrimaryKeyConstraint('go_qualifier_no', name='go_qualifier_pk'),
        Index('go_qualifier_uk', 'go_ref_no', 'qualifier', unique=True),
        {'comment': 'Contains GO qualifiers (e.g., contributes to, colocalizes with).',
     'schema': 'MULTI'}
    )

    go_qualifier_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for a go qualifier. Oracle sequence generated number.')
    go_ref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a GO reference association. Foreign key to the go_ref table.')
    qualifier: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The qualifier for a GO annotation. Coded: associated with, contributes to.')

    go_ref: Mapped['GoRef'] = relationship('GoRef', back_populates='go_qualifier')


class GorefDbxref(Base):
    __tablename__ = 'goref_dbxref'
    __table_args__ = (
        ForeignKeyConstraint(['dbxref_no'], ['MULTI.dbxref.dbxref_no'], name='goref_dbxref_dbxref_fk'),
        ForeignKeyConstraint(['go_ref_no'], ['MULTI.go_ref.go_ref_no'], ondelete='CASCADE', name='goref_dbxref_goref_fk'),
        PrimaryKeyConstraint('goref_dbxref_no', name='goref_dbxref_pk'),
        Index('goref_dbxref_goref_fk_i', 'go_ref_no'),
        Index('goref_dbxref_uk', 'dbxref_no', 'go_ref_no', 'support_type', unique=True),
        {'comment': 'Linking table between go_ref and dbxref tables.',
     'schema': 'MULTI'}
    )

    goref_dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), primary_key=True, comment='Assigned unique identifier for go supporting evidence.  Oracle sequence generated number.')
    go_ref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for a GO reference association. Foreign key to the go_ref table.')
    dbxref_no: Mapped[float] = mapped_column(NUMBER(10, 0, False), nullable=False, comment='Assigned unique identifier for an database cross reference. Foreign key to dbxref table.')
    support_type: Mapped[str] = mapped_column(VARCHAR(40), nullable=False, comment='The type of supporting evidence (Coded: WIth, From).')

    dbxref: Mapped['Dbxref'] = relationship('Dbxref', back_populates='goref_dbxref')
    go_ref: Mapped['GoRef'] = relationship('GoRef', back_populates='goref_dbxref')
