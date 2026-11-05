#!/usr/bin/env python3
"""Create the MULTI.REGULATION tables for CGD-curated TF -> target associations.

The locus Regulation tab shows PathoYeastract's documented associations (per-gene
JSON from load_pathoyeastract_flatfiles.py) plus associations curators enter
with the Curate Regulation tool, which are stored here. One row is one
evidence observation, with the same fields PathoYeastract records:

    REGULATION  tf_feature_no -> FEATURE      (transcription factor)
                target_feature_no -> FEATURE  (regulated gene)
                reference_no -> REFERENCE
                evidence_code      Direct (DNA binding) / Indirect (expression)
                experiment, mutant_type (expression rows: null, overexpression, ...),
                association_type (Positive / Negative / N/A),
                strain_background, condition_group, environmental_condition,
                log2fc, regulation_note, date_created, created_by
                pheno_annotation_no -> PHENO_ANNOTATION  (rows transferred from
                RNA accumulation phenotypes; null for curator-entered rows)

    REGULATOR_ALLOW  feature_no -> FEATURE
                Genes curators admit as regulators although they have no
                DNA-binding transcription factor GO annotation and are not a
                PathoYeastract regulator; note, date_created, created_by

Follows the schema's conventions: REGULATION_SEQ + a BIUR trigger (primary key,
CheckUser on created_by, audit columns immutable), an AUDR trigger writing
UPDATE_LOG / DELETE_LOG, and the CURATOR / DBSELECT grants.

Idempotent: objects that already exist are left alone, and columns added
since a table was created are added to it. Run once per database (dev, then
prod), and again after updating this script:

    python scripts/regulation/create_regulation_table.py            # create
    python scripts/regulation/create_regulation_table.py --check    # report only
    python scripts/regulation/create_regulation_table.py --drop     # remove (dev)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from sqlalchemy import text  # noqa: E402

from cgd.db.engine import SessionLocal  # noqa: E402

TABLE_DDL = """
CREATE TABLE MULTI.REGULATION (
    REGULATION_NO           NUMBER(10,0) NOT NULL,
    TF_FEATURE_NO           NUMBER(10,0) NOT NULL,
    TARGET_FEATURE_NO       NUMBER(10,0) NOT NULL,
    REFERENCE_NO            NUMBER(10,0) NOT NULL,
    EVIDENCE_CODE           VARCHAR2(20) NOT NULL,
    EXPERIMENT              VARCHAR2(240) NOT NULL,
    MUTANT_TYPE             VARCHAR2(40),
    ASSOCIATION_TYPE        VARCHAR2(20) DEFAULT 'N/A' NOT NULL,
    STRAIN_BACKGROUND       VARCHAR2(100),
    CONDITION_GROUP         VARCHAR2(100),
    ENVIRONMENTAL_CONDITION VARCHAR2(1000),
    LOG2FC                  NUMBER,
    REGULATION_NOTE         VARCHAR2(1000),
    PHENO_ANNOTATION_NO     NUMBER(10,0),
    DATE_CREATED            DATE DEFAULT SYSDATE NOT NULL,
    CREATED_BY              VARCHAR2(12) DEFAULT SUBSTR(USER,1,12) NOT NULL,
    CONSTRAINT REGULATION_PK PRIMARY KEY (REGULATION_NO) USING INDEX TABLESPACE INDEX1,
    CONSTRAINT REGULATION_UK UNIQUE (TF_FEATURE_NO, TARGET_FEATURE_NO, REFERENCE_NO, EXPERIMENT,
                                     MUTANT_TYPE, STRAIN_BACKGROUND, ENVIRONMENTAL_CONDITION,
                                     PHENO_ANNOTATION_NO)
        USING INDEX TABLESPACE INDEX1,
    CONSTRAINT REGULATION_TF_FK FOREIGN KEY (TF_FEATURE_NO)
        REFERENCES MULTI.FEATURE (FEATURE_NO) ON DELETE CASCADE,
    CONSTRAINT REGULATION_TARGET_FK FOREIGN KEY (TARGET_FEATURE_NO)
        REFERENCES MULTI.FEATURE (FEATURE_NO) ON DELETE CASCADE,
    CONSTRAINT REGULATION_REF_FK FOREIGN KEY (REFERENCE_NO)
        REFERENCES MULTI.REFERENCE (REFERENCE_NO) ON DELETE CASCADE,
    CONSTRAINT REGULATION_PHENO_FK FOREIGN KEY (PHENO_ANNOTATION_NO)
        REFERENCES MULTI.PHENO_ANNOTATION (PHENO_ANNOTATION_NO) ON DELETE CASCADE,
    CONSTRAINT REGULATION_EVIDENCE_CK CHECK (EVIDENCE_CODE IN ('Direct', 'Indirect')),
    CONSTRAINT REGULATION_ASSOC_CK CHECK (ASSOCIATION_TYPE IN ('Positive', 'Negative', 'N/A'))
) TABLESPACE DATA2
"""

PHENO_COMMENT = ("COMMENT ON COLUMN MULTI.REGULATION.PHENO_ANNOTATION_NO IS 'RNA accumulation phenotype annotation "
                 "the row was transferred from (transfer_phenotype_regulation.py); null for curator-entered rows.'")

COMMENTS = [
    "COMMENT ON TABLE MULTI.REGULATION IS 'Curator-entered transcriptional regulatory associations "
    "(transcription factor -> target gene), one row per evidence observation.'",
    "COMMENT ON COLUMN MULTI.REGULATION.REGULATION_NO IS 'Assigned unique identifier. Oracle generated "
    "sequence number.'",
    "COMMENT ON COLUMN MULTI.REGULATION.TF_FEATURE_NO IS 'Feature of the transcription factor.'",
    "COMMENT ON COLUMN MULTI.REGULATION.TARGET_FEATURE_NO IS 'Feature of the regulated (target) gene.'",
    "COMMENT ON COLUMN MULTI.REGULATION.REFERENCE_NO IS 'Reference reporting the association.'",
    "COMMENT ON COLUMN MULTI.REGULATION.EVIDENCE_CODE IS 'Direct (DNA binding) or Indirect (expression).'",
    "COMMENT ON COLUMN MULTI.REGULATION.EXPERIMENT IS 'Experiment, e.g. ChIP-seq or RNA-seq analysis - "
    "WT vs TF Deletion.'",
    "COMMENT ON COLUMN MULTI.REGULATION.MUTANT_TYPE IS 'Expression rows: how the regulator was mutated "
    "(null, reduction of function, repressible, overexpression, gain of function, ...).'",
    "COMMENT ON COLUMN MULTI.REGULATION.ASSOCIATION_TYPE IS 'Positive (activation), Negative (repression) "
    "or N/A.'",
    "COMMENT ON COLUMN MULTI.REGULATION.CONDITION_GROUP IS 'Environmental condition group (PathoYeastract "
    "vocabulary).'",
    "COMMENT ON COLUMN MULTI.REGULATION.LOG2FC IS 'Log2 fold change of the target, when reported.'",
    PHENO_COMMENT,
    "COMMENT ON COLUMN MULTI.REGULATION.DATE_CREATED IS 'Date the record was entered into the database.'",
    "COMMENT ON COLUMN MULTI.REGULATION.CREATED_BY IS 'Person who entered the record into the database.'",
]

INDEXES = {
    "REGULATION_TARGET_I": "CREATE INDEX MULTI.REGULATION_TARGET_I ON MULTI.REGULATION (TARGET_FEATURE_NO) "
                           "TABLESPACE INDEX1",
    "REGULATION_REF_I": "CREATE INDEX MULTI.REGULATION_REF_I ON MULTI.REGULATION (REFERENCE_NO) "
                        "TABLESPACE INDEX1",
    "REGULATION_PHENO_I": "CREATE INDEX MULTI.REGULATION_PHENO_I ON MULTI.REGULATION (PHENO_ANNOTATION_NO) "
                          "TABLESPACE INDEX1",
}

# Columns added after the table was first created: {name: ALTER TABLE ... ADD}
ADDED_COLUMNS = {
    "MUTANT_TYPE": "ALTER TABLE MULTI.REGULATION ADD (MUTANT_TYPE VARCHAR2(40))",
    "PHENO_ANNOTATION_NO": "ALTER TABLE MULTI.REGULATION ADD (PHENO_ANNOTATION_NO NUMBER(10,0) "
                           "CONSTRAINT REGULATION_PHENO_FK REFERENCES MULTI.PHENO_ANNOTATION (PHENO_ANNOTATION_NO) "
                           "ON DELETE CASCADE)",
}
ADDED_COMMENTS = {
    "PHENO_ANNOTATION_NO": PHENO_COMMENT,
}

# The unique key gained MUTANT_TYPE (a null and an overexpression mutant in one
# paper are two observations) and PHENO_ANNOTATION_NO (two phenotype
# annotations, e.g. of two alleles, are two observations)
UK_COLUMNS = ["TF_FEATURE_NO", "TARGET_FEATURE_NO", "REFERENCE_NO", "EXPERIMENT", "MUTANT_TYPE",
              "STRAIN_BACKGROUND", "ENVIRONMENTAL_CONDITION", "PHENO_ANNOTATION_NO"]
UK_DDL = (f"ALTER TABLE MULTI.REGULATION ADD CONSTRAINT REGULATION_UK UNIQUE ({', '.join(UK_COLUMNS)}) "
          "USING INDEX TABLESPACE INDEX1")

SEQUENCE_DDL = "CREATE SEQUENCE MULTI.REGULATION_SEQ START WITH 1 INCREMENT BY 1 NOCACHE"

BIUR_DDL = """
CREATE OR REPLACE TRIGGER MULTI.REGULATION_BIUR
BEFORE INSERT OR UPDATE ON MULTI.REGULATION
FOR EACH ROW
DECLARE
  v_IsValidUser         dbuser.userid%TYPE;
BEGIN

  IF INSERTING THEN

    IF (:new.regulation_no IS NULL) THEN
        SELECT regulation_seq.NEXTVAL INTO :new.regulation_no FROM DUAL;
    END IF;

    v_IsValidUser := CheckUser(:new.created_by);

  ELSE

    IF (:new.regulation_no != :old.regulation_no) THEN
        RAISE_APPLICATION_ERROR
            (-20000, 'Primary key cannot be updated');
    END IF;

    IF (:new.date_created != :old.date_created) THEN
        RAISE_APPLICATION_ERROR
            (-20001, 'Audit columns cannot be updated.');
    END IF;

    IF (:new.created_by != :old.created_by) THEN
        RAISE_APPLICATION_ERROR
            (-20001, 'Audit columns cannot be updated.');
    END IF;

  END IF;

END Regulation_BIUR;
"""

# Columns whose updates are written to UPDATE_LOG
LOGGED_COLUMNS = [
    "TF_FEATURE_NO", "TARGET_FEATURE_NO", "REFERENCE_NO", "EVIDENCE_CODE", "EXPERIMENT", "MUTANT_TYPE",
    "ASSOCIATION_TYPE", "STRAIN_BACKGROUND", "CONDITION_GROUP", "ENVIRONMENTAL_CONDITION",
    "LOG2FC", "REGULATION_NOTE", "PHENO_ANNOTATION_NO",
]


def _audr_ddl() -> str:
    updates = "\n".join(
        f"""    IF (((:old.{c} IS NULL) AND (:new.{c} IS NOT NULL)) OR ((:old.{c} IS NOT NULL) AND (:new.{c} IS NULL))
        OR (:old.{c} != :new.{c}))
    THEN
        AuditLog.InsertUpdateLog('REGULATION', '{c}', :old.regulation_no, :old.{c}, :new.{c}, USER);
    END IF;
"""
        for c in LOGGED_COLUMNS
    )
    row = " || '[:]' ||\n             ".join(f":old.{c}" for c in LOGGED_COLUMNS + ["DATE_CREATED", "CREATED_BY"])
    return f"""
CREATE OR REPLACE TRIGGER MULTI.REGULATION_AUDR
AFTER UPDATE OR DELETE ON MULTI.REGULATION
FOR EACH ROW
DECLARE
    v_row       delete_log.deleted_row%TYPE;
BEGIN
  IF UPDATING THEN

{updates}
  ELSE

    v_row := SUBSTR({row}, 1, 4000);

    AuditLog.InsertDeleteLog('REGULATION', :old.regulation_no, v_row, USER);

  END IF;

END Regulation_AUDR;
"""


ALLOW_TABLE_DDL = """
CREATE TABLE MULTI.REGULATOR_ALLOW (
    FEATURE_NO              NUMBER(10,0) NOT NULL,
    ALLOW_NOTE              VARCHAR2(1000),
    DATE_CREATED            DATE DEFAULT SYSDATE NOT NULL,
    CREATED_BY              VARCHAR2(12) DEFAULT SUBSTR(USER,1,12) NOT NULL,
    CONSTRAINT REGULATOR_ALLOW_PK PRIMARY KEY (FEATURE_NO) USING INDEX TABLESPACE INDEX1,
    CONSTRAINT REGULATOR_ALLOW_FEAT_FK FOREIGN KEY (FEATURE_NO)
        REFERENCES MULTI.FEATURE (FEATURE_NO) ON DELETE CASCADE
) TABLESPACE DATA2
"""

ALLOW_COMMENTS = [
    "COMMENT ON TABLE MULTI.REGULATOR_ALLOW IS 'Genes curators admit as regulators on the Regulation tab "
    "although they have no DNA-binding transcription factor GO annotation and are not PathoYeastract "
    "regulators.'",
    "COMMENT ON COLUMN MULTI.REGULATOR_ALLOW.FEATURE_NO IS 'The admitted gene. FK to the feature table.'",
    "COMMENT ON COLUMN MULTI.REGULATOR_ALLOW.ALLOW_NOTE IS 'Why the gene is admitted.'",
    "COMMENT ON COLUMN MULTI.REGULATOR_ALLOW.DATE_CREATED IS 'Date the record was entered into the database.'",
    "COMMENT ON COLUMN MULTI.REGULATOR_ALLOW.CREATED_BY IS 'Person who entered the record into the database.'",
]

ALLOW_BIUR_DDL = """
CREATE OR REPLACE TRIGGER MULTI.REGULATOR_ALLOW_BIUR
BEFORE INSERT OR UPDATE ON MULTI.REGULATOR_ALLOW
FOR EACH ROW
DECLARE
  v_IsValidUser         dbuser.userid%TYPE;
BEGIN

  IF INSERTING THEN

    v_IsValidUser := CheckUser(:new.created_by);

  ELSE

    IF (:new.feature_no != :old.feature_no) THEN
        RAISE_APPLICATION_ERROR
            (-20000, 'Primary key cannot be updated');
    END IF;

    IF (:new.date_created != :old.date_created) THEN
        RAISE_APPLICATION_ERROR
            (-20001, 'Audit columns cannot be updated.');
    END IF;

    IF (:new.created_by != :old.created_by) THEN
        RAISE_APPLICATION_ERROR
            (-20001, 'Audit columns cannot be updated.');
    END IF;

  END IF;

END Regulator_Allow_BIUR;
"""

ALLOW_AUDR_DDL = """
CREATE OR REPLACE TRIGGER MULTI.REGULATOR_ALLOW_AUDR
AFTER UPDATE OR DELETE ON MULTI.REGULATOR_ALLOW
FOR EACH ROW
DECLARE
    v_row       delete_log.deleted_row%TYPE;
BEGIN
  IF UPDATING THEN

    IF (((:old.allow_note IS NULL) AND (:new.allow_note IS NOT NULL))
        OR ((:old.allow_note IS NOT NULL) AND (:new.allow_note IS NULL))
        OR (:old.allow_note != :new.allow_note))
    THEN
        AuditLog.InsertUpdateLog('REGULATOR_ALLOW', 'ALLOW_NOTE', :old.feature_no, :old.allow_note,
                                 :new.allow_note, USER);
    END IF;

  ELSE

    v_row := SUBSTR(:old.feature_no || '[:]' || :old.allow_note || '[:]' ||
                    :old.date_created || '[:]' || :old.created_by, 1, 4000);

    AuditLog.InsertDeleteLog('REGULATOR_ALLOW', :old.feature_no, v_row, USER);

  END IF;

END Regulator_Allow_AUDR;
"""

GRANTS = [
    "GRANT SELECT, INSERT, UPDATE, DELETE ON MULTI.REGULATION TO CURATOR",
    "GRANT SELECT ON MULTI.REGULATION TO DBSELECT",
    "GRANT SELECT, INSERT, UPDATE, DELETE ON MULTI.REGULATOR_ALLOW TO CURATOR",
    "GRANT SELECT ON MULTI.REGULATOR_ALLOW TO DBSELECT",
]


def run(db, ddl: str) -> None:
    """Execute DDL as-is; text() would read the triggers' :new / :old as binds."""
    db.connection().exec_driver_sql(ddl)


def exists(db, object_type: str, name: str) -> bool:
    return bool(db.execute(
        text("SELECT COUNT(*) FROM all_objects WHERE owner = 'MULTI' AND object_type = :t AND object_name = :n"),
        {"t": object_type, "n": name},
    ).scalar())


def column_exists(db, table: str, column: str) -> bool:
    return bool(db.execute(
        text("SELECT COUNT(*) FROM all_tab_columns WHERE owner = 'MULTI' AND table_name = :t "
             "AND column_name = :c"),
        {"t": table, "c": column},
    ).scalar())


def uk_columns(db) -> list[str]:
    return [r[0] for r in db.execute(text(
        "SELECT column_name FROM all_cons_columns WHERE owner = 'MULTI' AND constraint_name = 'REGULATION_UK' "
        "ORDER BY position"
    ))]


def invalid_objects(db) -> list[str]:
    rows = db.execute(text(
        "SELECT object_type || ' ' || object_name FROM all_objects "
        "WHERE owner = 'MULTI' AND (object_name LIKE 'REGULATION%' OR object_name LIKE 'REGULATOR_ALLOW%') "
        "AND status != 'VALID'"
    ))
    return [r[0] for r in rows]


def report(db) -> None:
    for object_type, name in (("TABLE", "REGULATION"), ("SEQUENCE", "REGULATION_SEQ"),
                              ("TRIGGER", "REGULATION_BIUR"), ("TRIGGER", "REGULATION_AUDR"),
                              ("TABLE", "REGULATOR_ALLOW"), ("TRIGGER", "REGULATOR_ALLOW_BIUR"),
                              ("TRIGGER", "REGULATOR_ALLOW_AUDR")):
        print(f"{object_type:8} {name:22} {'present' if exists(db, object_type, name) else 'MISSING'}")
    if exists(db, "TABLE", "REGULATION"):
        for column in ADDED_COLUMNS:
            state = "present" if column_exists(db, "REGULATION", column) else "MISSING"
            print(f"COLUMN   REGULATION.{column:19} {state}")
        print("REGULATION rows:", db.execute(text("SELECT COUNT(*) FROM MULTI.REGULATION")).scalar())
    if exists(db, "TABLE", "REGULATOR_ALLOW"):
        print("REGULATOR_ALLOW rows:", db.execute(text("SELECT COUNT(*) FROM MULTI.REGULATOR_ALLOW")).scalar())
    bad = invalid_objects(db)
    if bad:
        print("INVALID:", ", ".join(bad))


def create(db) -> None:
    if exists(db, "TABLE", "REGULATION"):
        print("table REGULATION exists; skipped")
    else:
        run(db, TABLE_DDL)
        for statement in COMMENTS:
            run(db, statement)
        print("created table REGULATION")
    for column, ddl in ADDED_COLUMNS.items():
        if not column_exists(db, "REGULATION", column):
            run(db, ddl)
            if column in ADDED_COMMENTS:
                run(db, ADDED_COMMENTS[column])
            print(f"added column REGULATION.{column}")
    if uk_columns(db) != UK_COLUMNS:
        run(db, "ALTER TABLE MULTI.REGULATION DROP CONSTRAINT REGULATION_UK DROP INDEX")
        run(db, UK_DDL)
        print(f"rebuilt unique key REGULATION_UK ({', '.join(UK_COLUMNS)})")
    for name, ddl in INDEXES.items():
        if not exists(db, "INDEX", name):
            run(db, ddl)
            print(f"created index {name}")
    if not exists(db, "SEQUENCE", "REGULATION_SEQ"):
        run(db, SEQUENCE_DDL)
        print("created sequence REGULATION_SEQ")
    # CREATE OR REPLACE keeps triggers in step with this script
    run(db, BIUR_DDL)
    run(db, _audr_ddl())
    print("created/replaced triggers REGULATION_BIUR, REGULATION_AUDR")
    if exists(db, "TABLE", "REGULATOR_ALLOW"):
        print("table REGULATOR_ALLOW exists; skipped")
    else:
        run(db, ALLOW_TABLE_DDL)
        for statement in ALLOW_COMMENTS:
            run(db, statement)
        print("created table REGULATOR_ALLOW")
    run(db, ALLOW_BIUR_DDL)
    run(db, ALLOW_AUDR_DDL)
    print("created/replaced triggers REGULATOR_ALLOW_BIUR, REGULATOR_ALLOW_AUDR")
    for grant in GRANTS:
        run(db, grant)
    print("granted CURATOR / DBSELECT")
    bad = invalid_objects(db)
    if bad:
        raise SystemExit(f"Objects failed to compile: {', '.join(bad)}")


def drop(db) -> None:
    if exists(db, "TABLE", "REGULATOR_ALLOW"):
        count = db.execute(text("SELECT COUNT(*) FROM MULTI.REGULATOR_ALLOW")).scalar()
        if count:
            raise SystemExit(f"REGULATOR_ALLOW holds {count} rows; refusing to drop")
        run(db, "DROP TABLE MULTI.REGULATOR_ALLOW")
        print("dropped table REGULATOR_ALLOW (and its triggers)")
    if exists(db, "TABLE", "REGULATION"):
        count = db.execute(text("SELECT COUNT(*) FROM MULTI.REGULATION")).scalar()
        if count:
            raise SystemExit(f"REGULATION holds {count} rows; refusing to drop")
        run(db, "DROP TABLE MULTI.REGULATION")
        print("dropped table REGULATION (and its triggers)")
    if exists(db, "SEQUENCE", "REGULATION_SEQ"):
        run(db, "DROP SEQUENCE MULTI.REGULATION_SEQ")
        print("dropped sequence REGULATION_SEQ")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="report what exists; change nothing")
    group.add_argument("--drop", action="store_true", help="drop the empty tables and sequence")
    args = parser.parse_args()
    with SessionLocal() as db:
        if args.check:
            report(db)
        elif args.drop:
            drop(db)
        else:
            create(db)
            report(db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
