#!/usr/bin/env python3
"""Read-only introspection to scope the C. tropicalis tRNA load.

Dumps:
  A) C. tropicalis genome scaffolding (organism_no, genome versions, contigs +
     their genomic SEQ seq_no that tRNA FEAT_LOCATIONs would point at).
  B) A fully-worked example of how existing tRNAs are modeled in species that
     already have them (C. glabrata, then C. albicans): FEATURE row, FEAT_LOCATION,
     SEQ, FEAT_PROPERTY, FEAT_RELATIONSHIP children, DBXREF_FEAT.
  C) Controlled-vocab / convention facts: tRNA feature_name & dbxref_id patterns,
     property_type values used by tRNAs, whether tRNAs carry genomic SEQ, and the
     relevant CODE rows.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from sqlalchemy import text  # noqa: E402
from cgd.db.engine import SessionLocal  # noqa: E402
from cgd.core.settings import settings  # noqa: E402

S = settings.db_schema or "MULTI"


def q(db, sql, **kw):
    return db.execute(text(sql), kw).fetchall()


def show(title, rows, cols=None):
    print(f"\n--- {title} ---")
    if not rows:
        print("  (none)")
        return
    if cols:
        print("  " + " | ".join(cols))
    for r in rows:
        print("  " + " | ".join("" if v is None else str(v)[:70] for v in r))


def main():
    with SessionLocal() as db:
        # ---- A) C. tropicalis scaffolding ----
        print("=" * 70)
        print("A) C. tropicalis genome scaffolding")
        print("=" * 70)
        org = q(db, f"""
            SELECT organism_no, organism_name, organism_abbrev, common_name
            FROM {S}.organism WHERE organism_name LIKE 'Candida tropicalis%'
        """)
        show("organism", org, ["organism_no", "name", "abbrev", "common_name"])
        if not org:
            return
        org_no = org[0][0]

        show("genome_version(s)", q(db, f"""
            SELECT genome_version_no, genome_version, is_ver_current, description
            FROM {S}.genome_version WHERE organism_no = :o
        """, o=org_no), ["gv_no", "version", "current", "descr"])

        show("contig/chromosome features + genomic seq_no (root_seq_no targets)", q(db, f"""
            SELECT f.feature_no, f.feature_name, f.feature_type,
                   s.seq_no, s.seq_type, s.seq_length, s.is_seq_current, s.genome_version_no
            FROM {S}.feature f
            JOIN {S}.seq s ON s.feature_no = f.feature_no
            WHERE f.organism_no = :o
              AND f.feature_type IN ('contig','chromosome')
            ORDER BY f.feature_name
        """, o=org_no), ["feat_no", "name", "ftype", "seq_no", "seq_type", "len", "cur", "gv_no"])

        # existing dbxref_id range for tropicalis (how new IDs are minted)
        show("tropicalis dbxref_id sample (ID convention)", q(db, f"""
            SELECT feature_type, MIN(dbxref_id), MAX(dbxref_id), COUNT(*)
            FROM {S}.feature WHERE organism_no = :o
            GROUP BY feature_type ORDER BY feature_type
        """, o=org_no), ["ftype", "min_dbxref", "max_dbxref", "n"])

        # ---- B) worked tRNA examples from species that have them ----
        for sp in ("Candida glabrata%", "Candida albicans%"):
            print("\n" + "=" * 70)
            print(f"B) Example tRNAs in {sp}")
            print("=" * 70)
            ex = q(db, f"""
                SELECT f.feature_no, f.feature_name, f.gene_name, f.dbxref_id,
                       f.feature_type, f.source, f.headline
                FROM {S}.feature f JOIN {S}.organism o ON o.organism_no = f.organism_no
                WHERE o.organism_name LIKE :sp AND f.feature_type = 'tRNA'
                  AND ROWNUM <= 5
            """, sp=sp)
            show("sample tRNA FEATURE rows", ex,
                 ["feat_no", "feat_name", "gene_name", "dbxref_id", "ftype", "source", "headline"])
            if not ex:
                continue
            fno = ex[0][0]
            show(f"FEAT_LOCATION for feature_no={fno}", q(db, f"""
                SELECT feat_location_no, root_seq_no, start_coord, stop_coord,
                       strand, is_loc_current, coord_version
                FROM {S}.feat_location WHERE feature_no = :f
            """, f=fno), ["fl_no", "root_seq_no", "start", "stop", "strand", "cur", "coord_ver"])
            show(f"SEQ for feature_no={fno}", q(db, f"""
                SELECT seq_no, seq_type, is_seq_current, seq_length, source, SUBSTR(residues,1,40)
                FROM {S}.seq WHERE feature_no = :f
            """, f=fno), ["seq_no", "seq_type", "cur", "len", "source", "residues[:40]"])
            show(f"FEAT_PROPERTY for feature_no={fno}", q(db, f"""
                SELECT feat_property_no, property_type, property_value, source
                FROM {S}.feat_property WHERE feature_no = :f
            """, f=fno), ["fp_no", "property_type", "value", "source"])
            show(f"FEAT_RELATIONSHIP (as parent) for feature_no={fno}", q(db, f"""
                SELECT fr.child_feature_no, cf.feature_type, cf.feature_name, fr.relationship_type
                FROM {S}.feat_relationship fr
                JOIN {S}.feature cf ON cf.feature_no = fr.child_feature_no
                WHERE fr.parent_feature_no = :f
            """, f=fno), ["child_feat_no", "child_type", "child_name", "rel_type"])
            show(f"DBXREF_FEAT for feature_no={fno}", q(db, f"""
                SELECT df.dbxref_feat_no, d.dbxref_id, d.dbxref_type, d.source
                FROM {S}.dbxref_feat df JOIN {S}.dbxref d ON d.dbxref_no = df.dbxref_no
                WHERE df.feature_no = :f
            """, f=fno), ["df_no", "dbxref_id", "dbxref_type", "source"])

        # ---- C) conventions & controlled vocab ----
        print("\n" + "=" * 70)
        print("C) Conventions & controlled vocab (all species with tRNAs)")
        print("=" * 70)
        show("tRNA feature_name / gene_name patterns per species", q(db, f"""
            SELECT o.organism_abbrev, f.feature_name, f.gene_name, f.dbxref_id
            FROM {S}.feature f JOIN {S}.organism o ON o.organism_no = f.organism_no
            WHERE f.feature_type = 'tRNA' AND ROWNUM <= 40
            ORDER BY o.organism_abbrev, f.feature_name
        """), ["abbrev", "feature_name", "gene_name", "dbxref_id"])
        show("distinct property_type used by tRNA features", q(db, f"""
            SELECT fp.property_type, COUNT(*) FROM {S}.feat_property fp
            WHERE fp.feature_no IN (SELECT feature_no FROM {S}.feature WHERE feature_type='tRNA')
            GROUP BY fp.property_type ORDER BY 2 DESC
        """), ["property_type", "n"])
        show("do tRNAs carry SEQ? seq_type breakdown", q(db, f"""
            SELECT s.seq_type, s.is_seq_current, COUNT(*) FROM {S}.seq s
            WHERE s.feature_no IN (SELECT feature_no FROM {S}.feature WHERE feature_type='tRNA')
            GROUP BY s.seq_type, s.is_seq_current ORDER BY 3 DESC
        """), ["seq_type", "cur", "n"])
        show("do tRNAs have child features (relationship types)?", q(db, f"""
            SELECT fr.relationship_type, cf.feature_type, COUNT(*)
            FROM {S}.feat_relationship fr
            JOIN {S}.feature cf ON cf.feature_no = fr.child_feature_no
            WHERE fr.parent_feature_no IN (SELECT feature_no FROM {S}.feature WHERE feature_type='tRNA')
            GROUP BY fr.relationship_type, cf.feature_type
        """), ["rel_type", "child_type", "n"])
        show("CODE: FEATURE.FEATURE_TYPE tRNA", q(db, f"""
            SELECT tab_name, col_name, code_value, description FROM {S}.code
            WHERE tab_name='FEATURE' AND col_name='FEATURE_TYPE' AND code_value='tRNA'
        """), ["tab", "col", "value", "descr"])
        show("CODE: FEATURE.SOURCE values", q(db, f"""
            SELECT code_value, description FROM {S}.code
            WHERE tab_name='FEATURE' AND col_name='SOURCE' ORDER BY code_value
        """), ["value", "descr"])
        show("CODE: SEQ.SOURCE values", q(db, f"""
            SELECT code_value FROM {S}.code
            WHERE tab_name='SEQ' AND col_name='SOURCE' ORDER BY code_value
        """), ["value"])
        show("CODE: FEAT_PROPERTY.PROPERTY_TYPE values", q(db, f"""
            SELECT code_value, description FROM {S}.code
            WHERE tab_name='FEAT_PROPERTY' AND col_name='PROPERTY_TYPE' ORDER BY code_value
        """), ["value", "descr"])


if __name__ == "__main__":
    main()
