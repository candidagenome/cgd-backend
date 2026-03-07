#!/usr/bin/env python3
"""
Load new GO annotations based on orthology and delete duplicate IEAs.

This script loads newly transferred GO annotations based on orthology.
It reads files containing new annotations to add and annotations to delete,
and updates the database accordingly.

Based on load_newGOannots_delDuplicateIEAs.pl by Prachi Shah (Apr 2008)

Usage:
    python load_go_annotations_ortho.py --strain C_albicans_SC5314 --from-file species.tab

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Data directory
    LOG_DIR: Log directory
    ADMIN_USER: Admin username
    PROJECT_ACRONYM: Project acronym
"""

import argparse
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
PROJECT = os.getenv("PROJECT", "CGD")

# Reference IDs for GO transfer (project-specific)
REFERENCE_IDS = {
    "CGD": "CAL0121033",
    "AspGD": "ASPL0000000005",
}

# Valid GO qualifiers
VALID_QUALIFIERS = {"colocalizes_with", "contributes_to", "not"}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class GOAnnotationLoader:
    """Load GO annotations from orthology transfer."""

    def __init__(self, session, strain_abbrev: str):
        self.session = session
        self.strain_abbrev = strain_abbrev

        # Get organism_no
        self.organism_no = self._get_organism_no()
        if not self.organism_no:
            raise ValueError(f"No organism found for: {strain_abbrev}")

        # Get reference number for GO transfer
        ref_dbid = REFERENCE_IDS.get(PROJECT, REFERENCE_IDS["CGD"])
        self.reference_no = self._get_reference_no(ref_dbid)
        if not self.reference_no:
            raise ValueError(f"No reference found for DBID: {ref_dbid}")

        # Caches
        self.dbid_to_feat_no: dict[str, int] = {}
        self.feat_name_to_dbid: dict[str, str] = {}
        self.goid_to_go_no: dict[str, int] = {}
        self.go_refs_by_annot: dict[int, set[int]] = {}
        self.comp_dbid_to_dbxref_no: dict[str, int] = {}

        # Counters
        self.go_annot_insert_count = 0
        self.go_ref_insert_count = 0
        self.go_qual_insert_count = 0
        self.goref_dbxref_insert_count = 0
        self.delete_count = 0

    def _get_organism_no(self) -> int | None:
        """Get organism_no for strain."""
        query = text(f"""
            SELECT organism_no FROM {DB_SCHEMA}.organism
            WHERE organism_abbrev = :abbrev
        """)
        result = self.session.execute(query, {"abbrev": self.strain_abbrev}).first()
        return result[0] if result else None

    def _get_reference_no(self, dbxref_id: str) -> int | None:
        """Get reference_no for a DBXREF ID."""
        query = text(f"""
            SELECT reference_no FROM {DB_SCHEMA}.reference
            WHERE dbxref_id = :dbid
        """)
        result = self.session.execute(query, {"dbid": dbxref_id}).first()
        return result[0] if result else None

    def load_caches(self) -> None:
        """Load database caches."""
        # Load DBID to feature_no mapping
        query = text(f"""
            SELECT feature_no, feature_name, dbxref_id
            FROM {DB_SCHEMA}.feature
            WHERE organism_no = :org_no
        """)
        result = self.session.execute(query, {"org_no": self.organism_no})
        for feat_no, feat_name, dbid in result:
            if dbid:
                self.dbid_to_feat_no[dbid] = feat_no
            if feat_name:
                self.feat_name_to_dbid[feat_name] = dbid

        # Load GOID to go_no mapping
        query = text(f"SELECT go_no, goid FROM {DB_SCHEMA}.go")
        result = self.session.execute(query)
        for go_no, goid in result:
            self.goid_to_go_no[str(goid)] = go_no

        # Load GO refs by annotation
        query = text(f"""
            SELECT go_ref_no, go_annotation_no FROM {DB_SCHEMA}.go_ref
        """)
        result = self.session.execute(query)
        for go_ref_no, go_annot_no in result:
            if go_annot_no not in self.go_refs_by_annot:
                self.go_refs_by_annot[go_annot_no] = set()
            self.go_refs_by_annot[go_annot_no].add(go_ref_no)

        logger.info(f"Loaded {len(self.dbid_to_feat_no)} features")
        logger.info(f"Loaded {len(self.goid_to_go_no)} GO terms")

    def load_ortholog_mappings(self, ortholog_file: Path, dbxref_source: str, dbxref_type: str) -> None:
        """
        Load ortholog DBID to DBXREF_NO mappings.

        Args:
            ortholog_file: Path to ortholog file
            dbxref_source: DBXREF source (e.g., 'SGD')
            dbxref_type: DBXREF type (e.g., 'Gene ID')
        """
        if not ortholog_file.exists():
            logger.warning(f"Ortholog file not found: {ortholog_file}")
            return

        with open(ortholog_file, "r") as f:
            for line in f:
                if line.startswith("#"):
                    continue

                parts = line.strip().split("\t")
                if len(parts) < 6:
                    continue

                comp_dbid = parts[5]  # Comparative DBID

                # Look up DBXREF_NO
                query = text(f"""
                    SELECT dbxref_no FROM {DB_SCHEMA}.dbxref
                    WHERE UPPER(source) = :source
                    AND dbxref_type = :dtype
                    AND dbxref_id = :dbid
                """)
                result = self.session.execute(query, {
                    "source": dbxref_source.upper(),
                    "dtype": dbxref_type,
                    "dbid": comp_dbid,
                }).first()

                if result:
                    self.comp_dbid_to_dbxref_no[comp_dbid] = result[0]

        logger.info(f"Loaded {len(self.comp_dbid_to_dbxref_no)} ortholog mappings")

    def delete_redundant_ieas(self, annots_to_delete_file: Path) -> int:
        """
        Delete redundant IEA annotations.

        Args:
            annots_to_delete_file: File containing annotations to delete

        Returns:
            Number of annotations deleted
        """
        if not annots_to_delete_file.exists():
            logger.warning(f"Delete file not found: {annots_to_delete_file}")
            return 0

        deleted = 0

        with open(annots_to_delete_file, "r") as f:
            for line in f:
                if line.startswith("!"):
                    continue

                parts = line.strip().split("\t")
                if len(parts) < 9:
                    continue

                db, dbid, gene_name, qualifier, goid, reference, evidence, with_col, aspect = parts[:9]

                # Parse GOID
                goid = re.sub(r"^GO:0*", "", goid)

                # Parse reference to get reference_no
                ref_match = re.search(rf"{PROJECT_ACRONYM}_REF:(\w+\d+)", reference)
                if not ref_match:
                    continue

                ref_dbid = ref_match.group(1)
                ref_no = self._get_reference_no(ref_dbid)
                if not ref_no:
                    continue

                # Get GO annotation
                go_no = self.goid_to_go_no.get(goid)
                feat_no = self.dbid_to_feat_no.get(dbid)

                if not go_no or not feat_no:
                    continue

                # Find matching annotations
                query = text(f"""
                    SELECT go_annotation_no
                    FROM {DB_SCHEMA}.go_annotation
                    WHERE go_no = :go_no
                    AND feature_no = :feat_no
                    AND go_evidence = :evidence
                """)
                result = self.session.execute(query, {
                    "go_no": go_no,
                    "feat_no": feat_no,
                    "evidence": evidence,
                })

                for row in result:
                    go_annot_no = row[0]

                    # Delete GO_REF entry
                    delete_ref = text(f"""
                        DELETE FROM {DB_SCHEMA}.go_ref
                        WHERE reference_no = :ref_no
                        AND go_annotation_no = :annot_no
                    """)
                    self.session.execute(delete_ref, {
                        "ref_no": ref_no,
                        "annot_no": go_annot_no,
                    })

                    # Update cache
                    if go_annot_no in self.go_refs_by_annot:
                        self.go_refs_by_annot[go_annot_no].discard(go_annot_no)

                    # Delete GO_ANNOTATION if no more refs
                    if not self.go_refs_by_annot.get(go_annot_no):
                        delete_annot = text(f"""
                            DELETE FROM {DB_SCHEMA}.go_annotation
                            WHERE go_annotation_no = :annot_no
                        """)
                        self.session.execute(delete_annot, {"annot_no": go_annot_no})
                        deleted += 1
                        logger.debug(f"Deleted annotation: {go_annot_no}")

        self.session.commit()
        logger.info(f"Deleted {deleted} redundant IEA annotations")
        return deleted

    def delete_prior_transferred_ieas(self) -> int:
        """
        Delete previously transferred IEA annotations.

        Returns:
            Number of annotations deleted
        """
        # Find all transferred IEAs for this strain
        query = text(f"""
            SELECT DISTINCT gr.go_ref_no, gr.go_annotation_no
            FROM {DB_SCHEMA}.go_ref gr
            JOIN {DB_SCHEMA}.go_annotation ga ON gr.go_annotation_no = ga.go_annotation_no
            JOIN {DB_SCHEMA}.feature f ON ga.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.goref_dbxref gd ON gr.go_ref_no = gd.go_ref_no
            WHERE gr.reference_no = :ref_no
            AND f.organism_no = :org_no
            AND gd.support_type = 'With'
        """)

        result = self.session.execute(query, {
            "ref_no": self.reference_no,
            "org_no": self.organism_no,
        })

        rows = list(result)
        deleted = 0

        for go_ref_no, go_annot_no in rows:
            # Delete GO_REF
            delete_ref = text(f"""
                DELETE FROM {DB_SCHEMA}.go_ref
                WHERE go_ref_no = :ref_no
            """)
            self.session.execute(delete_ref, {"ref_no": go_ref_no})

            # Update cache
            if go_annot_no in self.go_refs_by_annot:
                self.go_refs_by_annot[go_annot_no].discard(go_ref_no)

            # Delete GO_ANNOTATION if no more refs
            if not self.go_refs_by_annot.get(go_annot_no):
                delete_annot = text(f"""
                    DELETE FROM {DB_SCHEMA}.go_annotation
                    WHERE go_annotation_no = :annot_no
                """)
                self.session.execute(delete_annot, {"annot_no": go_annot_no})
                deleted += 1

        self.session.commit()
        logger.info(f"Deleted {deleted} previously transferred IEAs")
        return deleted

    def insert_new_annotations(self, new_annotations_file: Path) -> dict:
        """
        Insert new GO annotations.

        Args:
            new_annotations_file: File containing new annotations

        Returns:
            Dict with insert counts
        """
        if not new_annotations_file.exists():
            logger.warning(f"New annotations file not found: {new_annotations_file}")
            return {}

        with open(new_annotations_file, "r") as f:
            for line in f:
                line = line.strip()

                if line.startswith("ASPECT") or not line:
                    continue

                parts = line.split("\t")
                if len(parts) < 4:
                    continue

                aspect = parts[0]
                dbid = parts[1]
                goid = parts[2]
                qualifier = parts[3] if len(parts) > 3 else None
                comp_dbids = parts[4].split(",") if len(parts) > 4 and parts[4] else []

                # Clean up GOID
                goid = re.sub(r"^GO:", "", goid)
                goid = goid.lstrip("0") or "0"

                # Clean up qualifier
                if qualifier == "none":
                    qualifier = None
                if qualifier:
                    qualifier = qualifier.replace(" ", "_")
                    if qualifier not in VALID_QUALIFIERS:
                        qualifier = None

                has_qualifier = "Y" if qualifier else "N"

                # Look up IDs
                go_no = self.goid_to_go_no.get(goid)
                feat_no = self.dbid_to_feat_no.get(dbid)

                if not go_no:
                    logger.warning(f"Unknown GOID: {goid}")
                    continue

                if not feat_no:
                    logger.warning(f"Unknown DBID: {dbid}")
                    continue

                # Check if annotation exists
                check_query = text(f"""
                    SELECT go_annotation_no
                    FROM {DB_SCHEMA}.go_annotation
                    WHERE go_no = :go_no
                    AND feature_no = :feat_no
                    AND go_evidence = 'IEA'
                    AND annotation_type = 'computational'
                    AND source = :source
                """)
                result = self.session.execute(check_query, {
                    "go_no": go_no,
                    "feat_no": feat_no,
                    "source": PROJECT_ACRONYM,
                }).first()

                if result:
                    go_annot_no = result[0]
                else:
                    # Insert new annotation
                    insert_annot = text(f"""
                        INSERT INTO {DB_SCHEMA}.go_annotation
                        (go_no, feature_no, go_evidence, annotation_type, source, created_by)
                        VALUES (:go_no, :feat_no, 'IEA', 'computational', :source, :user)
                    """)
                    self.session.execute(insert_annot, {
                        "go_no": go_no,
                        "feat_no": feat_no,
                        "source": PROJECT_ACRONYM,
                        "user": ADMIN_USER,
                    })

                    # Get the inserted ID
                    get_id = text(f"""
                        SELECT go_annotation_no
                        FROM {DB_SCHEMA}.go_annotation
                        WHERE go_no = :go_no
                        AND feature_no = :feat_no
                        AND go_evidence = 'IEA'
                        AND annotation_type = 'computational'
                        AND source = :source
                    """)
                    result = self.session.execute(get_id, {
                        "go_no": go_no,
                        "feat_no": feat_no,
                        "source": PROJECT_ACRONYM,
                    }).first()
                    go_annot_no = result[0]

                    self.go_annot_insert_count += 1
                    logger.debug(f"Inserted annotation for goid={goid}, dbid={dbid}")

                # Check if GO_REF exists
                check_ref = text(f"""
                    SELECT go_ref_no
                    FROM {DB_SCHEMA}.go_ref
                    WHERE reference_no = :ref_no
                    AND go_annotation_no = :annot_no
                """)
                result = self.session.execute(check_ref, {
                    "ref_no": self.reference_no,
                    "annot_no": go_annot_no,
                }).first()

                if result:
                    go_ref_no = result[0]
                else:
                    # Insert GO_REF
                    insert_ref = text(f"""
                        INSERT INTO {DB_SCHEMA}.go_ref
                        (reference_no, go_annotation_no, has_qualifier, has_supporting_evidence, created_by)
                        VALUES (:ref_no, :annot_no, :has_qual, 'Y', :user)
                    """)
                    self.session.execute(insert_ref, {
                        "ref_no": self.reference_no,
                        "annot_no": go_annot_no,
                        "has_qual": has_qualifier,
                        "user": ADMIN_USER,
                    })

                    # Get inserted ID
                    result = self.session.execute(check_ref, {
                        "ref_no": self.reference_no,
                        "annot_no": go_annot_no,
                    }).first()
                    go_ref_no = result[0]

                    self.go_ref_insert_count += 1

                # Insert qualifier if needed
                if qualifier:
                    check_qual = text(f"""
                        SELECT go_qualifier_no
                        FROM {DB_SCHEMA}.go_qualifier
                        WHERE go_ref_no = :ref_no
                        AND qualifier = :qual
                    """)
                    result = self.session.execute(check_qual, {
                        "ref_no": go_ref_no,
                        "qual": qualifier,
                    }).first()

                    if not result:
                        insert_qual = text(f"""
                            INSERT INTO {DB_SCHEMA}.go_qualifier
                            (go_ref_no, qualifier)
                            VALUES (:ref_no, :qual)
                        """)
                        self.session.execute(insert_qual, {
                            "ref_no": go_ref_no,
                            "qual": qualifier,
                        })
                        self.go_qual_insert_count += 1

                # Insert GOREF_DBXREF for each comparison DBID
                for comp_dbid in comp_dbids:
                    comp_dbid = comp_dbid.strip()
                    if not comp_dbid:
                        continue

                    dbxref_no = self.comp_dbid_to_dbxref_no.get(comp_dbid)
                    if not dbxref_no:
                        logger.warning(f"Unknown comp DBID: {comp_dbid}")
                        continue

                    # Check if exists
                    check_dbxref = text(f"""
                        SELECT go_ref_no
                        FROM {DB_SCHEMA}.goref_dbxref
                        WHERE go_ref_no = :ref_no
                        AND dbxref_no = :dbxref_no
                        AND support_type = 'With'
                    """)
                    result = self.session.execute(check_dbxref, {
                        "ref_no": go_ref_no,
                        "dbxref_no": dbxref_no,
                    }).first()

                    if not result:
                        insert_dbxref = text(f"""
                            INSERT INTO {DB_SCHEMA}.goref_dbxref
                            (go_ref_no, dbxref_no, support_type)
                            VALUES (:ref_no, :dbxref_no, 'With')
                        """)
                        self.session.execute(insert_dbxref, {
                            "ref_no": go_ref_no,
                            "dbxref_no": dbxref_no,
                        })
                        self.goref_dbxref_insert_count += 1

        self.session.commit()

        return {
            "go_annotations": self.go_annot_insert_count,
            "go_refs": self.go_ref_insert_count,
            "go_qualifiers": self.go_qual_insert_count,
            "goref_dbxrefs": self.goref_dbxref_insert_count,
        }


def load_go_annotations_ortho(
    strain_abbrev: str,
    from_tabfile: Path | None = None,
) -> bool:
    """
    Main function to load GO annotations from orthology.

    Args:
        strain_abbrev: Strain abbreviation
        from_tabfile: Tab file listing species to transfer from

    Returns:
        True on success, False on failure
    """
    # Set up logging
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / "load" / f"load_newGOannotations_delDuplicateIEAs_{strain_abbrev}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Starting GO annotation load for {strain_abbrev}")
    logger.info(f"Started at {datetime.now()}")

    # Define data files
    data_dir = DATA_DIR / "ortholog_GOtransfer"
    annots_to_delete = data_dir / f"AnnotsToDelete_{strain_abbrev}.txt"
    new_annotations = data_dir / f"newAnnotations_{strain_abbrev}.txt"

    try:
        with SessionLocal() as session:
            loader = GOAnnotationLoader(session, strain_abbrev)

            # Load caches
            loader.load_caches()

            # Delete redundant IEAs
            if annots_to_delete.exists():
                loader.delete_redundant_ieas(annots_to_delete)

            # Delete prior transferred IEAs
            loader.delete_prior_transferred_ieas()

            # Insert new annotations
            if new_annotations.exists():
                results = loader.insert_new_annotations(new_annotations)
                logger.info(f"\nInsert summary:")
                logger.info(f"  GO_ANNOTATION: {results.get('go_annotations', 0)}")
                logger.info(f"  GO_REF: {results.get('go_refs', 0)}")
                logger.info(f"  GO_QUALIFIER: {results.get('go_qualifiers', 0)}")
                logger.info(f"  GOREF_DBXREF: {results.get('goref_dbxrefs', 0)}")

            # Rename processed files
            for file_path in [annots_to_delete, new_annotations]:
                if file_path.exists():
                    old_path = file_path.with_suffix(file_path.suffix + ".old")
                    file_path.rename(old_path)
                    logger.info(f"Renamed {file_path} to {old_path}")

            logger.info(f"Finished at {datetime.now()}")
            return True

    except Exception as e:
        logger.exception(f"Error loading GO annotations: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load new GO annotations from orthology transfer"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--from-file",
        type=Path,
        help="Tab file listing species to transfer GO from",
    )

    args = parser.parse_args()

    success = load_go_annotations_ortho(args.strain, args.from_file)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
