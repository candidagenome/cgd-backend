#!/usr/bin/env python3
"""
Load new GO annotations transferred from orthologs and delete duplicate IEAs.

This script:
1. Reads new GO annotations from orthology transfer
2. Deletes redundant IEA annotations
3. Deletes previously transferred IEAs
4. Inserts new GO annotations with proper references and qualifiers

Based on load_newGOannots_delDuplicateIEAs.pl.

Usage:
    python load_go_annotations_ortho_transfer.py <strain_abbrev> <from_tabfile>
    python load_go_annotations_ortho_transfer.py C_albicans_SC5314 from_species.txt

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/var/www/cgd"))
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
ADMIN_USER = os.getenv("ADMIN_USER", "ADMIN").upper()

# Data directory for ortholog GO transfer
ORTHO_TRANSFER_DIR = DATA_DIR / "ortholog_GOtransfer"

# Reference DBIDs for GO transfer
REFERENCE_DBIDS = {
    "CGD": "CAL0121033",
    "AspGD": "ASPL0000000005",
}

# Valid qualifiers
VALID_QUALIFIERS = {"colocalizes_with", "contributes_to", "not"}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_organism_no(session, strain_abbrev: str) -> int | None:
    """Get organism_no for a strain abbreviation."""
    query = text(f"""
        SELECT organism_no
        FROM {DB_SCHEMA}.organism
        WHERE organism_abbrev = :abbrev
    """)
    result = session.execute(query, {"abbrev": strain_abbrev}).fetchone()
    return result[0] if result else None


def get_reference_no(session, dbxref_id: str) -> int | None:
    """Get reference_no for a dbxref_id."""
    query = text(f"""
        SELECT reference_no
        FROM {DB_SCHEMA}.reference
        WHERE dbxref_id = :dbxref_id
    """)
    result = session.execute(query, {"dbxref_id": dbxref_id}).fetchone()
    return result[0] if result else None


def get_dbid_to_feature_no(session, organism_no: int) -> tuple[dict, dict]:
    """
    Get mappings for feature identification.

    Returns:
        dbid_to_feat_no: mapping of dbxref_id to feature_no
        feat_name_to_dbid: mapping of feature_name to dbxref_id
    """
    query = text(f"""
        SELECT feature_no, feature_name, dbxref_id
        FROM {DB_SCHEMA}.feature
        WHERE organism_no = :org_no
    """)

    dbid_to_feat_no = {}
    feat_name_to_dbid = {}

    for row in session.execute(query, {"org_no": organism_no}).fetchall():
        feat_no, feat_name, dbid = row
        if dbid:
            dbid_to_feat_no[dbid] = feat_no
        if feat_name and dbid:
            feat_name_to_dbid[feat_name] = dbid

    return dbid_to_feat_no, feat_name_to_dbid


def get_goid_to_go_no(session) -> dict[int, int]:
    """Get mapping of GOID to go_no."""
    query = text(f"""
        SELECT go_no, goid
        FROM {DB_SCHEMA}.go
    """)

    goid_to_go_no = {}
    for row in session.execute(query).fetchall():
        go_no, goid = row
        goid_to_go_no[goid] = go_no

    return goid_to_go_no


def get_go_refs_for_annotation(session) -> dict[int, set[int]]:
    """Get mapping of go_annotation_no to set of go_ref_nos."""
    query = text(f"""
        SELECT DISTINCT go_ref_no, go_annotation_no
        FROM {DB_SCHEMA}.go_ref
    """)

    go_refs = {}
    for row in session.execute(query).fetchall():
        go_ref_no, go_annot_no = row
        if go_annot_no not in go_refs:
            go_refs[go_annot_no] = set()
        go_refs[go_annot_no].add(go_ref_no)

    return go_refs


def get_comp_dbid_to_dbxref_no(
    session,
    ortholog_file: Path,
    feat_name_to_dbid: dict,
    dbxref_source: str,
    dbxref_type: str,
) -> dict[str, int]:
    """
    Read ortholog file and get dbxref_no for comparison organisms.

    Returns mapping of comp_dbid to dbxref_no.
    """
    comp_dbid_to_dbxref_no = {}

    if not ortholog_file.exists():
        logger.warning(f"Ortholog file not found: {ortholog_file}")
        return comp_dbid_to_dbxref_no

    with open(ortholog_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 6:
                continue

            # Format: featNm, geneNm1, dbid1, featNm2, geneNm2, compdbid
            comp_dbid = parts[5]

            # Look up dbxref_no
            query = text(f"""
                SELECT dbxref_no
                FROM {DB_SCHEMA}.dbxref
                WHERE UPPER(source) = :source
                AND dbxref_type = :type
                AND dbxref_id = :id
            """)

            result = session.execute(
                query,
                {
                    "source": dbxref_source.upper(),
                    "type": dbxref_type,
                    "id": comp_dbid,
                },
            ).fetchone()

            if result:
                comp_dbid_to_dbxref_no[comp_dbid] = result[0]

    return comp_dbid_to_dbxref_no


def delete_redundant_ieas(
    session,
    annots_to_delete_file: Path,
    dbid_to_feat_no: dict,
    goid_to_go_no: dict,
    go_refs_for_annotation: dict,
    log_messages: list,
) -> int:
    """
    Delete redundant IEA annotations.

    Returns count of deleted annotations.
    """
    total_deletes = 0

    if not annots_to_delete_file.exists():
        logger.warning(f"Annotations to delete file not found: {annots_to_delete_file}")
        return 0

    with open(annots_to_delete_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("!"):
                continue

            parts = line.split("\t")
            if len(parts) < 9:
                continue

            # Gene association format
            db, dbid, gene_name, qualifier, goid, reference, evidence = parts[:7]

            # Parse GOID
            goid = goid.replace("GO:", "").lstrip("0")
            goid_int = int(goid) if goid else 0

            # Parse reference to get dbxref_id
            ref_match = reference.split(":")
            if len(ref_match) < 2:
                continue
            ref_dbxref_id = ref_match[-1]

            ref_no = get_reference_no(session, ref_dbxref_id)
            if not ref_no:
                log_messages.append(f"No reference found for {ref_dbxref_id}\n")
                continue

            if goid_int not in goid_to_go_no:
                continue

            if dbid not in dbid_to_feat_no:
                continue

            log_messages.append(
                f"Deleting annotation: goid={goid}, dbid={dbid}, evidence={evidence}\n"
            )

            # Find matching go_annotation records
            query = text(f"""
                SELECT go_annotation_no
                FROM {DB_SCHEMA}.go_annotation
                WHERE go_no = :go_no
                AND feature_no = :feat_no
                AND go_evidence = :evidence
            """)

            results = session.execute(
                query,
                {
                    "go_no": goid_to_go_no[goid_int],
                    "feat_no": dbid_to_feat_no[dbid],
                    "evidence": evidence,
                },
            ).fetchall()

            for (go_annot_no,) in results:
                # Find go_ref for this annotation and reference
                query = text(f"""
                    SELECT go_ref_no
                    FROM {DB_SCHEMA}.go_ref
                    WHERE reference_no = :ref_no
                    AND go_annotation_no = :annot_no
                """)

                ref_result = session.execute(
                    query,
                    {"ref_no": ref_no, "annot_no": go_annot_no},
                ).fetchone()

                if ref_result:
                    go_ref_no = ref_result[0]

                    # Delete go_ref (cascades to go_qualifier and goref_dbxref)
                    session.execute(
                        text(f"DELETE FROM {DB_SCHEMA}.go_ref WHERE go_ref_no = :no"),
                        {"no": go_ref_no},
                    )

                    # Update tracking
                    if go_annot_no in go_refs_for_annotation:
                        go_refs_for_annotation[go_annot_no].discard(go_ref_no)

                    log_messages.append(
                        f"Deleted GO_REF: reference={ref_no}, go_annotation_no={go_annot_no}\n"
                    )

                # Delete go_annotation if no more refs
                if (
                    go_annot_no not in go_refs_for_annotation
                    or not go_refs_for_annotation[go_annot_no]
                ):
                    session.execute(
                        text(
                            f"DELETE FROM {DB_SCHEMA}.go_annotation "
                            f"WHERE go_annotation_no = :no"
                        ),
                        {"no": go_annot_no},
                    )
                    total_deletes += 1
                    log_messages.append(
                        f"Deleted GO_ANNOTATION: go_annotation_no={go_annot_no}\n"
                    )

    log_messages.append(f"\nTotal {total_deletes} IEAs were deleted\n\n")
    return total_deletes


def delete_prior_transferred_ieas(
    session,
    ref_no: int,
    organism_no: int,
    go_refs_for_annotation: dict,
    log_messages: list,
) -> int:
    """
    Delete all previously transferred IEA annotations for this organism.

    Returns count of deleted annotations.
    """
    log_messages.append("Deleting all previous transferred IEA annotations\n")

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

    results = session.execute(
        query, {"ref_no": ref_no, "org_no": organism_no}
    ).fetchall()

    total_deletes = 0

    for go_ref_no, go_annot_no in results:
        # Delete go_ref
        session.execute(
            text(f"DELETE FROM {DB_SCHEMA}.go_ref WHERE go_ref_no = :no"),
            {"no": go_ref_no},
        )

        if go_annot_no in go_refs_for_annotation:
            go_refs_for_annotation[go_annot_no].discard(go_ref_no)

        log_messages.append(
            f"Deleted GO_REF: reference={ref_no}, go_annotation_no={go_annot_no}\n"
        )

        # Delete go_annotation if no more refs
        if (
            go_annot_no not in go_refs_for_annotation
            or not go_refs_for_annotation[go_annot_no]
        ):
            session.execute(
                text(
                    f"DELETE FROM {DB_SCHEMA}.go_annotation "
                    f"WHERE go_annotation_no = :no"
                ),
                {"no": go_annot_no},
            )
            total_deletes += 1
            log_messages.append(
                f"Deleted GO_ANNOTATION: go_annotation_no={go_annot_no}\n"
            )

    log_messages.append(
        f"\nTotal {total_deletes} previously transferred IEAs were deleted\n\n"
    )
    return total_deletes


def insert_new_annotations(
    session,
    new_annotations_file: Path,
    ref_no: int,
    dbid_to_feat_no: dict,
    goid_to_go_no: dict,
    comp_dbid_to_dbxref_no: dict,
    log_messages: list,
) -> tuple[int, int, int, int]:
    """
    Insert new GO annotations.

    Returns counts of (go_annotation, go_ref, go_qualifier, goref_dbxref) inserts.
    """
    tot_go_annot = 0
    tot_go_ref = 0
    tot_go_qual = 0
    tot_goref_dbxref = 0

    if not new_annotations_file.exists():
        logger.warning(f"New annotations file not found: {new_annotations_file}")
        return (0, 0, 0, 0)

    with open(new_annotations_file) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.upper().startswith("ASPECT"):
                continue

            parts = line.split("\t")
            if len(parts) < 4:
                continue

            aspect, dbid, goid, qualifier = parts[:4]
            comp_dbids = parts[4].split(",") if len(parts) > 4 and parts[4] else []

            # Parse GOID
            goid = goid.replace("GO:", "").lstrip("0")
            goid_int = int(goid) if goid else 0

            # Validate qualifier
            if qualifier == "none":
                qualifier = None
            if qualifier:
                qualifier = qualifier.replace(" ", "_")
                if qualifier not in VALID_QUALIFIERS:
                    qualifier = None

            has_qual = "Y" if qualifier else "N"

            # Check required lookups
            if goid_int not in goid_to_go_no:
                continue

            if dbid not in dbid_to_feat_no:
                logger.warning(
                    f"Could not identify FEATURE_NO for DBXREF {dbid}: line {line_no}"
                )
                continue

            # Check if annotation already exists
            query = text(f"""
                SELECT go_annotation_no
                FROM {DB_SCHEMA}.go_annotation
                WHERE go_no = :go_no
                AND feature_no = :feat_no
                AND go_evidence = 'IEA'
                AND annotation_type = 'computational'
                AND source = :source
            """)

            result = session.execute(
                query,
                {
                    "go_no": goid_to_go_no[goid_int],
                    "feat_no": dbid_to_feat_no[dbid],
                    "source": PROJECT_ACRONYM,
                },
            ).fetchone()

            if result:
                go_annot_no = result[0]
            else:
                # Insert GO_ANNOTATION
                insert_query = text(f"""
                    INSERT INTO {DB_SCHEMA}.go_annotation
                    (go_no, feature_no, go_evidence, annotation_type, source, created_by)
                    VALUES (:go_no, :feat_no, 'IEA', 'computational', :source, :user)
                    RETURNING go_annotation_no
                """)

                result = session.execute(
                    insert_query,
                    {
                        "go_no": goid_to_go_no[goid_int],
                        "feat_no": dbid_to_feat_no[dbid],
                        "source": PROJECT_ACRONYM,
                        "user": ADMIN_USER,
                    },
                )
                go_annot_no = result.fetchone()[0]
                tot_go_annot += 1

                log_messages.append(
                    f"GO_ANNOTATION Insert: goid={goid}, dbid={dbid}\n"
                )

            # Check if go_ref exists
            query = text(f"""
                SELECT go_ref_no
                FROM {DB_SCHEMA}.go_ref
                WHERE reference_no = :ref_no
                AND go_annotation_no = :annot_no
            """)

            result = session.execute(
                query, {"ref_no": ref_no, "annot_no": go_annot_no}
            ).fetchone()

            if result:
                go_ref_no = result[0]
            else:
                # Insert GO_REF
                insert_query = text(f"""
                    INSERT INTO {DB_SCHEMA}.go_ref
                    (reference_no, go_annotation_no, has_qualifier,
                     has_supporting_evidence, created_by)
                    VALUES (:ref_no, :annot_no, :has_qual, 'Y', :user)
                    RETURNING go_ref_no
                """)

                result = session.execute(
                    insert_query,
                    {
                        "ref_no": ref_no,
                        "annot_no": go_annot_no,
                        "has_qual": has_qual,
                        "user": ADMIN_USER,
                    },
                )
                go_ref_no = result.fetchone()[0]
                tot_go_ref += 1

                log_messages.append(
                    f"GO_REF Insert: goid={goid}, dbid={dbid}\n"
                )

            # Insert qualifier if present
            if qualifier:
                query = text(f"""
                    SELECT go_qualifier_no
                    FROM {DB_SCHEMA}.go_qualifier
                    WHERE go_ref_no = :ref_no
                    AND qualifier = :qual
                """)

                result = session.execute(
                    query, {"ref_no": go_ref_no, "qual": qualifier}
                ).fetchone()

                if not result:
                    session.execute(
                        text(f"""
                            INSERT INTO {DB_SCHEMA}.go_qualifier (go_ref_no, qualifier)
                            VALUES (:ref_no, :qual)
                        """),
                        {"ref_no": go_ref_no, "qual": qualifier},
                    )
                    tot_go_qual += 1

                    log_messages.append(
                        f"GO_QUALIFIER Insert: goid={goid}, dbid={dbid}, qual={qualifier}\n"
                    )

            # Insert support evidence (With/From)
            for comp_dbid in comp_dbids:
                if comp_dbid not in comp_dbid_to_dbxref_no:
                    log_messages.append(
                        f"Could not get DBXREF_NO for COMPDBID: {comp_dbid}\n"
                    )
                    continue

                dbxref_no = comp_dbid_to_dbxref_no[comp_dbid]

                # Check if goref_dbxref exists
                query = text(f"""
                    SELECT 1
                    FROM {DB_SCHEMA}.goref_dbxref
                    WHERE go_ref_no = :ref_no
                    AND dbxref_no = :dbxref_no
                    AND support_type = 'With'
                """)

                result = session.execute(
                    query, {"ref_no": go_ref_no, "dbxref_no": dbxref_no}
                ).fetchone()

                if not result:
                    session.execute(
                        text(f"""
                            INSERT INTO {DB_SCHEMA}.goref_dbxref
                            (go_ref_no, dbxref_no, support_type)
                            VALUES (:ref_no, :dbxref_no, 'With')
                        """),
                        {"ref_no": go_ref_no, "dbxref_no": dbxref_no},
                    )
                    tot_goref_dbxref += 1

                    log_messages.append(
                        f"GOREF_DBXREF Insert: GO_REF_NO={go_ref_no}, "
                        f"DBXREF_NO={dbxref_no}\n"
                    )

    log_messages.append(
        f"\nIn total: {tot_go_annot} GO_ANNOTATION, {tot_go_ref} GO_REF, "
        f"{tot_go_qual} GO_QUALIFIER, {tot_goref_dbxref} GOREF_DBXREF inserted\n\n"
    )

    return (tot_go_annot, tot_go_ref, tot_go_qual, tot_goref_dbxref)


def read_from_tabfile(filepath: Path) -> dict:
    """
    Read tab-delimited file with source organism information.

    Returns dict mapping organism_abbrev to info dict.
    """
    from_info = {}

    if not filepath.exists():
        return from_info

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 4:
                continue

            from_abbrev = parts[0]
            gene_assoc_ext = parts[1]
            ortho_dir = parts[2]
            ortho_file = parts[3]

            # Make ortho_dir absolute if relative
            if not ortho_dir.startswith("/"):
                ortho_dir = str(PROJECT_ROOT / ortho_dir)
            if not ortho_dir.endswith("/"):
                ortho_dir += "/"

            from_info[from_abbrev] = {
                "gene_assoc_ext": gene_assoc_ext,
                "ortho_dir": ortho_dir,
                "ortho_file": ortho_file,
            }

    return from_info


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load new GO annotations from orthology transfer"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Receiving strain abbreviation",
    )
    parser.add_argument(
        "from_tabfile",
        help="Tab-delimited file listing source species",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    strain_abbrev = args.strain_abbrev
    from_tabfile = Path(args.from_tabfile)

    # Make path absolute if relative
    if not from_tabfile.is_absolute():
        from_tabfile = PROJECT_ROOT / from_tabfile

    if not from_tabfile.exists():
        logger.error(f"From tabfile not found: {from_tabfile}")
        return 1

    # Set up log file
    log_file = LOG_DIR / "load" / f"load_newGOannotations_delDuplicateIEAs_{strain_abbrev}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_messages = []

    # Input files
    annots_to_delete_file = ORTHO_TRANSFER_DIR / f"AnnotsToDelete_{strain_abbrev}.txt"
    new_annotations_file = ORTHO_TRANSFER_DIR / f"newAnnotations_{strain_abbrev}.txt"

    try:
        with SessionLocal() as session:
            # Get organism
            organism_no = get_organism_no(session, strain_abbrev)
            if not organism_no:
                logger.error(f"No organism found for {strain_abbrev}")
                return 1

            logger.info(f"Processing strain: {strain_abbrev}")

            # Get reference for GO transfer
            ref_dbid = REFERENCE_DBIDS.get(PROJECT_ACRONYM, "CAL0121033")
            ref_no = get_reference_no(session, ref_dbid)
            if not ref_no:
                logger.error(f"Reference not found: {ref_dbid}")
                return 1

            # Get mappings
            logger.info("Getting feature mappings...")
            dbid_to_feat_no, feat_name_to_dbid = get_dbid_to_feature_no(
                session, organism_no
            )
            logger.info(f"Found {len(dbid_to_feat_no)} feature mappings")

            logger.info("Getting GO mappings...")
            goid_to_go_no = get_goid_to_go_no(session)
            logger.info(f"Found {len(goid_to_go_no)} GO term mappings")

            # Read from-species information
            from_info = read_from_tabfile(from_tabfile)
            logger.info(f"Found {len(from_info)} source organisms")

            # Get ortholog dbxref mappings
            comp_dbid_to_dbxref_no = {}
            for from_abbrev, info in from_info.items():
                gene_assoc_ext = info["gene_assoc_ext"]

                if gene_assoc_ext.upper() == PROJECT_ACRONYM.upper():
                    dbxref_source = PROJECT_ACRONYM
                    dbxref_type = f"{PROJECT_ACRONYM}ID Primary"
                else:
                    dbxref_source = gene_assoc_ext.upper()
                    dbxref_type = "Gene ID"

                ortholog_file = Path(info["ortho_dir"]) / info["ortho_file"]

                mappings = get_comp_dbid_to_dbxref_no(
                    session,
                    ortholog_file,
                    feat_name_to_dbid,
                    dbxref_source,
                    dbxref_type,
                )
                comp_dbid_to_dbxref_no.update(mappings)

            logger.info(f"Found {len(comp_dbid_to_dbxref_no)} ortholog dbxref mappings")

            # Get existing go_refs
            go_refs_for_annotation = get_go_refs_for_annotation(session)

            # Delete redundant IEAs
            logger.info("Deleting redundant IEAs...")
            delete_redundant_ieas(
                session,
                annots_to_delete_file,
                dbid_to_feat_no,
                goid_to_go_no,
                go_refs_for_annotation,
                log_messages,
            )

            # Delete prior transferred IEAs
            logger.info("Deleting prior transferred IEAs...")
            delete_prior_transferred_ieas(
                session,
                ref_no,
                organism_no,
                go_refs_for_annotation,
                log_messages,
            )

            # Insert new annotations
            logger.info("Inserting new annotations...")
            insert_new_annotations(
                session,
                new_annotations_file,
                ref_no,
                dbid_to_feat_no,
                goid_to_go_no,
                comp_dbid_to_dbxref_no,
                log_messages,
            )

            # Commit changes
            session.commit()

            # Rename processed files
            for filepath in [annots_to_delete_file, new_annotations_file]:
                if filepath.exists():
                    shutil.move(str(filepath), str(filepath) + ".old")

            log_messages.append("Finished.\n\n")
            logger.info("GO annotation transfer completed successfully")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    # Write log file
    with open(log_file, "w") as f:
        for msg in log_messages:
            f.write(f"{datetime.now()}: {msg}")

    logger.info(f"Log written to {log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
