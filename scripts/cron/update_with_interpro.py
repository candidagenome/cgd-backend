#!/usr/bin/env python3
"""
Update GO annotations based on InterPro domain predictions.

This script reads InterProScan results and updates GO annotations for features
based on domain predictions. It handles GO term obsolescence, replacement,
taxonomy constraints, and removes redundant annotations.

Based on updateWithIpro.pl by CGD team.

Usage:
    python update_with_interpro.py --strain C_albicans_SC5314
    python update_with_interpro.py --strain C_albicans_SC5314 --download --update

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name
    PROJECT_ACRONYM: Project acronym (CGD or AspGD)
    DATA_DIR: Directory for data files
    LOG_DIR: Directory for log files
"""

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configuration from environment
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")
DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data/cgd"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/var/log/cgd"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")

# InterPro2GO source URL
INTERPRO2GO_URL = "https://ftp.ebi.ac.uk/pub/databases/interpro/current_release/interpro2go"

# E-value cutoff for domain predictions
EVAL_CUTOFF = 1.0

# Citation pattern for InterPro-based GO annotations
IPR_CITATION_PATTERN = "%Prediction of Gene Ontology (GO) annotations based on protein characteristics (e.g., domains and motifs)%"

# Commit interval
COMMIT_LIMIT = 1000

# GO IDs that have been replaced with newer terms
GOID_REPLACED = {
    8415: 16746,
    16291: 47617,
    16455: 1104,
    16481: 45892,
    23034: 35556,
    32583: 6355,
    45449: 6355,
    45941: 45893,
    267: 5575,
    5626: 5575,
    5625: 5575,
    1950: 5886,
    5624: 16020,
    299: 16021,
    300: 19898,
    42598: 31982,
    5792: 43231,
}

# GO IDs that should not be used (too general or deprecated)
GOID_DELETED = {
    5515, 3702, 3704, 3709, 3711, 6350, 8159,
    16251, 16564, 16565, 16566, 16986, 30528,
    5488, 33903, 4437, 4428,
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@dataclass
class FeatureGOAnnotation:
    """A GO annotation for a feature."""
    go_annotation_no: int
    goid: int
    go_evidence: str


@dataclass
class InterProDomain:
    """An InterPro domain assignment."""
    feature_name: str
    interpro_id: str
    e_value: float


@dataclass
class ProcessingStats:
    """Statistics for the processing run."""
    features_processed: int = 0
    go_annotations_inserted: int = 0
    go_refs_inserted: int = 0
    goref_dbxrefs_inserted: int = 0
    nd_ancestors_found: int = 0
    nd_deletions: int = 0
    unique_feature_go: int = 0
    unique_feature_go_ipro: int = 0


def download_interpro2go(output_file: Path) -> bool:
    """Download the Interpro2GO mapping file."""
    try:
        logger.info(f"Downloading Interpro2GO from {INTERPRO2GO_URL}")
        urlretrieve(INTERPRO2GO_URL, output_file)
        logger.info(f"Downloaded to {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error downloading Interpro2GO: {e}")
        return False


def read_go_obo(obo_file: Path) -> set[int]:
    """
    Read GO OBO file and return set of obsolete GO IDs.

    Returns set of numeric GO IDs (without GO: prefix).
    """
    obsolete_goids = set()
    current_id = None

    logger.info(f"Parsing GO OBO file {obo_file}")

    with open(obo_file) as f:
        for line in f:
            line = line.strip()

            if line == "[Term]":
                current_id = None

            # Match id: GO:0000123 -> extract 123
            match = re.match(r"^id: GO:0*([1-9]\d*)$", line)
            if match:
                current_id = int(match.group(1))

            if line == "is_obsolete: true" and current_id:
                obsolete_goids.add(current_id)

    logger.info(f"Found {len(obsolete_goids)} obsolete GO terms")
    return obsolete_goids


def read_iprscan_results(
    iprscan_file: Path,
) -> tuple[dict[str, set[str]], dict[str, set[int]], set[str]]:
    """
    Read InterProScan results file.

    Returns:
        - ipr_ids_for_feature: feature_name -> set of InterPro IDs
        - goids_for_ipr_id: InterPro ID -> set of GO IDs
        - ipr_id_seen: set of all InterPro IDs seen
    """
    ipr_ids_for_feature: dict[str, set[str]] = {}
    goids_for_ipr_id: dict[str, set[int]] = {}
    ipr_id_seen: set[str] = set()

    ipro_for_feat_count = 0
    go_for_ipro_count = 0

    logger.info(f"Parsing InterProScan file {iprscan_file}")

    with open(iprscan_file) as f:
        for line in f:
            if not line.strip():
                continue

            parts = line.strip().split("\t")
            if len(parts) < 14:
                continue

            feature = parts[0]
            # checksum = parts[1]
            # seq_length = parts[2]
            # method = parts[3]
            # db_id = parts[4]
            # db_desc = parts[5]
            # match_start = parts[6]
            # match_end = parts[7]
            match_eval = parts[8]
            # status = parts[9]
            # analysis_date = parts[10]
            interpro_id = parts[11]
            # interpro_desc = parts[12]
            go_desc = parts[13] if len(parts) > 13 else ""

            # Skip entries without InterPro ID
            if interpro_id == "NULL":
                continue

            # Skip entries without e-value
            if match_eval == "NA":
                continue

            # Parse e-value
            if match_eval == "-":
                e_value = 0.0
            else:
                try:
                    e_value = float(match_eval)
                except ValueError:
                    e_value = 0.0

            # Skip if e-value above cutoff
            if e_value >= EVAL_CUTOFF:
                continue

            ipr_id_seen.add(interpro_id)

            # Add InterPro ID for feature
            if feature not in ipr_ids_for_feature:
                ipr_ids_for_feature[feature] = set()
            if interpro_id not in ipr_ids_for_feature[feature]:
                ipro_for_feat_count += 1
                ipr_ids_for_feature[feature].add(interpro_id)

            # Extract GO IDs from description
            goid_matches = re.findall(r"\(GO:0*([1-9]\d*)\)", go_desc)
            for goid_str in goid_matches:
                goid = int(goid_str)
                if interpro_id not in goids_for_ipr_id:
                    goids_for_ipr_id[interpro_id] = set()
                if goid not in goids_for_ipr_id[interpro_id]:
                    go_for_ipro_count += 1
                    goids_for_ipr_id[interpro_id].add(goid)

    logger.info(f"Found {ipro_for_feat_count} unique domain assignments")
    logger.info(f"Found {go_for_ipro_count} unique GO associations for domains")

    return ipr_ids_for_feature, goids_for_ipr_id, ipr_id_seen


def read_interpro2go(
    interpro2go_file: Path,
    ipr_id_seen: set[str],
    goids_for_ipr_id: dict[str, set[int]],
) -> int:
    """
    Read Interpro2GO mapping file and add additional GO associations.

    Returns count of new associations added.
    """
    new_count = 0

    logger.info(f"Parsing Interpro2GO file {interpro2go_file}")

    with open(interpro2go_file) as f:
        for line in f:
            if not line.strip():
                continue

            # Format: InterPro:IPR000001 description > GO:term ; GO:0000001
            match = re.match(r"^InterPro:(IPR\d+).*GO:0*([1-9]\d*)$", line.strip())
            if not match:
                continue

            interpro_id = match.group(1)
            goid = int(match.group(2))

            # Only process InterPro IDs we've seen in the scan results
            if interpro_id not in ipr_id_seen:
                continue

            if interpro_id not in goids_for_ipr_id:
                goids_for_ipr_id[interpro_id] = set()

            if goid not in goids_for_ipr_id[interpro_id]:
                new_count += 1
                goids_for_ipr_id[interpro_id].add(goid)

    logger.info(f"Added {new_count} additional GO associations from Interpro2GO")
    return new_count


def get_reference_no(session, citation_pattern: str) -> int | None:
    """Get reference_no for the InterPro-based GO annotation reference."""
    query = text(f"""
        SELECT reference_no
        FROM {DB_SCHEMA}.reference
        WHERE citation LIKE :pattern
    """)
    result = session.execute(query, {"pattern": citation_pattern}).fetchone()
    return result[0] if result else None


def get_feature_info(session, feature_name: str) -> tuple[int | None, str | None]:
    """Get feature_no and feature_type for a feature."""
    query = text(f"""
        SELECT feature_no, feature_type
        FROM {DB_SCHEMA}.feature
        WHERE feature_name = :name
    """)
    result = session.execute(query, {"name": feature_name}).fetchone()
    if result:
        return result[0], result[1]
    return None, None


def get_go_no(session, goid: int) -> int | None:
    """Get go_no for a GO ID."""
    query = text(f"""
        SELECT go_no
        FROM {DB_SCHEMA}.go
        WHERE goid = :goid
    """)
    result = session.execute(query, {"goid": goid}).fetchone()
    return result[0] if result else None


def get_feature_go_annotations(
    session, feature_no: int, reference_no: int
) -> list[FeatureGOAnnotation]:
    """Get existing GO annotations for a feature (excluding InterPro reference)."""
    query = text(f"""
        SELECT ga.go_annotation_no, g.goid, ga.go_evidence
        FROM {DB_SCHEMA}.go_annotation ga
        JOIN {DB_SCHEMA}.go g ON ga.go_no = g.go_no
        JOIN {DB_SCHEMA}.go_ref gr ON (
            ga.go_annotation_no = gr.go_annotation_no
            AND gr.reference_no != :ref_no
        )
        WHERE ga.feature_no = :feat_no
    """)

    annotations = []
    for row in session.execute(
        query, {"feat_no": feature_no, "ref_no": reference_no}
    ).fetchall():
        annotations.append(FeatureGOAnnotation(
            go_annotation_no=row[0],
            goid=row[1],
            go_evidence=row[2],
        ))

    return annotations


def get_go_ancestors(session, goid: int) -> set[int]:
    """Get all ancestor GO IDs for a GO term."""
    query = text(f"""
        SELECT DISTINCT g1.goid
        FROM {DB_SCHEMA}.go_path gp
        JOIN {DB_SCHEMA}.go g1 ON gp.ancestor_go_no = g1.go_no
        JOIN {DB_SCHEMA}.go g2 ON (gp.child_go_no = g2.go_no AND g2.goid = :goid)
    """)

    ancestors = set()
    for row in session.execute(query, {"goid": goid}).fetchall():
        ancestors.add(row[0])

    return ancestors


def get_go_descendants(session, goid: int) -> set[int]:
    """Get all descendant GO IDs for a GO term."""
    query = text(f"""
        SELECT DISTINCT g1.goid
        FROM {DB_SCHEMA}.go_path gp
        JOIN {DB_SCHEMA}.go g1 ON gp.child_go_no = g1.go_no
        JOIN {DB_SCHEMA}.go g2 ON (gp.ancestor_go_no = g2.go_no AND g2.goid = :goid)
    """)

    descendants = set()
    for row in session.execute(query, {"goid": goid}).fetchall():
        descendants.add(row[0])

    return descendants


def delete_current_ipro_annotations(
    session, strain_abbrev: str, reference_no: int
) -> None:
    """Delete existing InterPro-based GO annotations for a strain."""
    logger.info("Deleting existing InterPro GO annotations")

    # Delete GO annotations that only have the InterPro reference
    delete_ga_query = text(f"""
        DELETE FROM {DB_SCHEMA}.go_annotation
        WHERE go_annotation_no IN (
            SELECT ga.go_annotation_no
            FROM {DB_SCHEMA}.go_annotation ga
            JOIN {DB_SCHEMA}.feature f ON ga.feature_no = f.feature_no
            JOIN {DB_SCHEMA}.organism o ON (
                f.organism_no = o.organism_no
                AND o.organism_abbrev = :strain
            )
            JOIN {DB_SCHEMA}.go_ref gr ON (
                ga.go_annotation_no = gr.go_annotation_no
                AND gr.reference_no = :ref_no
            )
            WHERE ga.go_annotation_no NOT IN (
                SELECT go_annotation_no
                FROM {DB_SCHEMA}.go_ref
                WHERE reference_no != :ref_no
            )
        )
    """)

    session.execute(
        delete_ga_query, {"strain": strain_abbrev, "ref_no": reference_no}
    )

    # Delete remaining GO refs with this reference
    delete_gr_query = text(f"""
        DELETE FROM {DB_SCHEMA}.go_ref
        WHERE reference_no = :ref_no
    """)
    session.execute(delete_gr_query, {"ref_no": reference_no})

    session.commit()
    logger.info("Deleted existing annotations")


def delete_go_annotation(session, go_annotation_no: int) -> None:
    """Delete a GO annotation and its related records."""
    query = text(f"""
        DELETE FROM {DB_SCHEMA}.go_annotation
        WHERE go_annotation_no = :ga_no
    """)
    session.execute(query, {"ga_no": go_annotation_no})


def check_existing_go_annotation(
    session, go_no: int, feature_no: int
) -> int | None:
    """Check if a GO annotation already exists."""
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
        query, {"go_no": go_no, "feat_no": feature_no, "source": PROJECT_ACRONYM}
    ).fetchone()
    return result[0] if result else None


def insert_go_annotation(
    session, go_no: int, feature_no: int
) -> int:
    """Insert a new GO annotation and return the go_annotation_no."""
    query = text(f"""
        INSERT INTO {DB_SCHEMA}.go_annotation
        (go_no, feature_no, go_evidence, annotation_type, source, created_by)
        VALUES (:go_no, :feat_no, 'IEA', 'computational', :source, :user)
    """)
    session.execute(
        query,
        {
            "go_no": go_no,
            "feat_no": feature_no,
            "source": PROJECT_ACRONYM,
            "user": ADMIN_USER.upper(),
        },
    )

    # Get the inserted ID
    id_query = text(f"""
        SELECT go_annotation_no
        FROM {DB_SCHEMA}.go_annotation
        WHERE go_no = :go_no
        AND feature_no = :feat_no
        AND go_evidence = 'IEA'
        AND annotation_type = 'computational'
        AND source = :source
        ORDER BY go_annotation_no DESC
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(
        id_query, {"go_no": go_no, "feat_no": feature_no, "source": PROJECT_ACRONYM}
    ).fetchone()
    return result[0]


def check_existing_go_ref(
    session, reference_no: int, go_annotation_no: int
) -> int | None:
    """Check if a GO ref already exists."""
    query = text(f"""
        SELECT go_ref_no
        FROM {DB_SCHEMA}.go_ref
        WHERE reference_no = :ref_no
        AND go_annotation_no = :ga_no
        AND has_qualifier = 'N'
        AND has_supporting_evidence = 'Y'
    """)
    result = session.execute(
        query, {"ref_no": reference_no, "ga_no": go_annotation_no}
    ).fetchone()
    return result[0] if result else None


def insert_go_ref(
    session, reference_no: int, go_annotation_no: int
) -> int:
    """Insert a new GO ref and return the go_ref_no."""
    query = text(f"""
        INSERT INTO {DB_SCHEMA}.go_ref
        (reference_no, go_annotation_no, has_qualifier, has_supporting_evidence, created_by)
        VALUES (:ref_no, :ga_no, 'N', 'Y', :user)
    """)
    session.execute(
        query,
        {
            "ref_no": reference_no,
            "ga_no": go_annotation_no,
            "user": ADMIN_USER.upper(),
        },
    )

    # Get the inserted ID
    id_query = text(f"""
        SELECT go_ref_no
        FROM {DB_SCHEMA}.go_ref
        WHERE reference_no = :ref_no
        AND go_annotation_no = :ga_no
        ORDER BY go_ref_no DESC
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(
        id_query, {"ref_no": reference_no, "ga_no": go_annotation_no}
    ).fetchone()
    return result[0]


def check_existing_dbxref(session, dbxref_id: str) -> int | None:
    """Check if a dbxref exists for an InterPro ID."""
    query = text(f"""
        SELECT dbxref_no
        FROM {DB_SCHEMA}.dbxref
        WHERE dbxref_id = :id
    """)
    result = session.execute(query, {"id": dbxref_id}).fetchone()
    return result[0] if result else None


def insert_dbxref(session, interpro_id: str) -> int:
    """Insert a new dbxref for an InterPro ID."""
    query = text(f"""
        INSERT INTO {DB_SCHEMA}.dbxref
        (source, dbxref_type, dbxref_id, created_by)
        VALUES ('EBI', 'InterPro ID', :id, :user)
    """)
    session.execute(
        query, {"id": interpro_id, "user": ADMIN_USER.upper()}
    )

    # Get the inserted ID
    id_query = text(f"""
        SELECT dbxref_no
        FROM {DB_SCHEMA}.dbxref
        WHERE dbxref_id = :id
        ORDER BY dbxref_no DESC
        FETCH FIRST 1 ROW ONLY
    """)
    result = session.execute(id_query, {"id": interpro_id}).fetchone()
    return result[0]


def check_existing_goref_dbxref(
    session, go_ref_no: int, dbxref_no: int
) -> int | None:
    """Check if a goref_dbxref already exists."""
    query = text(f"""
        SELECT goref_dbxref_no
        FROM {DB_SCHEMA}.goref_dbxref
        WHERE go_ref_no = :gr_no
        AND dbxref_no = :dx_no
        AND support_type = 'With'
    """)
    result = session.execute(
        query, {"gr_no": go_ref_no, "dx_no": dbxref_no}
    ).fetchone()
    return result[0] if result else None


def insert_goref_dbxref(
    session, go_ref_no: int, dbxref_no: int
) -> None:
    """Insert a new goref_dbxref."""
    query = text(f"""
        INSERT INTO {DB_SCHEMA}.goref_dbxref
        (go_ref_no, dbxref_no, support_type)
        VALUES (:gr_no, :dx_no, 'With')
    """)
    session.execute(query, {"gr_no": go_ref_no, "dx_no": dbxref_no})


def is_valid_goid_for_taxon(session, goid: int, organism_no: int) -> bool:
    """
    Check if a GO ID is valid for the given organism based on taxon constraints.

    This is a simplified check - the Perl version uses GO::TaxonTriggers.
    For now, we assume all GO IDs are valid (no taxonomy constraints).
    """
    # TODO: Implement full taxon constraint checking if needed
    return True


def insert_full_go_annotation(
    session,
    go_no: int,
    interpro_id: str,
    feature_no: int,
    reference_no: int,
    stats: ProcessingStats,
) -> None:
    """Insert a complete GO annotation with ref and dbxref."""
    # Check/insert GO annotation
    go_ann_no = check_existing_go_annotation(session, go_no, feature_no)
    if not go_ann_no:
        go_ann_no = insert_go_annotation(session, go_no, feature_no)
        stats.go_annotations_inserted += 1

    # Check/insert GO ref
    go_ref_no = check_existing_go_ref(session, reference_no, go_ann_no)
    if not go_ref_no:
        go_ref_no = insert_go_ref(session, reference_no, go_ann_no)
        stats.go_refs_inserted += 1

    # Check/insert dbxref for InterPro ID
    dbxref_no = check_existing_dbxref(session, interpro_id)
    if not dbxref_no:
        dbxref_no = insert_dbxref(session, interpro_id)

    # Check/insert goref_dbxref
    goref_dbxref_no = check_existing_goref_dbxref(session, go_ref_no, dbxref_no)
    if not goref_dbxref_no:
        insert_goref_dbxref(session, go_ref_no, dbxref_no)
        stats.goref_dbxrefs_inserted += 1


def process_feature(
    session,
    feature_name: str,
    ipr_ids: set[str],
    goids_for_ipr_id: dict[str, set[int]],
    reference_no: int,
    obsolete_goids: set[int],
    do_update: bool,
    stats: ProcessingStats,
    summary_lines: list[str],
) -> None:
    """Process GO annotations for a single feature."""
    # Get feature info
    feature_no, feature_type = get_feature_info(session, feature_name)

    if not feature_no:
        logger.debug(f"Skipping {feature_name}: not found in database")
        return

    if feature_type and "pseudogene" in feature_type.lower():
        logger.debug(f"Skipping {feature_name}: pseudogene")
        return

    stats.features_processed += 1
    logger.debug(f"Processing {feature_name}")

    # Get existing GO annotations for this feature
    existing_annotations = get_feature_go_annotations(session, feature_no, reference_no)
    goid_for_ga: dict[int, int] = {}
    ev_for_ga: dict[int, str] = {}
    for annot in existing_annotations:
        goid_for_ga[annot.go_annotation_no] = annot.goid
        ev_for_ga[annot.go_annotation_no] = annot.go_evidence

    # Collect descendants of current GO annotations
    curr_descendants: dict[int, set[int]] = {}  # goid -> set of ga_no
    for ga_no, goid in goid_for_ga.items():
        for desc_goid in get_go_descendants(session, goid):
            if desc_goid not in curr_descendants:
                curr_descendants[desc_goid] = set()
            curr_descendants[desc_goid].add(ga_no)

    # First pass: identify ND annotations to delete
    ga_nos_to_delete: set[int] = set()
    use_goid_for_goid: dict[int, int] = {}
    all_goids: set[int] = set()

    for ipr_id in ipr_ids:
        if ipr_id not in goids_for_ipr_id:
            continue

        for goid in goids_for_ipr_id[ipr_id]:
            all_goids.add(goid)

            # Handle replaced GO IDs
            use_goid = GOID_REPLACED.get(goid, goid)
            use_goid_for_goid[goid] = use_goid

            # Skip invalid GO IDs
            if not is_valid_goid_for_taxon(session, use_goid, 0):
                continue
            if use_goid in obsolete_goids:
                continue
            if use_goid in GOID_DELETED:
                continue

            # Check if this GO ID is a descendant of an existing ND annotation
            if use_goid in curr_descendants:
                for ga_no in curr_descendants[use_goid]:
                    if ev_for_ga.get(ga_no) == "ND":
                        ga_nos_to_delete.add(ga_no)

    # Delete ND annotations that have domain-derived descendants
    if do_update:
        for ga_no in ga_nos_to_delete:
            delete_go_annotation(session, ga_no)
            stats.nd_deletions += 1

    stats.nd_ancestors_found += len(ga_nos_to_delete)

    # Collect ancestors of remaining GO annotations
    curr_ancestors: dict[int, set[int]] = {}  # goid -> set of ga_no
    for ga_no, goid in goid_for_ga.items():
        if ga_no in ga_nos_to_delete:
            continue
        for anc_goid in get_go_ancestors(session, goid):
            if anc_goid not in curr_ancestors:
                curr_ancestors[anc_goid] = set()
            curr_ancestors[anc_goid].add(ga_no)

    # Second pass: process each InterPro domain
    goids_added: set[int] = set()

    for ipr_id in ipr_ids:
        if ipr_id not in goids_for_ipr_id:
            summary_lines.append(f"{feature_name}\t{ipr_id}\tNA\t0\tNo GOIDs")
            continue

        domain_goids = goids_for_ipr_id[ipr_id]

        # Collect ancestors within this domain's GO terms
        ipr_ancestors: dict[int, set[int]] = {}  # goid -> set of source goids
        insert_goid: dict[int, bool] = {}
        note_for_goid: dict[int, str] = {}

        for goid in domain_goids:
            insert_goid[goid] = True
            note_for_goid[goid] = ""

            use_goid = use_goid_for_goid.get(goid, goid)

            # Note if GO ID was replaced
            if use_goid != goid:
                note_for_goid[goid] = f"GOID {goid} replaced with {use_goid}"

            # Check validity
            if not is_valid_goid_for_taxon(session, use_goid, 0):
                insert_goid[goid] = False
                if note_for_goid[goid]:
                    note_for_goid[goid] += "; "
                note_for_goid[goid] += "Taxonomy constraint violation"
            elif use_goid in obsolete_goids:
                insert_goid[goid] = False
                if note_for_goid[goid]:
                    note_for_goid[goid] += "; "
                note_for_goid[goid] += "GOID obsolete"
            elif use_goid in GOID_DELETED:
                insert_goid[goid] = False
                if note_for_goid[goid]:
                    note_for_goid[goid] += "; "
                note_for_goid[goid] += "GOID deleted"
            else:
                # Collect ancestors for valid GO terms
                for anc_goid in get_go_ancestors(session, use_goid):
                    if anc_goid not in ipr_ancestors:
                        ipr_ancestors[anc_goid] = set()
                    ipr_ancestors[anc_goid].add(goid)

        # Now decide which GO terms to insert
        for goid in domain_goids:
            use_goid = use_goid_for_goid.get(goid, goid)
            go_no = get_go_no(session, use_goid)

            if insert_goid[goid] and not go_no:
                insert_goid[goid] = False
                if note_for_goid[goid]:
                    note_for_goid[goid] += "; "
                note_for_goid[goid] += f"No GO_NO in DB for GOID {use_goid}"

            # Skip if ancestor of other domain terms
            elif insert_goid[goid] and use_goid in ipr_ancestors:
                insert_goid[goid] = False
                if note_for_goid[goid]:
                    note_for_goid[goid] += "; "
                note_for_goid[goid] += (
                    "GOID is ancestor of other GO terms for this domain: "
                    + " ".join(str(g) for g in ipr_ancestors[use_goid])
                )

            # Skip if ancestor of existing annotations
            elif insert_goid[goid] and use_goid in curr_ancestors:
                insert_goid[goid] = False
                if note_for_goid[goid]:
                    note_for_goid[goid] += "; "
                note_for_goid[goid] += (
                    "GOID is ancestor of existing annotations: "
                    + " ".join(str(ga) for ga in curr_ancestors[use_goid])
                )

            # Insert the annotation
            elif insert_goid[goid]:
                # Note if descendant of deleted ND annotation
                if use_goid in curr_descendants:
                    if note_for_goid[goid]:
                        note_for_goid[goid] += "; "
                    note_for_goid[goid] += (
                        "GOID is descendant of existing GO terms: "
                        + " ".join(
                            f"{goid_for_ga[ga]}:{ev_for_ga[ga]}"
                            + (" (deleted)" if ga in ga_nos_to_delete else "")
                            for ga in curr_descendants[use_goid]
                        )
                    )

                goids_added.add(use_goid)
                stats.unique_feature_go_ipro += 1

                if do_update and go_no:
                    insert_full_go_annotation(
                        session, go_no, ipr_id, feature_no, reference_no, stats
                    )

            # Write summary line
            summary_lines.append(
                f"{feature_name}\t{ipr_id}\t{goid}\t{1 if insert_goid[goid] else 0}\t{note_for_goid[goid]}"
            )

    stats.unique_feature_go += len(goids_added)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update GO annotations based on InterPro domain predictions"
    )
    parser.add_argument(
        "--strain",
        required=True,
        help="Strain abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Actually update the database (default: dry run)",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download fresh Interpro2GO mapping file",
    )

    args = parser.parse_args()
    strain_abbrev = args.strain

    # Set up paths
    domain_dir = DATA_DIR / "domain"
    iprscan_file = domain_dir / strain_abbrev / f"{strain_abbrev}_proteins.tsv"
    go_obo_file = DATA_DIR / "GO" / "gene_ontology.obo"
    interpro2go_file = domain_dir / "interpro2go.txt"

    log_file = LOG_DIR / f"{strain_abbrev}_domain_GO_tx.log"
    summary_file = domain_dir / strain_abbrev / f"{strain_abbrev}_ipro2go_summary.txt"

    # Validate required files
    if not iprscan_file.exists():
        logger.error(f"InterProScan file not found: {iprscan_file}")
        return 1

    if not go_obo_file.exists():
        logger.error(f"GO OBO file not found: {go_obo_file}")
        return 1

    # Download Interpro2GO if requested
    if args.download:
        domain_dir.mkdir(parents=True, exist_ok=True)
        if not download_interpro2go(interpro2go_file):
            return 1

    if not interpro2go_file.exists():
        logger.error(f"Interpro2GO file not found: {interpro2go_file}")
        return 1

    logger.info(f"Processing InterPro GO annotations for {strain_abbrev}")

    try:
        # Read data files
        obsolete_goids = read_go_obo(go_obo_file)
        ipr_ids_for_feature, goids_for_ipr_id, ipr_id_seen = read_iprscan_results(
            iprscan_file
        )
        read_interpro2go(interpro2go_file, ipr_id_seen, goids_for_ipr_id)

        with SessionLocal() as session:
            # Get reference number for InterPro-based annotations
            reference_no = get_reference_no(session, IPR_CITATION_PATTERN)
            if not reference_no:
                logger.error("Could not find reference for InterPro GO annotations")
                return 1

            logger.info(f"Using reference_no: {reference_no}")

            # Delete current InterPro annotations if updating
            if args.update:
                delete_current_ipro_annotations(session, strain_abbrev, reference_no)

            # Process each feature
            stats = ProcessingStats()
            summary_lines: list[str] = []
            summary_lines.append("Feature\tInterPro ID\tGO ID\tAdded\tNote")

            transaction_count = 0

            for feature_name in ipr_ids_for_feature:
                process_feature(
                    session,
                    feature_name,
                    ipr_ids_for_feature[feature_name],
                    goids_for_ipr_id,
                    reference_no,
                    obsolete_goids,
                    args.update,
                    stats,
                    summary_lines,
                )

                transaction_count += 1
                if args.update and transaction_count >= COMMIT_LIMIT:
                    session.commit()
                    transaction_count = 0

            # Final commit
            if args.update:
                session.commit()

        # Write summary file
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        with open(summary_file, "w") as f:
            f.write("\n".join(summary_lines) + "\n")
        logger.info(f"Summary written to {summary_file}")

        # Print statistics
        logger.info(f"Processed {stats.features_processed} features")
        logger.info(f"Unique feature::GO associations: {stats.unique_feature_go}")
        logger.info(f"Unique feature::GO::InterPro associations: {stats.unique_feature_go_ipro}")
        logger.info(f"ND ancestors identified: {stats.nd_ancestors_found}")

        if args.update:
            logger.info(f"GO annotations inserted: {stats.go_annotations_inserted}")
            logger.info(f"GO refs inserted: {stats.go_refs_inserted}")
            logger.info(f"GOREF_DBXREF rows inserted: {stats.goref_dbxrefs_inserted}")
            logger.info(f"ND annotations deleted: {stats.nd_deletions}")
        else:
            logger.info("Dry run - no database changes made")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
