#!/usr/bin/env python3
"""
Load ortholog groups from CGOB clusters into the database.

This script loads ortholog group data from a CGD-format clusters file
into the HOMOLOGY_GROUP and FEAT_HOMOLOGY tables.

Input file format: Tab-delimited file with columns for each strain.
The first row is a header with strain names.
Each subsequent row represents an ortholog group with ORF names.
Empty cells or "---" indicate no ortholog in that strain.

Original Perl: loadCGOB2DB.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import (
    Dbxref,
    DbxrefHomology,
    FeatHomology,
    Feature,
    HomologyGroup,
    Organism,
)

load_dotenv()

logger = logging.getLogger(__name__)

# Homology group settings
DEFAULT_GROUP_TYPE = "ortholog"
DEFAULT_METHOD = "CGOB"


def setup_logging(log_file: Path = None, verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def get_strain_abbrevs(session: Session) -> dict[str, str]:
    """
    Get mapping of organism name variants to organism_abbrev.

    Returns:
        Dictionary mapping various name forms to organism_abbrev
    """
    organisms = session.query(Organism).all()
    abbrev_map = {}

    for org in organisms:
        abbrev = org.organism_abbrev
        if abbrev:
            # Map abbreviation to itself
            abbrev_map[abbrev] = abbrev

            # Map organism name
            if org.organism_name:
                abbrev_map[org.organism_name] = abbrev

            # Map common variations
            if org.common_name:
                abbrev_map[org.common_name] = abbrev

    return abbrev_map


def find_feature_by_name(
    session: Session,
    name: str,
    organism_abbrev: str = None,
) -> Feature | None:
    """
    Find feature by feature_name.

    Args:
        session: Database session
        name: Feature name to search
        organism_abbrev: Optional organism abbreviation to filter by

    Returns:
        Feature object or None
    """
    query = session.query(Feature).filter(Feature.feature_name == name)

    if organism_abbrev:
        # Join with Organism table to filter
        org = session.query(Organism).filter(
            Organism.organism_abbrev == organism_abbrev
        ).first()
        if org:
            query = query.filter(Feature.organism_no == org.organism_no)

    return query.first()


def parse_clusters_file(filepath: Path) -> tuple[list[str], list[list[str]]]:
    """
    Parse CGD clusters file.

    Args:
        filepath: Path to clusters file

    Returns:
        Tuple of (strain_headers, clusters) where clusters is list of ORF lists
    """
    strains = []
    clusters = []

    with open(filepath) as f:
        # First line is header with strain names
        header = f.readline().strip()
        strains = header.split("\t")

        # Remaining lines are ortholog groups
        for line_num, line in enumerate(f, 2):
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            orfs = []

            for i, part in enumerate(parts):
                part = part.strip()
                # Skip empty or placeholder values
                if part and part != "---":
                    orfs.append((i, part))  # (column_index, orf_name)

            # Need at least 2 members for a valid ortholog group
            if len(orfs) >= 2:
                clusters.append(orfs)

    logger.info(f"Parsed {len(strains)} strains and {len(clusters)} clusters")
    return strains, clusters


def delete_previous_ortholog_groups(
    session: Session,
    group_type: str,
    method: str,
) -> int:
    """
    Delete previous ortholog groups of the same type/method.

    Args:
        session: Database session
        group_type: Homology group type
        method: Method used to create groups

    Returns:
        Number of groups deleted
    """
    # Get homology_group_nos to delete
    groups = session.query(HomologyGroup).filter(
        and_(
            HomologyGroup.homology_group_type == group_type,
            HomologyGroup.method == method,
        )
    ).all()

    count = 0
    for group in groups:
        # Delete feat_homology entries
        session.query(FeatHomology).filter(
            FeatHomology.homology_group_no == group.homology_group_no
        ).delete()

        # Delete dbxref_homology entries
        session.query(DbxrefHomology).filter(
            DbxrefHomology.homology_group_no == group.homology_group_no
        ).delete()

        # Delete the group
        session.delete(group)
        count += 1

    if count > 0:
        session.flush()
        logger.info(f"Deleted {count} previous ortholog groups")

    return count


def create_homology_group(
    session: Session,
    group_type: str,
    method: str,
    created_by: str,
) -> int:
    """
    Create a new homology group.

    Args:
        session: Database session
        group_type: Type of homology group
        method: Method used to create the group
        created_by: User creating the record

    Returns:
        homology_group_no of created group
    """
    new_group = HomologyGroup(
        homology_group_type=group_type,
        method=method,
        created_by=created_by[:12],
    )
    session.add(new_group)
    session.flush()

    return new_group.homology_group_no


def create_feat_homology(
    session: Session,
    feature_no: int,
    homology_group_no: int,
    created_by: str,
) -> bool:
    """
    Create feat_homology entry.

    Args:
        session: Database session
        feature_no: Feature number
        homology_group_no: Homology group number
        created_by: User creating the record

    Returns:
        True if created, False if already existed
    """
    existing = session.query(FeatHomology).filter(
        and_(
            FeatHomology.feature_no == feature_no,
            FeatHomology.homology_group_no == homology_group_no,
        )
    ).first()

    if existing:
        return False

    new_entry = FeatHomology(
        feature_no=feature_no,
        homology_group_no=homology_group_no,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    return True


def create_dbxref_homology(
    session: Session,
    dbxref_no: int,
    homology_group_no: int,
    created_by: str,
) -> bool:
    """
    Create dbxref_homology entry for external orthologs.

    Args:
        session: Database session
        dbxref_no: Dbxref number
        homology_group_no: Homology group number
        created_by: User creating the record

    Returns:
        True if created, False if already existed
    """
    existing = session.query(DbxrefHomology).filter(
        and_(
            DbxrefHomology.dbxref_no == dbxref_no,
            DbxrefHomology.homology_group_no == homology_group_no,
        )
    ).first()

    if existing:
        return False

    new_entry = DbxrefHomology(
        dbxref_no=dbxref_no,
        homology_group_no=homology_group_no,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    return True


def get_or_create_external_dbxref(
    session: Session,
    external_id: str,
    source: str,
    created_by: str,
) -> int:
    """
    Get existing external dbxref or create new one.

    Args:
        session: Database session
        external_id: External identifier
        source: Source database name
        created_by: User creating the record

    Returns:
        dbxref_no
    """
    existing = session.query(Dbxref).filter(
        and_(
            Dbxref.dbxref_id == external_id,
            Dbxref.source == source,
            Dbxref.dbxref_type == "Gene ID",
        )
    ).first()

    if existing:
        return existing.dbxref_no

    new_dbxref = Dbxref(
        dbxref_id=external_id,
        source=source,
        dbxref_type="Gene ID",
        created_by=created_by[:12],
    )
    session.add(new_dbxref)
    session.flush()

    return new_dbxref.dbxref_no


def load_ortholog_groups(
    session: Session,
    strains: list[str],
    clusters: list[list[tuple[int, str]]],
    strain_abbrev_map: dict[str, str],
    group_type: str,
    method: str,
    created_by: str,
    external_source: str = None,
) -> dict:
    """
    Load ortholog groups into the database.

    Args:
        session: Database session
        strains: List of strain names from header
        clusters: List of ortholog clusters (each is list of (col_idx, orf_name))
        strain_abbrev_map: Mapping of strain names to abbreviations
        group_type: Type of homology group
        method: Method used to create groups
        created_by: User creating the records
        external_source: Source name for external orthologs (non-DB strains)

    Returns:
        Dictionary with statistics
    """
    stats = {
        "clusters_processed": 0,
        "groups_created": 0,
        "feat_homology_created": 0,
        "dbxref_homology_created": 0,
        "features_not_found": 0,
        "external_orthologs": 0,
    }

    for cluster_idx, cluster in enumerate(clusters):
        if (cluster_idx + 1) % 100 == 0:
            logger.info(f"Processing cluster {cluster_idx + 1}...")

        # Get feature_nos for valid ORFs in this cluster
        valid_features = []
        external_orfs = []

        for col_idx, orf_name in cluster:
            # Get strain abbreviation for this column
            if col_idx < len(strains):
                strain_name = strains[col_idx]
                strain_abbrev = strain_abbrev_map.get(strain_name)
            else:
                strain_abbrev = None

            # Try to find feature in database
            feature = find_feature_by_name(session, orf_name, strain_abbrev)

            if feature:
                valid_features.append(feature.feature_no)
            elif external_source:
                # Track as external ortholog
                external_orfs.append((orf_name, strain_name if col_idx < len(strains) else "unknown"))
            else:
                logger.debug(f"Feature not found: {orf_name}")
                stats["features_not_found"] += 1

        # Need at least 2 features to create a group
        if len(valid_features) + len(external_orfs) < 2:
            continue

        stats["clusters_processed"] += 1

        # Create homology group
        hg_no = create_homology_group(session, group_type, method, created_by)
        stats["groups_created"] += 1

        # Add features to group
        for feature_no in valid_features:
            if create_feat_homology(session, feature_no, hg_no, created_by):
                stats["feat_homology_created"] += 1

        # Add external orthologs via dbxref
        if external_source and external_orfs:
            for ext_orf, ext_strain in external_orfs:
                source = f"{external_source} ({ext_strain})"
                dbxref_no = get_or_create_external_dbxref(
                    session, ext_orf, source, created_by
                )
                if create_dbxref_homology(session, dbxref_no, hg_no, created_by):
                    stats["dbxref_homology_created"] += 1
                    stats["external_orthologs"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load ortholog groups from CGOB clusters into the database"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input clusters file (tab-delimited)",
    )
    parser.add_argument(
        "--group-type",
        default=DEFAULT_GROUP_TYPE,
        help=f"Homology group type (default: {DEFAULT_GROUP_TYPE})",
    )
    parser.add_argument(
        "--method",
        default=DEFAULT_METHOD,
        help=f"Method used to create groups (default: {DEFAULT_METHOD})",
    )
    parser.add_argument(
        "--external-source",
        help="Source name for external (non-DB) orthologs",
    )
    parser.add_argument(
        "--created-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user name for created_by field",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Path to log file",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse file but don't modify database",
    )
    parser.add_argument(
        "--skip-delete",
        action="store_true",
        help="Skip deleting previous ortholog groups",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Group type: {args.group_type}")
    logger.info(f"Method: {args.method}")

    # Parse input file
    strains, clusters = parse_clusters_file(args.input_file)

    if not clusters:
        logger.warning("No valid clusters found in input file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(clusters)} ortholog clusters")
        logger.info(f"Strains in file: {strains}")
        return

    try:
        with SessionLocal() as session:
            # Get strain abbreviation mapping
            strain_abbrev_map = get_strain_abbrevs(session)

            # Map header strains to abbreviations
            for i, strain in enumerate(strains):
                if strain not in strain_abbrev_map:
                    logger.warning(
                        f"Strain '{strain}' not found in database, "
                        "orthologs will be treated as external"
                    )

            # Delete previous groups if requested
            if not args.skip_delete:
                delete_previous_ortholog_groups(
                    session, args.group_type, args.method
                )

            # Load ortholog groups
            stats = load_ortholog_groups(
                session,
                strains,
                clusters,
                strain_abbrev_map,
                args.group_type,
                args.method,
                args.created_by,
                args.external_source,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Clusters processed: {stats['clusters_processed']}")
            logger.info(f"  Homology groups created: {stats['groups_created']}")
            logger.info(
                f"  Feat_homology entries created: {stats['feat_homology_created']}"
            )
            if stats["dbxref_homology_created"] > 0:
                logger.info(
                    f"  Dbxref_homology entries created: "
                    f"{stats['dbxref_homology_created']}"
                )
            if stats["external_orthologs"] > 0:
                logger.info(
                    f"  External orthologs: {stats['external_orthologs']}"
                )
            if stats["features_not_found"] > 0:
                logger.warning(
                    f"  Features not found: {stats['features_not_found']}"
                )
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading ortholog groups: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
