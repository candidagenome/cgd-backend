#!/usr/bin/env python3
"""
Load OrthoMCL ortholog clusters into the database.

This script loads ortholog cluster data from OrthoMCL output format into
the HOMOLOGY_GROUP, FEAT_HOMOLOGY, and DBXREF_HOMOLOGY tables.

It handles:
- Parsing OrthoMCL cluster files
- Creating homology groups for each cluster
- Linking database features via FEAT_HOMOLOGY
- Linking external sequences via DBXREF_HOMOLOGY
- Optionally adding cross-strain aliases

Input format: OrthoMCL output with format:
  cluster_id: species1|gene1 species2|gene2 ...

Original Perl: loadCryptoOrthologs.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import (
    Alias,
    Dbxref,
    DbxrefHomology,
    FeatAlias,
    FeatHomology,
    Feature,
    FeatProperty,
    HomologyGroup,
    Organism,
    Url,
    UrlHomology,
    WebDisplay,
)

load_dotenv()

logger = logging.getLogger(__name__)

# Default settings
DEFAULT_GROUP_TYPE = "ortholog"
DEFAULT_METHOD = "OrthoMCL"
DEFAULT_DBXREF_SOURCE = "Orthologs in Cryptococcus species"
DEFAULT_ALIAS_TYPE = "Other strain feature name"


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def get_db_organisms(session: Session) -> tuple[dict, dict, dict]:
    """
    Get organism information from database.

    Returns:
        Tuple of:
        - species dict: name -> True
        - strain_abbrevs dict: name -> abbrev
        - species_for_strain dict: abbrev -> species_name
    """
    species = {}
    strain_abbrevs = {}
    species_for_strain = {}

    # Get all species
    species_orgs = session.query(Organism).filter(
        Organism.taxonomic_rank == 'Species'
    ).all()

    for sp in species_orgs:
        species[sp.organism_name] = True

        # Get strains for this species
        strains = session.query(Organism).filter(
            and_(
                Organism.parent_organism_no == sp.organism_no,
                Organism.taxonomic_rank == 'Strain',
            )
        ).all()

        for strain in strains:
            strain_abbrevs[strain.organism_name] = strain.organism_abbrev
            species_for_strain[strain.organism_abbrev] = sp.organism_name

    return species, strain_abbrevs, species_for_strain


def get_db_orfs(session: Session) -> dict:
    """
    Get all non-deleted ORF features from database.

    Returns:
        Dict mapping feature_name -> {feature_no, gene_name, dbxref_id}
    """
    # Get features that don't have 'deleted' property
    deleted_features = session.query(FeatProperty.feature_no).filter(
        FeatProperty.property_value.ilike('%deleted%')
    ).subquery()

    features = session.query(Feature).filter(
        and_(
            Feature.feature_type == 'ORF',
            ~Feature.feature_no.in_(deleted_features),
        )
    ).all()

    result = {}
    for feat in features:
        result[feat.feature_name] = {
            'feature_no': feat.feature_no,
            'gene_name': feat.gene_name or '',
            'dbxref_id': feat.dbxref_id,
        }

    logger.info(f"Collected {len(result)} ORFs from database")
    return result


def parse_orthomcl_cluster(line: str) -> tuple[str, dict]:
    """
    Parse an OrthoMCL cluster line.

    Format: cluster_id: species1|gene1 species2|gene2 ...

    Returns:
        Tuple of (cluster_id, {strain: [genes]})
    """
    if ':' not in line:
        return None, None

    parts = line.strip().split(':')
    if len(parts) < 2:
        return None, None

    cluster_id = parts[0].strip()
    members_str = parts[1].strip()

    cluster_members = defaultdict(list)

    for member in members_str.split():
        if '|' in member:
            strain, gene = member.split('|', 1)
            cluster_members[strain].append(gene)

    return cluster_id, dict(cluster_members)


def delete_existing_homology_groups(
    session: Session,
    group_type: str,
    method: str,
) -> int:
    """Delete existing homology groups of specified type/method."""
    # Get groups to delete
    groups = session.query(HomologyGroup).filter(
        and_(
            HomologyGroup.homology_group_type == group_type,
            HomologyGroup.method == method,
        )
    ).all()

    count = 0
    for group in groups:
        # Delete feat_homology
        session.query(FeatHomology).filter(
            FeatHomology.homology_group_no == group.homology_group_no
        ).delete()

        # Delete dbxref_homology
        session.query(DbxrefHomology).filter(
            DbxrefHomology.homology_group_no == group.homology_group_no
        ).delete()

        # Delete url_homology
        session.query(UrlHomology).filter(
            UrlHomology.homology_group_no == group.homology_group_no
        ).delete()

        # Delete group
        session.delete(group)
        count += 1

    if count > 0:
        session.flush()
        logger.info(f"Deleted {count} existing homology groups")

    return count


def delete_other_strain_aliases(session: Session, alias_type: str) -> int:
    """Delete aliases of specified type."""
    count = session.query(Alias).filter(
        Alias.alias_type == alias_type
    ).delete()

    if count > 0:
        session.flush()
        logger.info(f"Deleted {count} '{alias_type}' aliases")

    return count


def create_homology_group(
    session: Session,
    group_type: str,
    method: str,
    group_id: str,
    created_by: str,
) -> int:
    """Create a new homology group."""
    new_group = HomologyGroup(
        homology_group_type=group_type,
        method=method,
        homology_group_id=group_id,
        created_by=created_by[:12],
    )
    session.add(new_group)
    session.flush()
    return new_group.homology_group_no


def create_feat_homology(
    session: Session,
    homology_group_no: int,
    feature_no: int,
    created_by: str,
) -> bool:
    """Create feat_homology entry if not exists."""
    existing = session.query(FeatHomology).filter(
        and_(
            FeatHomology.homology_group_no == homology_group_no,
            FeatHomology.feature_no == feature_no,
        )
    ).first()

    if existing:
        return False

    new_entry = FeatHomology(
        homology_group_no=homology_group_no,
        feature_no=feature_no,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    return True


def get_or_create_dbxref(
    session: Session,
    gene_id: str,
    strain: str,
    source: str,
    created_by: str,
) -> int:
    """Get existing dbxref or create new one."""
    existing = session.query(Dbxref).filter(
        and_(
            Dbxref.source == source,
            Dbxref.dbxref_type == 'Gene ID',
            Dbxref.dbxref_id == gene_id,
        )
    ).first()

    if existing:
        return existing.dbxref_no

    new_dbxref = Dbxref(
        source=source,
        dbxref_type='Gene ID',
        dbxref_id=gene_id,
        description=strain,
        created_by=created_by[:12],
    )
    session.add(new_dbxref)
    session.flush()
    return new_dbxref.dbxref_no


def create_dbxref_homology(
    session: Session,
    homology_group_no: int,
    dbxref_no: int,
    strain: str,
    created_by: str,
) -> bool:
    """Create dbxref_homology entry if not exists."""
    existing = session.query(DbxrefHomology).filter(
        and_(
            DbxrefHomology.homology_group_no == homology_group_no,
            DbxrefHomology.dbxref_no == dbxref_no,
        )
    ).first()

    if existing:
        return False

    new_entry = DbxrefHomology(
        homology_group_no=homology_group_no,
        dbxref_no=dbxref_no,
        name=strain,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    return True


def get_or_create_alias(
    session: Session,
    alias_name: str,
    alias_type: str,
    created_by: str,
) -> int:
    """Get existing alias or create new one."""
    existing = session.query(Alias).filter(
        and_(
            Alias.alias_name == alias_name,
            Alias.alias_type == alias_type,
        )
    ).first()

    if existing:
        return existing.alias_no

    new_alias = Alias(
        alias_name=alias_name,
        alias_type=alias_type,
        created_by=created_by[:12],
    )
    session.add(new_alias)
    session.flush()
    return new_alias.alias_no


def create_feat_alias(
    session: Session,
    feature_no: int,
    alias_no: int,
) -> bool:
    """Create feat_alias entry if not exists."""
    existing = session.query(FeatAlias).filter(
        and_(
            FeatAlias.feature_no == feature_no,
            FeatAlias.alias_no == alias_no,
        )
    ).first()

    if existing:
        return False

    new_entry = FeatAlias(
        feature_no=feature_no,
        alias_no=alias_no,
    )
    session.add(new_entry)
    return True


def get_or_create_url(
    session: Session,
    url: str,
    source: str,
    created_by: str,
) -> int:
    """Get existing URL or create new one."""
    existing = session.query(Url).filter(Url.url == url).first()

    if existing:
        return existing.url_no

    new_url = Url(
        url=url,
        source=source,
        url_type='Other',
        substitution_value='HOMOLOGY_GROUP',
        created_by=created_by[:12],
    )
    session.add(new_url)
    session.flush()

    logger.info(f"Created URL: {url}")
    return new_url.url_no


def create_url_homology(
    session: Session,
    homology_group_no: int,
    url_no: int,
    created_by: str,
) -> bool:
    """Create url_homology entry if not exists."""
    existing = session.query(UrlHomology).filter(
        and_(
            UrlHomology.homology_group_no == homology_group_no,
            UrlHomology.url_no == url_no,
        )
    ).first()

    if existing:
        return False

    new_entry = UrlHomology(
        homology_group_no=homology_group_no,
        url_no=url_no,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    return True


def load_orthomcl_clusters(
    session: Session,
    input_file: Path,
    strain_map: dict[str, str],
    db_orfs: dict,
    db_strains: dict,
    db_species: dict,
    species_for_strain: dict,
    group_type: str,
    method: str,
    dbxref_source: str,
    alias_type: str,
    external_url_no: int | None,
    created_by: str,
) -> dict:
    """
    Load OrthoMCL clusters into database.

    Args:
        session: Database session
        input_file: Path to OrthoMCL output file
        strain_map: Mapping of shorthand -> strain_abbrev
        db_orfs: Dict of DB ORFs
        db_strains: Dict of DB strain abbrevs
        db_species: Dict of DB species names
        species_for_strain: strain_abbrev -> species_name
        group_type: Homology group type
        method: Homology method
        dbxref_source: Source for dbxref entries
        alias_type: Type for cross-strain aliases
        external_url_no: URL number for external links
        created_by: User creating records

    Returns:
        Statistics dictionary
    """
    stats = {
        "clusters_processed": 0,
        "singleton_skipped": 0,
        "no_db_skipped": 0,
        "groups_created": 0,
        "feat_homology_created": 0,
        "dbxref_homology_created": 0,
        "aliases_created": 0,
    }

    # Collect cluster data
    gene_list_for_cluster = {}
    strain_for_gene = {}

    with open(input_file) as f:
        for line in f:
            cluster_id, cluster_members = parse_orthomcl_cluster(line)
            if not cluster_id:
                continue

            # Skip singletons
            total_genes = sum(len(genes) for genes in cluster_members.values())
            if total_genes < 2:
                stats["singleton_skipped"] += 1
                continue

            # Check if cluster has any DB genes
            has_db_gene = False
            gene_list = []

            for strain_short, genes in cluster_members.items():
                # Map shorthand to strain abbrev if possible
                strain = strain_map.get(strain_short, strain_short)

                for gene_id in genes:
                    if gene_id in db_orfs:
                        has_db_gene = True

                    strain_for_gene[gene_id] = strain
                    gene_list.append(gene_id)

            if not has_db_gene:
                stats["no_db_skipped"] += 1
                continue

            gene_list_for_cluster[cluster_id] = gene_list

    logger.info(f"Collected {len(gene_list_for_cluster)} clusters")
    logger.info(f"Skipped {stats['singleton_skipped']} singletons")
    logger.info(f"Skipped {stats['no_db_skipped']} clusters with no DB genes")

    # Load clusters to database
    for cluster_id, gene_list in gene_list_for_cluster.items():
        stats["clusters_processed"] += 1

        # Create homology group
        hg_no = create_homology_group(
            session, group_type, method, cluster_id, created_by
        )
        stats["groups_created"] += 1

        # Add URL homology if configured
        if external_url_no:
            create_url_homology(session, hg_no, external_url_no, created_by)

        # Track features and aliases by species
        feat_list_for_species = defaultdict(list)
        alias_list_for_species = defaultdict(list)

        for gene_id in gene_list:
            strain = strain_for_gene.get(gene_id)
            species = species_for_strain.get(strain) if strain else None

            # Check if gene is in database
            if gene_id in db_orfs:
                feat_no = db_orfs[gene_id]['feature_no']

                if create_feat_homology(session, hg_no, feat_no, created_by):
                    stats["feat_homology_created"] += 1

                if species:
                    feat_list_for_species[species].append(feat_no)

            elif strain and strain in db_strains:
                # Non-DB gene from a DB strain - create dbxref
                dbxref_no = get_or_create_dbxref(
                    session, gene_id, strain, dbxref_source, created_by
                )
                if create_dbxref_homology(session, hg_no, dbxref_no, strain, created_by):
                    stats["dbxref_homology_created"] += 1

                # Track for alias creation
                if species and species in db_species:
                    alias_list_for_species[species].append(gene_id)

            else:
                # External gene - create dbxref
                dbxref_no = get_or_create_dbxref(
                    session, gene_id, strain or 'unknown', dbxref_source, created_by
                )
                if create_dbxref_homology(session, hg_no, dbxref_no, strain or 'unknown', created_by):
                    stats["dbxref_homology_created"] += 1

        # Add cross-strain aliases
        for species, feat_nos in feat_list_for_species.items():
            if species in alias_list_for_species:
                for alias_name in alias_list_for_species[species]:
                    alias_no = get_or_create_alias(
                        session, alias_name, alias_type, created_by
                    )
                    for feat_no in feat_nos:
                        if create_feat_alias(session, feat_no, alias_no):
                            stats["aliases_created"] += 1

        if stats["clusters_processed"] % 100 == 0:
            logger.info(f"Processed {stats['clusters_processed']} clusters...")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load OrthoMCL ortholog clusters into the database"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input OrthoMCL clusters file",
    )
    parser.add_argument(
        "--group-type",
        default=DEFAULT_GROUP_TYPE,
        help=f"Homology group type (default: {DEFAULT_GROUP_TYPE})",
    )
    parser.add_argument(
        "--method",
        default=DEFAULT_METHOD,
        help=f"Homology method (default: {DEFAULT_METHOD})",
    )
    parser.add_argument(
        "--dbxref-source",
        default=DEFAULT_DBXREF_SOURCE,
        help=f"Source for dbxref entries (default: {DEFAULT_DBXREF_SOURCE})",
    )
    parser.add_argument(
        "--alias-type",
        default=DEFAULT_ALIAS_TYPE,
        help=f"Alias type for cross-strain aliases (default: {DEFAULT_ALIAS_TYPE})",
    )
    parser.add_argument(
        "--external-url",
        help="External URL template for ortholog groups",
    )
    parser.add_argument(
        "--strain-map",
        nargs="*",
        help="Strain mappings in format SHORT=abbrev (e.g., CALB=C_albicans)",
    )
    parser.add_argument(
        "--created-by",
        default=os.getenv("DB_USER", "SCRIPT"),
        help="Database user for created_by field",
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
        help="Skip deleting previous data",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    # Parse strain mappings
    strain_map = {}
    if args.strain_map:
        for mapping in args.strain_map:
            if '=' in mapping:
                short, abbrev = mapping.split('=', 1)
                strain_map[short] = abbrev

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Group type: {args.group_type}")
    logger.info(f"Method: {args.method}")
    if strain_map:
        logger.info(f"Strain mappings: {strain_map}")

    if args.dry_run:
        logger.info("DRY RUN - parsing file only")
        cluster_count = 0
        singleton_count = 0
        with open(args.input_file) as f:
            for line in f:
                cluster_id, members = parse_orthomcl_cluster(line)
                if cluster_id:
                    total = sum(len(g) for g in members.values())
                    if total >= 2:
                        cluster_count += 1
                    else:
                        singleton_count += 1
        logger.info(f"Found {cluster_count} valid clusters")
        logger.info(f"Found {singleton_count} singletons")
        return

    try:
        with SessionLocal() as session:
            # Get organism information
            db_species, strain_abbrevs, species_for_strain = get_db_organisms(session)
            db_strains = {v: True for v in strain_abbrevs.values()}

            logger.info(f"Found {len(db_species)} species in database")
            logger.info(f"Found {len(db_strains)} strains in database")

            # Get database ORFs
            db_orfs = get_db_orfs(session)

            # Set up external URL if provided
            external_url_no = None
            if args.external_url:
                external_url_no = get_or_create_url(
                    session, args.external_url, args.dbxref_source, args.created_by
                )

            # Delete existing data if not skipped
            if not args.skip_delete:
                delete_existing_homology_groups(
                    session, args.group_type, args.method
                )
                delete_other_strain_aliases(session, args.alias_type)

            # Load clusters
            stats = load_orthomcl_clusters(
                session,
                args.input_file,
                strain_map,
                db_orfs,
                db_strains,
                db_species,
                species_for_strain,
                args.group_type,
                args.method,
                args.dbxref_source,
                args.alias_type,
                external_url_no,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Clusters processed: {stats['clusters_processed']}")
            logger.info(f"  Singletons skipped: {stats['singleton_skipped']}")
            logger.info(f"  No-DB-gene clusters skipped: {stats['no_db_skipped']}")
            logger.info(f"  Homology groups created: {stats['groups_created']}")
            logger.info(f"  Feat_homology created: {stats['feat_homology_created']}")
            logger.info(f"  Dbxref_homology created: {stats['dbxref_homology_created']}")
            if stats["aliases_created"] > 0:
                logger.info(f"  Aliases created: {stats['aliases_created']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading ortholog clusters: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
