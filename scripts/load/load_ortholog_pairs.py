#!/usr/bin/env python3
"""
Load pairwise ortholog pairs into the database.

This script loads pairwise ortholog assignments from InParanoid or similar
tools between two species/strains. It clusters pairs and creates homology
groups in the HOMOLOGY_GROUP, FEAT_HOMOLOGY, and DBXREF_HOMOLOGY tables.

Input format: Tab-delimited file with two columns:
  - Column 1: Query feature name (TO strain)
  - Column 2: Target feature dbxref_id (FROM strain)

Lines beginning with # are skipped as comments.

Original Perl: loadOrthologPairs2MultiDB.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
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
    Dbxref,
    DbxrefHomology,
    FeatHomology,
    FeatUrl,
    Feature,
    HomologyGroup,
    Organism,
    Url,
    WebDisplay,
)

load_dotenv()

logger = logging.getLogger(__name__)

# Default settings
DEFAULT_GROUP_TYPE = "ortholog"
DEFAULT_METHOD = "InParanoid"


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def get_organism(session: Session, organism_abbrev: str) -> Organism:
    """
    Get organism by abbreviation.

    Args:
        session: Database session
        organism_abbrev: Organism abbreviation

    Returns:
        Organism object

    Raises:
        ValueError: If organism not found
    """
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == organism_abbrev
    ).first()

    if not organism:
        raise ValueError(f"Organism not found: {organism_abbrev}")

    return organism


def get_strain_abbrev_map(session: Session) -> dict[int, str]:
    """
    Get mapping of organism_no to organism_abbrev.

    Args:
        session: Database session

    Returns:
        Dictionary mapping organism_no to organism_abbrev
    """
    organisms = session.query(Organism).all()
    return {org.organism_no: org.organism_abbrev for org in organisms}


def find_feature_by_name(session: Session, name: str) -> Feature | None:
    """Find feature by feature_name."""
    return session.query(Feature).filter(
        Feature.feature_name == name
    ).first()


def find_feature_by_dbxref(session: Session, dbxref_id: str) -> Feature | None:
    """Find feature by dbxref_id."""
    return session.query(Feature).filter(
        Feature.dbxref_id == dbxref_id
    ).first()


def get_or_create_url(
    session: Session,
    url: str,
    source: str,
    acronym: str,
    created_by: str,
) -> int:
    """
    Get existing URL or create new one.

    Args:
        session: Database session
        url: URL string
        source: Source description
        acronym: Project acronym
        created_by: User creating the record

    Returns:
        url_no
    """
    existing = session.query(Url).filter(Url.url == url).first()

    if existing:
        logger.debug(f"URL exists: {url}")
        return existing.url_no

    new_url = Url(
        url=url,
        source=source,
        url_type=f"query by {acronym} ORF name",
        substitution_value="FEATURE",
        created_by=created_by[:12],
    )
    session.add(new_url)
    session.flush()

    logger.info(f"URL inserted: {url}")
    return new_url.url_no


def get_or_create_web_display(
    session: Session,
    url_no: int,
    source: str,
    created_by: str,
) -> int | None:
    """
    Get existing web display or create new one.

    Args:
        session: Database session
        url_no: URL number
        source: Source description
        created_by: User creating the record

    Returns:
        web_display_no or None
    """
    existing = session.query(WebDisplay).filter(
        and_(
            WebDisplay.url_no == url_no,
            WebDisplay.web_page_name == "Locus",
            WebDisplay.label_location == source,
            WebDisplay.label_type == "Text",
            WebDisplay.label_name == source,
            WebDisplay.is_default == "N",
        )
    ).first()

    if existing:
        logger.debug(f"WEB_DISPLAY exists for url_no={url_no}")
        return existing.web_display_no

    new_wd = WebDisplay(
        url_no=url_no,
        web_page_name="Locus",
        label_location=source,
        label_type="Text",
        label_name=source,
        is_default="N",
        created_by=created_by[:12],
    )
    session.add(new_wd)
    session.flush()

    logger.info(f"WEB_DISPLAY inserted for url_no={url_no}")
    return new_wd.web_display_no


def delete_existing_homology_groups(
    session: Session,
    group_type: str,
    method: str,
    to_org_no: int,
    from_org_no: int,
) -> int:
    """
    Delete existing homology groups between two organisms.

    Args:
        session: Database session
        group_type: Homology group type
        method: Homology method
        to_org_no: TO organism number
        from_org_no: FROM organism number

    Returns:
        Number of groups deleted
    """
    # Find homology groups that have features from both organisms
    # with matching type and method
    sql = text("""
        SELECT DISTINCT hg.homology_group_no
        FROM MULTI.homology_group hg
        WHERE hg.homology_group_type = :group_type
          AND hg.method = :method
          AND hg.homology_group_no IN (
              SELECT fh1.homology_group_no
              FROM MULTI.feat_homology fh1
              JOIN MULTI.feature f1 ON fh1.feature_no = f1.feature_no
              WHERE f1.organism_no = :to_org_no
          )
          AND hg.homology_group_no IN (
              SELECT fh2.homology_group_no
              FROM MULTI.feat_homology fh2
              JOIN MULTI.feature f2 ON fh2.feature_no = f2.feature_no
              WHERE f2.organism_no = :from_org_no
          )
    """)

    result = session.execute(sql, {
        "group_type": group_type,
        "method": method,
        "to_org_no": to_org_no,
        "from_org_no": from_org_no,
    })
    group_nos = [row[0] for row in result]

    count = 0
    for hg_no in group_nos:
        # Delete feat_homology entries
        session.query(FeatHomology).filter(
            FeatHomology.homology_group_no == hg_no
        ).delete()

        # Delete dbxref_homology entries
        session.query(DbxrefHomology).filter(
            DbxrefHomology.homology_group_no == hg_no
        ).delete()

        # Delete the group
        session.query(HomologyGroup).filter(
            HomologyGroup.homology_group_no == hg_no
        ).delete()
        count += 1

    if count > 0:
        session.flush()
        logger.info(f"Deleted {count} existing homology groups")

    return count


def create_homology_group(
    session: Session,
    group_type: str,
    method: str,
    created_by: str,
) -> int:
    """Create a new homology group."""
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
    """Create feat_homology entry if not exists."""
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
    name: str,
    created_by: str,
) -> bool:
    """Create dbxref_homology entry if not exists."""
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
        name=name,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    return True


def create_feat_url(
    session: Session,
    feature_no: int,
    url_no: int,
) -> bool:
    """Create feat_url entry if not exists."""
    existing = session.query(FeatUrl).filter(
        and_(
            FeatUrl.feature_no == feature_no,
            FeatUrl.url_no == url_no,
        )
    ).first()

    if existing:
        return False

    new_entry = FeatUrl(
        feature_no=feature_no,
        url_no=url_no,
    )
    session.add(new_entry)
    return True


def get_or_create_dbxref(
    session: Session,
    gene_id: str,
    species_desc: str,
    source: str,
    created_by: str,
) -> int:
    """Get existing dbxref or create new one for external ortholog."""
    existing = session.query(Dbxref).filter(
        and_(
            Dbxref.source == source,
            Dbxref.dbxref_type == "Gene ID",
            Dbxref.dbxref_id == gene_id,
        )
    ).first()

    if existing:
        return existing.dbxref_no

    new_dbxref = Dbxref(
        source=source,
        dbxref_type="Gene ID",
        dbxref_id=gene_id,
        description=species_desc,
        created_by=created_by[:12],
    )
    session.add(new_dbxref)
    session.flush()

    return new_dbxref.dbxref_no


def cluster_pairs(
    session: Session,
    pairs_file: Path,
    strain_abbrev_map: dict[int, str],
) -> tuple[dict, dict, dict, dict]:
    """
    Parse pairs file and cluster orthologs.

    Uses union-find algorithm to merge pairs into clusters.

    Args:
        session: Database session
        pairs_file: Path to input file
        strain_abbrev_map: organism_no -> organism_abbrev mapping

    Returns:
        Tuple of (cluster_to_strains, strain_for_feat, geneid_for_feat, cluster_count)
    """
    cluster_for_feat = {}  # feat_no -> cluster_id
    feats_in_cluster = defaultdict(list)  # cluster_id -> [feat_nos]
    strain_for_feat = {}  # feat_no -> strain_abbrev
    geneid_for_feat = {}  # feat_no -> gene_id

    cluster_count = 0

    with open(pairs_file) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                logger.warning(f"Line {line_num}: Invalid format, skipping")
                continue

            query_name = parts[0]
            target_dbxref = parts[1]

            # Find query feature by name
            query_feat = find_feature_by_name(session, query_name)
            if not query_feat:
                continue

            # Find target feature by dbxref_id
            target_feat = find_feature_by_dbxref(session, target_dbxref)
            if not target_feat:
                continue

            query_no = query_feat.feature_no
            target_no = target_feat.feature_no

            geneid_for_feat[query_no] = query_feat.feature_name
            geneid_for_feat[target_no] = target_feat.feature_name

            strain_for_feat[query_no] = strain_abbrev_map.get(query_feat.organism_no)
            strain_for_feat[target_no] = strain_abbrev_map.get(target_feat.organism_no)

            # Cluster merging logic
            q_cluster = cluster_for_feat.get(query_no)
            t_cluster = cluster_for_feat.get(target_no)

            if q_cluster is not None and t_cluster is not None:
                # Both already clustered - merge if different
                if q_cluster != t_cluster:
                    # Merge t_cluster into q_cluster
                    for feat_no in feats_in_cluster[t_cluster]:
                        cluster_for_feat[feat_no] = q_cluster
                        feats_in_cluster[q_cluster].append(feat_no)
                    del feats_in_cluster[t_cluster]

            elif q_cluster is not None:
                # Query clustered, add target to same cluster
                cluster_for_feat[target_no] = q_cluster
                feats_in_cluster[q_cluster].append(target_no)

            elif t_cluster is not None:
                # Target clustered, add query to same cluster
                cluster_for_feat[query_no] = t_cluster
                feats_in_cluster[t_cluster].append(query_no)

            else:
                # Neither clustered - create new cluster
                cluster_count += 1
                cluster_for_feat[query_no] = cluster_count
                cluster_for_feat[target_no] = cluster_count
                feats_in_cluster[cluster_count].extend([query_no, target_no])

    # Convert to strain->gene_ids structure
    strain_gene_ids_for_cluster = defaultdict(lambda: defaultdict(list))
    for cluster_id, feat_nos in feats_in_cluster.items():
        for feat_no in feat_nos:
            strain = strain_for_feat.get(feat_no)
            gene_id = geneid_for_feat.get(feat_no)
            if strain and gene_id:
                strain_gene_ids_for_cluster[cluster_id][strain].append(gene_id)

    return strain_gene_ids_for_cluster, strain_for_feat, geneid_for_feat, feats_in_cluster


def load_ortholog_pairs(
    session: Session,
    pairs_file: Path,
    to_org: Organism,
    from_org: Organism,
    group_type: str,
    method: str,
    source: str,
    internal_url_no: int,
    created_by: str,
) -> dict:
    """
    Load ortholog pairs into database.

    Args:
        session: Database session
        pairs_file: Path to input file
        to_org: TO organism
        from_org: FROM organism
        group_type: Homology group type
        method: Homology method
        source: Source description
        internal_url_no: URL number for internal links
        created_by: User creating records

    Returns:
        Statistics dictionary
    """
    stats = {
        "clusters_processed": 0,
        "groups_created": 0,
        "feat_homology_created": 0,
        "dbxref_homology_created": 0,
        "feat_urls_created": 0,
    }

    strain_abbrev_map = get_strain_abbrev_map(session)

    logger.info("Clustering ortholog pairs...")
    strain_gene_ids, strain_for_feat, geneid_for_feat, feats_in_cluster = cluster_pairs(
        session, pairs_file, strain_abbrev_map
    )

    logger.info(f"Found {len(feats_in_cluster)} clusters")

    # Get DB members - features in strains that are in our database
    db_cluster_members = defaultdict(lambda: defaultdict(list))

    for cluster_id, strain_genes in strain_gene_ids.items():
        for strain, gene_ids in strain_genes.items():
            for gene_id in gene_ids:
                feat = find_feature_by_name(session, gene_id)
                if feat:
                    db_cluster_members[cluster_id][strain].append(feat.feature_no)

    # Load each cluster
    for cluster_id in sorted(strain_gene_ids.keys()):
        logger.debug(f"Processing cluster {cluster_id}")
        stats["clusters_processed"] += 1

        # Create homology group
        hg_no = create_homology_group(session, group_type, method, created_by)
        stats["groups_created"] += 1

        # Add DB features to group
        if cluster_id in db_cluster_members:
            for strain, feat_nos in db_cluster_members[cluster_id].items():
                for feat_no in feat_nos:
                    if create_feat_homology(session, feat_no, hg_no, created_by):
                        stats["feat_homology_created"] += 1

                    if internal_url_no:
                        if create_feat_url(session, feat_no, internal_url_no):
                            stats["feat_urls_created"] += 1

        # Handle external orthologs (strains not in DB)
        for strain, gene_ids in strain_gene_ids[cluster_id].items():
            # Skip if this strain's features are already in DB
            if cluster_id in db_cluster_members and strain in db_cluster_members[cluster_id]:
                continue

            for gene_id in gene_ids:
                dbxref_no = get_or_create_dbxref(
                    session, gene_id, strain, source, created_by
                )
                if create_dbxref_homology(session, dbxref_no, hg_no, strain, created_by):
                    stats["dbxref_homology_created"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load pairwise ortholog pairs into the database"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input tab-delimited pairs file",
    )
    parser.add_argument(
        "--to-strain",
        required=True,
        help="TO strain abbreviation",
    )
    parser.add_argument(
        "--from-strain",
        required=True,
        help="FROM strain abbreviation",
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
        "--source",
        help="Source description (default: 'Orthologous genes in <genus> species')",
    )
    parser.add_argument(
        "--internal-url",
        default="/cgi-bin/locus.pl?locus=_SUBSTITUTE_THIS_",
        help="Internal URL template for feature links",
    )
    parser.add_argument(
        "--acronym",
        default="CGD",
        help="Project acronym (default: CGD)",
    )
    parser.add_argument(
        "--genus",
        default="Candida",
        help="Genus name for source (default: Candida)",
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
        help="Skip deleting previous ortholog groups",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    # Set default source
    source = args.source or f"Orthologous genes in {args.genus} species"

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"TO strain: {args.to_strain}")
    logger.info(f"FROM strain: {args.from_strain}")
    logger.info(f"Group type: {args.group_type}")
    logger.info(f"Method: {args.method}")
    logger.info(f"Source: {source}")

    if args.dry_run:
        logger.info("DRY RUN - parsing file only")
        count = 0
        with open(args.input_file) as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    count += 1
        logger.info(f"Found {count} pairs in input file")
        return

    try:
        with SessionLocal() as session:
            # Get organisms
            to_org = get_organism(session, args.to_strain)
            from_org = get_organism(session, args.from_strain)

            logger.info(f"TO organism: {to_org.organism_name}")
            logger.info(f"FROM organism: {from_org.organism_name}")

            # Set up URL
            internal_url_no = get_or_create_url(
                session,
                args.internal_url,
                source,
                args.acronym,
                args.created_by,
            )
            get_or_create_web_display(session, internal_url_no, source, args.created_by)

            # Delete existing groups if not skipped
            if not args.skip_delete:
                delete_existing_homology_groups(
                    session,
                    args.group_type,
                    args.method,
                    to_org.organism_no,
                    from_org.organism_no,
                )

            # Load ortholog pairs
            stats = load_ortholog_pairs(
                session,
                args.input_file,
                to_org,
                from_org,
                args.group_type,
                args.method,
                source,
                internal_url_no,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Clusters processed: {stats['clusters_processed']}")
            logger.info(f"  Homology groups created: {stats['groups_created']}")
            logger.info(f"  Feat_homology created: {stats['feat_homology_created']}")
            if stats["dbxref_homology_created"] > 0:
                logger.info(
                    f"  Dbxref_homology created: {stats['dbxref_homology_created']}"
                )
            if stats["feat_urls_created"] > 0:
                logger.info(f"  Feat_urls created: {stats['feat_urls_created']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading ortholog pairs: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
