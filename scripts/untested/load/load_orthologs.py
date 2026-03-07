#!/usr/bin/env python3
"""
Load ortholog or best hit information into the database.

This script loads ortholog/best hit data into the following tables:
- DBXREF: External database cross-references
- DBXREF_URL: Links between DBXREF and URL
- DBXREF_FEAT: Links between DBXREF and features

The identifiers are stored in the DBXREF table and linked to template URLs
in the URL table via DBXREF_URL. Features are linked via DBXREF_FEAT.

Input file format (tab-delimited):
- Column 1: ORF name (this strain)
- Column 2: Gene name (this strain)
- Column 3: Database ID (this strain)
- Column 4: ORF name (other organism)
- Column 5: Gene name (other organism)
- Column 6: Database ID (other organism)

Original Perl: loadOrthologs.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, delete, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Dbxref, DbxrefFeat, DbxrefUrl, Feature, Organism, Url

load_dotenv()

logger = logging.getLogger(__name__)

# Valid DBXREF source types
VALID_TYPES = {
    "SGD",
    "SGD_BEST_HIT",
    "C. dubliniensis GeneDB",
    "POMBASE",
    "POMBASE_BEST_HIT",
    "CGD",
    "CGD_BEST_HIT",
    "AspGD",
    "AspGD_BEST_HIT",
    "BROAD_NEUROSPORA",
    "BROAD_NEUROSPORA_BEST_HIT",
}

# URL templates for each type
URL_TEMPLATES = {
    "SGD": "https://www.yeastgenome.org/locus/_SUBSTITUTE_THIS_",
    "SGD_BEST_HIT": "https://www.yeastgenome.org/locus/_SUBSTITUTE_THIS_",
    "C. dubliniensis GeneDB": "https://fungidb.org/fungidb/app/record/gene/_SUBSTITUTE_THIS_",
    "POMBASE": "https://www.pombase.org/gene/_SUBSTITUTE_THIS_",
    "POMBASE_BEST_HIT": "https://www.pombase.org/gene/_SUBSTITUTE_THIS_",
    "CGD": "https://www.candidagenome.org/locus/_SUBSTITUTE_THIS_",
    "CGD_BEST_HIT": "https://www.candidagenome.org/locus/_SUBSTITUTE_THIS_",
    "AspGD": "https://fungidb.org/fungidb/app/record/gene/_SUBSTITUTE_THIS_",
    "AspGD_BEST_HIT": "https://fungidb.org/fungidb/app/record/gene/_SUBSTITUTE_THIS_",
    "BROAD_NEUROSPORA": "https://fungidb.org/fungidb/app/record/gene/_SUBSTITUTE_THIS_",
    "BROAD_NEUROSPORA_BEST_HIT": "https://fungidb.org/fungidb/app/record/gene/_SUBSTITUTE_THIS_",
}


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


def normalize_type(source_type: str) -> str:
    """
    Normalize the DBXREF source type.

    Args:
        source_type: User-provided type

    Returns:
        Normalized type string
    """
    type_lower = source_type.lower()

    # Handle C. dubliniensis variations
    if any(x in type_lower for x in ["cdub", "c_dub", "c. dub", "candida dub"]):
        return "C. dubliniensis GeneDB"

    # Return original if it's a valid type
    if source_type in VALID_TYPES:
        return source_type

    raise ValueError(f"'{source_type}' is not a recognized DBXREF type. "
                     f"Valid types: {', '.join(sorted(VALID_TYPES))}")


def get_organism(session: Session, strain_abbrev: str) -> Organism:
    """
    Get organism by abbreviation.

    Args:
        session: Database session
        strain_abbrev: Organism abbreviation

    Returns:
        Organism object

    Raises:
        ValueError: If organism not found
    """
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == strain_abbrev
    ).first()

    if not organism:
        raise ValueError(f"'{strain_abbrev}' is not a recognized organism abbreviation")

    return organism


def get_template_url_no(session: Session, source_type: str) -> int | None:
    """
    Get the URL number for the template URL.

    Args:
        session: Database session
        source_type: DBXREF source type

    Returns:
        url_no or None if no URL defined for this type
    """
    url_template = URL_TEMPLATES.get(source_type)
    if not url_template:
        return None

    url_obj = session.query(Url).filter(Url.url == url_template).first()
    return url_obj.url_no if url_obj else None


def delete_previous_mappings(
    session: Session,
    source_type: str,
    organism_no: int,
    schema: str = "MULTI",
) -> dict:
    """
    Delete previous ortholog/best hit mappings.

    Args:
        session: Database session
        source_type: DBXREF source type
        organism_no: Organism number to filter by
        schema: Database schema name

    Returns:
        Dictionary with deletion counts
    """
    stats = {
        "dbxref_feat_deleted": 0,
        "go_annotation_deleted": 0,
    }

    # Delete from DBXREF_FEAT
    # Get dbxref_nos for this source type
    dbxref_subquery = session.query(Dbxref.dbxref_no).filter(
        and_(
            Dbxref.source == source_type,
            Dbxref.dbxref_type == "Gene ID",
        )
    ).subquery()

    # Get feature_nos for this organism
    feature_subquery = session.query(Feature.feature_no).filter(
        Feature.organism_no == organism_no
    ).subquery()

    result = session.execute(
        delete(DbxrefFeat).where(
            and_(
                DbxrefFeat.dbxref_no.in_(dbxref_subquery.select()),
                DbxrefFeat.feature_no.in_(feature_subquery.select()),
            )
        )
    )
    stats["dbxref_feat_deleted"] = result.rowcount
    logger.info(f"Deleted {stats['dbxref_feat_deleted']} rows from DBXREF_FEAT")

    # Delete GO annotations (cascades to GO_REF and GOREF_DBXREF)
    # This uses raw SQL due to the complex nested subqueries
    delete_go_sql = text(f"""
        DELETE FROM {schema}.go_annotation
        WHERE go_annotation_no IN (
            SELECT go_annotation_no FROM {schema}.go_ref
            WHERE go_ref_no IN (
                SELECT go_ref_no FROM {schema}.goref_dbxref
                WHERE dbxref_no IN (
                    SELECT dbxref_no FROM {schema}.dbxref
                    WHERE source = :source_type
                    AND dbxref_type = 'Gene ID'
                )
            )
        )
        AND feature_no IN (
            SELECT feature_no FROM {schema}.feature
            WHERE organism_no = :organism_no
        )
    """)

    result = session.execute(
        delete_go_sql,
        {"source_type": source_type, "organism_no": organism_no}
    )
    stats["go_annotation_deleted"] = result.rowcount
    logger.info(f"Deleted {stats['go_annotation_deleted']} rows from GO_ANNOTATION")

    session.commit()
    return stats


def get_or_create_dbxref(
    session: Session,
    dbxref_id: str,
    source: str,
    gene_name: str | None,
    created_by: str,
) -> int:
    """
    Get existing DBXREF or create new one.

    Args:
        session: Database session
        dbxref_id: External database ID
        source: Source database
        gene_name: Gene name (stored as description)
        created_by: User creating the record

    Returns:
        dbxref_no
    """
    existing = session.query(Dbxref).filter(
        and_(
            Dbxref.dbxref_id == dbxref_id,
            Dbxref.source == source,
            Dbxref.dbxref_type == "Gene ID",
        )
    ).first()

    if existing:
        # Check if gene name needs updating
        if gene_name and existing.description != gene_name:
            existing.description = gene_name
            logger.debug(f"Updated description for DBXREF {dbxref_id}")
        return existing.dbxref_no

    # Create new DBXREF
    new_dbxref = Dbxref(
        dbxref_id=dbxref_id,
        source=source,
        dbxref_type="Gene ID",
        description=gene_name,
        created_by=created_by[:12],
    )
    session.add(new_dbxref)
    session.flush()

    logger.debug(f"Created DBXREF for {dbxref_id} with dbxref_no={new_dbxref.dbxref_no}")
    return new_dbxref.dbxref_no


def create_dbxref_feat_if_not_exists(
    session: Session,
    dbxref_no: int,
    feature_no: int,
) -> bool:
    """
    Create DBXREF_FEAT entry if it doesn't exist.

    Returns:
        True if created, False if already existed
    """
    existing = session.query(DbxrefFeat).filter(
        and_(
            DbxrefFeat.dbxref_no == dbxref_no,
            DbxrefFeat.feature_no == feature_no,
        )
    ).first()

    if existing:
        return False

    new_entry = DbxrefFeat(
        dbxref_no=dbxref_no,
        feature_no=feature_no,
    )
    session.add(new_entry)
    return True


def create_dbxref_url_if_not_exists(
    session: Session,
    dbxref_no: int,
    url_no: int,
) -> bool:
    """
    Create DBXREF_URL entry if it doesn't exist.

    Returns:
        True if created, False if already existed
    """
    existing = session.query(DbxrefUrl).filter(
        and_(
            DbxrefUrl.dbxref_no == dbxref_no,
            DbxrefUrl.url_no == url_no,
        )
    ).first()

    if existing:
        return False

    new_entry = DbxrefUrl(
        dbxref_no=dbxref_no,
        url_no=url_no,
    )
    session.add(new_entry)
    return True


def parse_ortholog_file(filepath: Path) -> list[dict]:
    """
    Parse the ortholog/best hit input file.

    Args:
        filepath: Path to input file

    Returns:
        List of dictionaries with ortholog data
    """
    entries = []

    logger.info(f"Parsing input file: {filepath}")

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()

            # Skip comments and empty lines
            if line.startswith("#") or not line:
                continue

            parts = line.split("\t")
            if len(parts) < 6:
                logger.warning(f"Line {line_num}: insufficient columns, skipping")
                continue

            entry = {
                "orf1": parts[0],      # ORF name (this strain)
                "gene1": parts[1],     # Gene name (this strain)
                "dbid1": parts[2],     # Database ID (this strain)
                "orf2": parts[3],      # ORF name (other organism)
                "gene2": parts[4],     # Gene name (other organism)
                "dbid2": parts[5],     # Database ID (other organism)
            }
            entries.append(entry)

    logger.info(f"Parsed {len(entries)} entries from input file")
    return entries


def load_mappings(
    session: Session,
    entries: list[dict],
    source_type: str,
    url_no: int | None,
    created_by: str,
) -> dict:
    """
    Load ortholog mappings into the database.

    Args:
        session: Database session
        entries: List of ortholog entries
        source_type: DBXREF source type
        url_no: URL number for template URL (or None)
        created_by: User performing the load

    Returns:
        Dictionary with statistics
    """
    stats = {
        "entries_processed": 0,
        "dbxrefs_created": 0,
        "dbxref_feats_created": 0,
        "dbxref_urls_created": 0,
        "features_not_found": 0,
    }

    logger.info("Loading mappings...")

    for i, entry in enumerate(entries, 1):
        if i % 100 == 0:
            logger.info(f"Processing entry {i}...")

        orf1 = entry["orf1"]
        gene2 = entry["gene2"]
        dbid2 = entry["dbid2"]

        logger.debug(f"Processing: {orf1} | {gene2} | {dbid2}")

        # Find the feature by name
        feature = session.query(Feature).filter(
            Feature.feature_name == orf1
        ).first()

        if not feature:
            logger.warning(f"Can't map {orf1} to a feature")
            stats["features_not_found"] += 1
            continue

        stats["entries_processed"] += 1

        # Create/get DBXREF
        dbxref_no = get_or_create_dbxref(
            session, dbid2, source_type, gene2, created_by
        )
        stats["dbxrefs_created"] += 1

        # Create DBXREF_FEAT
        if create_dbxref_feat_if_not_exists(session, dbxref_no, feature.feature_no):
            stats["dbxref_feats_created"] += 1

        # Create DBXREF_URL if we have a URL
        if url_no:
            if create_dbxref_url_if_not_exists(session, dbxref_no, url_no):
                stats["dbxref_urls_created"] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load ortholog or best hit information into the database"
    )
    parser.add_argument(
        "strain_abbrev",
        help="Standard organism abbreviation (e.g., C_albicans_SC5314)",
    )
    parser.add_argument(
        "source_type",
        help="DBXREF source type (e.g., SGD, SGD_BEST_HIT)",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        help="Input data file (tab-delimited)",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        help="Directory containing input file",
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
        help="Skip deleting previous mappings",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    logger.info(f"Started at {datetime.now()}")

    try:
        # Normalize and validate source type
        source_type = normalize_type(args.source_type)
        logger.info(f"Source type: {source_type}")

        # Determine input file path
        if args.input_file:
            input_file = args.input_file
        elif args.input_dir:
            # Look for appropriate file in directory
            logger.error("--input-dir requires --input-file to specify filename")
            sys.exit(1)
        else:
            logger.error("Must specify --input-file")
            sys.exit(1)

        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            sys.exit(1)

        # Parse input file first (before database operations)
        entries = parse_ortholog_file(input_file)

        if args.dry_run:
            logger.info("DRY RUN - no database modifications")
            logger.info(f"Would process {len(entries)} entries")
            return

        with SessionLocal() as session:
            # Validate strain
            organism = get_organism(session, args.strain_abbrev)
            logger.info(f"Organism: {organism.organism_name} (organism_no={organism.organism_no})")

            # Get template URL number
            url_no = get_template_url_no(session, source_type)
            if url_no:
                logger.info(f"Using URL template with url_no={url_no}")
            else:
                logger.warning("No URL template found for this source type")

            # Delete previous mappings
            if not args.skip_delete:
                delete_stats = delete_previous_mappings(
                    session, source_type, organism.organism_no
                )
            else:
                logger.info("Skipping deletion of previous mappings")

            # Load new mappings
            load_stats = load_mappings(
                session,
                entries,
                source_type,
                url_no,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Entries processed: {load_stats['entries_processed']}")
            logger.info(f"  DBXREFs created: {load_stats['dbxrefs_created']}")
            logger.info(f"  DBXREF_FEATs created: {load_stats['dbxref_feats_created']}")
            logger.info(f"  DBXREF_URLs created: {load_stats['dbxref_urls_created']}")
            if load_stats["features_not_found"] > 0:
                logger.warning(f"  Features not found: {load_stats['features_not_found']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading orthologs: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
