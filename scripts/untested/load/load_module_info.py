#!/usr/bin/env python3
"""
Load module information (external IDs and template URLs).

This script loads external identifiers and their associated template URLs
into the EXTERNAL_ID, TEMPLATE_URL, and EI_TU tables.

The template URLs contain a placeholder (_SUBSTITUTE_THIS_) that gets
replaced with the external ID to create the final URL.

Input file format: One feature name (ORF) per line

Original Perl: loadModuleInfo.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import (
    Alias,
    EiTu,
    ExternalId,
    FeatAlias,
    Feature,
    TemplateUrl,
)

load_dotenv()

logger = logging.getLogger(__name__)


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


def find_feature_by_name(session: Session, name: str) -> Feature | None:
    """
    Find feature by feature_name or alias.

    Args:
        session: Database session
        name: Feature name or alias to search

    Returns:
        Feature object or None
    """
    # Try by feature_name first
    feature = session.query(Feature).filter(
        Feature.feature_name == name
    ).first()

    if feature:
        return feature

    # Try by alias
    feat_alias = session.query(FeatAlias).join(
        Alias, FeatAlias.alias_no == Alias.alias_no
    ).filter(
        Alias.alias_name == name
    ).first()

    if feat_alias:
        return session.query(Feature).filter(
            Feature.feature_no == feat_alias.feature_no
        ).first()

    return None


def get_or_create_template_url(
    session: Session,
    template_url: str,
    source: str,
    description: str,
    created_by: str,
) -> int:
    """
    Get existing template URL or create new one.

    Args:
        session: Database session
        template_url: URL template with _SUBSTITUTE_THIS_ placeholder
        source: Source name
        description: Description of the URL
        created_by: User creating the record

    Returns:
        template_url_no
    """
    existing = session.query(TemplateUrl).filter(
        and_(
            TemplateUrl.template_url == template_url,
            TemplateUrl.source == source,
        )
    ).first()

    if existing:
        return existing.template_url_no

    new_entry = TemplateUrl(
        template_url=template_url,
        source=source,
        description=description,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    session.flush()

    logger.info(f"Created template URL for source: {source}")
    return new_entry.template_url_no


def get_or_create_external_id(
    session: Session,
    external_id: str,
    source: str,
    tab_name: str,
    primary_key: int,
    created_by: str,
) -> int:
    """
    Get existing external ID or create new one.

    Args:
        session: Database session
        external_id: External identifier value
        source: Source name
        tab_name: Table name being linked
        primary_key: Primary key value in the table
        created_by: User creating the record

    Returns:
        external_id_no
    """
    existing = session.query(ExternalId).filter(
        and_(
            ExternalId.external_id == external_id,
            ExternalId.source == source,
            ExternalId.tab_name == tab_name,
            ExternalId.primary_key == primary_key,
        )
    ).first()

    if existing:
        return existing.external_id_no

    new_entry = ExternalId(
        external_id=external_id,
        source=source,
        tab_name=tab_name,
        primary_key=primary_key,
        created_by=created_by[:12],
    )
    session.add(new_entry)
    session.flush()

    return new_entry.external_id_no


def create_ei_tu_link(
    session: Session,
    external_id_no: int,
    template_url_no: int,
) -> bool:
    """
    Create link between external ID and template URL.

    Args:
        session: Database session
        external_id_no: External ID number
        template_url_no: Template URL number

    Returns:
        True if created, False if already existed
    """
    existing = session.query(EiTu).filter(
        and_(
            EiTu.external_id_no == external_id_no,
            EiTu.template_url_no == template_url_no,
        )
    ).first()

    if existing:
        return False

    new_entry = EiTu(
        external_id_no=external_id_no,
        template_url_no=template_url_no,
    )
    session.add(new_entry)
    return True


def parse_input_file(filepath: Path) -> list[str]:
    """
    Parse input file with feature names.

    Args:
        filepath: Path to input file

    Returns:
        List of feature names
    """
    entries = []

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(line)

    logger.info(f"Parsed {len(entries)} entries from input file")
    return entries


def load_module_info(
    session: Session,
    entries: list[str],
    template_url: str,
    source: str,
    description: str,
    created_by: str,
) -> dict:
    """
    Load module information into the database.

    Args:
        session: Database session
        entries: List of feature names
        template_url: URL template
        source: Source name
        description: Description
        created_by: User creating the records

    Returns:
        Dictionary with statistics
    """
    stats = {
        "entries_processed": 0,
        "external_ids_created": 0,
        "links_created": 0,
        "features_not_found": 0,
        "duplicates_skipped": 0,
    }

    # Create template URL first
    template_url_no = get_or_create_template_url(
        session, template_url, source, description, created_by
    )

    seen_features = {}

    for orf in entries:
        feature = find_feature_by_name(session, orf)

        if not feature:
            logger.warning(f"Cannot find feature for: {orf}")
            stats["features_not_found"] += 1
            continue

        # Skip if already processed this feature
        if feature.feature_no in seen_features:
            logger.debug(
                f"Feature {feature.feature_no} already processed as "
                f"{seen_features[feature.feature_no]}, skipping {orf}"
            )
            stats["duplicates_skipped"] += 1
            continue

        stats["entries_processed"] += 1

        # Create external ID
        external_id_no = get_or_create_external_id(
            session,
            orf,
            source,
            "FEATURE",
            feature.feature_no,
            created_by,
        )
        stats["external_ids_created"] += 1

        # Link external ID to template URL
        if create_ei_tu_link(session, external_id_no, template_url_no):
            stats["links_created"] += 1
            logger.debug(f"Linked {orf} to template URL")

        seen_features[feature.feature_no] = orf

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load module information (external IDs and template URLs)"
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input file with feature names (one per line)",
    )
    parser.add_argument(
        "--template-url",
        default="http://weissmanlab.ucsf.edu/jan/cgd/geneLink/gene_SUBSTITUTE_THIS_.htm",
        help="Template URL with _SUBSTITUTE_THIS_ placeholder",
    )
    parser.add_argument(
        "--source",
        default="Transcription Modules",
        help="Source name for the external IDs",
    )
    parser.add_argument(
        "--description",
        default="",
        help="Description for the template URL",
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

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_file, args.verbose)

    # Validate input file
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Source: {args.source}")
    logger.info(f"Template URL: {args.template_url}")
    logger.info(f"Created by: {args.created_by}")

    # Parse input file
    entries = parse_input_file(args.input_file)

    if not entries:
        logger.warning("No entries found in input file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(entries)} entries")
        for entry in entries[:5]:
            logger.info(f"  {entry}")
        if len(entries) > 5:
            logger.info(f"  ... and {len(entries) - 5} more")
        return

    try:
        with SessionLocal() as session:
            stats = load_module_info(
                session,
                entries,
                args.template_url,
                args.source,
                args.description,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Entries processed: {stats['entries_processed']}")
            logger.info(f"  External IDs created: {stats['external_ids_created']}")
            logger.info(f"  Links created: {stats['links_created']}")
            if stats["features_not_found"] > 0:
                logger.warning(
                    f"  Features not found: {stats['features_not_found']}"
                )
            if stats["duplicates_skipped"] > 0:
                logger.info(
                    f"  Duplicates skipped: {stats['duplicates_skipped']}"
                )
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading module info: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
