#!/usr/bin/env python3
"""
Load GO Slim terms into the GO_SET table.

This script reads a GO Slim OBO file and loads the GO terms into the
GO_SET table, associating them with a named GO set (e.g., "Candida GO-Slim").

Original Perl: load_GoSlimTerms.pl
Author: CGD Team
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import delete, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Go, GoSet

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


def parse_go_slim_file(filepath: Path) -> list[int]:
    """
    Parse a GO Slim OBO file and extract GO IDs.

    Args:
        filepath: Path to the GO Slim OBO file

    Returns:
        List of GO IDs (as integers, without leading zeros)
    """
    go_ids = []
    pattern = re.compile(r"^id: GO:(\d+)")

    logger.info(f"Parsing GO Slim file: {filepath}")

    with open(filepath) as f:
        for line in f:
            match = pattern.match(line)
            if match:
                goid_str = match.group(1)
                # Remove leading zeros
                goid = int(goid_str)
                go_ids.append(goid)
                logger.debug(f"Found GO ID: {goid}")

    logger.info(f"Found {len(go_ids)} GO IDs in file")
    return go_ids


def delete_existing_go_set(session: Session, go_set_name: str) -> int:
    """
    Delete existing entries for a GO set.

    Args:
        session: Database session
        go_set_name: Name of the GO set to delete

    Returns:
        Number of rows deleted
    """
    result = session.execute(
        delete(GoSet).where(GoSet.go_set_name == go_set_name)
    )
    count = result.rowcount
    logger.info(f"Deleted {count} existing rows from GO_SET for '{go_set_name}'")
    return count


def get_go_no_for_goid(session: Session, goid: int) -> int | None:
    """
    Look up go_no for a given GO ID.

    Args:
        session: Database session
        goid: GO ID (integer)

    Returns:
        go_no or None if not found
    """
    go = session.query(Go).filter(Go.goid == goid).first()
    return go.go_no if go else None


def insert_go_set_entry(
    session: Session,
    go_no: int,
    go_set_name: str,
    created_by: str,
) -> None:
    """
    Insert a new GO_SET entry.

    Args:
        session: Database session
        go_no: GO number (foreign key to GO table)
        go_set_name: Name of the GO set
        created_by: User creating the entry
    """
    go_set = GoSet(
        go_no=go_no,
        go_set_name=go_set_name,
        created_by=created_by[:12],  # Truncate to 12 chars
    )
    session.add(go_set)


def load_go_slim_terms(
    session: Session,
    go_slim_file: Path,
    go_set_name: str,
    created_by: str,
) -> dict:
    """
    Load GO Slim terms into the database.

    Args:
        session: Database session
        go_slim_file: Path to GO Slim OBO file
        go_set_name: Name for the GO set
        created_by: User performing the load

    Returns:
        Dictionary with statistics
    """
    stats = {
        "go_ids_in_file": 0,
        "deleted": 0,
        "inserted": 0,
        "not_found": 0,
    }

    # Parse GO IDs from file
    go_ids = parse_go_slim_file(go_slim_file)
    stats["go_ids_in_file"] = len(go_ids)

    # Delete existing entries
    stats["deleted"] = delete_existing_go_set(session, go_set_name)

    # Insert new entries
    for goid in go_ids:
        go_no = get_go_no_for_goid(session, goid)

        if go_no is None:
            logger.error(f"Could not find go_no for GO ID: {goid}")
            stats["not_found"] += 1
            raise ValueError(f"Could not find go_no for GO ID: {goid}")

        logger.debug(f"Processing goid={goid}, go_no={go_no}")
        insert_go_set_entry(session, go_no, go_set_name, created_by)
        stats["inserted"] += 1
        logger.debug(f"Inserted GO:{goid:07d} into GO_SET table")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load GO Slim terms into the GO_SET table"
    )
    parser.add_argument(
        "created_by",
        help="Database user name for created_by field",
    )
    parser.add_argument(
        "--go-slim-file",
        type=Path,
        help="Path to GO Slim OBO file (default: from config)",
    )
    parser.add_argument(
        "--go-set-name",
        default=None,
        help="Name for the GO set (default: '<genus> GO-Slim')",
    )
    parser.add_argument(
        "--genus",
        default="Candida",
        help="Genus name for default GO set name (default: Candida)",
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

    # Determine GO set name
    go_set_name = args.go_set_name or f"{args.genus} GO-Slim"

    # Determine GO Slim file path
    go_slim_file = args.go_slim_file
    if not go_slim_file:
        # Try to get from environment or use default
        html_root = os.getenv("HTML_ROOT_DIR", "/www/candidagenome/html")
        go_slim_filename = os.getenv("GO_SLIM_FILENAME", "candida.goslim_generic.obo")
        go_slim_file = Path(html_root) / go_slim_filename

    if not go_slim_file.exists():
        logger.error(f"GO Slim file not found: {go_slim_file}")
        sys.exit(1)

    logger.info(f"GO Slim file: {go_slim_file}")
    logger.info(f"GO set name: {go_set_name}")
    logger.info(f"Created by: {args.created_by}")

    if args.dry_run:
        logger.info("DRY RUN - parsing file only")
        go_ids = parse_go_slim_file(go_slim_file)
        logger.info(f"Would load {len(go_ids)} GO terms into set '{go_set_name}'")
        return

    try:
        with SessionLocal() as session:
            stats = load_go_slim_terms(
                session,
                go_slim_file,
                go_set_name,
                args.created_by,
            )

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  GO IDs in file: {stats['go_ids_in_file']}")
            logger.info(f"  Deleted: {stats['deleted']}")
            logger.info(f"  Inserted: {stats['inserted']}")
            if stats["not_found"] > 0:
                logger.warning(f"  Not found: {stats['not_found']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading GO Slim terms: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
