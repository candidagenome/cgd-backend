"""
CLI management commands for CGD.

Usage:
    python -m cgd.cli.commands reindex
"""
from __future__ import annotations

import argparse
import logging
import sys

from cgd.core.elasticsearch import get_es_client
from cgd.db.engine import SessionLocal
from cgd.api.services.es_indexer import rebuild_index

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def cmd_reindex() -> None:
    """Rebuild the Elasticsearch index from database."""
    logger.info("Starting Elasticsearch reindex...")

    es = get_es_client()
    db = SessionLocal()

    try:
        # Test ES connection
        if not es.ping():
            logger.error("Cannot connect to Elasticsearch. Is it running?")
            sys.exit(1)

        summary = rebuild_index(db, es)
        logger.info("Reindex completed successfully!")
        logger.info(f"Summary: {summary}")

    except Exception as e:
        logger.error(f"Reindex failed: {e}")
        sys.exit(1)

    finally:
        db.close()
        es.close()


def main() -> None:
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="CGD management commands",
        prog="python -m cgd.cli.commands"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # reindex command
    subparsers.add_parser(
        "reindex",
        help="Rebuild Elasticsearch index from database"
    )

    args = parser.parse_args()

    if args.command == "reindex":
        cmd_reindex()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
