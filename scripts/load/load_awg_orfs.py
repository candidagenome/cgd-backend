#!/usr/bin/env python3
"""
Load Annotation Working Group (AWG) ORFs into the database.

This script reads AWG-annotated ORF sequences from a FASTA file and:
1. Finds features that don't exist in the database
2. Updates feature names if a locus exists with a gene name
3. Creates new features for ORFs not found

The AWG ORFs may have introns added or extensions from the annotation
working group's curation.

Input: FASTA file with AWG-annotated sequences
Header format: >orf19.XXX; Contig; gene_name; description

Original Perl: loadAWG_ORFs.pl
Converted to Python: 2024
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Alias, FeatAlias, Feature

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
    Find feature by feature_name.

    Args:
        session: Database session
        name: Feature name to search

    Returns:
        Feature object or None
    """
    return session.query(Feature).filter(
        Feature.feature_name == name
    ).first()


def find_feature_by_alias(session: Session, alias_name: str) -> Feature | None:
    """
    Find feature by alias name.

    Args:
        session: Database session
        alias_name: Alias to search

    Returns:
        Feature object or None
    """
    feat_alias = session.query(FeatAlias).join(
        Alias, FeatAlias.alias_no == Alias.alias_no
    ).filter(
        Alias.alias_name == alias_name
    ).first()

    if feat_alias:
        return session.query(Feature).filter(
            Feature.feature_no == feat_alias.feature_no
        ).first()

    return None


def find_feature_by_gene_name(session: Session, gene_name: str) -> Feature | None:
    """
    Find feature by gene_name (standard name).

    Args:
        session: Database session
        gene_name: Gene name to search

    Returns:
        Feature object or None
    """
    return session.query(Feature).filter(
        Feature.gene_name == gene_name.upper()
    ).first()


def parse_fasta_file(filepath: Path) -> list[dict]:
    """
    Parse FASTA file with AWG ORF sequences.

    Args:
        filepath: Path to FASTA file

    Returns:
        List of dictionaries with id, description, gene_name, sequence
    """
    try:
        from Bio import SeqIO
    except ImportError:
        logger.error("BioPython is required for this script.")
        logger.error("Install with: pip install biopython")
        sys.exit(1)

    entries = []

    for record in SeqIO.parse(filepath, "fasta"):
        # Clean up ID (remove trailing semicolon)
        orf_id = record.id.rstrip(";")

        # Parse description to get gene name
        # Format: Contig; gene_name; description
        desc_parts = record.description.split("; ")
        gene_name = None

        if len(desc_parts) >= 3:
            gene_name = desc_parts[2].strip()
            # Remove asterisks and whitespace
            gene_name = re.sub(r"\*", "", gene_name)
            gene_name = gene_name.strip()
            if not gene_name:
                gene_name = None

        entries.append({
            "id": orf_id,
            "description": record.description,
            "gene_name": gene_name,
            "sequence": str(record.seq),
        })

    logger.info(f"Parsed {len(entries)} sequences from FASTA file")
    return entries


def create_feature(
    session: Session,
    feature_name: str,
    brief_id: str,
    created_by: str,
) -> int:
    """
    Create a new feature entry.

    Args:
        session: Database session
        feature_name: Feature name
        brief_id: Brief description
        created_by: User creating the record

    Returns:
        feature_no of created feature
    """
    new_feature = Feature(
        feature_name=feature_name,
        is_on_pmap="N",
        created_by=created_by[:12],
        brief_id=brief_id[:100] if brief_id else None,
    )
    session.add(new_feature)
    session.flush()

    return new_feature.feature_no


def load_awg_orfs(
    session: Session,
    entries: list[dict],
    created_by: str,
) -> dict:
    """
    Load AWG ORFs into the database.

    Args:
        session: Database session
        entries: List of ORF entry dictionaries
        created_by: User creating the records

    Returns:
        Dictionary with statistics
    """
    stats = {
        "entries_processed": 0,
        "features_found": 0,
        "features_updated": 0,
        "features_created": 0,
        "not_found": [],
    }

    for entry in entries:
        orf_id = entry["id"]
        gene_name = entry["gene_name"]
        description = entry["description"]

        stats["entries_processed"] += 1

        # Try to find feature by name
        feature = find_feature_by_name(session, orf_id)

        if feature and feature.gene_name:
            # Feature exists with standard name - skip
            stats["features_found"] += 1
            continue

        # Try by alias
        if not feature:
            feature = find_feature_by_alias(session, orf_id)

        if feature and feature.gene_name:
            stats["features_found"] += 1
            continue

        # Try by gene name if available
        if gene_name and not feature:
            gene_feature = find_feature_by_gene_name(session, gene_name)

            if gene_feature:
                # Found by gene name - update feature_name
                if gene_feature.feature_name == gene_feature.gene_name:
                    logger.info(f"{orf_id} -> {gene_name} (updating feature_name)")
                    gene_feature.feature_name = orf_id
                    stats["features_updated"] += 1
                    continue

        # Feature not found - create new one
        if not feature:
            logger.info(f"Creating new feature: {orf_id}")
            create_feature(
                session,
                orf_id,
                "ORF Predicted by Annotation Working Group",
                created_by,
            )
            stats["features_created"] += 1
            stats["not_found"].append({
                "id": orf_id,
                "description": description,
            })

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load Annotation Working Group ORFs from FASTA file"
    )
    parser.add_argument(
        "fasta_file",
        type=Path,
        help="Input FASTA file with AWG-annotated sequences",
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
    if not args.fasta_file.exists():
        logger.error(f"FASTA file not found: {args.fasta_file}")
        sys.exit(1)

    logger.info(f"FASTA file: {args.fasta_file}")
    logger.info(f"Created by: {args.created_by}")

    # Parse FASTA file
    entries = parse_fasta_file(args.fasta_file)

    if not entries:
        logger.warning("No sequences found in FASTA file")
        return

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(entries)} sequences")
        return

    try:
        with SessionLocal() as session:
            stats = load_awg_orfs(session, entries, args.created_by)

            session.commit()
            logger.info("Transaction committed successfully")

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Entries processed: {stats['entries_processed']}")
            logger.info(f"  Features already existed: {stats['features_found']}")
            logger.info(f"  Features updated: {stats['features_updated']}")
            logger.info(f"  Features created: {stats['features_created']}")
            logger.info("=" * 50)

            if stats["not_found"]:
                logger.info(f"\nNew features created:")
                for nf in stats["not_found"][:10]:
                    logger.info(f"  {nf['id']}: {nf['description'][:60]}...")
                if len(stats["not_found"]) > 10:
                    logger.info(f"  ... and {len(stats['not_found']) - 10} more")

    except Exception as e:
        logger.error(f"Error loading AWG ORFs: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
