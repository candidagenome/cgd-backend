#!/usr/bin/env python3
"""
Load C. tropicalis GO annotations from InterProScan results.

This script loads GO annotations (IEA evidence only) from InterProScan output.

Usage:
    python load_go_annotations.py --tsv FILE [--dry-run]

Data files needed:
    - InterProScan all_results.tsv file with GO annotations

GO annotations from InterProScan have evidence code IEA (Inferred from Electronic Annotation).

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
from sqlalchemy import text

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from cgd.db.engine import SessionLocal

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "MULTI")
ADMIN_USER = os.getenv("ADMIN_USER", "cgdadmin").upper()
SOURCE = "InterProScan"
GO_EVIDENCE = "IEA"  # Inferred from Electronic Annotation
ANNOTATION_TYPE = "computational"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_go_terms_from_iprscan(tsv_file: Path) -> Dict[str, Set[str]]:
    """
    Parse InterProScan TSV and extract GO terms per protein.

    Returns dict of {protein_id: set of GO IDs}
    """
    protein_go_terms: Dict[str, Set[str]] = {}

    with open(tsv_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            fields = line.split('\t')
            if len(fields) < 14:
                continue

            protein_id = fields[0]
            go_annotations = fields[13] if len(fields) > 13 else ''

            if not go_annotations or go_annotations == '-':
                continue

            # Parse GO terms (format: GO:0005515|GO:0006468)
            # Or format: GO:0005515(InterPro)|GO:0006468(InterPro)
            go_pattern = re.compile(r'GO:\d{7}')
            go_ids = go_pattern.findall(go_annotations)

            if go_ids:
                if protein_id not in protein_go_terms:
                    protein_go_terms[protein_id] = set()
                protein_go_terms[protein_id].update(go_ids)

    return protein_go_terms


def get_feature_no_by_protein_id(session, protein_id: str) -> Optional[int]:
    """Get feature_no by protein ID."""
    query = text(f"""
        SELECT feature_no
        FROM {DB_SCHEMA}.feature
        WHERE feature_name = :protein_id
    """)
    result = session.execute(query, {"protein_id": protein_id}).first()
    if result:
        return result[0]

    query = text(f"""
        SELECT feature_no
        FROM {DB_SCHEMA}.feature
        WHERE dbxref_id LIKE :pattern
    """)
    result = session.execute(query, {"pattern": f"%{protein_id}%"}).first()
    return result[0] if result else None


def get_go_no_by_goid(session, goid: str) -> Optional[int]:
    """Get go_no by GO ID (e.g., GO:0005515)."""
    # Extract numeric part from GO:NNNNNNN format
    goid_num = int(goid.replace("GO:", ""))

    query = text(f"""
        SELECT go_no
        FROM {DB_SCHEMA}.go
        WHERE goid = :goid
    """)
    result = session.execute(query, {"goid": goid_num}).first()
    return result[0] if result else None


def create_go_annotation(
    session,
    go_no: int,
    feature_no: int,
    go_evidence: str,
    annotation_type: str,
    source: str,
    dry_run: bool = False
) -> bool:
    """Create GO annotation entry."""
    # Check if exists
    query = text(f"""
        SELECT go_annotation_no
        FROM {DB_SCHEMA}.go_annotation
        WHERE go_no = :go_no
        AND feature_no = :feature_no
        AND go_evidence = :go_evidence
        AND annotation_type = :annotation_type
        AND source = :source
    """)
    result = session.execute(query, {
        "go_no": go_no,
        "feature_no": feature_no,
        "go_evidence": go_evidence,
        "annotation_type": annotation_type,
        "source": source,
    }).first()

    if result:
        return False

    if dry_run:
        return True

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.go_annotation (
            go_no, feature_no, go_evidence, annotation_type, source, created_by
        ) VALUES (
            :go_no, :feature_no, :go_evidence, :annotation_type, :source, :created_by
        )
    """)
    session.execute(insert, {
        "go_no": go_no,
        "feature_no": feature_no,
        "go_evidence": go_evidence,
        "annotation_type": annotation_type,
        "source": source,
        "created_by": ADMIN_USER,
    })
    return True


def load_go_annotations(session, tsv_file: Path, dry_run: bool = False):
    """Load GO annotations from InterProScan TSV."""
    logger.info(f"Parsing GO terms from InterProScan TSV: {tsv_file}")
    protein_go_terms = parse_go_terms_from_iprscan(tsv_file)

    total_go_terms = sum(len(terms) for terms in protein_go_terms.values())
    logger.info(f"Found {total_go_terms} GO term assignments for {len(protein_go_terms)} proteins")

    annotations_loaded = 0
    proteins_found = 0
    proteins_not_found = 0
    go_terms_not_found = 0

    for protein_id, go_ids in protein_go_terms.items():
        feature_no = get_feature_no_by_protein_id(session, protein_id)

        if not feature_no:
            proteins_not_found += 1
            continue

        proteins_found += 1

        for goid in go_ids:
            go_no = get_go_no_by_goid(session, goid)

            if not go_no:
                go_terms_not_found += 1
                continue

            created = create_go_annotation(
                session,
                go_no=go_no,
                feature_no=feature_no,
                go_evidence=GO_EVIDENCE,
                annotation_type=ANNOTATION_TYPE,
                source=SOURCE,
                dry_run=dry_run
            )
            if created:
                annotations_loaded += 1

        if proteins_found % 500 == 0:
            logger.info(f"Processed {proteins_found} proteins...")
            if not dry_run:
                session.commit()

    if not dry_run:
        session.commit()

    logger.info("=" * 60)
    logger.info(f"Proteins found in database: {proteins_found}")
    logger.info(f"Proteins not found: {proteins_not_found}")
    logger.info(f"GO annotations loaded: {annotations_loaded}")
    logger.info(f"GO terms not found in GO table: {go_terms_not_found}")


def main():
    parser = argparse.ArgumentParser(description="Load C. tropicalis GO annotations")
    parser.add_argument("--tsv", required=True, type=Path, help="InterProScan TSV results file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis GO annotations (IEA) from InterProScan")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        load_go_annotations(session, args.tsv, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
