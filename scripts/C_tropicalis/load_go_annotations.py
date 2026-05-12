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


def parse_gff_protein_mapping(gff_file: Path) -> Dict[str, str]:
    """Parse GFF file to get protein_id -> gene_id mapping."""
    protein_to_gene = {}

    with open(gff_file, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            line = line.strip()
            if not line:
                continue

            fields = line.split('\t')
            if len(fields) < 9:
                continue

            feature_type = fields[2]
            attributes = fields[8]

            if feature_type == 'CDS':
                attr_dict = {}
                for attr in attributes.split(';'):
                    if '=' in attr:
                        key, value = attr.split('=', 1)
                        attr_dict[key.strip()] = value.strip()

                protein_id = attr_dict.get('protein_id', '')
                gene_id = attr_dict.get('gene_id', '')
                if protein_id and gene_id:
                    protein_to_gene[protein_id] = gene_id

    return protein_to_gene


def get_feature_no_by_protein_id(
    session,
    protein_id: str,
    protein_to_gene: Optional[Dict[str, str]] = None
) -> Optional[int]:
    """Get feature_no by protein ID.

    If protein_to_gene mapping is provided, use it to look up by gene_id.
    """
    # Try gene_id first if mapping is available
    if protein_to_gene:
        gene_id = protein_to_gene.get(protein_id)
        if gene_id:
            query = text(f"""
                SELECT feature_no
                FROM {DB_SCHEMA}.feature
                WHERE feature_name = :gene_id
            """)
            result = session.execute(query, {"gene_id": gene_id}).first()
            if result:
                return result[0]

    # Fall back to protein_id lookup
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
            go_annotation_no, go_no, feature_no, go_evidence, annotation_type, source
        ) VALUES (
            {DB_SCHEMA}.go_annotation_seq.NEXTVAL, :go_no, :feature_no, :go_evidence, :annotation_type, :source
        )
    """)
    session.execute(insert, {
        "go_no": go_no,
        "feature_no": feature_no,
        "go_evidence": go_evidence,
        "annotation_type": annotation_type,
        "source": source,
    })
    return True


def ensure_code(session, table_name: str, col_name: str, code_value: str, description: str, dry_run: bool = False) -> bool:
    """Ensure a code value exists in the code table."""
    query = text(f"""
        SELECT code_no
        FROM {DB_SCHEMA}.code
        WHERE tab_name = :tab_name
        AND col_name = :col_name
        AND code_value = :code_value
    """)
    result = session.execute(query, {
        "tab_name": table_name,
        "col_name": col_name,
        "code_value": code_value
    }).first()

    if result:
        return True

    if dry_run:
        logger.info(f"[DRY RUN] Would create code entry for {table_name}.{col_name}: {code_value}")
        return True

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.code (
            tab_name, col_name, code_value, description, created_by
        ) VALUES (
            :tab_name, :col_name, :code_value, :description, :created_by
        )
    """)
    session.execute(insert, {
        "tab_name": table_name,
        "col_name": col_name,
        "code_value": code_value,
        "description": description,
        "created_by": ADMIN_USER,
    })
    session.commit()
    logger.info(f"Created code entry for {table_name}.{col_name}: {code_value}")
    return True


def ensure_dbuser(session, userid: str, dry_run: bool = False) -> bool:
    """Ensure the database user exists in dbuser table."""
    query = text(f"""
        SELECT dbuser_no
        FROM {DB_SCHEMA}.dbuser
        WHERE userid = :userid
    """)
    result = session.execute(query, {"userid": userid}).first()

    if result:
        return True

    if dry_run:
        logger.info(f"[DRY RUN] Would create dbuser entry for: {userid}")
        return True

    # Ensure 'current' status code exists
    ensure_code(session, "DBUSER", "STATUS", "current", "Current database user", dry_run)

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.dbuser (
            userid, first_name, last_name, status, email
        ) VALUES (
            :userid, :first_name, :last_name, :status, :email
        )
    """)
    session.execute(insert, {
        "userid": userid,
        "first_name": "Database",
        "last_name": "System",
        "status": "current",
        "email": "cgd-admin@lists.stanford.edu",
    })
    session.commit()
    logger.info(f"Created dbuser entry for: {userid}")
    return True


def load_go_annotations(
    session,
    tsv_file: Path,
    gff_file: Optional[Path] = None,
    dry_run: bool = False
):
    """Load GO annotations from InterProScan TSV.

    Args:
        session: Database session
        tsv_file: InterProScan TSV results file
        gff_file: Optional GFF file for protein_id to gene_id mapping
        dry_run: If True, don't make changes
    """
    # Parse GFF for protein_id -> gene_id mapping if provided
    protein_to_gene = {}
    if gff_file:
        logger.info(f"Parsing GFF for protein mapping: {gff_file}")
        protein_to_gene = parse_gff_protein_mapping(gff_file)
        logger.info(f"Found {len(protein_to_gene)} protein-to-gene mappings")

    # Ensure the database user exists (required by trigger)
    ensure_dbuser(session, "MULTI", dry_run)

    # Ensure required codes exist
    ensure_code(session, "GO_ANNOTATION", "GO_EVIDENCE", GO_EVIDENCE,
                "Inferred from Electronic Annotation", dry_run)
    ensure_code(session, "GO_ANNOTATION", "ANNOTATION_TYPE", ANNOTATION_TYPE,
                "Computational GO annotation", dry_run)
    ensure_code(session, "GO_ANNOTATION", "SOURCE", SOURCE,
                "InterProScan GO annotation source", dry_run)

    logger.info(f"Parsing GO terms from InterProScan TSV: {tsv_file}")
    protein_go_terms = parse_go_terms_from_iprscan(tsv_file)

    total_go_terms = sum(len(terms) for terms in protein_go_terms.values())
    logger.info(f"Found {total_go_terms} GO term assignments for {len(protein_go_terms)} proteins")

    annotations_loaded = 0
    proteins_found = 0
    proteins_not_found = 0
    go_terms_not_found = 0

    for protein_id, go_ids in protein_go_terms.items():
        feature_no = get_feature_no_by_protein_id(session, protein_id, protein_to_gene)

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
    parser.add_argument("--gff", type=Path, help="GFF file for protein_id to gene_id mapping")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis GO annotations (IEA) from InterProScan")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        load_go_annotations(session, args.tsv, args.gff, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
