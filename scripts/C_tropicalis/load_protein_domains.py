#!/usr/bin/env python3
"""
Load C. tropicalis protein domain data from InterProScan results.

This script loads protein domains from InterProScan TSV output into the
protein_detail table, which is displayed on the Protein tab.

Usage:
    python load_protein_domains.py --tsv FILE [--dry-run]

Data files needed:
    - InterProScan all_results.tsv file

InterProScan TSV columns:
    0: Protein accession (e.g., EER30087.1)
    1: Sequence MD5 digest
    2: Sequence length
    3: Analysis (e.g., Pfam, SMART, Gene3D)
    4: Signature accession (e.g., PF00001)
    5: Signature description
    6: Start location
    7: Stop location
    8: E-value / Score
    9: Status (T=true match)
    10: Date
    11: InterPro accession (e.g., IPR000001)
    12: InterPro description
    13: GO annotations (pipe-separated)
    14: Pathways (pipe-separated)

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

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

# Domain databases to load
DOMAIN_DATABASES = {"Pfam", "SMART", "Gene3D", "CDD", "SUPERFAMILY", "PRINTS", "ProSiteProfiles"}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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


def parse_gff_protein_mapping(gff_file: Path) -> Dict[str, str]:
    """Parse GFF file to get protein_id -> gene_id mapping.

    The InterProScan results use protein_id (e.g., EER30082.1) but the
    feature_name in the database is gene_id (e.g., CTRG_01181).
    """
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

            # Extract protein_id from CDS features
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


def parse_iprscan_tsv(tsv_file: Path) -> List[Dict]:
    """Parse InterProScan TSV output."""
    domains = []

    with open(tsv_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            fields = line.split('\t')
            if len(fields) < 11:
                continue

            protein_id = fields[0]
            analysis = fields[3]
            sig_accession = fields[4]
            sig_description = fields[5] if len(fields) > 5 else ''
            start = int(fields[6]) if fields[6].isdigit() else 0
            end = int(fields[7]) if fields[7].isdigit() else 0
            evalue = fields[8] if len(fields) > 8 else ''
            status = fields[9] if len(fields) > 9 else ''
            ipr_accession = fields[11] if len(fields) > 11 else ''
            ipr_description = fields[12] if len(fields) > 12 else ''

            # Only include true matches from selected databases
            if status != 'T':
                continue

            if analysis not in DOMAIN_DATABASES:
                continue

            domains.append({
                'protein_id': protein_id,
                'analysis': analysis,
                'sig_accession': sig_accession,
                'sig_description': sig_description,
                'start': start,
                'end': end,
                'evalue': evalue,
                'ipr_accession': ipr_accession,
                'ipr_description': ipr_description,
            })

    return domains


def get_protein_info_no(session, protein_id: str) -> Optional[int]:
    """Get protein_info_no by protein ID (feature_name)."""
    query = text(f"""
        SELECT pi.protein_info_no
        FROM {DB_SCHEMA}.protein_info pi
        JOIN {DB_SCHEMA}.feature f ON pi.feature_no = f.feature_no
        WHERE f.feature_name = :protein_id
    """)
    result = session.execute(query, {"protein_id": protein_id}).first()
    return result[0] if result else None


def create_protein_info(session, feature_no: int, dry_run: bool = False) -> Optional[int]:
    """Create protein_info record if it doesn't exist."""
    # Check if exists
    query = text(f"""
        SELECT protein_info_no
        FROM {DB_SCHEMA}.protein_info
        WHERE feature_no = :feature_no
    """)
    result = session.execute(query, {"feature_no": feature_no}).first()
    if result:
        return result[0]

    if dry_run:
        return None

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.protein_info (
            feature_no, created_by
        ) VALUES (
            :feature_no, :created_by
        )
    """)
    session.execute(insert, {
        "feature_no": feature_no,
        "created_by": ADMIN_USER,
    })

    result = session.execute(query, {"feature_no": feature_no}).first()
    return result[0] if result else None


def get_feature_no_by_protein_id(
    session,
    protein_id: str,
    protein_to_gene: Optional[Dict[str, str]] = None
) -> Optional[int]:
    """Get feature_no by protein ID.

    If protein_to_gene mapping is provided, use it to look up by gene_id.
    Otherwise, fall back to looking up by protein_id directly.
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

    # Fall back to protein_id lookup (for backward compatibility)
    query = text(f"""
        SELECT feature_no
        FROM {DB_SCHEMA}.feature
        WHERE feature_name = :protein_id
    """)
    result = session.execute(query, {"protein_id": protein_id}).first()
    return result[0] if result else None


def create_protein_detail(
    session,
    protein_info_no: int,
    domain: Dict,
    dry_run: bool = False
) -> bool:
    """Create protein_detail record for a domain."""
    # Use the signature description or InterPro description
    detail_value = domain['sig_description'] or domain['ipr_description'] or domain['sig_accession']
    # Truncate to 240 chars max
    if len(detail_value) > 240:
        detail_value = detail_value[:237] + "..."

    # Check if exists (unique constraint is on protein_info_no, type, value, start, stop)
    query = text(f"""
        SELECT protein_detail_no
        FROM {DB_SCHEMA}.protein_detail
        WHERE protein_info_no = :protein_info_no
        AND protein_detail_type = :type
        AND protein_detail_value = :value
        AND start_coord = :start
        AND stop_coord = :stop
    """)
    result = session.execute(query, {
        "protein_info_no": protein_info_no,
        "type": "domain",
        "value": detail_value,
        "start": domain['start'],
        "stop": domain['end'],
    }).first()

    if result:
        return False

    if dry_run:
        return True

    insert = text(f"""
        INSERT INTO {DB_SCHEMA}.protein_detail (
            protein_info_no, protein_detail_group, protein_detail_type,
            protein_detail_value, start_coord, stop_coord,
            interpro_dbxref_id, member_dbxref_id, created_by
        ) VALUES (
            :protein_info_no, :group, :type,
            :value, :start, :stop,
            :interpro_id, :member_id, :created_by
        )
    """)
    session.execute(insert, {
        "protein_info_no": protein_info_no,
        "group": domain['analysis'],
        "type": "domain",
        "value": detail_value,
        "start": domain['start'],
        "stop": domain['end'],
        "interpro_id": domain['ipr_accession'] if domain['ipr_accession'] else None,
        "member_id": domain['sig_accession'],
        "created_by": ADMIN_USER,
    })
    return True


def load_protein_domains(
    session,
    tsv_file: Path,
    gff_file: Optional[Path] = None,
    dry_run: bool = False
):
    """Load protein domains from InterProScan TSV into protein_detail table.

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

    # Ensure code values exist for domain databases
    logger.info("Ensuring code values exist for domain databases...")
    for db_name in DOMAIN_DATABASES:
        ensure_code(
            session,
            "PROTEIN_DETAIL",
            "PROTEIN_DETAIL_GROUP",
            db_name,
            f"{db_name} protein domain database",
            dry_run
        )

    logger.info(f"Parsing InterProScan TSV: {tsv_file}")
    domains = parse_iprscan_tsv(tsv_file)
    logger.info(f"Found {len(domains)} domain annotations")

    # Group by protein
    protein_domains: Dict[str, List[Dict]] = {}
    for domain in domains:
        pid = domain['protein_id']
        if pid not in protein_domains:
            protein_domains[pid] = []
        protein_domains[pid].append(domain)

    logger.info(f"Domains for {len(protein_domains)} proteins")

    domains_loaded = 0
    proteins_found = 0
    proteins_not_found = 0
    protein_info_created = 0

    for protein_id, domain_list in protein_domains.items():
        # Get or create protein_info
        protein_info_no = get_protein_info_no(session, protein_id)

        if not protein_info_no:
            # Need to create protein_info first
            feature_no = get_feature_no_by_protein_id(session, protein_id, protein_to_gene)
            if not feature_no:
                proteins_not_found += 1
                continue

            protein_info_no = create_protein_info(session, feature_no, dry_run)
            if protein_info_no:
                protein_info_created += 1

        if not protein_info_no:
            proteins_not_found += 1
            continue

        proteins_found += 1

        for domain in domain_list:
            created = create_protein_detail(
                session,
                protein_info_no,
                domain,
                dry_run
            )
            if created:
                domains_loaded += 1

        if proteins_found % 500 == 0:
            logger.info(f"Processed {proteins_found} proteins...")
            if not dry_run:
                session.commit()

    if not dry_run:
        session.commit()

    logger.info("=" * 60)
    logger.info(f"Proteins found in database: {proteins_found}")
    logger.info(f"Proteins not found: {proteins_not_found}")
    logger.info(f"Protein info records created: {protein_info_created}")
    logger.info(f"Domain annotations loaded: {domains_loaded}")


def main():
    parser = argparse.ArgumentParser(description="Load C. tropicalis protein domains")
    parser.add_argument("--tsv", required=True, type=Path, help="InterProScan TSV results file")
    parser.add_argument("--gff", type=Path, help="GFF file for protein_id to gene_id mapping")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Loading C. tropicalis protein domains from InterProScan")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[DRY RUN MODE]")

    with SessionLocal() as session:
        load_protein_domains(session, args.tsv, args.gff, args.dry_run)

    logger.info("Done!")


if __name__ == "__main__":
    main()
