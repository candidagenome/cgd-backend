#!/usr/bin/env python3
"""
Update DBXREF entries from UniProt data.

This script updates UniProt/Swiss-Prot/TrEMBL database cross-references
by downloading the latest data from UniProt and updating the DBXREF,
DBXREF_FEAT, and DBXREF_URL tables.

Original Perl: updateDbxrefFromUniProt.pl
Converted to Python: 2024
"""

import argparse
import gzip
import logging
import re
import sys
import urllib.request
from datetime import datetime
from io import StringIO
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Dbxref, DbxrefFeat, DbxrefUrl, Feature, Url

load_dotenv()

logger = logging.getLogger(__name__)

# UniProt configuration
UNIPROT_BASE_URL = "https://rest.uniprot.org/uniprotkb/stream"
UNIPROT_QUERY_TEMPLATE = "organism_id:{taxon_id}"
DEFAULT_TAXON_ID = 5476  # Candida albicans

# Source names
SWISSPROT_SOURCE = 'Swiss-Prot'
TREMBL_SOURCE = 'TrEMBL'
UNIPROT_SOURCE = 'UniProt'

# DBXREF types
PROTEIN_ID_TYPE = 'protein ID'
GENE_NAME_TYPE = 'gene name'


def setup_logging(verbose: bool = False, log_file: Path = None) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler()]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def get_feature_map(session: Session) -> dict:
    """
    Get mapping of feature_name to feature_no.

    Returns:
        Dict mapping feature_name (uppercase) to feature_no
    """
    features = session.query(Feature).all()
    return {f.feature_name.upper(): f.feature_no for f in features}


def get_existing_dbxrefs(session: Session, source: str) -> dict:
    """
    Get existing DBXREFs for a source.

    Returns:
        Dict mapping dbxref_id to dbxref_no
    """
    dbxrefs = session.query(Dbxref).filter(
        Dbxref.source == source
    ).all()
    return {d.dbxref_id: d.dbxref_no for d in dbxrefs}


def get_url_no(session: Session, source: str) -> int | None:
    """Get URL number for UniProt source."""
    url = session.query(Url).filter(
        Url.source == source
    ).first()
    return url.url_no if url else None


def fetch_uniprot_data(
    taxon_id: int,
    format_type: str = 'tsv',
) -> str:
    """
    Fetch UniProt data for an organism.

    Args:
        taxon_id: NCBI taxonomy ID
        format_type: Output format (tsv, fasta, etc.)

    Returns:
        Response text
    """
    query = UNIPROT_QUERY_TEMPLATE.format(taxon_id=taxon_id)
    url = f"{UNIPROT_BASE_URL}?query={query}&format={format_type}&fields=accession,id,gene_names,organism_name,reviewed"

    logger.info(f"Fetching UniProt data for taxon {taxon_id}")
    logger.debug(f"URL: {url}")

    try:
        with urllib.request.urlopen(url, timeout=300) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        logger.error(f"Error fetching UniProt data: {e}")
        return ""


def parse_uniprot_tsv(data: str) -> list[dict]:
    """
    Parse UniProt TSV data.

    Args:
        data: TSV data string

    Returns:
        List of record dicts with keys: accession, entry_name, gene_names, reviewed
    """
    records = []
    lines = data.strip().split('\n')

    if not lines:
        return records

    # Skip header
    for line in lines[1:]:
        parts = line.split('\t')
        if len(parts) >= 4:
            accession = parts[0].strip()
            entry_name = parts[1].strip()
            gene_names = parts[2].strip()
            reviewed = parts[4].strip() if len(parts) > 4 else ''

            # Parse gene names - first one is primary, rest are synonyms
            primary_gene = ''
            gene_list = []
            if gene_names:
                # Gene names format: "Primary {Synonym1} {Synonym2}"
                match = re.match(r'^(\S+)', gene_names)
                if match:
                    primary_gene = match.group(1)
                gene_list = re.findall(r'\S+', gene_names)

            is_swissprot = reviewed.lower() == 'reviewed'

            records.append({
                'accession': accession,
                'entry_name': entry_name,
                'primary_gene': primary_gene,
                'gene_names': gene_list,
                'is_swissprot': is_swissprot,
            })

    return records


def create_or_update_dbxref(
    session: Session,
    dbxref_id: str,
    source: str,
    dbxref_type: str,
    description: str,
    created_by: str,
    existing_dbxrefs: dict,
) -> tuple[int, bool]:
    """
    Create or update a DBXREF entry.

    Returns:
        Tuple of (dbxref_no, is_new)
    """
    dbxref_no = existing_dbxrefs.get(dbxref_id)

    if dbxref_no:
        # Update existing
        dbxref = session.query(Dbxref).filter(
            Dbxref.dbxref_no == dbxref_no
        ).first()
        if dbxref and dbxref.description != description:
            dbxref.description = description
        return dbxref_no, False

    # Create new
    dbxref = Dbxref(
        dbxref_id=dbxref_id,
        source=source,
        dbxref_type=dbxref_type,
        description=description[:240] if description else None,
        created_by=created_by,
    )
    session.add(dbxref)
    session.flush()

    existing_dbxrefs[dbxref_id] = dbxref.dbxref_no
    return dbxref.dbxref_no, True


def link_dbxref_to_feature(
    session: Session,
    dbxref_no: int,
    feature_no: int,
) -> bool:
    """
    Link DBXREF to feature.

    Returns:
        True if created, False if already exists
    """
    existing = session.query(DbxrefFeat).filter(
        and_(
            DbxrefFeat.dbxref_no == dbxref_no,
            DbxrefFeat.feature_no == feature_no,
        )
    ).first()

    if existing:
        return False

    link = DbxrefFeat(
        dbxref_no=dbxref_no,
        feature_no=feature_no,
    )
    session.add(link)
    session.flush()

    return True


def link_dbxref_to_url(
    session: Session,
    dbxref_no: int,
    url_no: int,
) -> bool:
    """
    Link DBXREF to URL.

    Returns:
        True if created, False if already exists
    """
    existing = session.query(DbxrefUrl).filter(
        and_(
            DbxrefUrl.dbxref_no == dbxref_no,
            DbxrefUrl.url_no == url_no,
        )
    ).first()

    if existing:
        return False

    link = DbxrefUrl(
        dbxref_no=dbxref_no,
        url_no=url_no,
    )
    session.add(link)
    session.flush()

    return True


def update_uniprot_dbxrefs(
    session: Session,
    taxon_id: int,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Update UniProt DBXREF entries.

    Args:
        session: Database session
        taxon_id: NCBI taxonomy ID
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'records_fetched': 0,
        'swissprot_created': 0,
        'swissprot_updated': 0,
        'trembl_created': 0,
        'trembl_updated': 0,
        'feat_links_created': 0,
        'url_links_created': 0,
        'features_not_found': 0,
        'errors': 0,
    }

    # Get feature mapping
    feature_map = get_feature_map(session)
    logger.info(f"Loaded {len(feature_map)} features")

    # Get existing DBXREFs
    swissprot_dbxrefs = get_existing_dbxrefs(session, SWISSPROT_SOURCE)
    trembl_dbxrefs = get_existing_dbxrefs(session, TREMBL_SOURCE)
    logger.info(f"Existing Swiss-Prot DBXREFs: {len(swissprot_dbxrefs)}")
    logger.info(f"Existing TrEMBL DBXREFs: {len(trembl_dbxrefs)}")

    # Get URL numbers
    swissprot_url_no = get_url_no(session, SWISSPROT_SOURCE)
    trembl_url_no = get_url_no(session, TREMBL_SOURCE)

    # Fetch UniProt data
    data = fetch_uniprot_data(taxon_id)
    if not data:
        logger.error("No data fetched from UniProt")
        return stats

    # Parse records
    records = parse_uniprot_tsv(data)
    stats['records_fetched'] = len(records)
    logger.info(f"Parsed {len(records)} UniProt records")

    # Process records
    for record in records:
        accession = record['accession']
        primary_gene = record['primary_gene'].upper()
        is_swissprot = record['is_swissprot']

        # Determine source
        source = SWISSPROT_SOURCE if is_swissprot else TREMBL_SOURCE
        existing_dbxrefs = swissprot_dbxrefs if is_swissprot else trembl_dbxrefs
        url_no = swissprot_url_no if is_swissprot else trembl_url_no

        # Get feature_no for primary gene
        feat_no = feature_map.get(primary_gene)
        if not feat_no:
            # Try alternate gene names
            for gene_name in record['gene_names']:
                feat_no = feature_map.get(gene_name.upper())
                if feat_no:
                    break

        if not feat_no:
            stats['features_not_found'] += 1
            logger.debug(f"Feature not found for {accession}: {primary_gene}")
            continue

        try:
            # Create or update DBXREF
            description = record['entry_name']
            dbxref_no, is_new = create_or_update_dbxref(
                session,
                accession,
                source,
                PROTEIN_ID_TYPE,
                description,
                created_by,
                existing_dbxrefs,
            )

            if is_swissprot:
                if is_new:
                    stats['swissprot_created'] += 1
                else:
                    stats['swissprot_updated'] += 1
            else:
                if is_new:
                    stats['trembl_created'] += 1
                else:
                    stats['trembl_updated'] += 1

            # Link to feature
            if link_dbxref_to_feature(session, dbxref_no, feat_no):
                stats['feat_links_created'] += 1

            # Link to URL
            if url_no and link_dbxref_to_url(session, dbxref_no, url_no):
                stats['url_links_created'] += 1

        except Exception as e:
            logger.error(f"Error processing {accession}: {e}")
            stats['errors'] += 1

        # Flush periodically
        if (stats['swissprot_created'] + stats['trembl_created']) % 100 == 0:
            session.flush()

    session.flush()
    return stats


def load_from_file(
    session: Session,
    data_file: Path,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Update UniProt DBXREFs from a local file.

    Args:
        session: Database session
        data_file: UniProt TSV file
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'records_fetched': 0,
        'swissprot_created': 0,
        'swissprot_updated': 0,
        'trembl_created': 0,
        'trembl_updated': 0,
        'feat_links_created': 0,
        'url_links_created': 0,
        'features_not_found': 0,
        'errors': 0,
    }

    # Get feature mapping
    feature_map = get_feature_map(session)
    logger.info(f"Loaded {len(feature_map)} features")

    # Get existing DBXREFs
    swissprot_dbxrefs = get_existing_dbxrefs(session, SWISSPROT_SOURCE)
    trembl_dbxrefs = get_existing_dbxrefs(session, TREMBL_SOURCE)

    # Get URL numbers
    swissprot_url_no = get_url_no(session, SWISSPROT_SOURCE)
    trembl_url_no = get_url_no(session, TREMBL_SOURCE)

    # Read file
    if data_file.suffix == '.gz':
        with gzip.open(data_file, 'rt') as f:
            data = f.read()
    else:
        with open(data_file) as f:
            data = f.read()

    # Parse and process
    records = parse_uniprot_tsv(data)
    stats['records_fetched'] = len(records)

    for record in records:
        accession = record['accession']
        primary_gene = record['primary_gene'].upper()
        is_swissprot = record['is_swissprot']

        source = SWISSPROT_SOURCE if is_swissprot else TREMBL_SOURCE
        existing_dbxrefs = swissprot_dbxrefs if is_swissprot else trembl_dbxrefs
        url_no = swissprot_url_no if is_swissprot else trembl_url_no

        feat_no = feature_map.get(primary_gene)
        if not feat_no:
            for gene_name in record['gene_names']:
                feat_no = feature_map.get(gene_name.upper())
                if feat_no:
                    break

        if not feat_no:
            stats['features_not_found'] += 1
            continue

        try:
            description = record['entry_name']
            dbxref_no, is_new = create_or_update_dbxref(
                session,
                accession,
                source,
                PROTEIN_ID_TYPE,
                description,
                created_by,
                existing_dbxrefs,
            )

            if is_swissprot:
                if is_new:
                    stats['swissprot_created'] += 1
                else:
                    stats['swissprot_updated'] += 1
            else:
                if is_new:
                    stats['trembl_created'] += 1
                else:
                    stats['trembl_updated'] += 1

            if link_dbxref_to_feature(session, dbxref_no, feat_no):
                stats['feat_links_created'] += 1

            if url_no and link_dbxref_to_url(session, dbxref_no, url_no):
                stats['url_links_created'] += 1

        except Exception as e:
            logger.error(f"Error processing {accession}: {e}")
            stats['errors'] += 1

        if (stats['swissprot_created'] + stats['trembl_created']) % 100 == 0:
            session.flush()

    session.flush()
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update UniProt DBXREF entries in database"
    )
    parser.add_argument(
        "--taxon-id",
        type=int,
        default=DEFAULT_TAXON_ID,
        help=f"NCBI taxonomy ID (default: {DEFAULT_TAXON_ID})",
    )
    parser.add_argument(
        "--data-file",
        type=Path,
        help="Local UniProt TSV file (instead of fetching from web)",
    )
    parser.add_argument(
        "--created-by",
        default="SCRIPT",
        help="User name for audit (default: SCRIPT)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Log file path",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without modifying database",
    )

    args = parser.parse_args()

    setup_logging(args.verbose, args.log_file)

    logger.info(f"Started at {datetime.now()}")

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            if args.data_file:
                if not args.data_file.exists():
                    logger.error(f"Data file not found: {args.data_file}")
                    sys.exit(1)
                stats = load_from_file(
                    session,
                    args.data_file,
                    args.created_by,
                    args.dry_run,
                )
            else:
                stats = update_uniprot_dbxrefs(
                    session,
                    args.taxon_id,
                    args.created_by,
                    args.dry_run,
                )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Records fetched: {stats['records_fetched']}")
            logger.info(f"  Swiss-Prot created: {stats['swissprot_created']}")
            logger.info(f"  Swiss-Prot updated: {stats['swissprot_updated']}")
            logger.info(f"  TrEMBL created: {stats['trembl_created']}")
            logger.info(f"  TrEMBL updated: {stats['trembl_updated']}")
            logger.info(f"  Feature links created: {stats['feat_links_created']}")
            logger.info(f"  URL links created: {stats['url_links_created']}")
            logger.info(f"  Features not found: {stats['features_not_found']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
