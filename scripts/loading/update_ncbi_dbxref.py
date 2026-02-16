#!/usr/bin/env python3
"""
Update NCBI DBXREF entries in database.

This script updates NCBI Gene, RefSeq, and other NCBI database
cross-references by fetching current data from NCBI and updating
the DBXREF, DBXREF_FEAT, and DBXREF_URL tables.

Original Perl: updateNCBIDbxref.pl
Converted to Python: 2024
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from Bio import Entrez
from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Dbxref, DbxrefFeat, DbxrefUrl, Feature, Url

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
DEFAULT_EMAIL = "cgd-admin@stanford.edu"
DEFAULT_TAXON_ID = 5476  # Candida albicans

# NCBI sources and types
NCBI_GENE_SOURCE = 'NCBI'
NCBI_GENE_TYPE = 'Gene ID'
REFSEQ_PROTEIN_SOURCE = 'RefSeq'
REFSEQ_PROTEIN_TYPE = 'protein version ID'
REFSEQ_DNA_SOURCE = 'RefSeq'
REFSEQ_DNA_TYPE = 'DNA version ID'


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


def get_existing_dbxrefs(session: Session, source: str, dbxref_type: str) -> dict:
    """
    Get existing DBXREFs for a source and type.

    Returns:
        Dict mapping dbxref_id to dbxref_no
    """
    dbxrefs = session.query(Dbxref).filter(
        and_(
            Dbxref.source == source,
            Dbxref.dbxref_type == dbxref_type,
        )
    ).all()
    return {d.dbxref_id: d.dbxref_no for d in dbxrefs}


def get_url_no(session: Session, source: str, url_type: str = None) -> int | None:
    """Get URL number for source."""
    query = session.query(Url).filter(Url.source == source)
    if url_type:
        query = query.filter(Url.url_type == url_type)
    url = query.first()
    return url.url_no if url else None


def fetch_ncbi_gene_data(
    taxon_id: int,
    email: str,
    batch_size: int = 500,
) -> list[dict]:
    """
    Fetch NCBI Gene data for an organism.

    Args:
        taxon_id: NCBI taxonomy ID
        email: Email for NCBI API
        batch_size: Number of records per batch

    Returns:
        List of gene records
    """
    Entrez.email = email

    # Search for genes
    logger.info(f"Searching NCBI Gene for taxon {taxon_id}")
    try:
        search_handle = Entrez.esearch(
            db="gene",
            term=f"txid{taxon_id}[Organism]",
            retmax=100000,
        )
        search_results = Entrez.read(search_handle)
        search_handle.close()

        gene_ids = search_results.get('IdList', [])
        logger.info(f"Found {len(gene_ids)} genes")

    except Exception as e:
        logger.error(f"Error searching NCBI Gene: {e}")
        return []

    # Fetch gene records in batches
    records = []
    for i in range(0, len(gene_ids), batch_size):
        batch_ids = gene_ids[i:i + batch_size]
        try:
            fetch_handle = Entrez.efetch(
                db="gene",
                id=','.join(batch_ids),
                rettype="gene_table",
                retmode="text",
            )
            # Parse gene table format
            batch_text = fetch_handle.read()
            fetch_handle.close()

            batch_records = parse_gene_table(batch_text)
            records.extend(batch_records)

            logger.debug(f"Fetched batch {i}-{i + len(batch_ids)}")
            time.sleep(0.34)  # Be nice to NCBI

        except Exception as e:
            logger.error(f"Error fetching batch {i}: {e}")

    return records


def parse_gene_table(text: str) -> list[dict]:
    """
    Parse NCBI gene table format.

    Returns:
        List of dicts with gene_id, symbol, locus_tag, description, refseq_protein, refseq_dna
    """
    records = []
    lines = text.strip().split('\n')

    for line in lines:
        if line.startswith('#') or not line.strip():
            continue

        parts = line.split('\t')
        if len(parts) < 3:
            continue

        record = {
            'gene_id': parts[0].strip() if len(parts) > 0 else '',
            'symbol': parts[1].strip() if len(parts) > 1 else '',
            'locus_tag': parts[2].strip() if len(parts) > 2 else '',
            'description': parts[3].strip() if len(parts) > 3 else '',
            'chromosome': parts[4].strip() if len(parts) > 4 else '',
            'map_location': parts[5].strip() if len(parts) > 5 else '',
            'type_of_gene': parts[6].strip() if len(parts) > 6 else '',
        }

        if record['gene_id']:
            records.append(record)

    return records


def fetch_refseq_ids(
    gene_ids: list[str],
    email: str,
    batch_size: int = 100,
) -> dict:
    """
    Fetch RefSeq IDs linked to NCBI Gene IDs.

    Returns:
        Dict mapping gene_id to {'protein': [...], 'dna': [...]}
    """
    Entrez.email = email
    refseq_map = {}

    for i in range(0, len(gene_ids), batch_size):
        batch_ids = gene_ids[i:i + batch_size]
        try:
            # Get links from gene to RefSeq
            link_handle = Entrez.elink(
                dbfrom="gene",
                db="nuccore,protein",
                id=batch_ids,
            )
            link_results = Entrez.read(link_handle)
            link_handle.close()

            for result in link_results:
                gene_id = result.get('IdList', [''])[0]
                if not gene_id:
                    continue

                refseq_map[gene_id] = {'protein': [], 'dna': []}

                for linkset in result.get('LinkSetDb', []):
                    db_to = linkset.get('DbTo', '')
                    links = [link['Id'] for link in linkset.get('Link', [])]

                    if db_to == 'protein':
                        refseq_map[gene_id]['protein'].extend(links)
                    elif db_to == 'nuccore':
                        refseq_map[gene_id]['dna'].extend(links)

            time.sleep(0.34)

        except Exception as e:
            logger.error(f"Error fetching RefSeq links for batch {i}: {e}")

    return refseq_map


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
        dbxref = session.query(Dbxref).filter(
            Dbxref.dbxref_no == dbxref_no
        ).first()
        if dbxref and description and dbxref.description != description:
            dbxref.description = description[:240]
        return dbxref_no, False

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
    """Link DBXREF to feature."""
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
    """Link DBXREF to URL."""
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


def update_ncbi_dbxrefs(
    session: Session,
    taxon_id: int,
    email: str,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Update NCBI DBXREF entries.

    Args:
        session: Database session
        taxon_id: NCBI taxonomy ID
        email: Email for NCBI API
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'genes_fetched': 0,
        'gene_dbxrefs_created': 0,
        'gene_dbxrefs_updated': 0,
        'refseq_protein_created': 0,
        'refseq_dna_created': 0,
        'feat_links_created': 0,
        'url_links_created': 0,
        'features_not_found': 0,
        'errors': 0,
    }

    # Get feature mapping
    feature_map = get_feature_map(session)
    logger.info(f"Loaded {len(feature_map)} features")

    # Get existing DBXREFs
    gene_dbxrefs = get_existing_dbxrefs(session, NCBI_GENE_SOURCE, NCBI_GENE_TYPE)
    refseq_protein_dbxrefs = get_existing_dbxrefs(session, REFSEQ_PROTEIN_SOURCE, REFSEQ_PROTEIN_TYPE)
    refseq_dna_dbxrefs = get_existing_dbxrefs(session, REFSEQ_DNA_SOURCE, REFSEQ_DNA_TYPE)

    logger.info(f"Existing NCBI Gene DBXREFs: {len(gene_dbxrefs)}")

    # Get URL numbers
    gene_url_no = get_url_no(session, NCBI_GENE_SOURCE, 'query by ID')
    refseq_url_no = get_url_no(session, REFSEQ_PROTEIN_SOURCE)

    # Fetch gene data
    gene_records = fetch_ncbi_gene_data(taxon_id, email)
    stats['genes_fetched'] = len(gene_records)
    logger.info(f"Fetched {len(gene_records)} gene records")

    # Process gene records
    for record in gene_records:
        gene_id = record['gene_id']
        locus_tag = record['locus_tag'].upper()
        symbol = record['symbol'].upper()
        description = record['description']

        # Find feature
        feat_no = feature_map.get(locus_tag)
        if not feat_no:
            feat_no = feature_map.get(symbol)

        if not feat_no:
            stats['features_not_found'] += 1
            logger.debug(f"Feature not found for gene {gene_id}: {locus_tag}")
            continue

        try:
            # Create or update Gene DBXREF
            dbxref_no, is_new = create_or_update_dbxref(
                session,
                gene_id,
                NCBI_GENE_SOURCE,
                NCBI_GENE_TYPE,
                description,
                created_by,
                gene_dbxrefs,
            )

            if is_new:
                stats['gene_dbxrefs_created'] += 1
            else:
                stats['gene_dbxrefs_updated'] += 1

            # Link to feature
            if link_dbxref_to_feature(session, dbxref_no, feat_no):
                stats['feat_links_created'] += 1

            # Link to URL
            if gene_url_no and link_dbxref_to_url(session, dbxref_no, gene_url_no):
                stats['url_links_created'] += 1

        except Exception as e:
            logger.error(f"Error processing gene {gene_id}: {e}")
            stats['errors'] += 1

        # Flush periodically
        if stats['gene_dbxrefs_created'] % 100 == 0:
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
    Update NCBI DBXREFs from a local file.

    Expected file format (tab-delimited):
    gene_id, symbol, locus_tag, description

    Args:
        session: Database session
        data_file: Data file path
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'genes_fetched': 0,
        'gene_dbxrefs_created': 0,
        'gene_dbxrefs_updated': 0,
        'refseq_protein_created': 0,
        'refseq_dna_created': 0,
        'feat_links_created': 0,
        'url_links_created': 0,
        'features_not_found': 0,
        'errors': 0,
    }

    # Get feature mapping
    feature_map = get_feature_map(session)
    logger.info(f"Loaded {len(feature_map)} features")

    # Get existing DBXREFs
    gene_dbxrefs = get_existing_dbxrefs(session, NCBI_GENE_SOURCE, NCBI_GENE_TYPE)

    # Get URL numbers
    gene_url_no = get_url_no(session, NCBI_GENE_SOURCE, 'query by ID')

    # Process file
    with open(data_file) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('\t')
            if len(parts) < 3:
                continue

            gene_id = parts[0].strip()
            symbol = parts[1].strip().upper() if len(parts) > 1 else ''
            locus_tag = parts[2].strip().upper() if len(parts) > 2 else ''
            description = parts[3].strip() if len(parts) > 3 else ''

            stats['genes_fetched'] += 1

            # Find feature
            feat_no = feature_map.get(locus_tag)
            if not feat_no:
                feat_no = feature_map.get(symbol)

            if not feat_no:
                stats['features_not_found'] += 1
                continue

            try:
                dbxref_no, is_new = create_or_update_dbxref(
                    session,
                    gene_id,
                    NCBI_GENE_SOURCE,
                    NCBI_GENE_TYPE,
                    description,
                    created_by,
                    gene_dbxrefs,
                )

                if is_new:
                    stats['gene_dbxrefs_created'] += 1
                else:
                    stats['gene_dbxrefs_updated'] += 1

                if link_dbxref_to_feature(session, dbxref_no, feat_no):
                    stats['feat_links_created'] += 1

                if gene_url_no and link_dbxref_to_url(session, dbxref_no, gene_url_no):
                    stats['url_links_created'] += 1

            except Exception as e:
                logger.error(f"Line {line_num}: Error processing gene {gene_id}: {e}")
                stats['errors'] += 1

            if stats['gene_dbxrefs_created'] % 100 == 0:
                session.flush()

    session.flush()
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update NCBI DBXREF entries in database"
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
        help="Local data file (instead of fetching from NCBI)",
    )
    parser.add_argument(
        "--email",
        default=DEFAULT_EMAIL,
        help=f"Email for NCBI API (default: {DEFAULT_EMAIL})",
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
                stats = update_ncbi_dbxrefs(
                    session,
                    args.taxon_id,
                    args.email,
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
            logger.info(f"  Genes fetched: {stats['genes_fetched']}")
            logger.info(f"  Gene DBXREFs created: {stats['gene_dbxrefs_created']}")
            logger.info(f"  Gene DBXREFs updated: {stats['gene_dbxrefs_updated']}")
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
