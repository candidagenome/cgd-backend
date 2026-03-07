#!/usr/bin/env python3
"""
Load PDB homology data into database.

This script loads PDB structural homology information from BLAST results
into the pdb_sequence, pdb_alignment, and pdb_alignment_sequence tables.

Part 3 of 3-part PDB pipeline:
    1. download_pdb_seq.py
    2. blast_pdb.py
    3. load_pdb.py

Original Perl: loadPDB.pl
Converted to Python: 2024
"""

import argparse
import logging
import math
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Organism

load_dotenv()

logger = logging.getLogger(__name__)

# Constants
PDB_SOURCE = 'PDB'
METHOD = 'ncbi-blast'
MATRIX = 'blosum62'


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def get_organism(session: Session, organism_abbrev: str) -> Organism:
    """Get organism by abbreviation."""
    organism = session.query(Organism).filter(
        Organism.organism_abbrev == organism_abbrev
    ).first()

    if not organism:
        raise ValueError(f"Organism not found: {organism_abbrev}")

    return organism


def check_taxid(session: Session, taxid: str, valid_taxids: set) -> str:
    """
    Check if taxid exists in taxonomy table.

    Args:
        session: Database session
        taxid: Taxon ID to check
        valid_taxids: Cache of valid taxids

    Returns:
        Taxid if valid, empty string otherwise
    """
    if taxid == 'NA' or not taxid:
        return ''

    if taxid in valid_taxids:
        return taxid

    result = session.execute(
        text("SELECT tax_term FROM taxonomy WHERE taxon_id = :taxid"),
        {'taxid': taxid}
    ).fetchone()

    if result:
        valid_taxids.add(taxid)
        return taxid

    return ''


def delete_existing_pdb_data(
    session: Session,
    organism: Organism,
    db_source: str,
) -> tuple[int, int]:
    """
    Delete existing PDB data for organism.

    Args:
        session: Database session
        organism: Organism object
        db_source: Database source name (e.g., 'CGD')

    Returns:
        Tuple of (alignments_deleted, sequences_deleted)
    """
    # Get existing alignment and sequence numbers
    result = session.execute(
        text("""
            SELECT pa.pdb_alignment_no, pa.query_seq_no
            FROM pdb_alignment pa
            JOIN pdb_sequence ps ON pa.query_seq_no = ps.pdb_sequence_no
            WHERE ps.taxon_id = :taxon_id
              AND ps.source = :source
        """),
        {'taxon_id': organism.taxon_id, 'source': db_source}
    ).fetchall()

    alignment_nos = [r[0] for r in result]
    sequence_nos = list(set(r[1] for r in result))

    # Delete alignment sequences first
    for aln_no in alignment_nos:
        session.execute(
            text("DELETE FROM pdb_alignment_sequence WHERE pdb_alignment_no = :aln_no"),
            {'aln_no': aln_no}
        )

    # Delete alignments
    for aln_no in alignment_nos:
        session.execute(
            text("DELETE FROM pdb_alignment WHERE pdb_alignment_no = :aln_no"),
            {'aln_no': aln_no}
        )

    # Delete query sequences
    for seq_no in sequence_nos:
        session.execute(
            text("DELETE FROM pdb_sequence WHERE pdb_sequence_no = :seq_no"),
            {'seq_no': seq_no}
        )

    return len(alignment_nos), len(sequence_nos)


def insert_pdb_sequence(
    session: Session,
    name: str,
    source: str,
    length: int,
    note: str,
    taxon_id: str,
    created_by: str,
) -> int:
    """
    Insert PDB sequence record.

    Returns:
        pdb_sequence_no
    """
    result = session.execute(
        text("""
            INSERT INTO pdb_sequence
            (sequence_name, source, sequence_length, note, taxon_id, created_by)
            VALUES (:name, :source, :length, :note, :taxid, :user)
            RETURNING pdb_sequence_no
        """),
        {
            'name': name,
            'source': source,
            'length': length,
            'note': note or None,
            'taxid': taxon_id if taxon_id else None,
            'user': created_by,
        }
    )
    return result.fetchone()[0]


def insert_pdb_alignment(
    session: Session,
    query_seq_no: int,
    target_seq_no: int,
    query_start: int,
    query_end: int,
    target_start: int,
    target_end: int,
    pct_aligned: float,
    pct_identical: float,
    pct_similar: float,
    pvalue: float,
    created_by: str,
) -> int:
    """
    Insert PDB alignment record.

    Returns:
        pdb_alignment_no
    """
    # Convert p-value to ln(p) for Oracle storage
    if not pvalue or pvalue <= 1e-261:
        score = math.log(1e-261)
    else:
        score = math.log(pvalue)

    result = session.execute(
        text("""
            INSERT INTO pdb_alignment
            (query_seq_no, target_seq_no, method, matrix,
             query_align_start_coord, query_align_stop_coord,
             target_align_start_coord, target_align_stop_coord,
             pct_aligned, pct_identical, pct_similar, score, created_by)
            VALUES
            (:q_no, :t_no, :method, :matrix,
             :q_start, :q_end, :t_start, :t_end,
             :aligned, :ident, :sim, :score, :user)
            RETURNING pdb_alignment_no
        """),
        {
            'q_no': query_seq_no,
            't_no': target_seq_no,
            'method': METHOD,
            'matrix': MATRIX,
            'q_start': query_start,
            'q_end': query_end,
            't_start': target_start,
            't_end': target_end,
            'aligned': pct_aligned,
            'ident': pct_identical,
            'sim': pct_similar,
            'score': score,
            'user': created_by,
        }
    )
    return result.fetchone()[0]


def insert_pdb_alignment_sequence(
    session: Session,
    alignment_no: int,
    query_seq: str,
    target_seq: str,
    align_symbol: str,
    created_by: str,
) -> None:
    """Insert PDB alignment sequence record."""
    session.execute(
        text("""
            INSERT INTO pdb_alignment_sequence
            (pdb_alignment_no, query_seq, target_seq, alignment_symbol, created_by)
            VALUES (:aln_no, :q_seq, :t_seq, :symbol, :user)
        """),
        {
            'aln_no': alignment_no,
            'q_seq': query_seq,
            't_seq': target_seq,
            'symbol': align_symbol,
            'user': created_by,
        }
    )


def load_pdb_data(
    session: Session,
    data_file: Path,
    organism: Organism,
    db_source: str,
    created_by: str,
) -> dict:
    """
    Load PDB data from BLAST results file.

    Args:
        session: Database session
        data_file: Tab-delimited BLAST results file
        organism: Organism object
        db_source: Database source name
        created_by: User name

    Returns:
        Statistics dict
    """
    stats = {
        'orfs_inserted': 0,
        'pdbs_inserted': 0,
        'alignments_inserted': 0,
    }

    valid_taxids = set()
    seq_no_for_orf = {}
    seq_no_for_pdb = {}
    alignment_pairs = set()

    # Load existing PDB sequences
    result = session.execute(
        text("""
            SELECT pdb_sequence_no, sequence_name
            FROM pdb_sequence
            WHERE source = 'PDB'
        """)
    ).fetchall()

    for row in result:
        seq_no_for_pdb[row[1]] = row[0]

    with open(data_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 17:
                continue

            (orf, orf_len, pdb, pdb_len, desc, pval_str,
             orf_start, orf_end, pdb_start, pdb_end,
             pct_similar, pct_identical, pct_aligned,
             taxid, query_seq, align_symbol, target_seq) = parts[:17]

            # Validate taxid
            taxid = check_taxid(session, taxid, valid_taxids)

            # Get/create query sequence
            if orf in seq_no_for_orf:
                q_no = seq_no_for_orf[orf]
            else:
                q_no = insert_pdb_sequence(
                    session, orf, db_source, int(orf_len), '',
                    str(organism.taxon_id), created_by
                )
                seq_no_for_orf[orf] = q_no
                stats['orfs_inserted'] += 1

            # Get/create target (PDB) sequence
            if pdb in seq_no_for_pdb:
                t_no = seq_no_for_pdb[pdb]
            else:
                t_no = insert_pdb_sequence(
                    session, pdb, PDB_SOURCE, int(pdb_len), desc,
                    taxid, created_by
                )
                seq_no_for_pdb[pdb] = t_no
                stats['pdbs_inserted'] += 1

            # Skip if no coordinates
            if not orf_start or not orf_end or not pdb_start or not pdb_end:
                continue

            # Skip duplicate alignments
            pair_key = f"{q_no}_{t_no}"
            if pair_key in alignment_pairs:
                continue
            alignment_pairs.add(pair_key)

            # Insert alignment
            try:
                pval = float(pval_str)
            except ValueError:
                pval = 0

            aln_no = insert_pdb_alignment(
                session, q_no, t_no,
                int(orf_start), int(orf_end),
                int(pdb_start), int(pdb_end),
                float(pct_aligned), float(pct_identical), float(pct_similar),
                pval, created_by
            )

            # Insert alignment sequence
            insert_pdb_alignment_sequence(
                session, aln_no, query_seq, target_seq, align_symbol, created_by
            )

            stats['alignments_inserted'] += 1

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load PDB homology data into database"
    )
    parser.add_argument(
        "organism",
        help="Organism abbreviation",
    )
    parser.add_argument(
        "created_by",
        help="Database user for audit",
    )
    parser.add_argument(
        "--data",
        type=Path,
        help="PDB BLAST results file (default: data/pdb/{organism}_pdb.out)",
    )
    parser.add_argument(
        "--db-source",
        default="CGD",
        help="Database source name (default: CGD)",
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

    setup_logging(args.verbose)

    logger.info(f"Started at {datetime.now()}")

    # Determine data file
    if args.data:
        data_file = args.data
    else:
        data_file = Path(f"data/pdb/{args.organism}_pdb.out")

    if not data_file.exists():
        logger.error(f"Data file not found: {data_file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            # Get organism
            organism = get_organism(session, args.organism)
            logger.info(f"Processing organism: {organism.organism_name}")

            # Delete existing data
            aln_del, seq_del = delete_existing_pdb_data(
                session, organism, args.db_source
            )
            logger.info(f"Deleted {aln_del} alignments and {seq_del} sequences")

            # Load new data
            stats = load_pdb_data(
                session, data_file, organism,
                args.db_source, args.created_by
            )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  ORF sequences inserted: {stats['orfs_inserted']}")
            logger.info(f"  PDB sequences inserted: {stats['pdbs_inserted']}")
            logger.info(f"  Alignments inserted: {stats['alignments_inserted']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
