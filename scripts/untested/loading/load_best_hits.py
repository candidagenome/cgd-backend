#!/usr/bin/env python3
"""
Load BLAST best hits data into homolog tables.

This script loads best hits data for a given MOD (Model Organism Database)
into the HOMOLOG and HOMOLOG_ALIGNMENT tables.

Original Perl: loadBestHits.pl
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
from cgd.models.models import Feature, Homolog, HomologAlignment, Taxonomy

load_dotenv()

logger = logging.getLogger(__name__)

# MOD to species mapping
MOD_TO_SPECIES = {
    'CGD': 'Candida albicans',
    'SGD': 'Saccharomyces cerevisiae',
    'AGD': 'Eremothecium gossypii',
    'TAIR': 'Arabidopsis thaliana',
    'WormBase': 'Caenorhabditis elegans',
    'FlyBase': 'Drosophila melanogaster',
    'ENSEMBL(HUMAN)': 'Homo sapiens',
    'S. pombe GeneDB': 'Schizosaccharomyces pombe',
}

METHOD = 'BLASTP'


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


def get_taxon_id(session: Session, species: str) -> int | None:
    """Get taxon_id for a species."""
    taxonomy = session.query(Taxonomy).filter(
        Taxonomy.tax_term == species
    ).first()

    return taxonomy.taxon_id if taxonomy else None


def get_feature_no(session: Session, feature_name: str) -> int | None:
    """Get feature_no for a feature name."""
    feature = session.query(Feature).filter(
        Feature.feature_name == feature_name
    ).first()

    return feature.feature_no if feature else None


def get_homologs_for_source(session: Session, source: str) -> dict:
    """
    Get existing homologs for a source.

    Returns:
        Dict mapping identifier to homolog_no
    """
    homologs = session.query(Homolog).filter(
        Homolog.source == source
    ).all()

    return {h.identifier: h.homolog_no for h in homologs}


def get_homolog_alignments_for_source(session: Session, source: str) -> dict:
    """
    Get existing homolog alignments for a source.

    Returns:
        Dict mapping homolog_alignment_no to alignment info
    """
    result = session.execute(
        text("""
            SELECT ha.homolog_alignment_no, ha.query_no, ha.target_no,
                   ha.method, ha.query_align_start_coord, ha.query_align_stop_coord,
                   ha.target_align_start_coord, ha.target_align_stop_coord
            FROM homolog_alignment ha
            JOIN homolog h ON ha.target_no = h.homolog_no
            WHERE h.source = :source
        """),
        {"source": source}
    )

    alignments = {}
    for row in result:
        key = row[0]
        value = f"{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}\t{row[6]}\t{row[7]}"
        alignments[key] = value

    return alignments


def clean_hit_description(description: str, mod: str) -> str:
    """Clean hit description based on MOD format."""
    if mod == 'TAIR':
        # Remove leading IDs like "68412.m06889"
        import re
        match = re.match(r'^(\d+\.m\d+?)\s(.+)$', description, re.IGNORECASE)
        if match:
            return match.group(2)

    elif mod == 'WormBase':
        # Remove leading IDs like "CE684120"
        import re
        match = re.match(r'^(CE\d+?)\s(.+)$', description, re.IGNORECASE)
        if match:
            return match.group(2)

    return description


def clean_hit_accession(accession: str, mod: str) -> str:
    """Clean hit accession based on MOD format."""
    if mod == 'ENSEMBL(HUMAN)':
        # Remove 'Translation:' prefix
        if accession.lower().startswith('translation:'):
            return accession.split(':', 1)[1]

    return accession


def load_best_hits(
    session: Session,
    data_file: Path,
    mod: str,
    score_type: str,
    update: bool = False,
    created_by: str = "SCRIPT",
    dry_run: bool = False,
) -> dict:
    """
    Load best hits data.

    Args:
        session: Database session
        data_file: Best hits data file
        mod: Model organism database name
        score_type: Score type ('e-value' or 'bit-score')
        update: Update existing data
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'homologs_inserted': 0,
        'homologs_updated': 0,
        'alignments_inserted': 0,
        'alignments_deleted': 0,
        'homologs_deleted': 0,
        'errors': 0,
    }

    # Get species and taxon_id
    species = MOD_TO_SPECIES.get(mod)
    if not species:
        logger.error(f"Unknown MOD: {mod}")
        return stats

    taxon_id = get_taxon_id(session, species)
    if not taxon_id:
        logger.warning(f"No taxon_id found for species: {species}")

    logger.info(f"MOD: {mod}, Species: {species}, Taxon ID: {taxon_id}")
    logger.info(f"Score type: {score_type}, Method: {METHOD}")

    # Load existing data if updating
    hits_to_homolog_no = {}
    homolog_alignments = {}

    if update:
        hits_to_homolog_no = get_homologs_for_source(session, mod)
        logger.info(f"Loaded {len(hits_to_homolog_no)} existing homologs")

        homolog_alignments = get_homolog_alignments_for_source(session, mod)
        logger.info(f"Loaded {len(homolog_alignments)} existing alignments")

    # Keep track of what we've seen (for cleanup)
    seen_homologs = set()
    seen_alignments = set()

    with open(data_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 19:
                continue

            (
                query_name, query_length, query_description,
                hit_name, hit_accession, hit_length, hit_length_aln,
                hit_strand, each_acc_nums, hit_description,
                e_value, hsp_percent, hsp_score, hsp_bits,
                hsp_strand, hit_hsp_start, hit_hsp_end,
                query_hsp_start, query_hsp_end
            ) = parts[:19]

            stats['processed'] += 1

            # Parse numeric values
            try:
                query_length = int(query_length)
                hit_length = int(hit_length)
                query_hsp_start = int(query_hsp_start)
                query_hsp_end = int(query_hsp_end)
                hit_hsp_start = int(hit_hsp_start)
                hit_hsp_end = int(hit_hsp_end)
                e_value = float(e_value) if e_value else 0.0
                hsp_score = float(hsp_score) if hsp_score else 0.0
            except ValueError:
                logger.warning(f"Invalid numeric value in line: {line[:50]}...")
                continue

            # Calculate percent aligned
            pct_aligned = abs(query_hsp_end - query_hsp_start + 1) * 100 / query_length
            pct_aligned = round(pct_aligned, 1)

            # Clean up accession and description
            hit_accession = clean_hit_accession(hit_accession, mod)
            hit_description = clean_hit_description(hit_description, mod)

            # Get feature_no
            feat_no = get_feature_no(session, query_name)
            if not feat_no:
                logger.debug(f"Feature not found: {query_name}")
                continue

            # Calculate score
            if score_type == 'e-value':
                if e_value == 0.0:
                    e_value = 1e-261
                score = math.log(e_value)
            else:
                score = hsp_score

            # Get or create homolog
            homolog_no = hits_to_homolog_no.get(hit_accession)

            if homolog_no and update:
                # Update existing homolog
                homolog = session.query(Homolog).filter(
                    Homolog.homolog_no == homolog_no
                ).first()

                if homolog:
                    homolog.length = hit_length
                    homolog.description = hit_description
                    homolog.taxon_id = taxon_id
                    stats['homologs_updated'] += 1

                seen_homologs.add(hit_accession)

            elif not homolog_no:
                # Create new homolog
                try:
                    homolog = Homolog(
                        identifier=hit_accession,
                        source=mod,
                        length=hit_length,
                        taxon_id=taxon_id,
                        description=hit_description,
                        created_by=created_by,
                    )
                    session.add(homolog)
                    session.flush()

                    homolog_no = homolog.homolog_no
                    hits_to_homolog_no[hit_accession] = homolog_no
                    stats['homologs_inserted'] += 1

                except Exception as e:
                    logger.error(f"Error inserting homolog {hit_accession}: {e}")
                    stats['errors'] += 1
                    continue

            # Insert homolog alignment
            try:
                alignment = HomologAlignment(
                    query_no=feat_no,
                    target_no=homolog_no,
                    method=METHOD,
                    query_align_start_coord=query_hsp_start,
                    query_align_stop_coord=query_hsp_end,
                    target_align_start_coord=hit_hsp_start,
                    target_align_stop_coord=hit_hsp_end,
                    score=score,
                    score_type=score_type,
                    pct_aligned=pct_aligned,
                )
                session.add(alignment)
                session.flush()

                stats['alignments_inserted'] += 1

                # Track alignment key for cleanup
                align_key = f"{feat_no}\t{homolog_no}\t{METHOD}\t{query_hsp_start}\t{query_hsp_end}\t{hit_hsp_start}\t{hit_hsp_end}"
                seen_alignments.add(align_key)

            except Exception as e:
                logger.error(
                    f"Error inserting alignment for {query_name} -> {hit_accession}: {e}"
                )
                stats['errors'] += 1

    # Cleanup old data if updating
    if update:
        # Delete old alignments
        for align_no, align_value in homolog_alignments.items():
            if align_value not in seen_alignments:
                try:
                    session.execute(
                        text("DELETE FROM homolog_alignment WHERE homolog_alignment_no = :no"),
                        {"no": align_no}
                    )
                    stats['alignments_deleted'] += 1
                except Exception as e:
                    logger.error(f"Error deleting alignment {align_no}: {e}")

        # Delete old homologs
        for identifier, homolog_no in hits_to_homolog_no.items():
            if identifier not in seen_homologs:
                try:
                    session.execute(
                        text("DELETE FROM homolog WHERE homolog_no = :no"),
                        {"no": homolog_no}
                    )
                    stats['homologs_deleted'] += 1
                except Exception as e:
                    logger.error(f"Error deleting homolog {homolog_no}: {e}")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load BLAST best hits data into homolog tables"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="Best hits data file",
    )
    parser.add_argument(
        "--mod",
        required=True,
        choices=list(MOD_TO_SPECIES.keys()),
        help="Model organism database name",
    )
    parser.add_argument(
        "--score-type",
        required=True,
        choices=['e-value', 'bit-score'],
        help="Score type",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing data (delete old, keep new)",
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

    # Validate input
    if not args.data_file.exists():
        logger.error(f"Data file not found: {args.data_file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            stats = load_best_hits(
                session,
                args.data_file,
                args.mod,
                args.score_type,
                args.update,
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
            logger.info(f"  Processed: {stats['processed']}")
            logger.info(f"  Homologs inserted: {stats['homologs_inserted']}")
            logger.info(f"  Homologs updated: {stats['homologs_updated']}")
            logger.info(f"  Alignments inserted: {stats['alignments_inserted']}")
            if args.update:
                logger.info(f"  Alignments deleted: {stats['alignments_deleted']}")
                logger.info(f"  Homologs deleted: {stats['homologs_deleted']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
