#!/usr/bin/env python3
"""
Load BioGRID interaction data into database.

This script loads BioGRID interaction data into the INTERACTION,
FEAT_INTERACTION, INTERACT_PHENO, and REF_LINK tables.

Original Perl: loadGrid.pl
Converted to Python: 2024
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import (
    FeatInteract,
    Feature,
    InteractPheno,
    Interaction,
    Phenotype,
    Reference,
    RefLink,
)

load_dotenv()

logger = logging.getLogger(__name__)

# Default values
DEFAULT_SOURCE = 'BioGRID'
DEFAULT_EXPERIMENT_TYPE = 'Affinity Capture-MS'


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


def get_reference_map(session: Session) -> dict:
    """
    Get mapping of pubmed to reference_no.

    Returns:
        Dict mapping pubmed to reference_no
    """
    references = session.query(Reference).filter(
        Reference.pubmed.isnot(None)
    ).all()
    return {r.pubmed: r.reference_no for r in references}


def get_or_create_interaction(
    session: Session,
    experiment_type: str,
    source: str,
    description: str,
    created_by: str,
) -> tuple[int, bool]:
    """
    Get or create an interaction entry.

    Returns:
        Tuple of (interaction_no, is_new)
    """
    interaction = session.query(Interaction).filter(
        and_(
            Interaction.experiment_type == experiment_type,
            Interaction.source == source,
            Interaction.description == description if description else Interaction.description.is_(None),
        )
    ).first()

    if interaction:
        return interaction.interaction_no, False

    interaction = Interaction(
        experiment_type=experiment_type,
        source=source,
        description=description if description else None,
        created_by=created_by,
    )
    session.add(interaction)
    session.flush()

    logger.debug(f"Created interaction: {interaction.interaction_no}")
    return interaction.interaction_no, True


def create_feat_interact(
    session: Session,
    feature_no: int,
    interaction_no: int,
    action: str,
    created_by: str,
) -> bool:
    """
    Create feat_interact entry.

    Returns:
        True if created, False if already exists
    """
    existing = session.query(FeatInteract).filter(
        and_(
            FeatInteract.feature_no == feature_no,
            FeatInteract.interaction_no == interaction_no,
            FeatInteract.action == action,
        )
    ).first()

    if existing:
        return False

    feat_interact = FeatInteract(
        feature_no=feature_no,
        interaction_no=interaction_no,
        action=action,
        created_by=created_by,
    )
    session.add(feat_interact)
    session.flush()

    return True


def get_or_create_phenotype(
    session: Session,
    source: str,
    experiment_type: str,
    mutant_type: str,
    observable: str,
    qualifier: str,
    created_by: str,
) -> int:
    """
    Get or create phenotype for interaction.

    Returns:
        phenotype_no
    """
    phenotype = session.query(Phenotype).filter(
        and_(
            Phenotype.source == source,
            Phenotype.experiment_type == experiment_type,
            Phenotype.mutant_type == mutant_type,
            Phenotype.observable == observable,
            Phenotype.qualifier == qualifier if qualifier else Phenotype.qualifier.is_(None),
        )
    ).first()

    if phenotype:
        return phenotype.phenotype_no

    phenotype = Phenotype(
        source=source,
        experiment_type=experiment_type,
        mutant_type=mutant_type,
        observable=observable,
        qualifier=qualifier if qualifier else None,
        created_by=created_by,
    )
    session.add(phenotype)
    session.flush()

    return phenotype.phenotype_no


def create_interact_pheno(
    session: Session,
    interaction_no: int,
    phenotype_no: int,
) -> bool:
    """
    Create interact_pheno entry.

    Returns:
        True if created, False if already exists
    """
    existing = session.query(InteractPheno).filter(
        and_(
            InteractPheno.interaction_no == interaction_no,
            InteractPheno.phenotype_no == phenotype_no,
        )
    ).first()

    if existing:
        return False

    interact_pheno = InteractPheno(
        interaction_no=interaction_no,
        phenotype_no=phenotype_no,
    )
    session.add(interact_pheno)
    session.flush()

    return True


def create_ref_link(
    session: Session,
    reference_no: int,
    tab_name: str,
    primary_key: int,
    col_name: str,
    created_by: str,
) -> bool:
    """
    Create reference link.

    Returns:
        True if created, False if already exists
    """
    existing = session.query(RefLink).filter(
        and_(
            RefLink.reference_no == reference_no,
            RefLink.tab_name == tab_name,
            RefLink.primary_key == primary_key,
            RefLink.col_name == col_name,
        )
    ).first()

    if existing:
        return False

    ref_link = RefLink(
        reference_no=reference_no,
        tab_name=tab_name,
        primary_key=primary_key,
        col_name=col_name,
        created_by=created_by,
    )
    session.add(ref_link)
    session.flush()

    return True


def load_biogrid_data(
    session: Session,
    data_file: Path,
    source: str,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Load BioGRID interaction data from file.

    Expected BioGRID file format (tab-delimited):
    BioGRID_Interaction_ID, Entrez_Gene_Interactor_A, Entrez_Gene_Interactor_B,
    BioGRID_ID_Interactor_A, BioGRID_ID_Interactor_B,
    Systematic_Name_Interactor_A, Systematic_Name_Interactor_B,
    Official_Symbol_Interactor_A, Official_Symbol_Interactor_B,
    Synonyms_Interactor_A, Synonyms_Interactor_B,
    Experimental_System, Experimental_System_Type,
    Author, Pubmed_ID, Organism_Interactor_A, Organism_Interactor_B,
    Throughput, Score, Modification, Phenotypes, Qualifications, Tags, Source_Database

    Args:
        session: Database session
        data_file: BioGRID data file path
        source: Source for interaction entries
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'interactions_created': 0,
        'feat_interact_created': 0,
        'interact_pheno_created': 0,
        'ref_links_created': 0,
        'bad_features': 0,
        'bad_references': 0,
        'skipped_other_organism': 0,
        'errors': 0,
    }

    # Get mappings
    feature_map = get_feature_map(session)
    logger.info(f"Loaded {len(feature_map)} features")

    reference_map = get_reference_map(session)
    logger.info(f"Loaded {len(reference_map)} references")

    with open(data_file) as f:
        # Skip header
        header = f.readline()

        for line_num, line in enumerate(f, 2):
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 15:
                logger.warning(f"Line {line_num}: insufficient columns")
                continue

            stats['processed'] += 1

            # Parse key fields
            systematic_name_a = parts[5].strip().upper() if len(parts) > 5 else ''
            systematic_name_b = parts[6].strip().upper() if len(parts) > 6 else ''
            experiment_type = parts[11].strip() if len(parts) > 11 else DEFAULT_EXPERIMENT_TYPE
            pubmed_str = parts[14].strip() if len(parts) > 14 else ''
            phenotypes = parts[20].strip() if len(parts) > 20 else ''

            # Get feature_nos for both interactors
            feat_no_a = feature_map.get(systematic_name_a)
            feat_no_b = feature_map.get(systematic_name_b)

            if not feat_no_a and not feat_no_b:
                # Neither feature found - likely from another organism
                stats['skipped_other_organism'] += 1
                continue

            if not feat_no_a:
                logger.debug(f"Line {line_num}: Feature A not found: {systematic_name_a}")
                stats['bad_features'] += 1
            if not feat_no_b:
                logger.debug(f"Line {line_num}: Feature B not found: {systematic_name_b}")
                stats['bad_features'] += 1

            # Get reference_no if pubmed provided
            reference_no = None
            if pubmed_str:
                try:
                    pubmed = int(pubmed_str)
                    reference_no = reference_map.get(pubmed)
                    if not reference_no:
                        logger.debug(f"Line {line_num}: Reference not found: {pubmed}")
                        stats['bad_references'] += 1
                except ValueError:
                    logger.warning(f"Line {line_num}: Invalid pubmed: {pubmed_str}")
                    stats['bad_references'] += 1

            try:
                # Create or get interaction
                description = f"{systematic_name_a} interacts with {systematic_name_b}"
                interaction_no, is_new = get_or_create_interaction(
                    session,
                    experiment_type,
                    source,
                    description,
                    created_by,
                )
                if is_new:
                    stats['interactions_created'] += 1

                # Create feat_interact entries for both interactors
                if feat_no_a:
                    if create_feat_interact(
                        session,
                        feat_no_a,
                        interaction_no,
                        'Bait',
                        created_by,
                    ):
                        stats['feat_interact_created'] += 1

                if feat_no_b:
                    if create_feat_interact(
                        session,
                        feat_no_b,
                        interaction_no,
                        'Hit',
                        created_by,
                    ):
                        stats['feat_interact_created'] += 1

                # Create interact_pheno if phenotypes present
                if phenotypes and phenotypes != '-':
                    for pheno_str in phenotypes.split('|'):
                        pheno_str = pheno_str.strip()
                        if pheno_str:
                            phenotype_no = get_or_create_phenotype(
                                session,
                                source,
                                experiment_type,
                                'interaction',
                                pheno_str,
                                None,
                                created_by,
                            )
                            if create_interact_pheno(session, interaction_no, phenotype_no):
                                stats['interact_pheno_created'] += 1

                # Create ref_link if we have a reference
                if reference_no and is_new:
                    if create_ref_link(
                        session,
                        reference_no,
                        'INTERACTION',
                        interaction_no,
                        'INTERACTION_NO',
                        created_by,
                    ):
                        stats['ref_links_created'] += 1

            except Exception as e:
                logger.error(f"Line {line_num}: Error: {e}")
                stats['errors'] += 1

            # Flush periodically
            if line_num % 500 == 0:
                session.flush()
                logger.debug(f"Processed {line_num} lines")

    session.flush()
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load BioGRID interaction data into database"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="BioGRID data file",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Source for interaction entries (default: {DEFAULT_SOURCE})",
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
            stats = load_biogrid_data(
                session,
                args.data_file,
                args.source,
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
            logger.info(f"  Interactions created: {stats['interactions_created']}")
            logger.info(f"  Feat_interact created: {stats['feat_interact_created']}")
            logger.info(f"  Interact_pheno created: {stats['interact_pheno_created']}")
            logger.info(f"  Ref links created: {stats['ref_links_created']}")
            logger.info(f"  Bad features: {stats['bad_features']}")
            logger.info(f"  Bad references: {stats['bad_references']}")
            logger.info(f"  Skipped (other organism): {stats['skipped_other_organism']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
