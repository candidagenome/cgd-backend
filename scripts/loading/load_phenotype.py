#!/usr/bin/env python3
"""
Load phenotype data into database.

This script loads phenotype data from a tab-delimited file into
PHENOTYPE, PHENO_ANNOTATION, EXPERIMENT, EXPT_PROPERTY, EXPT_EXPTPROP,
and REF_LINK tables.

Original Perl: loadPhenotype.pl
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
    Experiment,
    ExptExptprop,
    ExptProperty,
    Feature,
    PhenoAnnotation,
    Phenotype,
    Reference,
    RefLink,
)

load_dotenv()

logger = logging.getLogger(__name__)

# Default source
DEFAULT_SOURCE = 'CGD'


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
        Dict mapping feature_name to feature_no
    """
    features = session.query(Feature).all()
    return {f.feature_name: f.feature_no for f in features}


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
    Get or create a phenotype entry.

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

    logger.debug(f"Created phenotype: {observable} ({phenotype.phenotype_no})")
    return phenotype.phenotype_no


def get_or_create_expt_property(
    session: Session,
    property_type: str,
    property_value: str,
    property_description: str,
    created_by: str,
) -> int:
    """
    Get or create an experiment property entry.

    Returns:
        expt_property_no
    """
    expt_property = session.query(ExptProperty).filter(
        and_(
            ExptProperty.property_type == property_type,
            ExptProperty.property_value == property_value,
            ExptProperty.property_description == property_description if property_description else ExptProperty.property_description.is_(None),
        )
    ).first()

    if expt_property:
        return expt_property.expt_property_no

    expt_property = ExptProperty(
        property_type=property_type,
        property_value=property_value,
        property_description=property_description if property_description else None,
        created_by=created_by,
    )
    session.add(expt_property)
    session.flush()

    logger.debug(f"Created expt_property: {property_type}={property_value}")
    return expt_property.expt_property_no


def create_experiment(
    session: Session,
    source: str,
    experiment_comment: str,
    created_by: str,
) -> int:
    """
    Create a new experiment entry.

    Returns:
        experiment_no
    """
    experiment = Experiment(
        source=source,
        experiment_comment=experiment_comment if experiment_comment else None,
        created_by=created_by,
    )
    session.add(experiment)
    session.flush()

    logger.debug(f"Created experiment: {experiment.experiment_no}")
    return experiment.experiment_no


def link_experiment_property(
    session: Session,
    experiment_no: int,
    expt_property_no: int,
) -> None:
    """Link experiment to property."""
    existing = session.query(ExptExptprop).filter(
        and_(
            ExptExptprop.experiment_no == experiment_no,
            ExptExptprop.expt_property_no == expt_property_no,
        )
    ).first()

    if not existing:
        link = ExptExptprop(
            experiment_no=experiment_no,
            expt_property_no=expt_property_no,
        )
        session.add(link)
        session.flush()


def create_pheno_annotation(
    session: Session,
    feature_no: int,
    phenotype_no: int,
    experiment_no: int,
    created_by: str,
) -> int | None:
    """
    Create phenotype annotation linking feature to phenotype.

    Returns:
        pheno_annotation_no or None if already exists
    """
    existing = session.query(PhenoAnnotation).filter(
        and_(
            PhenoAnnotation.feature_no == feature_no,
            PhenoAnnotation.phenotype_no == phenotype_no,
            PhenoAnnotation.experiment_no == experiment_no if experiment_no else PhenoAnnotation.experiment_no.is_(None),
        )
    ).first()

    if existing:
        return None

    annotation = PhenoAnnotation(
        feature_no=feature_no,
        phenotype_no=phenotype_no,
        experiment_no=experiment_no if experiment_no else None,
        created_by=created_by,
    )
    session.add(annotation)
    session.flush()

    return annotation.pheno_annotation_no


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


def load_phenotypes(
    session: Session,
    data_file: Path,
    source: str,
    created_by: str,
    dry_run: bool = False,
) -> dict:
    """
    Load phenotype data from file.

    Expected file format (tab-delimited):
    feature_name, experiment_type, mutant_type, observable, qualifier,
    pubmed, strain_background, allele, details, chemical, condition,
    reporter, experiment_comment

    Args:
        session: Database session
        data_file: Data file path
        source: Source for phenotype entries
        created_by: User name for audit
        dry_run: If True, don't commit changes

    Returns:
        Statistics dict
    """
    stats = {
        'processed': 0,
        'phenotypes_created': 0,
        'annotations_created': 0,
        'ref_links_created': 0,
        'experiments_created': 0,
        'bad_features': 0,
        'bad_references': 0,
        'errors': 0,
    }

    # Get mappings
    feature_map = get_feature_map(session)
    logger.info(f"Loaded {len(feature_map)} features")

    reference_map = get_reference_map(session)
    logger.info(f"Loaded {len(reference_map)} references")

    with open(data_file) as f:
        # Skip header if present
        header = f.readline()
        if not header.startswith('#') and '\t' in header:
            # First line is data, process it
            f.seek(0)

        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('\t')
            if len(parts) < 5:
                logger.warning(f"Line {line_num}: insufficient columns")
                continue

            stats['processed'] += 1

            # Parse fields
            feature_name = parts[0].strip()
            experiment_type = parts[1].strip() if len(parts) > 1 else ''
            mutant_type = parts[2].strip() if len(parts) > 2 else ''
            observable = parts[3].strip() if len(parts) > 3 else ''
            qualifier = parts[4].strip() if len(parts) > 4 else ''
            pubmed_str = parts[5].strip() if len(parts) > 5 else ''
            strain_background = parts[6].strip() if len(parts) > 6 else ''
            allele = parts[7].strip() if len(parts) > 7 else ''
            details = parts[8].strip() if len(parts) > 8 else ''
            chemical = parts[9].strip() if len(parts) > 9 else ''
            condition = parts[10].strip() if len(parts) > 10 else ''
            reporter = parts[11].strip() if len(parts) > 11 else ''
            experiment_comment = parts[12].strip() if len(parts) > 12 else ''

            # Get feature_no
            feat_no = feature_map.get(feature_name.upper())
            if not feat_no:
                logger.warning(f"Line {line_num}: Bad feature name: {feature_name}")
                stats['bad_features'] += 1
                continue

            # Get reference_no if pubmed provided
            reference_no = None
            if pubmed_str:
                try:
                    pubmed = int(pubmed_str)
                    reference_no = reference_map.get(pubmed)
                    if not reference_no:
                        logger.warning(f"Line {line_num}: Reference not found: {pubmed}")
                        stats['bad_references'] += 1
                except ValueError:
                    logger.warning(f"Line {line_num}: Invalid pubmed: {pubmed_str}")
                    stats['bad_references'] += 1

            try:
                # Get or create phenotype
                phenotype_no = get_or_create_phenotype(
                    session,
                    source,
                    experiment_type,
                    mutant_type,
                    observable,
                    qualifier,
                    created_by,
                )
                if phenotype_no:
                    stats['phenotypes_created'] += 1

                # Create experiment if we have experiment properties
                experiment_no = None
                if any([strain_background, allele, details, chemical, condition, reporter]):
                    experiment_no = create_experiment(
                        session,
                        source,
                        experiment_comment,
                        created_by,
                    )
                    stats['experiments_created'] += 1

                    # Add experiment properties
                    if strain_background:
                        prop_no = get_or_create_expt_property(
                            session, 'strain_background', strain_background, None, created_by
                        )
                        link_experiment_property(session, experiment_no, prop_no)

                    if allele:
                        prop_no = get_or_create_expt_property(
                            session, 'allele', allele, None, created_by
                        )
                        link_experiment_property(session, experiment_no, prop_no)

                    if details:
                        prop_no = get_or_create_expt_property(
                            session, 'details', details, None, created_by
                        )
                        link_experiment_property(session, experiment_no, prop_no)

                    if chemical:
                        prop_no = get_or_create_expt_property(
                            session, 'chemical', chemical, None, created_by
                        )
                        link_experiment_property(session, experiment_no, prop_no)

                    if condition:
                        prop_no = get_or_create_expt_property(
                            session, 'condition', condition, None, created_by
                        )
                        link_experiment_property(session, experiment_no, prop_no)

                    if reporter:
                        prop_no = get_or_create_expt_property(
                            session, 'reporter', reporter, None, created_by
                        )
                        link_experiment_property(session, experiment_no, prop_no)

                # Create phenotype annotation
                annotation_no = create_pheno_annotation(
                    session,
                    feat_no,
                    phenotype_no,
                    experiment_no,
                    created_by,
                )
                if annotation_no:
                    stats['annotations_created'] += 1

                    # Create ref_link if we have a reference
                    if reference_no:
                        if create_ref_link(
                            session,
                            reference_no,
                            'PHENO_ANNOTATION',
                            annotation_no,
                            'PHENOTYPE_NO',
                            created_by,
                        ):
                            stats['ref_links_created'] += 1

            except Exception as e:
                logger.error(f"Line {line_num}: Error: {e}")
                stats['errors'] += 1

            # Flush periodically
            if line_num % 100 == 0:
                session.flush()
                logger.debug(f"Processed {line_num} lines")

    session.flush()
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load phenotype data into database"
    )
    parser.add_argument(
        "data_file",
        type=Path,
        help="Phenotype data file (TSV)",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Source for phenotype entries (default: {DEFAULT_SOURCE})",
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
            stats = load_phenotypes(
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
            logger.info(f"  Phenotypes created: {stats['phenotypes_created']}")
            logger.info(f"  Annotations created: {stats['annotations_created']}")
            logger.info(f"  Experiments created: {stats['experiments_created']}")
            logger.info(f"  Ref links created: {stats['ref_links_created']}")
            logger.info(f"  Bad features: {stats['bad_features']}")
            logger.info(f"  Bad references: {stats['bad_references']}")
            logger.info(f"  Errors: {stats['errors']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
