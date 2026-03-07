#!/usr/bin/env python3
"""
Bulk load phenotype data into the database.

This script loads phenotype data into the following tables:
- PHENOTYPE: Phenotype definitions
- PHENO_ANNOTATION: Feature-phenotype associations
- EXPERIMENT: Experiment records
- EXPT_PROPERTY: Experiment properties
- EXPT_EXPTPROP: Links experiments to properties
- REF_LINK: Reference associations

Input file format (tab-delimited with header row):
Column  Field
1       feature_name
2       gene_name
3       CGDID
4       experiment_type
5       mutant_type
6       observable
7       qualifier
8       allele
9       strain_background
10      chebi_ontology
11      chemical_pending
12      condition
13      details
14      reporter
15      virulence_model
16      numerical_value
17      fungal_anatomy_ontology
18      strain_name
19      pubmed
20      experiment_comment

Original Perl: bulkLoadPhenotype.pl
Author: Shuai Weng, updated by Prachi Shah (December 2009)
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
from cgd.models.models import (
    CvTerm,
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
DEFAULT_SOURCE = os.getenv("PROJECT_ACRONYM", "CGD")

# Experiment property types (matching column names)
PROPERTY_TYPES = [
    "Allele",
    "strain_background",
    "chebi_ontology",
    "Chemical_pending",
    "Condition",
    "Details",
    "Reporter",
    "virulence_model",
    "Numerical_value",
    "fungal_anatomy_ontology",
    "strain_name",
]


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


def clean_text(text: str | None) -> str | None:
    """Clean text by removing unwanted characters."""
    if not text:
        return None
    # Remove leading/trailing whitespace
    text = text.strip()
    # Remove non-printable characters
    text = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", text)
    return text if text else None


def get_feature(session: Session, feature_name: str = None, dbxref_id: str = None) -> Feature | None:
    """Get feature by name or CGDID."""
    if feature_name:
        feature = session.query(Feature).filter(
            Feature.feature_name == feature_name
        ).first()
        if feature:
            return feature

    if dbxref_id:
        feature = session.query(Feature).filter(
            Feature.dbxref_id == dbxref_id
        ).first()
        if feature:
            return feature

    return None


def get_reference_by_pubmed(session: Session, pubmed: int) -> Reference | None:
    """Get reference by PubMed ID."""
    return session.query(Reference).filter(
        Reference.pubmed == pubmed
    ).first()


def get_chebi_term_name(session: Session, chebi_id: str) -> str | None:
    """Look up CHEBI term name from CV_TERM table."""
    if not chebi_id:
        return None

    # Extract CHEBI:xxxxx format
    match = re.match(r"(CHEBI:\d+)", chebi_id)
    if not match:
        return None

    chebi_id = match.group(1)

    cv_term = session.query(CvTerm).filter(
        CvTerm.dbxref_id == chebi_id
    ).first()

    return cv_term.term_name if cv_term else None


def get_or_create_phenotype(
    session: Session,
    source: str,
    experiment_type: str,
    mutant_type: str,
    observable: str,
    qualifier: str | None,
    created_by: str,
) -> int | None:
    """Get existing phenotype or create new one."""
    # Build query based on whether qualifier is present
    query = session.query(Phenotype).filter(
        and_(
            Phenotype.source == source,
            Phenotype.experiment_type == experiment_type,
            Phenotype.mutant_type == mutant_type,
            Phenotype.observable == observable,
        )
    )

    if qualifier:
        query = query.filter(Phenotype.qualifier == qualifier)
    else:
        query = query.filter(Phenotype.qualifier.is_(None))

    existing = query.first()
    if existing:
        return existing.phenotype_no

    # Create new phenotype
    new_pheno = Phenotype(
        source=source,
        experiment_type=experiment_type,
        mutant_type=mutant_type,
        observable=observable,
        qualifier=qualifier,
        created_by=created_by[:12],
    )
    session.add(new_pheno)
    session.flush()

    logger.debug(f"Created phenotype: {experiment_type}, {mutant_type}, {observable}")
    return new_pheno.phenotype_no


def get_or_create_expt_property(
    session: Session,
    property_type: str,
    property_value: str,
    property_description: str | None,
    created_by: str,
) -> int | None:
    """Get existing experiment property or create new one."""
    query = session.query(ExptProperty).filter(
        and_(
            ExptProperty.property_type == property_type,
            ExptProperty.property_value == property_value,
        )
    )

    if property_description:
        query = query.filter(ExptProperty.property_description == property_description)
    else:
        query = query.filter(ExptProperty.property_description.is_(None))

    existing = query.first()
    if existing:
        return existing.expt_property_no

    # Create new property
    new_prop = ExptProperty(
        property_type=property_type,
        property_value=property_value,
        property_description=property_description,
        created_by=created_by[:12],
    )
    session.add(new_prop)
    session.flush()

    logger.debug(f"Created expt_property: {property_type}={property_value}")
    return new_prop.expt_property_no


def create_experiment(
    session: Session,
    source: str,
    experiment_comment: str | None,
    created_by: str,
) -> int:
    """Create a new experiment record."""
    new_expt = Experiment(
        source=source,
        experiment_comment=experiment_comment,
        created_by=created_by[:12],
    )
    session.add(new_expt)
    session.flush()

    logger.debug(f"Created experiment: {new_expt.experiment_no}")
    return new_expt.experiment_no


def create_expt_exptprop(
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
        new_link = ExptExptprop(
            experiment_no=experiment_no,
            expt_property_no=expt_property_no,
        )
        session.add(new_link)


def get_or_create_pheno_annotation(
    session: Session,
    feature_no: int,
    phenotype_no: int,
    experiment_no: int,
    created_by: str,
) -> int | None:
    """Get existing pheno_annotation or create new one."""
    existing = session.query(PhenoAnnotation).filter(
        and_(
            PhenoAnnotation.feature_no == feature_no,
            PhenoAnnotation.phenotype_no == phenotype_no,
            PhenoAnnotation.experiment_no == experiment_no,
        )
    ).first()

    if existing:
        return existing.pheno_annotation_no

    new_pa = PhenoAnnotation(
        feature_no=feature_no,
        phenotype_no=phenotype_no,
        experiment_no=experiment_no,
        created_by=created_by[:12],
    )
    session.add(new_pa)
    session.flush()

    logger.debug(f"Created pheno_annotation: {new_pa.pheno_annotation_no}")
    return new_pa.pheno_annotation_no


def create_ref_link_if_not_exists(
    session: Session,
    reference_no: int,
    pheno_annotation_no: int,
    created_by: str,
) -> bool:
    """Create REF_LINK entry if it doesn't exist."""
    existing = session.query(RefLink).filter(
        and_(
            RefLink.tab_name == "PHENO_ANNOTATION",
            RefLink.col_name == "PHENO_ANNOTATION_NO",
            RefLink.reference_no == reference_no,
            RefLink.primary_key == pheno_annotation_no,
        )
    ).first()

    if existing:
        return False

    new_link = RefLink(
        reference_no=reference_no,
        tab_name="PHENO_ANNOTATION",
        col_name="PHENO_ANNOTATION_NO",
        primary_key=pheno_annotation_no,
        created_by=created_by[:12],
    )
    session.add(new_link)
    return True


def parse_input_file(filepath: Path) -> list[dict]:
    """Parse the phenotype input file."""
    entries = []

    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            # Skip header
            if line_num == 1 or line.lower().startswith("feature_n"):
                continue

            line = line.strip()
            if not line:
                continue

            cols = line.split("\t")

            # Pad columns to expected length
            while len(cols) < 20:
                cols.append("")

            entry = {
                "line_num": line_num,
                "raw_line": line,
                "feature_name": clean_text(cols[0]),
                "gene_name": clean_text(cols[1]),
                "cgdid": clean_text(cols[2]),
                "experiment_type": clean_text(cols[3]),
                "mutant_type": clean_text(cols[4]),
                "observable": clean_text(cols[5]),
                "qualifier": clean_text(cols[6]),
                "properties": {
                    "Allele": clean_text(cols[7]),
                    "strain_background": clean_text(cols[8]),
                    "chebi_ontology": clean_text(cols[9]),
                    "Chemical_pending": clean_text(cols[10]),
                    "Condition": clean_text(cols[11]),
                    "Details": clean_text(cols[12]),
                    "Reporter": clean_text(cols[13]),
                    "virulence_model": clean_text(cols[14]),
                    "Numerical_value": clean_text(cols[15]),
                    "fungal_anatomy_ontology": clean_text(cols[16]),
                    "strain_name": clean_text(cols[17]),
                },
                "pubmed": clean_text(cols[18]),
                "experiment_comment": clean_text(cols[19]) if len(cols) > 19 else None,
            }

            # Handle "(none)" qualifier
            if entry["qualifier"] == "(none)":
                entry["qualifier"] = None

            entries.append(entry)

    logger.info(f"Parsed {len(entries)} entries from input file")
    return entries


def process_entry(
    session: Session,
    entry: dict,
    source: str,
    created_by: str,
) -> tuple[bool, str | None]:
    """
    Process a single phenotype entry.

    Returns:
        Tuple of (success, error_message)
    """
    # Validate required fields
    feat_name = entry["feature_name"]
    cgdid = entry["cgdid"]
    pubmed_str = entry["pubmed"]
    expt_type = entry["experiment_type"]
    mutant_type = entry["mutant_type"]
    observable = entry["observable"]

    if not (feat_name or cgdid):
        return False, "No feature_name or CGDID"

    if not pubmed_str:
        return False, "No PubMed ID"

    if not all([expt_type, mutant_type, observable]):
        return False, "Missing experiment_type, mutant_type, or observable"

    # Parse pubmed
    try:
        pubmed = int(re.search(r"\d+", pubmed_str).group())
    except (AttributeError, ValueError):
        return False, f"Invalid PubMed ID: {pubmed_str}"

    # Get feature
    feature = get_feature(session, feat_name, cgdid)
    if not feature:
        return False, f"Feature not found: {feat_name or cgdid}"

    # Get reference
    reference = get_reference_by_pubmed(session, pubmed)
    if not reference:
        return False, f"Reference not found for PubMed: {pubmed}"

    # Handle CHEBI lookup
    properties = entry["properties"].copy()
    if properties.get("chebi_ontology"):
        chebi_term = get_chebi_term_name(session, properties["chebi_ontology"])
        if not chebi_term:
            return False, f"CHEBI term not found: {properties['chebi_ontology']}"
        properties["chebi_ontology"] = chebi_term

    # Create/get phenotype
    phenotype_no = get_or_create_phenotype(
        session,
        source,
        expt_type,
        mutant_type,
        observable,
        entry["qualifier"],
        created_by,
    )
    if not phenotype_no:
        return False, "Failed to create phenotype"

    # Create experiment properties
    expt_prop_nos = []
    for prop_type, prop_value in properties.items():
        if not prop_value:
            continue

        # Handle pipe-separated value|description format
        prop_desc = None
        if "|" in prop_value:
            prop_value, prop_desc = prop_value.split("|", 1)

        expt_prop_no = get_or_create_expt_property(
            session, prop_type, prop_value, prop_desc, created_by
        )
        if expt_prop_no:
            expt_prop_nos.append(expt_prop_no)

    # Need at least one property
    if not expt_prop_nos:
        return False, "No experiment properties"

    # Create experiment
    experiment_no = create_experiment(
        session, source, entry["experiment_comment"], created_by
    )

    # Link experiment to properties
    for expt_prop_no in expt_prop_nos:
        create_expt_exptprop(session, experiment_no, expt_prop_no)

    # Create pheno_annotation
    pheno_annot_no = get_or_create_pheno_annotation(
        session,
        feature.feature_no,
        phenotype_no,
        experiment_no,
        created_by,
    )
    if not pheno_annot_no:
        return False, "Failed to create pheno_annotation"

    # Create ref_link
    create_ref_link_if_not_exists(
        session, reference.reference_no, pheno_annot_no, created_by
    )

    return True, None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Bulk load phenotype data into the database"
    )
    parser.add_argument(
        "created_by",
        help="Database user name",
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Input data file (tab-delimited)",
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Source for phenotype records (default: {DEFAULT_SOURCE})",
    )
    parser.add_argument(
        "--unloaded-file",
        type=Path,
        help="File to write unloaded rows",
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
    if not args.input_file.exists():
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Created by: {args.created_by}")
    logger.info(f"Source: {args.source}")

    # Parse input file
    entries = parse_input_file(args.input_file)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")
        logger.info(f"Would process {len(entries)} entries")
        return

    # Open unloaded file if specified
    unloaded_fh = None
    if args.unloaded_file:
        unloaded_fh = open(args.unloaded_file, "w")

    try:
        with SessionLocal() as session:
            stats = {
                "good_rows": 0,
                "bad_rows": 0,
            }

            for entry in entries:
                success, error = process_entry(
                    session,
                    entry,
                    args.source,
                    args.created_by,
                )

                if success:
                    stats["good_rows"] += 1
                    session.commit()
                else:
                    stats["bad_rows"] += 1
                    logger.warning(f"Line {entry['line_num']}: {error}")
                    if unloaded_fh:
                        unloaded_fh.write(entry["raw_line"] + "\n")
                    session.rollback()

            logger.info("=" * 50)
            logger.info("Load Summary:")
            logger.info(f"  Successful rows: {stats['good_rows']}")
            logger.info(f"  Failed rows: {stats['bad_rows']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error loading phenotype data: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)
    finally:
        if unloaded_fh:
            unloaded_fh.close()


if __name__ == "__main__":
    main()
