#!/usr/bin/env python3
"""
Load expression dataset into database.

This script loads expression data from CDT files into the
dataset, dataset_sample, dataset_display, and sample_value tables.

Original Perl: loadDataset.pl
Converted to Python: 2024
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from cgd.db.engine import SessionLocal
from cgd.models.models import Dataset, DatasetDisplay, DatasetSample, Feature, SampleValue

load_dotenv()

logger = logging.getLogger(__name__)


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


def load_dataset_params(
    session: Session,
    param_file: Path,
    dataset_type: str,
    created_by: str,
) -> tuple:
    """
    Load dataset parameters and create dataset entry.

    Args:
        session: Database session
        param_file: Parameter file path
        dataset_type: Dataset type (e.g., 'Expression connection')
        created_by: User name for audit

    Returns:
        Tuple of (dataset_no, params_dict)
    """
    params = {}
    name = None
    source = None

    with open(param_file) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t', 1)
            if len(parts) < 2:
                continue

            param, value = parts

            if line_num == 1 and param == 'name':
                name = value
            elif line_num == 2 and param == 'source':
                source = value
            else:
                params[param] = value

    if not name or not source:
        raise ValueError("Parameter file must have 'name' and 'source' as first two lines")

    # Create dataset
    dataset = Dataset(
        dataset_name=name,
        source=source,
        dataset_type=dataset_type,
        created_by=created_by,
    )
    session.add(dataset)
    session.flush()

    logger.info(f"Created dataset: {name} (dataset_no={dataset.dataset_no})")

    # Create dataset_display entries
    for param, value in params.items():
        display = DatasetDisplay(
            dataset_no=dataset.dataset_no,
            display_parameter=param,
            parameter_value=value,
        )
        session.add(display)
        logger.debug(f"Added display param: {param}={value}")

    session.flush()

    return dataset.dataset_no, params


def load_cdt_data(
    session: Session,
    data_file: Path,
    dataset_no: int,
    is_graphed: bool,
    created_by: str,
    feature_map: dict,
) -> dict:
    """
    Load CDT data file.

    Args:
        session: Database session
        data_file: CDT data file
        dataset_no: Dataset number
        is_graphed: Whether dataset has line graph
        created_by: User name for audit
        feature_map: Dict mapping feature_name to feature_no

    Returns:
        Statistics dict
    """
    stats = {
        'samples': 0,
        'values': 0,
        'feature_errors': 0,
        'missing_values': 0,
    }

    position_to_sample_no = {}

    with open(data_file) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            cols = line.split('\t')

            if line_num % 1000 == 0:
                logger.debug(f"Processing line {line_num}")

            if line_num == 1:
                # Header row - create samples
                for col_pos, col_value in enumerate(cols):
                    if col_pos < 4:
                        continue

                    sample_order = col_pos - 4 + 1
                    sample_name = col_value
                    unit = col_value

                    if is_graphed:
                        # Extract numeric part as sample name
                        sample_name = re.sub(r'[A-Za-z]', '', sample_name)
                        sample_name = sample_name.strip()

                        # Extract unit part
                        unit = re.sub(r'\d', '', unit)
                        unit = re.sub(r'^\.', '', unit)
                        unit = ' '.join(unit.split())

                    sample = DatasetSample(
                        dataset_no=dataset_no,
                        sample_name=sample_name,
                        sample_order=sample_order,
                        is_on_graph='Y' if is_graphed else 'N',
                        sample_unit=unit if is_graphed else None,
                        created_by=created_by,
                    )
                    session.add(sample)
                    session.flush()

                    position_to_sample_no[sample_order] = sample.dataset_sample_no
                    stats['samples'] += 1

                logger.info(f"Created {stats['samples']} samples")

            else:
                # Data rows
                feat_no = None

                for col_pos, col_value in enumerate(cols):
                    # Skip columns 0, 2, 3 (array location, gene name, weight)
                    if col_pos in (0, 2, 3):
                        continue

                    if col_pos == 1:
                        # ORF name column
                        orf = col_value.upper()
                        feat_no = feature_map.get(orf)
                        if not feat_no:
                            stats['feature_errors'] += 1
                            break

                    else:
                        # Data columns (col_pos >= 4)
                        sample_order = col_pos - 4 + 1
                        sample_no = position_to_sample_no.get(sample_order)

                        if not sample_no:
                            continue

                        if not col_value:
                            stats['missing_values'] += 1
                            continue

                        try:
                            sample_value = SampleValue(
                                dataset_sample_no=sample_no,
                                sample_value=float(col_value),
                                feature_no=feat_no,
                            )
                            session.add(sample_value)
                            stats['values'] += 1

                        except ValueError:
                            stats['missing_values'] += 1

                # Flush periodically
                if line_num % 1000 == 0:
                    session.flush()

    session.flush()
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load expression dataset into database"
    )
    parser.add_argument(
        "--param-file",
        type=Path,
        required=True,
        help="Parameter file with dataset name, source, and display params",
    )
    parser.add_argument(
        "--data-file",
        type=Path,
        required=True,
        help="CDT data file",
    )
    parser.add_argument(
        "--dataset-type",
        default="Expression connection",
        help="Dataset type (default: 'Expression connection')",
    )
    parser.add_argument(
        "--graphed",
        action="store_true",
        help="Dataset has line graph",
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
    if not args.param_file.exists():
        logger.error(f"Parameter file not found: {args.param_file}")
        sys.exit(1)

    if not args.data_file.exists():
        logger.error(f"Data file not found: {args.data_file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN - no database modifications")

    try:
        with SessionLocal() as session:
            # Get feature mapping
            feature_map = get_feature_map(session)
            logger.info(f"Loaded {len(feature_map)} features")

            # Load parameters and create dataset
            dataset_no, params = load_dataset_params(
                session,
                args.param_file,
                args.dataset_type,
                args.created_by,
            )

            # Load data
            stats = load_cdt_data(
                session,
                args.data_file,
                dataset_no,
                args.graphed,
                args.created_by,
                feature_map,
            )

            if not args.dry_run:
                session.commit()
                logger.info("Transaction committed")
            else:
                session.rollback()
                logger.info("Transaction rolled back (dry run)")

            logger.info("=" * 50)
            logger.info("Summary:")
            logger.info(f"  Dataset no: {dataset_no}")
            logger.info(f"  Samples created: {stats['samples']}")
            logger.info(f"  Values loaded: {stats['values']}")
            logger.info(f"  Feature errors: {stats['feature_errors']}")
            logger.info(f"  Missing values: {stats['missing_values']}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

    logger.info(f"Completed at {datetime.now()}")


if __name__ == "__main__":
    main()
