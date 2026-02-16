#!/usr/bin/env python3
"""
Validate PCL (Pre-Cluster) file format for expression data.

PCL files are tab-delimited expression data files used for clustering analysis.
This script validates the structure and content of PCL files.

Expected PCL format:
- Header row: UID, NAME, GWEIGHT, [experiment columns...]
- EWEIGHT row: EWEIGHT values for each experiment
- Data rows: gene UID, description, gweight, expression values

Original Perl: validPcl.pl
Converted to Python: 2024

Usage:
    python valid_pcl.py params.txt
    python valid_pcl.py params.txt -v  # suppress missing value warnings
    python valid_pcl.py params.txt -g  # suppress missing gene warnings
    python valid_pcl.py params.txt -n  # suppress newline warnings
"""

import argparse
import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def is_number(value: str) -> bool:
    """Check if a string represents a number."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def read_params(param_file: Path) -> dict:
    """
    Read parameter file.

    Args:
        param_file: Path to parameter file

    Returns:
        Dict of parameters
    """
    params = {}

    with open(param_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            if '=' in line:
                name, value = line.split('=', 1)
                params[name] = value

    # Set defaults for clustering parameters
    params['r'] = 1 if params.get('r') else 0
    params['p'] = 1  # default Pearson

    gene = int(params.get('gene', 0))
    expt = int(params.get('expt', 0))

    if gene == 3 or expt == 3:
        params['p'] = 0  # Euclidean

    if gene != 3:
        params['g'] = gene
    else:
        params['g'] = 1

    if expt != 3:
        params['e'] = expt
    else:
        params['e'] = 1

    return params


def crack_description(
    desc: str,
    line_num: int,
    suppress_gene_warning: bool = False,
) -> tuple[int, int]:
    """
    Parse and validate the description field.

    Expected format: locusId||gene_name||dbxrefId||go_term

    Args:
        desc: Description string
        line_num: Current line number
        suppress_gene_warning: Suppress missing gene warnings

    Returns:
        Tuple of (warnings, errors)
    """
    warnings = 0
    errors = 0

    # Split by || delimiter
    annotations = desc.split('||')

    for i, token in enumerate(annotations, start=1):
        token = token.strip() if token else ''

        if i == 1:  # locusId - required
            if not token:
                logger.error(f"missing locusId line {line_num}")
                errors += 1
        elif i == 2:  # gene name - optional
            if not token and not suppress_gene_warning:
                logger.warning(f"missing gene name line {line_num}")
                warnings += 1
        elif i == 3:  # dbxrefId
            if not token:
                logger.warning(f"missing dbxrefId line {line_num}")
                warnings += 1
        elif i == 4:  # annotation/go term
            if not token:
                logger.warning(f"missing annotation/go term line {line_num}")
                warnings += 1

    return warnings, errors


def validate_pcl(
    pcl_file: Path,
    suppress_value_warning: bool = False,
    suppress_newline_warning: bool = False,
    suppress_gene_warning: bool = False,
) -> tuple[int, int]:
    """
    Validate a PCL file.

    Args:
        pcl_file: Path to PCL file
        suppress_value_warning: Suppress missing value warnings
        suppress_newline_warning: Suppress newline warnings
        suppress_gene_warning: Suppress missing gene warnings

    Returns:
        Tuple of (total_warnings, total_errors)
    """
    warnings = 0
    errors = 0
    saw_header = False
    saw_eweight = False
    n_columns = 0

    with open(pcl_file, 'r') as f:
        for count, line in enumerate(f, start=1):
            items = line.rstrip('\n\r').split('\t')

            if not saw_header:
                # Validate header row
                if items[0] and items[0].lower() != 'uid':
                    logger.warning("first column of header not 'uid'")
                    warnings += 1

                if len(items) > 1 and items[1] and items[1].lower() != 'name':
                    logger.warning("second column of header not 'name'")
                    warnings += 1

                if len(items) > 2 and items[2] and items[2].lower() != 'gweight':
                    logger.warning("third column of header not 'gweight'")
                    warnings += 1

                saw_header = True
                n_columns = len(items)

            elif not saw_eweight:
                # Validate EWEIGHT row
                if items[0].lower() != 'eweight':
                    logger.warning(f"'eweight' title missing line {count}")
                    warnings += 1

                # Check that eweight values are numeric
                for i in range(3, len(items)):
                    if items[i] and not is_number(items[i]):
                        logger.warning("non-numeric in eweight row")
                        warnings += 1
                        break

                saw_eweight = True

                if n_columns != len(items):
                    logger.error(f"wrong number of columns line {count}")
                    errors += 1

            else:
                # Normal data row
                # Check UID
                if not items[0] and items[0] != '0':
                    logger.error(f"missing uid line {count}")
                    errors += 1

                # Check name/description
                if len(items) < 2 or not items[1]:
                    logger.warning(f"missing name/description line {count}")
                    warnings += 1

                # Check gweight
                if len(items) < 3 or not items[2]:
                    logger.error(f"missing gweight line {count}")
                    errors += 1
                elif not is_number(items[2]):
                    logger.warning(f"non-numeric in gweight column line {count}")
                    warnings += 1

                # Check data values
                for i in range(3, len(items)):
                    if items[i]:
                        v = items[i]
                        if not is_number(v):
                            # Check for control characters (from original Perl)
                            if v not in ('\n', '\r'):
                                logger.error(
                                    f"non-numeric in line {count} column {i}, "
                                    f"value is '{v}'"
                                )
                                errors += 1
                            elif not suppress_newline_warning:
                                logger.warning(
                                    f"non-numeric in line {count} column {i}, "
                                    f"value is '{v}'"
                                )
                                warnings += 1
                    else:
                        if not suppress_value_warning:
                            logger.warning(f"missing value in line {count}")
                            warnings += 1

                # Check column count
                if n_columns != len(items):
                    logger.error(f"wrong number of columns line {count}")
                    errors += 1

                # Validate description field
                if len(items) > 1 and items[1]:
                    w, e = crack_description(
                        items[1], count, suppress_gene_warning
                    )
                    warnings += w
                    errors += e

    return warnings, errors


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate PCL file format for expression data"
    )
    parser.add_argument(
        "param_file",
        type=Path,
        help="Parameter file containing pclFile path",
    )
    parser.add_argument(
        "-v", "--suppress-value-warning",
        action="store_true",
        help="Suppress missing value warnings",
    )
    parser.add_argument(
        "-n", "--suppress-newline-warning",
        action="store_true",
        help="Suppress newline warnings",
    )
    parser.add_argument(
        "-g", "--suppress-gene-warning",
        action="store_true",
        help="Suppress missing gene warnings",
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    # Read parameters
    params = read_params(args.param_file)

    # Build PCL file path
    pcl_file = Path(f"{params['pclFile']}.pcl")

    if not pcl_file.exists():
        logger.error(f"Could not open {pcl_file}")
        sys.exit(1)

    # Validate
    warnings, errors = validate_pcl(
        pcl_file,
        suppress_value_warning=args.suppress_value_warning,
        suppress_newline_warning=args.suppress_newline_warning,
        suppress_gene_warning=args.suppress_gene_warning,
    )

    print(f"{warnings} warnings")
    print(f"{errors} errors")

    if errors > 0:
        print("exiting with errors")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
