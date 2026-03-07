#!/usr/bin/env python3
"""
Update autosuggest suggestions.

This script updates the autosuggest data by running the data retrieval
script and posting the results to Solr.

Original: updateSuggestions (bash)
Converted to Python: 2024

Usage:
    python update_suggestions.py /path/to/cgd/root
    python update_suggestions.py /path/to/cgd/root --verbose
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def update_suggestions(root_dir: Path, verbose: bool = False) -> bool:
    """
    Update autosuggest suggestions.

    Args:
        root_dir: Root directory of the CGD installation
        verbose: Enable verbose output

    Returns:
        True if successful
    """
    # Define paths
    retrieve_script = root_dir / 'bin' / 'retrieveData.pl'
    xml_dir = root_dir / 'data' / 'autosuggest' / 'solr' / 'data' / 'xml'
    postem_script = xml_dir / 'postem'

    # Validate paths
    if not retrieve_script.exists():
        logger.error(f"retrieveData.pl not found: {retrieve_script}")
        return False

    if not xml_dir.exists():
        logger.error(f"XML directory not found: {xml_dir}")
        return False

    # Run retrieveData.pl
    logger.info(f"Running retrieveData.pl to {xml_dir}")
    result = subprocess.run(
        ['perl', str(retrieve_script), str(xml_dir)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        logger.error(f"retrieveData.pl failed: {result.stderr}")
        return False

    if verbose and result.stdout:
        print(result.stdout)

    # Change to xml directory and run postem
    original_dir = os.getcwd()
    try:
        os.chdir(xml_dir)

        if postem_script.exists():
            logger.info("Running postem to update Solr...")
            result = subprocess.run(
                [str(postem_script)],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                logger.error(f"postem failed: {result.stderr}")
                return False

            if verbose and result.stdout:
                print(result.stdout)
        else:
            logger.warning(f"postem script not found: {postem_script}")

    finally:
        os.chdir(original_dir)

    logger.info("Suggestions updated successfully")
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update autosuggest suggestions"
    )
    parser.add_argument(
        "root_dir",
        type=Path,
        help="Root directory of CGD installation",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    if not args.root_dir.exists():
        logger.error(f"Root directory not found: {args.root_dir}")
        sys.exit(1)

    success = update_suggestions(args.root_dir, args.verbose)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
