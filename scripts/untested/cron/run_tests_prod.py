#!/usr/bin/env python3
"""
Run production tests.

This script runs the test suite for the CGD production environment.

Original: runTestsProd (bash)
Converted to Python: 2024

Usage:
    python run_tests_prod.py /path/to/cgd/root
    python run_tests_prod.py /path/to/cgd/root --verbose
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


def run_tests(root_dir: Path, verbose: bool = False) -> dict:
    """
    Run production tests.

    Args:
        root_dir: Root directory of the CGD installation
        verbose: Enable verbose output

    Returns:
        Dict with test results
    """
    results = {
        'runem': None,
        'config_modules': None,
        'success': True,
    }

    # Change to root directory
    original_dir = os.getcwd()
    os.chdir(root_dir)

    try:
        # Run runem.pl tests
        runem_script = root_dir / 't' / 'runem.pl'
        if runem_script.exists():
            logger.info("Running runem.pl tests...")
            cmd = ['perl', str(runem_script), '-l']
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
            )
            results['runem'] = result.returncode
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            if result.returncode != 0:
                results['success'] = False
                logger.error("runem.pl tests failed")
        else:
            logger.warning(f"Test script not found: {runem_script}")

        # Run config module tests
        config_script = root_dir / 't' / 'testConfigModules.pl'
        if config_script.exists():
            logger.info("Running testConfigModules.pl...")
            cmd = ['perl', str(config_script)]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
            )
            results['config_modules'] = result.returncode
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            if result.returncode != 0:
                results['success'] = False
                logger.error("testConfigModules.pl tests failed")
        else:
            logger.warning(f"Test script not found: {config_script}")

    finally:
        os.chdir(original_dir)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run CGD production tests"
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

    results = run_tests(args.root_dir, args.verbose)

    if results['success']:
        logger.info("All tests passed")
        sys.exit(0)
    else:
        logger.error("Some tests failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
