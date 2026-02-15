#!/usr/bin/env python3
"""
Compare CHEBI OBO files between versions.

This utility script compares two CHEBI OBO files to identify terms
where the ID has changed between versions. This helps identify
potential issues when updating to a new CHEBI release.

Output format (tab-delimited):
- Column 1: Term name
- Column 2: New ID
- Column 3: Old ID

Original Perl: checkChebi.pl
Author: Shuai Weng (Nov. 2005)
Converted to Python: 2024
"""

import argparse
import logging
import re
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


def parse_obo_file(filepath: Path) -> tuple[dict, dict, dict]:
    """
    Parse CHEBI OBO file and extract term/ID mappings.

    Args:
        filepath: Path to OBO file

    Returns:
        Tuple of (id_for_term, term_for_id, entry_for_term) dictionaries
    """
    id_for_term = {}
    term_for_id = {}
    entry_for_term = {}

    current_term = None
    current_id = None
    current_entry = []

    with open(filepath) as f:
        for line in f:
            # Remove carriage returns
            line = line.replace("\r", "")

            # Check for new ID (start of new term)
            if line.lower().startswith("id:"):
                # Save previous term if exists
                if current_term and current_id:
                    id_for_term[current_term] = current_id
                    term_for_id[current_id] = current_term
                    entry_for_term[current_term] = "".join(current_entry)

                # Reset for new term
                current_term = None
                current_id = None
                current_entry = []

            # Extract ID
            match = re.match(r"^id:\s*(.+)$", line, re.IGNORECASE)
            if match:
                current_id = match.group(1).strip()

            # Extract name
            match = re.match(r"^name:\s*(.+)$", line, re.IGNORECASE)
            if match:
                current_term = match.group(1).strip()

            # Collect entry lines
            if current_id:
                current_entry.append(line)

    # Save last term
    if current_term and current_id:
        id_for_term[current_term] = current_id
        term_for_id[current_id] = current_term
        entry_for_term[current_term] = "".join(current_entry)

    logger.info(f"Parsed {len(id_for_term)} terms from {filepath}")
    return id_for_term, term_for_id, entry_for_term


def compare_versions(
    old_id_for_term: dict,
    old_term_for_id: dict,
    new_id_for_term: dict,
    new_term_for_id: dict,
) -> list[dict]:
    """
    Compare term/ID mappings between versions.

    Args:
        old_id_for_term: Old version term->ID mapping
        old_term_for_id: Old version ID->term mapping
        new_id_for_term: New version term->ID mapping
        new_term_for_id: New version ID->term mapping

    Returns:
        List of change dictionaries
    """
    changes = []

    for term in sorted(new_id_for_term.keys()):
        new_id = new_id_for_term[term]
        old_id = old_id_for_term.get(term)

        if old_id and old_id != new_id:
            change = {
                "term": term,
                "new_id": new_id,
                "old_id": old_id,
                "notes": [],
            }

            # Check what the new ID was associated with in old version
            if new_id in old_term_for_id:
                old_term_for_new_id = old_term_for_id[new_id]
                change["notes"].append(
                    f"New ID ({new_id}) was associated with "
                    f"'{old_term_for_new_id}' in old version"
                )

            # Check what the old ID is now associated with in new version
            if old_id in new_term_for_id:
                new_term_for_old_id = new_term_for_id[old_id]
                old_term_for_old_id = old_term_for_id.get(old_id, "unknown")
                change["notes"].append(
                    f"Old ID ({old_id}) for term '{old_term_for_old_id}' "
                    f"is now associated with '{new_term_for_old_id}' in new version"
                )

            changes.append(change)

    return changes


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Compare CHEBI OBO files between versions",
        epilog="Example: check_chebi.py chebi.obo.1.14 chebi.obo.1.15 changes.tsv",
    )
    parser.add_argument(
        "old_file",
        type=Path,
        help="Old version CHEBI OBO file",
    )
    parser.add_argument(
        "new_file",
        type=Path,
        help="New version CHEBI OBO file",
    )
    parser.add_argument(
        "output_file",
        type=Path,
        help="Output file for ID changes",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Validate input files
    if not args.old_file.exists():
        logger.error(f"Old file not found: {args.old_file}")
        sys.exit(1)

    if not args.new_file.exists():
        logger.error(f"New file not found: {args.new_file}")
        sys.exit(1)

    logger.info(f"Old file: {args.old_file}")
    logger.info(f"New file: {args.new_file}")
    logger.info(f"Output file: {args.output_file}")

    # Parse both files
    old_id_for_term, old_term_for_id, _ = parse_obo_file(args.old_file)
    new_id_for_term, new_term_for_id, _ = parse_obo_file(args.new_file)

    # Compare versions
    changes = compare_versions(
        old_id_for_term, old_term_for_id,
        new_id_for_term, new_term_for_id,
    )

    # Write output
    args.output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(args.output_file, "w") as f:
        f.write("Term\tNew ID\tOld ID\n")
        for change in changes:
            f.write(f"{change['term']}\t{change['new_id']}\t{change['old_id']}\n")

            # Print notes to console
            for note in change["notes"]:
                logger.info(note)

    logger.info("=" * 50)
    logger.info(f"Found {len(changes)} terms with changed IDs")
    logger.info(f"Results written to: {args.output_file}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
