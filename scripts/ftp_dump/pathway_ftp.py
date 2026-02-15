#!/usr/bin/env python3
"""
Dump biochemical pathways to FTP file.

This script connects to the Pathway Tools database and exports biochemical
pathway information including pathway names, reactions, EC numbers, genes,
and references.

Based on pathwayFTP.pl.

Usage:
    python pathway_ftp.py output_file
    python pathway_ftp.py /path/to/biochemical_pathways.tab
    python pathway_ftp.py --help

Arguments:
    output_file: Full path to the output file

Environment Variables:
    PATHWAY_DB: Pathway database name (default: calbi)
    PROJECT_ACRONYM: Project acronym (e.g., CGD)

Note:
    This script requires the pythoncyc library and a running Pathway Tools
    server with an accessible Unix socket at /tmp/ptools-socket.
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# Load environment variables
load_dotenv()

# Configuration from environment
PATHWAY_DB = os.getenv("PATHWAY_DB", "calbi")
PROJECT_ACRONYM = os.getenv("PROJECT_ACRONYM", "CGD")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def dictionary_order_key(item: str) -> str:
    """
    Return key for dictionary order sorting.

    Only letters, digits, and blanks are significant in comparisons.
    """
    # Remove non-alphanumeric characters except spaces
    cleaned = re.sub(r"[^\w\s]", "", item)
    return cleaned.upper()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Dump biochemical pathways to FTP file"
    )
    parser.add_argument(
        "output_file",
        type=Path,
        help="Full path to the output file",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    output_file = args.output_file

    # Check for Unix socket
    socket_path = Path("/tmp/ptools-socket")
    if not socket_path.exists():
        logger.error(f"Socket {socket_path} does not exist. Is Pathway Tools running?")
        return 1

    try:
        # Import pythoncyc (perlcyc equivalent for Python)
        try:
            import pythoncyc
        except ImportError:
            logger.error("pythoncyc library not installed. Install with: pip install pythoncyc")
            return 1

        # Connect to Pathway Tools
        logger.info(f"Connecting to Pathway Tools database: {PATHWAY_DB}")
        cyc = pythoncyc.select_organism(PATHWAY_DB)

        # Patterns to match
        ev_pattern = re.compile(r"EV")
        pwy_pattern = re.compile(r"PWY")

        # Get all pathways
        logger.info("Retrieving pathways...")
        pathways = cyc.all_pathways()
        logger.info(f"Found {len(pathways)} pathways")

        results = []

        for pathway in pathways:
            # Get references for pathway
            references = cyc.get_slot_values(pathway, "citations") or []

            ref_list = []
            for reference in references:
                if ev_pattern.search(str(reference)):
                    continue

                frame_ref = f"PUB-{reference}"

                # Get PubMed ID
                pmid = cyc.get_slot_value(frame_ref, "pubmed-id")
                if pmid:
                    ref_list.append(f"PMID:{pmid}")

                # Get URL (may contain reference ID)
                url = cyc.get_slot_value(frame_ref, "url")
                if url:
                    parts = url.split("=")
                    if len(parts) > 1:
                        ref_list.append(f"{PROJECT_ACRONYM}_REF:{parts[-1]}")

            ref_str = "|".join(ref_list)

            # Get pathway common name
            pathway_name = cyc.get_slot_value(pathway, "common-name") or pathway

            # Get all reactions
            reactions = cyc.get_slot_values(pathway, "reaction-list") or []

            for reaction in reactions:
                reaction_name = cyc.get_slot_value(reaction, "common-name") or ""

                # Check if reaction is actually a subpathway
                if pwy_pattern.search(str(reaction)):
                    # Subpathway
                    results.append(
                        f"{pathway_name}\t{reaction_name} (Pathway)\t\t\t"
                    )
                else:
                    # Regular reaction
                    ec = cyc.get_slot_value(reaction, "ec-number") or ""

                    # Get genes for this reaction
                    genes = cyc.genes_of_reaction(reaction) or []

                    if genes:
                        for gene in genes:
                            gene_name = cyc.get_slot_value(gene, "common-name") or gene
                            results.append(
                                f"{pathway_name}\t{reaction_name}\t{ec}\t{gene_name}\t{ref_str}"
                            )
                    elif reaction_name or ec:
                        results.append(
                            f"{pathway_name}\t{reaction_name}\t{ec}\t\t{ref_str}"
                        )

        # Sort results in dictionary order
        results.sort(key=dictionary_order_key)

        # Write output
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            f.write("\n".join(results) + "\n")

        logger.info(f"Wrote {len(results)} pathway entries to {output_file}")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
