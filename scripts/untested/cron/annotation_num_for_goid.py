#!/usr/bin/env python3
"""
Calculate total annotation count for each GO ID.

This script calculates the number of gene annotations for each GO term,
including propagated annotations through the GO hierarchy (ancestors).

The output file contains tab-separated GO IDs and their annotation counts.

Environment Variables:
    DATABASE_URL: Database connection URL
    DATA_DIR: Directory to write output file (default: /tmp)
"""

import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Add parent directory to path to import cgd modules
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from cgd.db.engine import SessionLocal

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_annotated_genes(session) -> list[tuple[str, str]]:
    """
    Get all genes with GO annotations.

    Returns:
        List of (feature_name, gene_name) tuples
    """
    query = text("""
        SELECT DISTINCT f.feature_name, f.gene_name
        FROM go_annotation ga
        JOIN feature f ON ga.feature_no = f.feature_no
        WHERE f.feature_name IS NOT NULL
        ORDER BY f.feature_name
    """)

    result = session.execute(query)
    return [(row[0], row[1]) for row in result]


def get_goids_for_gene(session, feature_name: str) -> list[int]:
    """
    Get all GO IDs annotated to a gene.

    Args:
        session: Database session
        feature_name: Gene/feature name

    Returns:
        List of GO IDs (as integers)
    """
    query = text("""
        SELECT DISTINCT g.goid
        FROM go_annotation ga
        JOIN feature f ON ga.feature_no = f.feature_no
        JOIN go g ON ga.go_no = g.go_no
        WHERE UPPER(f.feature_name) = UPPER(:feature_name)
    """)

    result = session.execute(query, {"feature_name": feature_name})
    return [row[0] for row in result]


def get_all_ancestors(session, goid: int) -> list[int]:
    """
    Get all ancestor GO IDs for a given GO ID.

    Args:
        session: Database session
        goid: GO ID to find ancestors for

    Returns:
        List of ancestor GO IDs (including the input GO ID)
    """
    # Get the go_no for this goid
    go_no_query = text("SELECT go_no FROM go WHERE goid = :goid")
    result = session.execute(go_no_query, {"goid": goid}).first()

    if not result:
        return [goid]

    go_no = result[0]

    # Get all ancestors from go_path table
    ancestors_query = text("""
        SELECT DISTINCT g.goid
        FROM go_path gp
        JOIN go g ON gp.ancestor_go_no = g.go_no
        WHERE gp.child_go_no = :go_no
    """)

    result = session.execute(ancestors_query, {"go_no": go_no})
    ancestors = [row[0] for row in result]

    # Include the original goid
    if goid not in ancestors:
        ancestors.append(goid)

    return ancestors


def calculate_annotation_numbers() -> bool:
    """
    Calculate annotation counts for all GO IDs.

    Returns:
        True on success, False on failure
    """
    data_dir = Path(os.getenv("DATA_DIR", "/tmp"))
    output_file = data_dir / "annotationNum.list"

    logger.info("Starting GO annotation count calculation")

    # Dictionary to store annotation counts per GO ID
    annot_count_per_goid: dict[int, int] = defaultdict(int)

    # Cache for ancestor lookups
    ancestors_cache: dict[int, list[int]] = {}

    # Track gene-goid pairs we've already counted
    counted_pairs: set[tuple[str, int]] = set()

    try:
        with SessionLocal() as session:
            # Get all annotated genes
            logger.info("Retrieving annotated genes...")
            genes = get_annotated_genes(session)
            logger.info(f"Found {len(genes)} annotated genes")

            # Process each gene
            for i, (feature_name, gene_name) in enumerate(genes):
                if i % 100 == 0:
                    logger.info(f"Processing gene {i + 1}/{len(genes)}: {feature_name}")

                # Get GO IDs for this gene
                goids = get_goids_for_gene(session, feature_name)

                for goid in goids:
                    # Get ancestors (use cache if available)
                    if goid in ancestors_cache:
                        ancestors = ancestors_cache[goid]
                    else:
                        ancestors = get_all_ancestors(session, goid)
                        ancestors_cache[goid] = ancestors

                    # Count each ancestor (including the direct annotation)
                    for ancestor_goid in ancestors:
                        pair = (feature_name, ancestor_goid)
                        if pair not in counted_pairs:
                            annot_count_per_goid[ancestor_goid] += 1
                            counted_pairs.add(pair)

        # Write output file
        logger.info(f"Writing results to {output_file}")
        with open(output_file, "w") as f:
            for goid, count in sorted(annot_count_per_goid.items()):
                f.write(f"{goid}\t{count}\n")

        logger.info(f"Wrote {len(annot_count_per_goid)} GO term counts")
        return True

    except Exception as e:
        logger.exception(f"Error calculating annotation numbers: {e}")
        return False


def main() -> int:
    """Main entry point."""
    success = calculate_annotation_numbers()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
