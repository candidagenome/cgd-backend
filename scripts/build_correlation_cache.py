#!/usr/bin/env python3
"""
Build pre-computed correlation cache for the Similar Expression Genes feature.

This script computes pairwise Pearson correlations between all genes
and saves them in a format optimized for fast retrieval.

Usage:
    python scripts/build_correlation_cache.py [--organism ORGANISM]

The output is stored at /data/cache/expression/correlations_{organism}.npz
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List

import numpy as np
from scipy.stats import pearsonr

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cache directory
EXPRESSION_CACHE_DIR = Path("/data/cache/expression")

# Organisms to process
ORGANISMS = [
    "C_albicans_SC5314",
    "C_auris_B8441",
    "C_glabrata_CBS138",
    "C_dubliniensis_CD36",
    "C_parapsilosis_CDC317",
]


def load_profiles(organism_key: str) -> Dict[str, Dict[str, float]]:
    """Load expression profiles from JSON cache."""
    cache_file = EXPRESSION_CACHE_DIR / f"expression_profiles_{organism_key}.json"

    if not cache_file.exists():
        logger.error(f"Cache file not found: {cache_file}")
        return {}

    logger.info(f"Loading profiles from {cache_file}")
    with open(cache_file, 'r') as f:
        profiles = json.load(f)

    logger.info(f"Loaded {len(profiles)} gene profiles")
    return profiles


def build_correlation_matrix(
    profiles: Dict[str, Dict[str, float]],
    min_conditions: int = 5
) -> tuple:
    """
    Build correlation matrix for all genes.

    Returns:
        (gene_names, correlation_matrix, condition_counts)
    """
    # Get sorted list of gene names
    gene_names = sorted(profiles.keys())
    n_genes = len(gene_names)

    logger.info(f"Building correlation matrix for {n_genes} genes")

    # Get all unique conditions
    all_conditions = set()
    for profile in profiles.values():
        all_conditions.update(profile.keys())
    conditions = sorted(all_conditions)
    n_conditions = len(conditions)

    logger.info(f"Total conditions: {n_conditions}")

    # Build expression matrix (genes x conditions)
    # Use NaN for missing values
    expr_matrix = np.full((n_genes, n_conditions), np.nan)
    condition_to_idx = {c: i for i, c in enumerate(conditions)}

    for i, gene in enumerate(gene_names):
        profile = profiles[gene]
        for cond, fc in profile.items():
            j = condition_to_idx[cond]
            expr_matrix[i, j] = fc

    logger.info("Built expression matrix")

    # Pre-compute correlations for top N genes to each gene
    # For efficiency, we'll store only the top 100 correlations per gene
    TOP_N = 100

    # Initialize storage
    top_correlations = np.zeros((n_genes, TOP_N), dtype=np.float32)
    top_indices = np.zeros((n_genes, TOP_N), dtype=np.int32)
    top_pvalues = np.zeros((n_genes, TOP_N), dtype=np.float32)
    top_shared = np.zeros((n_genes, TOP_N), dtype=np.int16)

    start_time = time.time()

    for i in range(n_genes):
        if i % 500 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            logger.info(f"Processing gene {i}/{n_genes} ({rate:.1f} genes/sec)")

        gene_i = expr_matrix[i]
        valid_i = ~np.isnan(gene_i)

        correlations = []

        for j in range(n_genes):
            if i == j:
                continue

            gene_j = expr_matrix[j]
            valid_j = ~np.isnan(gene_j)

            # Find shared conditions (both genes have values)
            shared = valid_i & valid_j
            n_shared = np.sum(shared)

            if n_shared < min_conditions:
                continue

            # Get values for shared conditions
            vals_i = gene_i[shared]
            vals_j = gene_j[shared]

            # Compute correlation
            try:
                corr, pval = pearsonr(vals_i, vals_j)
                if not np.isnan(corr):
                    correlations.append((j, corr, pval, n_shared))
            except Exception:
                pass

        # Sort by correlation (descending) and keep top N
        correlations.sort(key=lambda x: x[1], reverse=True)
        top = correlations[:TOP_N]

        for k, (j, corr, pval, n_shared) in enumerate(top):
            top_indices[i, k] = j
            top_correlations[i, k] = corr
            top_pvalues[i, k] = pval
            top_shared[i, k] = n_shared

    elapsed = time.time() - start_time
    logger.info(f"Computed correlations in {elapsed:.1f} seconds")

    return gene_names, top_indices, top_correlations, top_pvalues, top_shared


def save_correlation_cache(
    organism_key: str,
    gene_names: List[str],
    top_indices: np.ndarray,
    top_correlations: np.ndarray,
    top_pvalues: np.ndarray,
    top_shared: np.ndarray
) -> None:
    """Save correlation cache to disk."""
    output_file = EXPRESSION_CACHE_DIR / f"correlations_{organism_key}.npz"

    # Save numpy arrays
    np.savez_compressed(
        output_file,
        gene_names=np.array(gene_names),
        top_indices=top_indices,
        top_correlations=top_correlations,
        top_pvalues=top_pvalues,
        top_shared=top_shared
    )

    size_mb = output_file.stat().st_size / (1024 * 1024)
    logger.info(f"Saved correlation cache to {output_file} ({size_mb:.1f} MB)")

    # Also save a JSON index for gene name -> index lookup
    index_file = EXPRESSION_CACHE_DIR / f"gene_index_{organism_key}.json"
    gene_index = {name: i for i, name in enumerate(gene_names)}

    with open(index_file, 'w') as f:
        json.dump(gene_index, f)

    logger.info(f"Saved gene index to {index_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Build correlation cache for similar genes"
    )
    parser.add_argument(
        "--organism",
        type=str,
        help="Specific organism to build (e.g., C_albicans_SC5314)"
    )
    args = parser.parse_args()

    organisms = [args.organism] if args.organism else ORGANISMS

    for organism_key in organisms:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {organism_key}")
        logger.info(f"{'='*60}")

        # Load profiles
        profiles = load_profiles(organism_key)
        if not profiles:
            logger.warning(f"No profiles found for {organism_key}, skipping")
            continue

        # Build correlation matrix
        gene_names, top_indices, top_corrs, top_pvals, top_shared = \
            build_correlation_matrix(profiles)

        # Save cache
        save_correlation_cache(
            organism_key,
            gene_names,
            top_indices,
            top_corrs,
            top_pvals,
            top_shared
        )

    logger.info("\nCorrelation cache building complete!")


if __name__ == "__main__":
    main()
