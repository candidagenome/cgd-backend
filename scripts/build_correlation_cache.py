#!/usr/bin/env python3
"""
Build pre-computed correlation cache for the Similar Expression Genes feature.

Computes Pearson, Spearman, and Cosine correlations for all gene pairs.

Usage:
    python scripts/build_correlation_cache.py [--organism ORGANISM] [--metric METRIC]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
from numpy import ma
from scipy.stats import pearsonr, spearmanr
from scipy.spatial.distance import cosine as cosine_distance

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

# Supported metrics
METRICS = ["pearson", "spearman", "cosine"]


def load_profiles(organism_key: str) -> dict:
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


def compute_correlation_row(
    gene_idx: int,
    expr_matrix: np.ndarray,
    valid_mask: np.ndarray,
    metric: str = "pearson",
    min_shared: int = 5
) -> list:
    """Compute correlations for one gene against all others.

    Args:
        gene_idx: Index of the query gene
        expr_matrix: Expression matrix (genes x conditions)
        valid_mask: Boolean mask of valid values
        metric: 'pearson', 'spearman', or 'cosine'
        min_shared: Minimum shared conditions required
    """
    n_genes = expr_matrix.shape[0]
    gene_i = expr_matrix[gene_idx]
    valid_i = valid_mask[gene_idx]

    results = []

    # For each target gene, compute correlation
    for j in range(n_genes):
        if j == gene_idx:
            continue

        gene_j = expr_matrix[j]
        valid_j = valid_mask[j]

        # Find shared valid indices
        shared_mask = valid_i & valid_j
        n_shared = np.sum(shared_mask)

        if n_shared < min_shared:
            continue

        # Get values for shared conditions
        x = gene_i[shared_mask]
        y = gene_j[shared_mask]

        try:
            if metric == "pearson":
                # Compute Pearson correlation manually for speed
                x_mean = x.mean()
                y_mean = y.mean()
                x_centered = x - x_mean
                y_centered = y - y_mean

                numerator = (x_centered * y_centered).sum()
                x_var = (x_centered ** 2).sum()
                y_var = (y_centered ** 2).sum()

                if x_var == 0 or y_var == 0:
                    continue

                corr = numerator / np.sqrt(x_var * y_var)

                # Approximate p-value
                if abs(corr) < 0.9999 and n_shared > 2:
                    t_stat = corr * np.sqrt((n_shared - 2) / (1 - corr ** 2))
                    pval = max(2 * np.exp(-0.5 * t_stat ** 2), 1e-10)
                else:
                    pval = 1e-10

            elif metric == "spearman":
                corr, pval = spearmanr(x, y)
                if pval is None or not np.isfinite(pval):
                    pval = 1e-10

            elif metric == "cosine":
                # Cosine similarity = 1 - cosine distance
                corr = 1.0 - cosine_distance(x, y)
                pval = None  # No p-value for cosine

            else:
                raise ValueError(f"Unknown metric: {metric}")

        except Exception:
            continue

        if not np.isfinite(corr):
            continue

        # For cosine, use a placeholder p-value
        if pval is None:
            pval = 0.0

        results.append((j, float(corr), float(pval), int(n_shared)))

    return results


def build_correlation_cache_parallel(
    profiles: dict,
    metric: str = "pearson",
    top_n: int = 100,
    min_shared: int = 5
) -> tuple:
    """Build correlation cache for a given metric.

    Args:
        profiles: Dict of gene_name -> {condition_id: fold_change}
        metric: 'pearson', 'spearman', or 'cosine'
        top_n: Number of top correlations to store per gene
        min_shared: Minimum shared conditions required
    """
    # Get sorted list of gene names
    gene_names = sorted(profiles.keys())
    n_genes = len(gene_names)

    logger.info(f"Building {metric} correlation matrix for {n_genes} genes")

    # Get all unique conditions
    all_conditions = set()
    for profile in profiles.values():
        all_conditions.update(profile.keys())
    conditions = sorted(all_conditions)
    n_conditions = len(conditions)

    logger.info(f"Total conditions: {n_conditions}")

    # Build expression matrix (genes x conditions)
    expr_matrix = np.full((n_genes, n_conditions), np.nan, dtype=np.float64)
    condition_to_idx = {c: i for i, c in enumerate(conditions)}

    for i, gene in enumerate(gene_names):
        profile = profiles[gene]
        for cond, fc in profile.items():
            j = condition_to_idx[cond]
            expr_matrix[i, j] = fc

    # Create valid mask
    valid_mask = ~np.isnan(expr_matrix)

    logger.info(f"Built expression matrix, computing {metric} correlations...")

    # Initialize storage for top N correlations per gene
    top_indices = np.zeros((n_genes, top_n), dtype=np.int32)
    top_correlations = np.zeros((n_genes, top_n), dtype=np.float32)
    top_pvalues = np.ones((n_genes, top_n), dtype=np.float32)
    top_shared = np.zeros((n_genes, top_n), dtype=np.int16)

    start_time = time.time()

    # Process genes
    for i in range(n_genes):
        if i % 500 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta = (n_genes - i) / rate if rate > 0 else 0
            logger.info(f"Processing gene {i}/{n_genes} ({rate:.1f}/sec, ETA: {eta/60:.1f} min)")

        # Compute correlations for this gene
        correlations = compute_correlation_row(i, expr_matrix, valid_mask, metric, min_shared)

        # Sort by correlation (descending) and keep top N
        correlations.sort(key=lambda x: x[1], reverse=True)
        top = correlations[:top_n]

        for k, (j, corr, pval, n_shared) in enumerate(top):
            top_indices[i, k] = j
            top_correlations[i, k] = corr
            top_pvalues[i, k] = pval
            top_shared[i, k] = n_shared

    elapsed = time.time() - start_time
    logger.info(f"Computed {metric} correlations in {elapsed:.1f} seconds ({elapsed/60:.1f} min)")

    return gene_names, top_indices, top_correlations, top_pvalues, top_shared


def save_correlation_cache(
    organism_key: str,
    metric: str,
    gene_names: list,
    top_indices: np.ndarray,
    top_correlations: np.ndarray,
    top_pvalues: np.ndarray,
    top_shared: np.ndarray
) -> None:
    """Save correlation cache to disk.

    Args:
        organism_key: Organism identifier
        metric: Correlation metric ('pearson', 'spearman', 'cosine')
        gene_names: List of gene names
        top_indices, top_correlations, top_pvalues, top_shared: Correlation data
    """
    # Use metric in filename (pearson uses original name for backwards compatibility)
    if metric == "pearson":
        output_file = EXPRESSION_CACHE_DIR / f"correlations_{organism_key}.npz"
        index_file = EXPRESSION_CACHE_DIR / f"gene_index_{organism_key}.json"
    else:
        output_file = EXPRESSION_CACHE_DIR / f"correlations_{metric}_{organism_key}.npz"
        index_file = EXPRESSION_CACHE_DIR / f"gene_index_{metric}_{organism_key}.json"

    np.savez_compressed(
        output_file,
        gene_names=np.array(gene_names),
        top_indices=top_indices,
        top_correlations=top_correlations,
        top_pvalues=top_pvalues,
        top_shared=top_shared
    )

    size_mb = output_file.stat().st_size / (1024 * 1024)
    logger.info(f"Saved {metric} correlation cache to {output_file} ({size_mb:.1f} MB)")

    # Also save a JSON index for gene name -> index lookup
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
    parser.add_argument(
        "--metric",
        type=str,
        choices=METRICS,
        help="Specific metric to build (pearson, spearman, cosine). Default: all"
    )
    args = parser.parse_args()

    organisms = [args.organism] if args.organism else ORGANISMS
    metrics = [args.metric] if args.metric else METRICS

    for organism_key in organisms:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {organism_key}")
        logger.info(f"{'='*60}")

        profiles = load_profiles(organism_key)
        if not profiles:
            logger.warning(f"No profiles found for {organism_key}, skipping")
            continue

        for metric in metrics:
            logger.info(f"\n--- Building {metric} correlations ---")

            gene_names, top_indices, top_corrs, top_pvals, top_shared = \
                build_correlation_cache_parallel(profiles, metric=metric)

            save_correlation_cache(
                organism_key,
                metric,
                gene_names,
                top_indices,
                top_corrs,
                top_pvals,
                top_shared
            )

    logger.info("\nCorrelation cache building complete!")


if __name__ == "__main__":
    main()
