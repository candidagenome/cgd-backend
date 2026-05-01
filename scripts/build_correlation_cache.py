#!/usr/bin/env python3
"""
Build pre-computed correlation cache for the Similar Expression Genes feature.

Uses numpy's masked arrays for efficient computation with missing data.

Usage:
    python scripts/build_correlation_cache.py [--organism ORGANISM]
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


def compute_correlation_row(gene_idx: int, expr_matrix: np.ndarray, valid_mask: np.ndarray, min_shared: int = 5) -> list:
    """Compute correlations for one gene against all others using vectorized operations."""
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

        # Compute Pearson correlation
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

        if not np.isfinite(corr):
            continue

        # Approximate p-value
        if abs(corr) < 0.9999 and n_shared > 2:
            t_stat = corr * np.sqrt((n_shared - 2) / (1 - corr ** 2))
            # Very rough p-value approximation
            pval = max(2 * np.exp(-0.5 * t_stat ** 2), 1e-10)
        else:
            pval = 1e-10

        results.append((j, float(corr), float(pval), int(n_shared)))

    return results


def build_correlation_cache_parallel(
    profiles: dict,
    top_n: int = 100,
    min_shared: int = 5
) -> tuple:
    """Build correlation cache using simple loop but optimized numpy operations."""
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
    expr_matrix = np.full((n_genes, n_conditions), np.nan, dtype=np.float64)
    condition_to_idx = {c: i for i, c in enumerate(conditions)}

    for i, gene in enumerate(gene_names):
        profile = profiles[gene]
        for cond, fc in profile.items():
            j = condition_to_idx[cond]
            expr_matrix[i, j] = fc

    # Create valid mask
    valid_mask = ~np.isnan(expr_matrix)

    logger.info("Built expression matrix, computing correlations...")

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
        correlations = compute_correlation_row(i, expr_matrix, valid_mask, min_shared)

        # Sort by correlation (descending) and keep top N
        correlations.sort(key=lambda x: x[1], reverse=True)
        top = correlations[:top_n]

        for k, (j, corr, pval, n_shared) in enumerate(top):
            top_indices[i, k] = j
            top_correlations[i, k] = corr
            top_pvalues[i, k] = pval
            top_shared[i, k] = n_shared

    elapsed = time.time() - start_time
    logger.info(f"Computed correlations in {elapsed:.1f} seconds ({elapsed/60:.1f} min)")

    return gene_names, top_indices, top_correlations, top_pvalues, top_shared


def save_correlation_cache(
    organism_key: str,
    gene_names: list,
    top_indices: np.ndarray,
    top_correlations: np.ndarray,
    top_pvalues: np.ndarray,
    top_shared: np.ndarray
) -> None:
    """Save correlation cache to disk."""
    output_file = EXPRESSION_CACHE_DIR / f"correlations_{organism_key}.npz"

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

        profiles = load_profiles(organism_key)
        if not profiles:
            logger.warning(f"No profiles found for {organism_key}, skipping")
            continue

        gene_names, top_indices, top_corrs, top_pvals, top_shared = \
            build_correlation_cache_parallel(profiles)

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
