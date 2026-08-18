#!/usr/bin/env python3
"""
Build expression profile cache files for the Similar Expression Genes feature.

This script pre-computes fold-change profiles for all genes in each organism
and saves them to JSON files. The API service loads these cache files on startup
for fast query response times.

Per-gene computation is delegated to the API service
(cgd.api.services.expression_service._build_expression_profile), so cached
values are always identical to live-computed ones: same organism/study
configuration, group-aware controls, and library-size normalization. Gene
locations are batch-fetched here (one chunked query) because the service's
per-gene lookup costs several DB round-trips per gene.

Usage:
    python scripts/expression/build_expression_cache.py [--organism ORGANISM] [--output-dir DIR]

Examples:
    # Build cache for all configured organisms
    python scripts/expression/build_expression_cache.py

    # Build cache for specific organism (use the HTS key)
    python scripts/expression/build_expression_cache.py --organism C_tropicalis_MYA3404

    # Specify output directory
    python scripts/expression/build_expression_cache.py --output-dir /data/cache
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

# Add repo root to path for imports (script lives in scripts/expression/)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, aliased

from cgd.core.settings import settings
from cgd.models.models import Feature, Seq, FeatLocation, Organism

# Single source of truth: import all configuration and the profile builder
# from the service so this script can never drift from the API's behavior.
from cgd.api.services.expression_service import (
    EXPRESSION_STUDIES,
    HTS_BASE_PATHS,
    ORGANISM_TO_HTS_KEY,
    _BigWigPool,
    _build_expression_profile,
    _map_chromosome_for_bigwig,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_chromosome_priority(chr_name: str, hts_key: str) -> int:
    """Determine priority for chromosome selection (mirrors the service)."""
    if hts_key == "C_albicans_SC5314":
        if chr_name.startswith("Ca22chr"):
            return 4
        elif chr_name.startswith("Ca21chr"):
            return 3
        elif chr_name.startswith("Ca20chr"):
            return 2
        elif "chr" in chr_name.lower():
            return 1
    elif hts_key == "C_parapsilosis_CDC317":
        if chr_name.startswith("Contig"):
            return 2
        elif "contig" in chr_name.lower():
            return 1
    else:
        if chr_name.startswith("Chr"):
            return 2
        elif "chr" in chr_name.lower():
            return 1
    return 0


def batch_get_gene_locations(
    db,
    feature_nos: List[int],
    hts_key: str
) -> Dict[int, Tuple[str, int, int]]:
    """
    Batch fetch gene locations for multiple features.

    Handles Oracle's 1000 item IN clause limit by chunking.

    Returns dict mapping feature_no to (chromosome, start, end) with the
    chromosome already mapped to bigWig naming.
    """
    if not feature_nos:
        return {}

    ChromFeature = aliased(Feature)

    CHUNK_SIZE = 900
    all_results = []

    for i in range(0, len(feature_nos), CHUNK_SIZE):
        chunk = feature_nos[i:i + CHUNK_SIZE]

        results = (
            db.query(
                FeatLocation.feature_no,
                FeatLocation.start_coord,
                FeatLocation.stop_coord,
                ChromFeature.feature_name.label('chromosome')
            )
            .join(Seq, Seq.seq_no == FeatLocation.root_seq_no)
            .join(ChromFeature, ChromFeature.feature_no == Seq.feature_no)
            .filter(
                FeatLocation.feature_no.in_(chunk),
                FeatLocation.is_loc_current == "Y",
                FeatLocation.root_seq_no.isnot(None)
            )
            .all()
        )
        all_results.extend(results)

    # Group by feature_no and select best chromosome
    feature_locations: Dict[int, List[Tuple[str, int, int, int]]] = {}
    for row in all_results:
        priority = get_chromosome_priority(row.chromosome, hts_key)
        feature_locations.setdefault(row.feature_no, []).append(
            (row.chromosome, row.start_coord, row.stop_coord, priority)
        )

    best_locations: Dict[int, Tuple[str, int, int]] = {}
    for feature_no, locs in feature_locations.items():
        locs.sort(key=lambda x: x[3], reverse=True)
        chr_name, start, end, _ = locs[0]

        mapped_chr = _map_chromosome_for_bigwig(chr_name, hts_key)
        if mapped_chr:
            best_locations[feature_no] = (mapped_chr, start, end)

    return best_locations


def build_expression_cache(
    db,
    organism_key: str
) -> Dict[str, Dict[str, float]]:
    """
    Build expression profiles for all ORF features in an organism.

    Uses the service's per-gene builder with batch-fetched locations and a
    shared bigWig handle pool so each study file is opened once per run.
    """
    logger.info(f"Building expression cache for {organism_key}")
    start_time = time.time()

    base_path = HTS_BASE_PATHS.get(organism_key)
    if not base_path or not base_path.exists():
        logger.error(f"HTS base path not found: {base_path}")
        return {}

    studies_config = EXPRESSION_STUDIES.get(organism_key, {})
    if not studies_config:
        logger.error(f"No studies configured for {organism_key}")
        return {}

    # Get organism from database
    organism_name_map = {v: k for k, v in ORGANISM_TO_HTS_KEY.items()}
    organism_name = organism_name_map.get(organism_key)
    if not organism_name:
        logger.error(f"No organism name mapping for {organism_key}")
        return {}

    organism_obj = db.query(Organism).filter(
        Organism.organism_name == organism_name
    ).first()
    if not organism_obj:
        logger.error(f"Organism not found in database: {organism_name}")
        return {}

    # Get all ORF features
    features = (
        db.query(Feature)
        .filter(
            Feature.organism_no == organism_obj.organism_no,
            Feature.feature_type == 'ORF'
        )
        .all()
    )
    logger.info(f"Found {len(features)} ORF features")

    # Batch fetch all gene locations in one chunked query
    logger.info("Fetching gene locations (batch query)...")
    location_map = batch_get_gene_locations(
        db, [f.feature_no for f in features], organism_key
    )
    logger.info(f"Got locations for {len(location_map)} genes")

    profiles: Dict[str, Dict[str, float]] = {}
    with _BigWigPool() as pool:
        for i, feature in enumerate(features, 1):
            location_info = location_map.get(feature.feature_no)
            if not location_info:
                continue

            profile = _build_expression_profile(
                db, feature, organism_key, base_path, studies_config,
                organism_key, pool=pool, location_info=location_info,
            )
            if profile:
                profiles[feature.feature_name] = profile

            if i % 500 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(features) - i) / rate if rate > 0 else 0
                logger.info(
                    f"  Processed {i}/{len(features)} genes "
                    f"({rate:.1f}/sec, ETA: {eta / 60:.1f} min)"
                )

    elapsed = time.time() - start_time
    logger.info(f"Built profiles for {len(profiles)} genes in {elapsed:.1f} seconds")

    return profiles


def save_cache(profiles: Dict, output_path: Path) -> None:
    """Save profiles to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(profiles, f)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"Saved cache to {output_path} ({size_mb:.1f} MB)")


def main():
    parser = argparse.ArgumentParser(
        description="Build expression profile cache files"
    )
    parser.add_argument(
        "--organism",
        type=str,
        help="Specific organism to build (e.g., C_albicans_SC5314). "
             "If not specified, builds for all organisms."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/data/cache/expression",
        help="Output directory for cache files (default: /data/cache/expression)"
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    # Determine which organisms to process
    if args.organism:
        if args.organism not in EXPRESSION_STUDIES:
            logger.error(f"Unknown organism: {args.organism}")
            logger.error(f"Available: {list(EXPRESSION_STUDIES.keys())}")
            sys.exit(1)
        organisms = [args.organism]
    else:
        organisms = list(EXPRESSION_STUDIES.keys())

    # Create database session
    engine = create_engine(settings.database_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    try:
        for organism_key in organisms:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {organism_key}")
            logger.info(f"{'='*60}")

            # Check if HTS data exists
            base_path = HTS_BASE_PATHS.get(organism_key)
            if not base_path or not base_path.exists():
                logger.warning(f"Skipping {organism_key} - HTS data not found at {base_path}")
                continue

            # Build cache
            profiles = build_expression_cache(db, organism_key)

            if profiles:
                # Save to file
                output_path = output_dir / f"expression_profiles_{organism_key}.json"
                save_cache(profiles, output_path)
            else:
                logger.warning(f"No profiles built for {organism_key}")

        logger.info("\nCache building complete!")

    finally:
        db.close()


if __name__ == "__main__":
    main()
