#!/usr/bin/env python3
"""
Build expression profile cache files for the Similar Expression Genes feature.

This script pre-computes fold-change profiles for all genes in each organism
and saves them to JSON files. The API service loads these cache files on startup
for fast query response times.

Usage:
    python scripts/build_expression_cache.py [--organism ORGANISM] [--output-dir DIR]

Examples:
    # Build cache for all organisms
    python scripts/build_expression_cache.py

    # Build cache for specific organism
    python scripts/build_expression_cache.py --organism C_albicans_SC5314

    # Specify output directory
    python scripts/build_expression_cache.py --output-dir /data/cache
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cgd.core.settings import settings
from cgd.models.models import Feature, Seq, FeatLocation, Organism

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import pyBigWig
try:
    import pyBigWig
    PYBIGWIG_AVAILABLE = True
except ImportError:
    PYBIGWIG_AVAILABLE = False
    logger.error("pyBigWig not installed - cannot build expression cache")
    sys.exit(1)


# ============================================================================
# Configuration (copied from expression_service.py for standalone operation)
# ============================================================================

HTS_BASE_PATHS = {
    "C_albicans_SC5314": Path("/data/HTS/C_albicans_SC5314/bam"),
    "C_auris_B8441": Path("/data/HTS/C_auris_B8441/bam"),
    "C_glabrata_CBS138": Path("/data/HTS/C_glabrata_CBS138/bam"),
    "C_dubliniensis_CD36": Path("/data/HTS/C_dubliniensis_CD36/bam"),
    "C_parapsilosis_CDC317": Path("/data/HTS/C_parapsilosis_CDC317/bam"),
}

ORGANISM_TO_HTS_KEY = {
    "Candida albicans SC5314": "C_albicans_SC5314",
    "Candida auris B8441": "C_auris_B8441",
    "Candida glabrata CBS138": "C_glabrata_CBS138",
    "Candida dubliniensis CD36": "C_dubliniensis_CD36",
    "Candida parapsilosis CDC317": "C_parapsilosis_CDC317",
}

# Import study configurations from service
from cgd.api.services.expression_service import (
    EXPRESSION_STUDIES,
    _get_bigwig_path,
    _map_chromosome_for_bigwig,
)


# ============================================================================
# Helper Functions
# ============================================================================

def get_chromosome_priority(chr_name: str, hts_key: str) -> int:
    """Determine priority for chromosome selection."""
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

    Returns dict mapping feature_no to (chromosome, start, end).
    """
    from sqlalchemy.orm import aliased

    if not feature_nos:
        return {}

    # Create alias for the chromosome feature
    ChromFeature = aliased(Feature)

    # Oracle has 1000 item limit for IN clause - chunk the query
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

        if (i + CHUNK_SIZE) % 5000 < CHUNK_SIZE:
            logger.info(f"  Fetched locations for {min(i + CHUNK_SIZE, len(feature_nos))}/{len(feature_nos)} features")

    # Group by feature_no and select best chromosome
    feature_locations: Dict[int, List[Tuple[str, int, int, int]]] = {}
    for row in all_results:
        feature_no = row.feature_no
        chr_name = row.chromosome
        priority = get_chromosome_priority(chr_name, hts_key)

        if feature_no not in feature_locations:
            feature_locations[feature_no] = []
        feature_locations[feature_no].append((chr_name, row.start_coord, row.stop_coord, priority))

    # Select best location for each feature
    best_locations: Dict[int, Tuple[str, int, int]] = {}
    for feature_no, locs in feature_locations.items():
        # Sort by priority descending, take first
        locs.sort(key=lambda x: x[3], reverse=True)
        best = locs[0]
        chr_name, start, end, _ = best

        # Map chromosome name for bigwig
        mapped_chr = _map_chromosome_for_bigwig(chr_name, hts_key)
        if mapped_chr:
            best_locations[feature_no] = (mapped_chr, start, end)

    return best_locations


def get_gene_location_for_feature(
    db,
    feature: Feature,
    hts_key: str
) -> Optional[Tuple[str, int, int]]:
    """Get gene location info for a specific feature (single gene version)."""
    locations = batch_get_gene_locations(db, [feature.feature_no], hts_key)
    return locations.get(feature.feature_no)


def old_get_gene_location_for_feature(
    db,
    feature: Feature,
    hts_key: str
) -> Optional[Tuple[str, int, int]]:
    """Get gene location info for a specific feature (old slow version)."""
    locations = (
        db.query(FeatLocation)
        .filter(
            FeatLocation.feature_no == feature.feature_no,
            FeatLocation.is_loc_current == "Y"
        )
        .all()
    )

    if not locations:
        return None

    best_location = None
    best_chromosome = None
    best_priority = -1

    for loc in locations:
        if not loc.root_seq_no:
            continue

        root_seq = db.query(Seq).filter(Seq.seq_no == loc.root_seq_no).first()
        if not root_seq:
            continue

        root_feature = db.query(Feature).filter(
            Feature.feature_no == root_seq.feature_no
        ).first()
        if not root_feature:
            continue

        chr_name = root_feature.feature_name
        priority = get_chromosome_priority(chr_name, hts_key)

        if priority > best_priority:
            best_priority = priority
            best_location = loc
            best_chromosome = chr_name

    if not best_location or not best_chromosome:
        return None

    mapped_chromosome = _map_chromosome_for_bigwig(best_chromosome, hts_key)
    if not mapped_chromosome:
        mapped_chromosome = best_chromosome

    return (mapped_chromosome, best_location.start_coord, best_location.stop_coord)


def batch_read_bigwig(
    bigwig_path: Path,
    gene_locations: List[Tuple[str, str, int, int]]
) -> Dict[str, float]:
    """Read expression values for multiple genes from a single bigwig file."""
    if not bigwig_path.exists():
        return {}

    results: Dict[str, float] = {}

    try:
        bw = pyBigWig.open(str(bigwig_path))
        if bw is None:
            return {}

        for feature_name, chromosome, start, end in gene_locations:
            try:
                if start > end:
                    start, end = end, start

                stats = bw.stats(chromosome, start - 1, end, type="mean")
                if stats and stats[0] is not None:
                    results[feature_name] = stats[0]
                else:
                    results[feature_name] = 0.0
            except Exception:
                pass

        bw.close()
    except Exception as e:
        logger.debug(f"Error reading bigwig {bigwig_path}: {e}")

    return results


def build_expression_cache(
    db,
    organism_key: str
) -> Dict[str, Dict[str, float]]:
    """
    Build expression profiles for all genes in an organism.

    Opens each bigwig file only once and reads all gene regions in batch.
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

    # Build feature_no -> feature_name mapping
    feature_name_map = {f.feature_no: f.feature_name for f in features}
    feature_nos = list(feature_name_map.keys())

    # Step 1: Get all gene locations in ONE batch query
    logger.info("Fetching gene locations (batch query)...")
    location_map = batch_get_gene_locations(db, feature_nos, organism_key)
    logger.info(f"Got locations for {len(location_map)} genes")

    # Build gene_locations list
    gene_locations: List[Tuple[str, str, int, int]] = []
    for feature_no, (chromosome, start, end) in location_map.items():
        feature_name = feature_name_map[feature_no]
        gene_locations.append((feature_name, chromosome, start, end))

    if not gene_locations:
        return {}

    # Step 2: Read raw values from each bigwig file (batch reads)
    raw_values: Dict[str, Dict[str, float]] = {}
    total_conditions = sum(len(s["conditions"]) for s in studies_config.values())
    processed = 0

    for study_id, study_info in studies_config.items():
        for cond_id in study_info["conditions"]:
            condition_key = f"{study_id}:{cond_id}"
            bigwig_path = _get_bigwig_path(base_path, study_id, cond_id, study_info)

            values = batch_read_bigwig(bigwig_path, gene_locations)
            if values:
                raw_values[condition_key] = values

            processed += 1
            if processed % 10 == 0:
                pct = processed * 100 // total_conditions
                logger.info(f"  Read {processed}/{total_conditions} conditions ({pct}%)")

    logger.info(f"Read raw values from {len(raw_values)} conditions")

    # Step 3: Compute fold changes
    profiles: Dict[str, Dict[str, float]] = {}

    for feature_name, chromosome, start, end in gene_locations:
        profile: Dict[str, float] = {}

        for study_id, study_info in studies_config.items():
            control_id = study_info["control"]
            control_key = f"{study_id}:{control_id}"

            control_values = raw_values.get(control_key, {})
            control_value = control_values.get(feature_name)

            if control_value is None or control_value <= 0:
                continue

            for cond_id in study_info["conditions"]:
                if cond_id == control_id:
                    continue

                condition_key = f"{study_id}:{cond_id}"
                cond_values = raw_values.get(condition_key, {})
                cond_value = cond_values.get(feature_name)

                if cond_value is not None:
                    fold_change = cond_value / control_value
                    profile[condition_key] = fold_change

        if profile:
            profiles[feature_name] = profile

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
