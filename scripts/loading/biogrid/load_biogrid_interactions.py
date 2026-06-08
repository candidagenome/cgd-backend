#!/usr/bin/env python3
"""
BioGRID Interaction Data Loader for CGD
=======================================
Fetches physical and genetic interaction data from BioGRID API
and loads it into CGD's database.

Usage:
    python load_biogrid_interactions.py [--dry-run]

Environment variables:
    BIOGRID_ACCESS_KEY: Your BioGRID API access key
    DATABASE_URL: Database connection string (set in .env)

Author: CGD Team
"""

import os
import sys
import logging
import urllib.request
import urllib.parse
from datetime import datetime
from typing import Optional

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from sqlalchemy import func
from cgd.db.engine import SessionLocal
from cgd.models.models import (
    Feature, Interaction, FeatInteract, Reference, RefLink
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Configuration
BIOGRID_API_URL = "https://webservice.thebiogrid.org/interactions/"
CANDIDA_ALBICANS_TAXID = "237561"
SOURCE_NAME = "BioGRID"
CREATED_BY = os.environ.get('DEFAULT_USER', 'SHUAI')  # Must exist in dbuser table

# Genetic interaction types (to separate from physical)
GENETIC_TYPES = {
    'Dosage Lethality',
    'Dosage Rescue',
    'Dosage Growth Defect',
    'Negative Genetic',
    'Positive Genetic',
    'Phenotypic Enhancement',
    'Phenotypic Suppression',
    'Synthetic Growth Defect',
    'Synthetic Haploinsufficiency',
    'Synthetic Lethality',
    'Synthetic Rescue',
}


def fetch_biogrid_data(access_key: str) -> list[dict]:
    """Fetch all C. albicans interactions from BioGRID API."""

    params = {
        'taxId': CANDIDA_ALBICANS_TAXID,
        'format': 'tab2',
        'max': '10000',
        'accesskey': access_key,
    }

    url = f"{BIOGRID_API_URL}?{urllib.parse.urlencode(params)}"
    log.info(f"Fetching BioGRID data from API...")

    try:
        with urllib.request.urlopen(url, timeout=120) as response:
            content = response.read().decode('utf-8')
    except Exception as e:
        log.error(f"Failed to fetch BioGRID data: {e}")
        raise

    interactions = []
    lines = content.strip().split('\n')

    for line in lines:
        if not line.strip():
            continue

        cols = line.split('\t')
        if len(cols) < 18:
            log.warning(f"Skipping malformed line: {line[:100]}...")
            continue

        interaction = {
            'biogrid_id': cols[0],
            'systematic_name_a': cols[5],
            'systematic_name_b': cols[6],
            'symbol_a': cols[7],
            'symbol_b': cols[8],
            'aliases_a': cols[9],
            'aliases_b': cols[10],
            'experiment_type': cols[11],
            'interaction_type': cols[12],  # 'physical' or 'genetic'
            'author': cols[13],
            'pmid': cols[14],
            'taxid_a': cols[15],
            'taxid_b': cols[16],
            'throughput': cols[17],
            'description': cols[20] if len(cols) > 20 and cols[20] != '-' else None,
        }
        interactions.append(interaction)

    log.info(f"Fetched {len(interactions)} interactions from BioGRID")
    return interactions


def build_feature_lookup(db) -> dict[str, int]:
    """Build lookup dict from systematic name to feature_no."""

    log.info("Building feature name lookup...")

    # Get all features and their aliases
    features = db.query(Feature.feature_no, Feature.feature_name, Feature.gene_name).all()

    lookup = {}
    for f in features:
        # Map by feature_name (systematic name like C4_00430W_A)
        if f.feature_name:
            lookup[f.feature_name] = f.feature_no
            # Also map without _A/_B suffix for flexibility
            base_name = f.feature_name.rstrip('_A').rstrip('_B')
            if base_name not in lookup:
                lookup[base_name] = f.feature_no

        # Map by gene_name
        if f.gene_name:
            lookup[f.gene_name] = f.feature_no

    log.info(f"Built lookup with {len(lookup)} name mappings")
    return lookup


def build_reference_lookup(db) -> dict[int, int]:
    """Build lookup dict from PMID to reference_no."""

    log.info("Building PMID lookup...")

    refs = db.query(Reference.pubmed, Reference.reference_no).filter(
        Reference.pubmed.isnot(None)
    ).all()

    lookup = {r.pubmed: r.reference_no for r in refs}
    log.info(f"Built lookup with {len(lookup)} PMIDs")
    return lookup


def find_feature_no(name: str, aliases: str, lookup: dict[str, int]) -> Optional[int]:
    """Try to find feature_no from name or aliases."""

    # Try exact match first
    if name in lookup:
        return lookup[name]

    # Try without _A/_B suffix
    base_name = name.rstrip('_A').rstrip('_B')
    if base_name in lookup:
        return lookup[base_name]

    # Try aliases
    if aliases and aliases != '-':
        for alias in aliases.split('|'):
            alias = alias.strip()
            if alias in lookup:
                return lookup[alias]

    return None


def load_interactions(db, interactions: list[dict], feature_lookup: dict,
                      pmid_lookup: dict, dry_run: bool = False) -> dict:
    """Load interactions into database."""

    stats = {
        'total': len(interactions),
        'physical': 0,
        'genetic': 0,
        'skipped_no_feature_a': 0,
        'skipped_no_feature_b': 0,
        'skipped_no_reference': 0,
        'skipped_inter_species': 0,
        'new_interactions': 0,
        'existing_interactions': 0,
    }

    # Build existing interaction lookup
    log.info("Building existing interaction lookup...")
    existing_interactions = {}
    for fi in db.query(FeatInteract).all():
        key = (fi.feature_no, fi.interaction_no, fi.action)
        existing_interactions[key] = fi

    interaction_cache = {}  # Cache for experiment_type -> interaction_no

    for idx, data in enumerate(interactions):
        if idx % 500 == 0:
            log.info(f"Processing interaction {idx + 1}/{len(interactions)}...")

        # Skip inter-species interactions
        if data['taxid_a'] != data['taxid_b']:
            stats['skipped_inter_species'] += 1
            continue

        # Determine if physical or genetic
        is_genetic = data['experiment_type'] in GENETIC_TYPES
        if is_genetic:
            stats['genetic'] += 1
        else:
            stats['physical'] += 1

        # Find feature_no for both interactors
        feature_no_a = find_feature_no(
            data['systematic_name_a'],
            data['aliases_a'],
            feature_lookup
        )
        feature_no_b = find_feature_no(
            data['systematic_name_b'],
            data['aliases_b'],
            feature_lookup
        )

        if feature_no_a is None:
            stats['skipped_no_feature_a'] += 1
            log.debug(f"Could not find feature for: {data['systematic_name_a']}")
            continue

        if feature_no_b is None:
            stats['skipped_no_feature_b'] += 1
            log.debug(f"Could not find feature for: {data['systematic_name_b']}")
            continue

        # Find reference
        pmid = None
        reference_no = None
        try:
            pmid = int(data['pmid'])
            reference_no = pmid_lookup.get(pmid)
        except (ValueError, TypeError):
            pass

        if reference_no is None:
            stats['skipped_no_reference'] += 1
            log.debug(f"Could not find reference for PMID: {data['pmid']}")
            continue

        # Normalize order: smaller feature_no first
        is_self_interaction = (feature_no_a == feature_no_b)
        if feature_no_a > feature_no_b:
            feature_no_a, feature_no_b = feature_no_b, feature_no_a
            action_a, action_b = 'Hit', 'Bait'
        else:
            action_a, action_b = 'Bait', 'Hit'

        # Create unique key for this specific interaction
        # Each BioGRID interaction gets its own Interaction record
        experiment_type = data['experiment_type']
        interaction_key = (feature_no_a, feature_no_b, experiment_type, reference_no)

        if interaction_key in interaction_cache:
            # Already processed this exact interaction
            stats['existing_interactions'] += 1
            continue

        if not dry_run:
            # Create new interaction record for each BioGRID interaction
            new_interaction = Interaction(
                experiment_type=experiment_type,
                source=SOURCE_NAME,
                description=data.get('description'),
                created_by=CREATED_BY,
            )
            db.add(new_interaction)
            db.flush()
            interaction_no = new_interaction.interaction_no

            # Link to reference via RefLink
            ref_link = RefLink(
                reference_no=reference_no,
                tab_name='INTERACTION',
                primary_key=interaction_no,
                col_name='INTERACTION_NO',
                created_by=CREATED_BY,
            )
            db.add(ref_link)

            # Create FeatInteract records
            feat_interact_a = FeatInteract(
                feature_no=feature_no_a,
                interaction_no=interaction_no,
                action=action_a,
                created_by=CREATED_BY,
            )
            db.add(feat_interact_a)

            # For self-interactions, only create one FeatInteract record
            if not is_self_interaction:
                feat_interact_b = FeatInteract(
                    feature_no=feature_no_b,
                    interaction_no=interaction_no,
                    action=action_b,
                    created_by=CREATED_BY,
                )
                db.add(feat_interact_b)

        interaction_cache[interaction_key] = True
        stats['new_interactions'] += 1

    if not dry_run:
        db.commit()
        log.info("Committed changes to database")

    return stats


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Load BioGRID interaction data into CGD')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse and validate data without writing to database')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Get API key
    access_key = os.environ.get('BIOGRID_ACCESS_KEY')
    if not access_key:
        log.error("BIOGRID_ACCESS_KEY environment variable not set")
        sys.exit(1)

    log.info("=" * 60)
    log.info("BioGRID Interaction Loader for CGD")
    log.info(f"Started at: {datetime.now()}")
    log.info(f"Dry run: {args.dry_run}")
    log.info("=" * 60)

    # Fetch data from BioGRID
    interactions = fetch_biogrid_data(access_key)

    # Connect to database
    db = SessionLocal()

    try:
        # Build lookups
        feature_lookup = build_feature_lookup(db)
        pmid_lookup = build_reference_lookup(db)

        # Load interactions
        stats = load_interactions(
            db, interactions, feature_lookup, pmid_lookup,
            dry_run=args.dry_run
        )

        # Print summary
        log.info("=" * 60)
        log.info("SUMMARY")
        log.info("=" * 60)
        log.info(f"Total interactions from BioGRID: {stats['total']}")
        log.info(f"  Physical: {stats['physical']}")
        log.info(f"  Genetic: {stats['genetic']}")
        log.info(f"Skipped:")
        log.info(f"  No feature A mapping: {stats['skipped_no_feature_a']}")
        log.info(f"  No feature B mapping: {stats['skipped_no_feature_b']}")
        log.info(f"  No reference mapping: {stats['skipped_no_reference']}")
        log.info(f"  Inter-species: {stats['skipped_inter_species']}")
        log.info(f"Results:")
        log.info(f"  New interactions: {stats['new_interactions']}")
        log.info(f"  Existing (skipped): {stats['existing_interactions']}")
        log.info("=" * 60)
        log.info(f"Completed at: {datetime.now()}")

    finally:
        db.close()


if __name__ == '__main__':
    main()
