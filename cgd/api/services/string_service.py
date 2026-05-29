"""
STRING Database Integration Service
====================================
Fetches protein-protein interaction data from STRING database API.

STRING API documentation: https://string-db.org/help/api/
"""

import logging
import urllib.request
import urllib.parse
import json
from typing import Optional
from functools import lru_cache

log = logging.getLogger(__name__)

# STRING API configuration
STRING_API_URL = "https://string-db.org/api"

# Candida species supported by STRING
STRING_SUPPORTED_TAXONS = {
    237561: "Candida albicans SC5314",
    284593: "Candida glabrata CBS138",
}

# Cache timeout in seconds (1 hour)
CACHE_TIMEOUT = 3600


def get_string_taxon_id(cgd_taxon_id: int) -> Optional[int]:
    """
    Check if the CGD taxon ID is supported by STRING.
    Returns the taxon ID if supported, None otherwise.
    """
    if cgd_taxon_id in STRING_SUPPORTED_TAXONS:
        return cgd_taxon_id
    return None


def fetch_string_interactions(
    gene_name: str,
    taxon_id: int,
    required_score: int = 400,
    network_type: str = "functional",
) -> list[dict]:
    """
    Fetch interaction data from STRING API for a given gene.

    Args:
        gene_name: Gene name or identifier
        taxon_id: NCBI taxon ID (must be in STRING_SUPPORTED_TAXONS)
        required_score: Minimum combined score (0-1000), default 400 (medium confidence)
        network_type: "functional" or "physical"

    Returns:
        List of interaction dictionaries with source, target, score, and evidence
    """
    if taxon_id not in STRING_SUPPORTED_TAXONS:
        log.debug(f"Taxon {taxon_id} not supported by STRING")
        return []

    params = {
        'identifiers': gene_name,
        'species': str(taxon_id),
        'required_score': str(required_score),
        'network_type': network_type,
    }

    url = f"{STRING_API_URL}/json/network?{urllib.parse.urlencode(params)}"
    log.info(f"Fetching STRING data for {gene_name} (taxon {taxon_id})")

    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'CGD/1.0 (Candida Genome Database)'}
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            log.debug(f"No STRING data found for {gene_name}")
            return []
        log.error(f"STRING API HTTP error: {e.code} - {e.reason}")
        return []
    except Exception as e:
        log.error(f"Failed to fetch STRING data: {e}")
        return []

    # Parse STRING response into our format
    interactions = []
    seen_pairs = set()

    for item in data:
        # STRING returns preferredName_A/B for gene names
        source = item.get('preferredName_A', item.get('stringId_A', ''))
        target = item.get('preferredName_B', item.get('stringId_B', ''))
        score = item.get('score', 0)

        if not source or not target:
            continue

        # Deduplicate (A-B same as B-A)
        pair_key = tuple(sorted([source, target]))
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        interactions.append({
            'source': source,
            'target': target,
            'score': score,
            'combined_score': int(score * 1000) if score <= 1 else int(score),
            'source_db': 'STRING',
            # STRING doesn't distinguish physical vs genetic in the same way
            'interaction_type': 'physical' if network_type == 'physical' else 'functional',
            'evidence_scores': {
                'nscore': item.get('nscore', 0),  # neighborhood
                'fscore': item.get('fscore', 0),  # fusion
                'pscore': item.get('pscore', 0),  # phylogeny
                'ascore': item.get('ascore', 0),  # coexpression
                'escore': item.get('escore', 0),  # experimental
                'dscore': item.get('dscore', 0),  # database
                'tscore': item.get('tscore', 0),  # textmining
            }
        })

    log.info(f"Found {len(interactions)} STRING interactions for {gene_name}")
    return interactions


def fetch_string_network(
    gene_names: list[str],
    taxon_id: int,
    required_score: int = 400,
    add_nodes: int = 0,
) -> dict:
    """
    Fetch a network of interactions for multiple genes from STRING.

    Args:
        gene_names: List of gene names
        taxon_id: NCBI taxon ID
        required_score: Minimum combined score (0-1000)
        add_nodes: Number of additional interactor nodes to add

    Returns:
        Dictionary with 'nodes' and 'edges' for network visualization
    """
    if taxon_id not in STRING_SUPPORTED_TAXONS:
        return {'nodes': [], 'edges': [], 'source': 'STRING'}

    # Join gene names with newline encoding for STRING API
    identifiers = '%0d'.join(gene_names)

    params = {
        'identifiers': identifiers,
        'species': str(taxon_id),
        'required_score': str(required_score),
        'add_nodes': str(add_nodes),
    }

    url = f"{STRING_API_URL}/json/network?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'CGD/1.0 (Candida Genome Database)'}
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        log.error(f"Failed to fetch STRING network: {e}")
        return {'nodes': [], 'edges': [], 'source': 'STRING'}

    # Build nodes and edges
    nodes = {}
    edges = []
    seen_pairs = set()

    for item in data:
        source = item.get('preferredName_A', '')
        target = item.get('preferredName_B', '')
        score = item.get('score', 0)

        if not source or not target:
            continue

        # Add nodes
        if source not in nodes:
            nodes[source] = {
                'id': source,
                'label': source,
                'is_query': source.upper() in [g.upper() for g in gene_names],
            }
        if target not in nodes:
            nodes[target] = {
                'id': target,
                'label': target,
                'is_query': target.upper() in [g.upper() for g in gene_names],
            }

        # Add edge (deduplicated)
        pair_key = tuple(sorted([source, target]))
        if pair_key not in seen_pairs:
            seen_pairs.add(pair_key)
            edges.append({
                'source': source,
                'target': target,
                'score': int(score * 1000) if score <= 1 else int(score),
                'interaction_type': 'string',  # Mark as STRING data
                'experiment_type': 'STRING combined',
                'experiment_count': 1,
            })

    return {
        'nodes': list(nodes.values()),
        'edges': edges,
        'source': 'STRING',
    }
