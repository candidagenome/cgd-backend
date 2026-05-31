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

# Candida species supported by STRING.
# albicans/glabrata/auris/tropicalis map by gene name or systematic name
# (== CGD feature_name). C. parapsilosis returns gene symbols where available
# and UniProt mnemonics otherwise, so unnamed genes are mapped back to CGD via
# the UniProt accession embedded in the STRING id (see source_accession below).
# C. dubliniensis is absent from STRING v12.
STRING_SUPPORTED_TAXONS = {
    237561: "Candida albicans SC5314",
    284593: "Candida glabrata CBS138",
    498019: "Candida auris B8441",
    294747: "Candida tropicalis MYA-3404",
    578454: "Candida parapsilosis CDC317",
}


def _string_accession(string_id: str) -> str:
    """Extract the bare protein accession from a STRING id like '578454.G8B991'."""
    if not string_id:
        return ''
    return string_id.split('.', 1)[1] if '.' in string_id else string_id

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
        # Bare protein accession (UniProt for Candida) for fallback CGD mapping
        source_accession = _string_accession(item.get('stringId_A', ''))
        target_accession = _string_accession(item.get('stringId_B', ''))
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
            'source_accession': source_accession,
            'target_accession': target_accession,
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


# STRING enrichment categories worth surfacing, mapped to friendly labels.
STRING_ENRICHMENT_CATEGORIES = {
    "Process": "GO Biological Process",
    "Function": "GO Molecular Function",
    "Component": "GO Cellular Component",
    "KEGG": "KEGG Pathway",
    "RCTM": "Reactome Pathway",
}


def fetch_string_enrichment(
    identifiers: list[str],
    taxon_id: int,
) -> list[dict]:
    """
    Run STRING functional enrichment on a set of proteins (typically a gene
    and its STRING network partners).

    Args:
        identifiers: list of gene/protein identifiers (STRING preferredNames)
        taxon_id: NCBI taxon ID (must be in STRING_SUPPORTED_TAXONS)

    Returns:
        List of enriched-term dicts: category, category_label, term, description,
        fdr, p_value, genes (count), background (count).
    """
    if taxon_id not in STRING_SUPPORTED_TAXONS:
        return []
    # Need at least a couple of genes for a meaningful test.
    ids = [i for i in dict.fromkeys(identifiers) if i]  # dedupe, keep order
    if len(ids) < 2:
        return []

    # POST to avoid URL-length limits for large neighborhoods.
    url = f"{STRING_API_URL}/json/enrichment"
    data = urllib.parse.urlencode({
        'identifiers': '\r'.join(ids),
        'species': str(taxon_id),
        'caller_identity': 'candidagenome.org',
    }).encode('utf-8')

    try:
        req = urllib.request.Request(
            url, data=data,
            headers={'User-Agent': 'CGD/1.0 (Candida Genome Database)'},
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            rows = json.loads(response.read().decode('utf-8'))
    except Exception as e:  # noqa: BLE001
        log.error(f"STRING enrichment failed for taxon {taxon_id}: {e}")
        return []

    results = []
    for r in rows:
        category = r.get('category', '')
        if category not in STRING_ENRICHMENT_CATEGORIES:
            continue
        # STRING returns the matched gene names as a comma-separated string
        # (preferredNames) or list, depending on version.
        pref = r.get('preferredNames', [])
        if isinstance(pref, str):
            pref = [p.strip() for p in pref.split(',') if p.strip()]
        results.append({
            'category': category,
            'category_label': STRING_ENRICHMENT_CATEGORIES[category],
            'term': r.get('term', ''),
            'description': r.get('description', ''),
            'fdr': r.get('fdr', 1.0),
            'p_value': r.get('p_value', 1.0),
            'genes': r.get('number_of_genes', 0),
            'background': r.get('number_of_genes_in_background', 0),
            'gene_names': pref,
        })

    # Most significant first.
    results.sort(key=lambda x: (x['fdr'], x['p_value']))
    return results
