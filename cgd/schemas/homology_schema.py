from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class HomologOut(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    organism_name: str
    dbxref_id: str


class HomologyGroupOut(BaseModel):
    homology_group_type: str  # ortholog, paralog, etc.
    method: str  # InParanoid, etc.
    members: list[HomologOut] = []


# --- Ortholog Cluster (for Homologs tab table display) ---

class OrthologOut(BaseModel):
    """Single ortholog entry for the table display."""
    sequence_id: str  # gene_name or feature_name (display name)
    feature_name: str  # systematic name for linking
    organism_name: str  # Full organism name
    source: str  # CGD, SGD, etc.
    status: typing.Optional[str] = None  # Verified, Uncharacterized, etc.
    is_query: bool = False  # True if this is the query gene itself
    url: typing.Optional[str] = None  # External link for non-CGD orthologs


class DownloadLinkOut(BaseModel):
    """Download link for cluster sequences."""
    label: str
    url: str


class OrthologClusterOut(BaseModel):
    """Ortholog cluster section data."""
    cluster_name: typing.Optional[str] = None  # Cluster identifier
    method: typing.Optional[str] = None  # e.g., CGOB, InParanoid
    cluster_url: typing.Optional[str] = None  # URL to ortholog cluster viewer
    download_links: list[DownloadLinkOut] = []  # Download sequence files
    orthologs: list[OrthologOut] = []


# --- Best Hits / External Homologs ---

class BestHitOut(BaseModel):
    """Single best hit entry."""
    feature_name: str  # Systematic name
    gene_name: typing.Optional[str] = None  # Standard gene name
    display_name: str  # "gene_name/feature_name" or just feature_name
    organism_name: str  # Full organism name
    url: typing.Optional[str] = None  # Link to locus page


class BestHitsInCGDOut(BaseModel):
    """Best hits in CGD species section."""
    # Dict: species_name -> list of best hits
    by_species: dict[str, list[BestHitOut]] = {}


class ExternalHomologOut(BaseModel):
    """External ortholog or best hit entry."""
    dbxref_id: str  # External ID
    display_name: str  # Gene name for display
    organism_name: str  # e.g., "S. cerevisiae"
    source: str  # SGD, POMBASE, etc.
    url: typing.Optional[str] = None  # Link to external database


class ExternalHomologsSectionOut(BaseModel):
    """Section for external orthologs or best hits."""
    # Dict: source (SGD, POMBASE, etc.) -> list of homologs
    by_source: dict[str, list[ExternalHomologOut]] = {}


class PhylogeneticTreeOut(BaseModel):
    """Phylogenetic tree section data."""
    # Newick format tree string for rendering
    newick_tree: typing.Optional[str] = None
    # Tree statistics
    tree_length: typing.Optional[float] = None  # Total tree length in subs/site
    leaf_count: typing.Optional[int] = None  # Number of leaves/species
    # Method used to build tree
    method: typing.Optional[str] = None  # e.g., "SEMPHY"
    # Download links for different tree formats
    download_links: list[DownloadLinkOut] = []


class HomologyDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    homology_groups: list[HomologyGroupOut]
    # Ortholog Cluster section (CGOB)
    ortholog_cluster: typing.Optional[OrthologClusterOut] = None
    # Phylogenetic Tree section
    phylogenetic_tree: typing.Optional[PhylogeneticTreeOut] = None
    # Best hits in CGD species (BLAST)
    best_hits_cgd: typing.Optional[BestHitsInCGDOut] = None
    # Orthologs in fungal species (external)
    orthologs_fungal: typing.Optional[ExternalHomologsSectionOut] = None
    # Best hits in fungal species (external)
    best_hits_fungal: typing.Optional[ExternalHomologsSectionOut] = None
    # Reciprocal best hits in other species (MGD, RGD, dictyBase)
    reciprocal_best_hits: typing.Optional[ExternalHomologsSectionOut] = None


class HomologyDetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "homology_groups": [...] },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, HomologyDetailsForOrganism]
