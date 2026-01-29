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


class HomologyDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    homology_groups: list[HomologyGroupOut]
    # New: Ortholog Cluster section
    ortholog_cluster: typing.Optional[OrthologClusterOut] = None


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
