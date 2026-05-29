from __future__ import annotations

import typing
from pydantic import BaseModel, ConfigDict


class ORMSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class InteractorOut(BaseModel):
    feature_name: str
    gene_name: typing.Optional[str] = None
    action: str  # Bait, Hit, etc.


class InteractionReferenceOut(BaseModel):
    """Reference info for interaction citations."""
    dbxref_id: typing.Optional[str] = None
    pubmed: typing.Optional[int] = None
    citation: typing.Optional[str] = None  # "Author et al. (Year) Title. Journal"


class InteractionOut(BaseModel):
    interaction_no: int
    experiment_type: str
    description: typing.Optional[str] = None
    source: str
    interactors: list[InteractorOut] = []
    references: list[InteractionReferenceOut] = []


class InteractionDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    interactions: list[InteractionOut]


class InteractionDetailsResponse(BaseModel):
    """
    {
      "results": {
        "Candida albicans": { "locus_display_name": "ACT1", "interactions": [...] },
        "Candida glabrata": { ... }
      }
    }
    """
    results: dict[str, InteractionDetailsForOrganism]


# Network graph schemas for Cytoscape visualization
class NetworkNode(BaseModel):
    """A node in the interaction network (a gene/feature)."""
    id: str  # feature_name (unique identifier)
    label: str  # display name (gene_name or feature_name)
    is_query: bool = False  # True if this is the queried gene


class NetworkEdge(BaseModel):
    """An edge in the interaction network (an interaction)."""
    source: str  # feature_name of source node
    target: str  # feature_name of target node
    interaction_type: str  # 'physical' or 'genetic'
    experiment_type: str  # e.g., 'Affinity Capture-Western'
    experiment_count: int = 1  # number of experiments supporting this edge


class InteractionNetworkForOrganism(BaseModel):
    """Network graph data for one organism."""
    locus_display_name: str
    taxon_id: int
    nodes: list[NetworkNode]
    edges: list[NetworkEdge]


class InteractionNetworkResponse(BaseModel):
    """
    Network graph response grouped by organism.
    """
    results: dict[str, InteractionNetworkForOrganism]
