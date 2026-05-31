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


class StringInteractionOut(BaseModel):
    """STRING database interaction."""
    interactor: str  # Gene name of the interactor
    interactor_feature_name: typing.Optional[str] = None  # CGD feature name if mapped
    combined_score: int  # Combined confidence score (0-1000)
    experimental_score: int = 0  # Experimental evidence score
    database_score: int = 0  # Database evidence score
    textmining_score: int = 0  # Text mining evidence score
    coexpression_score: int = 0  # Co-expression evidence score


class EnrichmentGene(BaseModel):
    """A gene annotated to an enriched term. feature_name links to CGD when known."""
    label: str                                      # display name (gene or systematic)
    feature_name: typing.Optional[str] = None       # CGD feature_name for /locus/ link


class StringEnrichmentTerm(BaseModel):
    """A functionally-enriched term among a gene's STRING network partners."""
    category: str          # STRING category code (Process, Function, KEGG, ...)
    category_label: str    # human-friendly label
    term: str              # term ID (e.g. GO:0006696)
    description: str       # term name
    fdr: float             # false discovery rate
    p_value: float
    genes: int             # number of network genes with this term
    background: int        # genome background count for this term
    gene_list: list[EnrichmentGene] = []  # the network genes annotated to this term


class StringEnrichmentForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    network_size: int                       # number of genes the enrichment ran over
    terms: list[StringEnrichmentTerm] = []


class StringEnrichmentResponse(BaseModel):
    results: dict[str, StringEnrichmentForOrganism]


class NetworkEnrichmentTerm(BaseModel):
    """An enriched GO term or phenotype among a gene's interaction partners,
    computed against CGD's own annotations (GO Term Finder / Phenotype
    Enrichment engines)."""
    category_label: str          # "GO Biological Process" ... or "Phenotype"
    term: str                    # GOID (GO:XXXXXXX) or phenotype observable
    description: str             # term/phenotype name
    query_count: int             # query genes annotated to this term
    fold_enrichment: float
    p_value: float
    fdr: typing.Optional[float] = None
    genes: list[EnrichmentGene] = []  # the query genes annotated to this term


class NetworkEnrichmentForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    gene_count: int              # genes the enrichment ran over (query + partners)
    include_string: bool         # whether STRING partners were included in the set
    go_terms: list[NetworkEnrichmentTerm] = []
    phenotype_terms: list[NetworkEnrichmentTerm] = []


class NetworkEnrichmentResponse(BaseModel):
    results: dict[str, NetworkEnrichmentForOrganism]


class InteractionDetailsForOrganism(BaseModel):
    locus_display_name: str
    taxon_id: int
    organism_no: typing.Optional[int] = None  # for GO/phenotype enrichment requests
    interactions: list[InteractionOut]
    string_interactions: list[StringInteractionOut] = []


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
class GoSlimTermOut(BaseModel):
    """A GO Slim term annotated to a network node."""
    goid: str  # e.g. "GO:0006950"
    term: str  # human-readable term name
    aspect: str  # 'P' (process), 'F' (function), 'C' (component)


class NetworkNode(BaseModel):
    """A node in the interaction network (a gene/feature)."""
    id: str  # feature_name (unique identifier)
    label: str  # display name (gene_name or feature_name)
    is_query: bool = False  # True if this is the queried gene
    # GO Slim annotations (CGD_GO_Slim set), via direct or ancestor mapping
    go_terms: list[GoSlimTermOut] = []  # all slim terms across P/F/C
    shared_go: list[GoSlimTermOut] = []  # slim terms also annotated to the query gene
    go_category: typing.Optional[str] = None  # representative biological-process term (for coloring)
    go_category_id: typing.Optional[str] = None  # GO id of go_category


class NetworkEdge(BaseModel):
    """An edge in the interaction network (an interaction)."""
    source: str  # feature_name of source node
    target: str  # feature_name of target node
    interaction_type: str  # 'physical', 'genetic', or 'string'
    experiment_type: str  # e.g., 'Affinity Capture-Western' or 'STRING combined'
    experiment_count: int = 1  # number of experiments supporting this edge
    source_db: str = "BioGRID"  # 'BioGRID' or 'STRING'
    score: typing.Optional[int] = None  # STRING confidence score (0-1000)


class SharedGoEdge(BaseModel):
    """A non-interaction link drawn between two nodes that share a GO Slim term."""
    source: str  # feature_name
    target: str  # feature_name
    shared_terms: list[GoSlimTermOut] = []


class InteractionNetworkForOrganism(BaseModel):
    """Network graph data for one organism."""
    locus_display_name: str
    taxon_id: int
    nodes: list[NetworkNode]
    edges: list[NetworkEdge]
    shared_go_edges: list[SharedGoEdge] = []


class InteractionNetworkResponse(BaseModel):
    """
    Network graph response grouped by organism.
    """
    results: dict[str, InteractionNetworkForOrganism]
