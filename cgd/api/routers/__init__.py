"""
CGD API Routers

This package contains FastAPI routers that define the API endpoints.

Routers:
- health_router: Health check endpoint
- locus_router: Locus/gene information endpoints
- reference_router: Reference/publication endpoints
- go_router: Gene Ontology annotation endpoints
- phenotype_router: Phenotype annotation endpoints
- search_router: Search functionality endpoints
- sequence_router: Sequence retrieval endpoints
- seq_tools_router: Sequence analysis tools
- batch_download_router: Batch file download endpoints
- feature_search_router: Feature search endpoints
- genome_version_router: Genome version information
- colleague_router: Colleague/researcher endpoints
- gene_registry_router: Gene name registry endpoints
- chromosome_router: Chromosome data endpoints
- webprimer_router: WebPrimer tool endpoints
- go_term_finder_router: GO Term Finder tool endpoints
- go_slim_mapper_router: GO Slim Mapper tool endpoints
- blast_router: BLAST search endpoints
- patmatch_router: Pattern matching endpoints
- restriction_mapper_router: Restriction enzyme mapping endpoints
- homology_router: Homology/ortholog endpoints
"""

from .health_router import router as health_router
from .locus_router import router as locus_router
from .reference_router import router as reference_router
from .go_router import router as go_router
from .phenotype_router import router as phenotype_router
from .search_router import router as search_router
from .sequence_router import router as sequence_router
from .seq_tools_router import router as seq_tools_router
from .batch_download_router import router as batch_download_router
from .feature_search_router import router as feature_search_router
from .genome_version_router import router as genome_version_router
from .colleague_router import router as colleague_router
from .gene_registry_router import router as gene_registry_router
from .chromosome_router import router as chromosome_router
from .webprimer_router import router as webprimer_router
from .go_term_finder_router import router as go_term_finder_router
from .go_slim_mapper_router import router as go_slim_mapper_router
from .blast_router import router as blast_router
from .patmatch_router import router as patmatch_router
from .restriction_mapper_router import router as restriction_mapper_router
from .homology_router import router as homology_router

__all__ = [
    "health_router",
    "locus_router",
    "reference_router",
    "go_router",
    "phenotype_router",
    "search_router",
    "sequence_router",
    "seq_tools_router",
    "batch_download_router",
    "feature_search_router",
    "genome_version_router",
    "colleague_router",
    "gene_registry_router",
    "chromosome_router",
    "webprimer_router",
    "go_term_finder_router",
    "go_slim_mapper_router",
    "blast_router",
    "patmatch_router",
    "restriction_mapper_router",
    "homology_router",
]
