"""Curation API routers for curator-only operations."""

from .todo_list_router import router as todo_list_router
from .go_curation_router import router as go_curation_router
from .reference_curation_router import router as reference_curation_router
from .phenotype_curation_router import router as phenotype_curation_router
from .colleague_curation_router import router as colleague_curation_router
from .locus_curation_router import router as locus_curation_router
from .litguide_curation_router import router as litguide_curation_router
from .note_curation_router import router as note_curation_router
from .feature_curation_router import router as feature_curation_router
from .link_curation_router import router as link_curation_router
from .gene_registry_curation_router import router as gene_registry_curation_router
from .paragraph_curation_router import router as paragraph_curation_router

__all__ = [
    "todo_list_router",
    "go_curation_router",
    "reference_curation_router",
    "phenotype_curation_router",
    "colleague_curation_router",
    "locus_curation_router",
    "litguide_curation_router",
    "note_curation_router",
    "feature_curation_router",
    "link_curation_router",
    "gene_registry_curation_router",
    "paragraph_curation_router",
]
