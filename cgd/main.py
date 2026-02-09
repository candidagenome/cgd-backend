# cgd/main.py
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import auth router
from cgd.auth import auth_router

# Import routers (routers should NOT call app.include_router() themselves)
from cgd.api.routers.health_router import router as health_router
from cgd.api.routers.locus_router import router as locus_router
from cgd.api.routers.reference_router import router as reference_router
from cgd.api.routers.chromosome_router import router as chromosome_router
from cgd.api.routers.go_router import router as go_router
from cgd.api.routers.phenotype_router import router as phenotype_router
from cgd.api.routers.search_router import router as search_router
from cgd.api.routers.sequence_router import router as sequence_router
from cgd.api.routers.seq_tools_router import router as seq_tools_router
from cgd.api.routers.blast_router import router as blast_router
from cgd.api.routers.patmatch_router import router as patmatch_router
from cgd.api.routers.batch_download_router import router as batch_download_router
from cgd.api.routers.restriction_mapper_router import router as restriction_mapper_router
from cgd.api.routers.feature_search_router import router as feature_search_router
from cgd.api.routers.genome_version_router import router as genome_version_router
from cgd.api.routers.colleague_router import router as colleague_router
from cgd.api.routers.gene_registry_router import router as gene_registry_router
from cgd.api.routers.webprimer_router import router as webprimer_router
from cgd.api.routers.go_term_finder_router import router as go_term_finder_router
from cgd.api.routers.go_slim_mapper_router import router as go_slim_mapper_router
from cgd.api.routers.homology_router import router as homology_router

# Import curation routers (require authentication)
from cgd.api.routers.curation import (
    todo_list_router,
    go_curation_router,
    reference_curation_router,
    phenotype_curation_router,
    colleague_curation_router,
    locus_curation_router,
    litguide_curation_router,
    note_curation_router,
    feature_curation_router,
    link_curation_router,
    gene_registry_curation_router,
    paragraph_curation_router,
    litreview_curation_router,
    ref_annotation_curation_router,
    db_search_router,
    sequence_curation_router,
    coordinate_curation_router,
    seq_alignment_router,
)


def create_app() -> FastAPI:
    app = FastAPI(
        title="CGD API",
        version="0.1.0",
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://localhost:3000",
            "https://www.candidagenome.org",
            "https://candidagenome.org",
            "https://dev.candidagenome.org",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(auth_router)
    app.include_router(health_router)
    app.include_router(locus_router)
    app.include_router(reference_router)
    app.include_router(chromosome_router)
    app.include_router(go_router)
    app.include_router(phenotype_router)
    app.include_router(search_router)
    app.include_router(sequence_router)
    app.include_router(seq_tools_router)
    app.include_router(blast_router)
    app.include_router(patmatch_router)
    app.include_router(batch_download_router)
    app.include_router(restriction_mapper_router)
    app.include_router(feature_search_router)
    app.include_router(genome_version_router)
    app.include_router(colleague_router)
    app.include_router(gene_registry_router)
    app.include_router(webprimer_router)
    app.include_router(go_term_finder_router)
    app.include_router(go_slim_mapper_router)
    app.include_router(homology_router)

    # Curation routers (require authentication)
    app.include_router(todo_list_router)
    app.include_router(go_curation_router)
    app.include_router(reference_curation_router)
    app.include_router(phenotype_curation_router)
    app.include_router(colleague_curation_router)
    app.include_router(locus_curation_router)
    app.include_router(litguide_curation_router)
    app.include_router(note_curation_router)
    app.include_router(feature_curation_router)
    app.include_router(link_curation_router)
    app.include_router(gene_registry_curation_router)
    app.include_router(paragraph_curation_router)
    app.include_router(litreview_curation_router)
    app.include_router(ref_annotation_curation_router)
    app.include_router(db_search_router)
    app.include_router(sequence_curation_router)
    app.include_router(coordinate_curation_router)
    app.include_router(seq_alignment_router)

    return app


# Uvicorn entrypoint: uvicorn cgd.main:app --reload
app = create_app()


