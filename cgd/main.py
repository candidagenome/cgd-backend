# cgd/main.py
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import routers (routers should NOT call app.include_router() themselves)
from cgd.api.routers.health_router import router as health_router
from cgd.api.routers.locus_router import router as locus_router
from cgd.api.routers.reference_router import router as reference_router
from cgd.api.routers.chromosome_router import router as chromosome_router


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
    app.include_router(health_router)
    app.include_router(locus_router)
    app.include_router(reference_router)
    app.include_router(chromosome_router)

    return app


# Uvicorn entrypoint: uvicorn cgd.main:app --reload
app = create_app()


