from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from cgd.api import frontend_seo
from cgd.db.deps import get_db

router = APIRouter(tags=["frontend"])


@router.api_route(
    "/locus/{name}",
    methods=["GET", "HEAD"],
    response_class=HTMLResponse,
    include_in_schema=False,
)
def locus_page(name: str, db: Session = Depends(get_db)):
    """
    Serve the SPA shell for locus pages with source-visible SEO metadata.

    Production nginx serves the frontend as static files, so Vite's
    transformIndexHtml hook does not run there. This route provides a small
    backend HTML rewrite for /locus/:name while React still handles the UI.
    """
    try:
        return HTMLResponse(
            content=frontend_seo.render_locus_html(db, name),
            headers={"Cache-Control": "no-cache"},
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail="Frontend index.html not found") from exc
