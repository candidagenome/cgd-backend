from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from cgd.core.settings import settings
from cgd.db.deps import get_db
from cgd.api.crud.search_crud import dispatch
from cgd.api.services import search_service
from cgd.schemas.search_schema import SearchResponse, ResolveResponse


# Schema for legacy dispatch endpoint
class SearchDispatchData(BaseModel):
    kind: str
    target: str
    params: dict[str, str]


class SearchDispatchResponse(BaseModel):
    dispatch: SearchDispatchData


router = APIRouter(prefix="/api/search", tags=["search"])


@router.get("/resolve", response_model=ResolveResponse)
def resolve_identifier(
    query: str = Query(..., min_length=1, description="Identifier to resolve"),
    db: Session = Depends(get_db),
):
    """
    Resolve an exact identifier to a direct URL.

    Checks if query matches exactly:
    - Gene/locus name (gene_name or feature_name)
    - Gene/locus CGDID (dbxref_id like CAL0001571)
    - Reference CGDID (dbxref_id like CAL0080639)

    If resolved, returns redirect_url for direct navigation.
    If not resolved, returns resolved=False and frontend should show search results.
    """
    return search_service.resolve_identifier(db, query)


@router.get("/quick", response_model=SearchResponse)
def quick_search(
    query: str = Query(..., min_length=1, description="Search query string"),
    limit: int = Query(20, ge=1, le=100, description="Max results per category"),
    db: Session = Depends(get_db),
):
    """
    Quick search across all categories (genes, GO terms, phenotypes, references).

    Returns results grouped by category.
    """
    return search_service.quick_search(db, query, limit)


@router.get("", response_model=SearchDispatchResponse)
def legacy_search_dispatch(
    class_: str = Query(..., alias="class"),
    item: str = Query(...),
):
    """
    Legacy search dispatch endpoint for compatibility with old CGI URLs.
    """
    if not settings.allow_search_dispatch:
        raise HTTPException(status_code=403, detail="Search dispatch disabled")

    res = dispatch(class_, item)
    if not res:
        raise HTTPException(status_code=404, detail=f"Unknown class: {class_}")

    return {"dispatch": {"kind": res.kind, "target": res.target, "params": res.params}}
