from fastapi import APIRouter, HTTPException, Query

from cgd.core.settings import settings
from cgd.api.crud.search_crud import dispatch
from cgd.api.schemas.search_schema import SearchDispatchResponse

router = APIRouter(prefix="/api/search", tags=["search"])


@router.get("", response_model=SearchDispatchResponse)
def legacy_search_dispatch(
    class_: str = Query(..., alias="class"),
    item: str = Query(...),
):
    if not settings.allow_search_dispatch:
        raise HTTPException(status_code=403, detail="Search dispatch disabled")

    res = dispatch(class_, item)
    if not res:
        raise HTTPException(status_code=404, detail=f"Unknown class: {class_}")

    return {"dispatch": {"kind": res.kind, "target": res.target, "params": res.params}}
