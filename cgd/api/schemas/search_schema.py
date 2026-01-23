from pydantic import BaseModel


class Dispatch(BaseModel):
    kind: str
    target: str
    params: dict[str, str]


class SearchDispatchResponse(BaseModel):
    dispatch: Dispatch
