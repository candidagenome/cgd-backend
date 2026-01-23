"""Legacy CGI-style search dispatch mapping.

If your Perl CGI has `search.pl?class=X&item=Y` that redirects to different pages,
this module provides a compatible mapping that returns an API target.

Expand this mapping as you migrate more endpoints.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DispatchResult:
    kind: str
    target: str
    params: dict[str, str]


CLASS_TO_ENDPOINT: dict[str, str] = {
    "locus": "/api/locus",
    "gene": "/api/locus",
    "feature": "/api/locus",
    # "reference": "/api/reference",
    # "phenotype": "/api/phenotype",
}


def dispatch(class_: str, item: str) -> Optional[DispatchResult]:
    cls = (class_ or "").strip().lower()
    target = CLASS_TO_ENDPOINT.get(cls)
    if not target:
        return None
    return DispatchResult(kind="api", target=target, params={"locus": item})
