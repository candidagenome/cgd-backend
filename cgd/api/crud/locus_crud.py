from __future__ import annotations

from sqlalchemy import or_, func
from sqlalchemy.orm import Session

from cgd.models.locus_model import Feature


def find_features_by_term(db: Session, term: str):
    t = term.strip()
    if not t:
        return []

    return (
        db.query(Feature)
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(t),
                func.upper(Feature.feature_name) == func.upper(t),
            )
        )
        .order_by(Feature.feature_no.asc())
        .all()
    )
