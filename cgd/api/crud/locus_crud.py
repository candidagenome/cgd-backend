# from __future__ import annotations
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from cgd.models.locus_model import Feature


def get_features_for_locus_name(db: Session, name: str) -> list[Feature]:
    """
    Match on:
      - Feature.feature_name (systematic-like)
      - Feature.gene_name (common gene name)
    """
    n = name.strip()
    return (
        db.query(Feature)
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
            )
        )
        .all()
    )

