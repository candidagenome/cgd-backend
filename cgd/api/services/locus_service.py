from __future__ import annotations

from sqlalchemy.orm import Session

from cgd.api.crud.locus_crud import find_features_by_term


def get_locus_summary(db: Session, name: str):
    rows = find_features_by_term(db, name)

    return {
        "results": [
            {
                "id": int(r.feature_no),
                "name": r.feature_name,
                "display_name": r.gene_name or r.feature_name,
            }
            for r in rows
        ]
    }
