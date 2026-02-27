# from __future__ import annotations
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from cgd.models.locus_model import Feature
from cgd.models.models import FeatAlias, Alias


def get_features_for_locus_name(db: Session, name: str) -> list[Feature]:
    """
    Match on:
      - Feature.feature_name (systematic-like)
      - Feature.gene_name (common gene name)
      - Alias.alias_name (alias names like HOG1 for Cd36_18080)

    Returns features from all organisms that match by gene_name, feature_name,
    or have an alias matching the query.
    """
    n = name.strip()
    upper_n = func.upper(n)

    # First, get features matching directly by gene_name or feature_name
    direct_matches = (
        db.query(Feature)
        .filter(
            or_(
                func.upper(Feature.gene_name) == upper_n,
                func.upper(Feature.feature_name) == upper_n,
            )
        )
        .all()
    )

    # Get feature_nos already found to avoid duplicates
    found_feature_nos = {f.feature_no for f in direct_matches}

    # Also search for features with matching aliases
    alias_matches = (
        db.query(Feature)
        .join(FeatAlias, Feature.feature_no == FeatAlias.feature_no)
        .join(Alias, FeatAlias.alias_no == Alias.alias_no)
        .filter(func.upper(Alias.alias_name) == upper_n)
        .all()
    )

    # Combine results, avoiding duplicates
    all_features = list(direct_matches)
    for feat in alias_matches:
        if feat.feature_no not in found_feature_nos:
            all_features.append(feat)
            found_feature_nos.add(feat.feature_no)

    return all_features

