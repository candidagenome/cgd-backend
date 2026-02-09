"""
Database Search Service - Search phenotypes and find associated features.

Mirrors functionality from legacy SearchDB.pm for curators to search
phenotypes and get their database IDs for curation purposes.
"""

import logging
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    PhenoAnnotation,
    Phenotype,
)

logger = logging.getLogger(__name__)


class DbSearchService:
    """Service for searching phenotypes and related data."""

    def __init__(self, db: Session):
        self.db = db

    def search_phenotypes(
        self,
        query: str,
        limit: int = 100,
    ) -> list[dict]:
        """
        Search for phenotypes matching the query string.

        Searches in observable, qualifier, experiment_type, and mutant_type fields.

        Args:
            query: Search term (supports wildcards with %)
            limit: Maximum results to return

        Returns:
            List of phenotype dictionaries with phenotype_no and display text
        """
        # Add wildcards for LIKE matching
        search_pattern = f"%{query}%"

        results = (
            self.db.query(Phenotype)
            .filter(
                (Phenotype.observable.ilike(search_pattern))
                | (Phenotype.qualifier.ilike(search_pattern))
                | (Phenotype.experiment_type.ilike(search_pattern))
                | (Phenotype.mutant_type.ilike(search_pattern))
            )
            .order_by(Phenotype.observable, Phenotype.qualifier)
            .limit(limit)
            .all()
        )

        return [
            {
                "phenotype_no": p.phenotype_no,
                "observable": p.observable,
                "qualifier": p.qualifier,
                "experiment_type": p.experiment_type,
                "mutant_type": p.mutant_type,
                "source": p.source,
                "display_text": self._format_phenotype_text(p),
            }
            for p in results
        ]

    def _format_phenotype_text(self, phenotype: Phenotype) -> str:
        """Format phenotype as display text."""
        parts = []
        if phenotype.observable:
            parts.append(phenotype.observable)
        if phenotype.qualifier:
            parts.append(f"({phenotype.qualifier})")
        if phenotype.mutant_type:
            parts.append(f"[{phenotype.mutant_type}]")
        return " ".join(parts) if parts else str(phenotype.phenotype_no)

    def get_phenotype_details(self, phenotype_no: int) -> Optional[dict]:
        """
        Get details for a specific phenotype including associated features.

        Args:
            phenotype_no: Phenotype ID

        Returns:
            Phenotype details with linked features, or None if not found
        """
        phenotype = (
            self.db.query(Phenotype)
            .filter(Phenotype.phenotype_no == phenotype_no)
            .first()
        )

        if not phenotype:
            return None

        # Get associated features through pheno_annotation
        annotations = (
            self.db.query(PhenoAnnotation, Feature)
            .join(Feature, PhenoAnnotation.feature_no == Feature.feature_no)
            .filter(PhenoAnnotation.phenotype_no == phenotype_no)
            .all()
        )

        features = [
            {
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "gene_name": feature.gene_name,
                "pheno_annotation_no": pa.pheno_annotation_no,
            }
            for pa, feature in annotations
        ]

        return {
            "phenotype_no": phenotype.phenotype_no,
            "observable": phenotype.observable,
            "qualifier": phenotype.qualifier,
            "experiment_type": phenotype.experiment_type,
            "mutant_type": phenotype.mutant_type,
            "source": phenotype.source,
            "display_text": self._format_phenotype_text(phenotype),
            "features": features,
            "feature_count": len(features),
        }

    def get_observable_values(self) -> list[str]:
        """Get distinct observable values for autocomplete."""
        results = (
            self.db.query(Phenotype.observable)
            .distinct()
            .filter(Phenotype.observable.isnot(None))
            .order_by(Phenotype.observable)
            .all()
        )
        return [r[0] for r in results if r[0]]

    def get_qualifier_values(self) -> list[str]:
        """Get distinct qualifier values for autocomplete."""
        results = (
            self.db.query(Phenotype.qualifier)
            .distinct()
            .filter(Phenotype.qualifier.isnot(None))
            .order_by(Phenotype.qualifier)
            .all()
        )
        return [r[0] for r in results if r[0]]

    def get_experiment_types(self) -> list[str]:
        """Get distinct experiment types for autocomplete."""
        results = (
            self.db.query(Phenotype.experiment_type)
            .distinct()
            .filter(Phenotype.experiment_type.isnot(None))
            .order_by(Phenotype.experiment_type)
            .all()
        )
        return [r[0] for r in results if r[0]]

    def get_mutant_types(self) -> list[str]:
        """Get distinct mutant types for autocomplete."""
        results = (
            self.db.query(Phenotype.mutant_type)
            .distinct()
            .filter(Phenotype.mutant_type.isnot(None))
            .order_by(Phenotype.mutant_type)
            .all()
        )
        return [r[0] for r in results if r[0]]
