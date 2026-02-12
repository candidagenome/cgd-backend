"""
Coordinate and Relationship Curation Service.

Mirrors functionality from legacy UpdateCoordRelation.pm for curators to
adjust feature coordinates, update feature/subfeature relationships,
and manage sequences.
"""

import logging
from typing import Optional

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    FeatLocation,
    FeatRelationship,
    Seq,
    Code,
)

logger = logging.getLogger(__name__)


class CoordinateCurationService:
    """Service for coordinate and relationship curation."""

    def __init__(self, db: Session):
        self.db = db

    def get_seq_sources(self) -> list[str]:
        """
        Get available sequence sources (assemblies/strains).

        Returns:
            List of distinct seq_source values
        """
        results = (
            self.db.query(Seq.source)
            .filter(
                Seq.is_seq_current == "Y",
                func.upper(Seq.seq_type) == "GENOMIC",
            )
            .distinct()
            .order_by(Seq.source)
            .all()
        )
        return [r[0] for r in results if r[0]]

    def get_feature_info(
        self,
        feature_name: str,
        seq_source: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Get feature information with coordinates and subfeatures.

        Args:
            feature_name: Feature/gene name
            seq_source: Assembly/strain to filter by

        Returns:
            Feature info with coordinates, subfeatures, and parents
        """
        # Find the feature
        feature = (
            self.db.query(Feature)
            .filter(
                or_(
                    func.upper(Feature.feature_name) == feature_name.upper(),
                    func.upper(Feature.gene_name) == feature_name.upper(),
                )
            )
            .first()
        )

        if not feature:
            return None

        # Get current location
        location_query = (
            self.db.query(FeatLocation, Seq)
            .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
            .filter(
                FeatLocation.feature_no == feature.feature_no,
                FeatLocation.is_loc_current == "Y",
            )
        )

        if seq_source:
            location_query = location_query.filter(Seq.source == seq_source)

        location_result = location_query.first()

        location = None
        root_feature_name = None
        if location_result:
            loc, seq = location_result
            location = {
                "feat_location_no": loc.feat_location_no,
                "start_coord": loc.start_coord,
                "stop_coord": loc.stop_coord,
                "strand": loc.strand,
                "seq_source": seq.source,
            }
            # Get root feature name (chromosome/contig)
            root_feature = (
                self.db.query(Feature)
                .filter(Feature.feature_no == seq.feature_no)
                .first()
            )
            if root_feature:
                root_feature_name = root_feature.feature_name

        # Get subfeatures (children)
        subfeatures = self._get_subfeatures(feature.feature_no, seq_source)

        # Get parents
        parents = self._get_parents(feature.feature_no, seq_source)

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "feature_type": feature.feature_type,
            "dbxref_id": feature.dbxref_id,
            "headline": feature.headline,
            "location": location,
            "root_feature_name": root_feature_name,
            "subfeatures": subfeatures,
            "parents": parents,
        }

    def _get_subfeatures(
        self,
        parent_feature_no: int,
        seq_source: Optional[str] = None,
    ) -> list[dict]:
        """Get subfeatures (children) for a feature."""
        # Query feat_relationship for children
        query = (
            self.db.query(
                Feature,
                FeatRelationship,
                FeatLocation,
            )
            .join(
                FeatRelationship,
                Feature.feature_no == FeatRelationship.child_feature_no,
            )
            .outerjoin(
                FeatLocation,
                and_(
                    Feature.feature_no == FeatLocation.feature_no,
                    FeatLocation.is_loc_current == "Y",
                ),
            )
            .filter(
                FeatRelationship.parent_feature_no == parent_feature_no,
            )
        )

        if seq_source:
            # Join to Seq to filter by source
            query = (
                query.join(
                    Seq,
                    FeatLocation.root_seq_no == Seq.seq_no,
                    isouter=True,
                )
                .filter(
                    or_(
                        Seq.source == seq_source,
                        Seq.source.is_(None),  # Include features without location
                    )
                )
            )

        results = query.order_by(FeatLocation.start_coord).all()

        subfeatures = []
        for feature, rel, loc in results:
            subfeatures.append({
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "feature_type": feature.feature_type,
                "relationship_type": rel.relationship_type,
                "rank": rel.rank,
                "start_coord": loc.start_coord if loc else None,
                "stop_coord": loc.stop_coord if loc else None,
                "strand": loc.strand if loc else None,
            })

        return subfeatures

    def _get_parents(
        self,
        child_feature_no: int,
        seq_source: Optional[str] = None,
    ) -> list[dict]:
        """Get parent features for a feature."""
        query = (
            self.db.query(
                Feature,
                FeatRelationship,
                FeatLocation,
            )
            .join(
                FeatRelationship,
                Feature.feature_no == FeatRelationship.parent_feature_no,
            )
            .outerjoin(
                FeatLocation,
                and_(
                    Feature.feature_no == FeatLocation.feature_no,
                    FeatLocation.is_loc_current == "Y",
                ),
            )
            .filter(
                FeatRelationship.child_feature_no == child_feature_no,
            )
        )

        if seq_source:
            query = (
                query.join(
                    Seq,
                    FeatLocation.root_seq_no == Seq.seq_no,
                    isouter=True,
                )
                .filter(
                    or_(
                        Seq.source == seq_source,
                        Seq.source.is_(None),
                    )
                )
            )

        results = query.all()

        parents = []
        for feature, rel, loc in results:
            parents.append({
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "gene_name": feature.gene_name,
                "feature_type": feature.feature_type,
                "relationship_type": rel.relationship_type,
                "rank": rel.rank,
                "start_coord": loc.start_coord if loc else None,
                "stop_coord": loc.stop_coord if loc else None,
                "strand": loc.strand if loc else None,
            })

        return parents

    def get_feature_types(self) -> list[str]:
        """Get available feature types."""
        results = (
            self.db.query(Feature.feature_type)
            .distinct()
            .order_by(Feature.feature_type)
            .all()
        )
        return [r[0] for r in results if r[0]]

    def get_relationship_types(self) -> list[str]:
        """Get available relationship types from CODE table."""
        results = (
            self.db.query(Code.code_value)
            .filter(
                Code.tab_name == "FEAT_RELATIONSHIP",
                Code.col_name == "RELATIONSHIP_TYPE",
            )
            .order_by(Code.code_value)
            .all()
        )
        return [r[0] for r in results]

    def get_feature_qualifiers(self) -> list[str]:
        """Get available feature qualifiers from CODE table."""
        results = (
            self.db.query(Code.code_value)
            .filter(
                Code.tab_name == "FEATURE",
                Code.col_name == "FEATURE_QUALIFIER",
            )
            .order_by(Code.code_value)
            .all()
        )
        return [r[0] for r in results]

    def preview_coordinate_changes(
        self,
        feature_name: str,
        seq_source: str,
        changes: list[dict],
    ) -> dict:
        """
        Preview coordinate changes without committing.

        Args:
            feature_name: Main feature name
            seq_source: Assembly/strain
            changes: List of changes with feature_no, new coordinates, etc.

        Returns:
            Preview showing old vs new values
        """
        # Get current feature info
        feature_info = self.get_feature_info(feature_name, seq_source)

        if not feature_info:
            return {"error": f"Feature {feature_name} not found"}

        # Build a map of current coordinates
        current_coords = {}
        if feature_info["location"]:
            current_coords[feature_info["feature_no"]] = {
                "feature_name": feature_info["feature_name"],
                "start_coord": feature_info["location"]["start_coord"],
                "stop_coord": feature_info["location"]["stop_coord"],
                "strand": feature_info["location"]["strand"],
            }

        for sub in feature_info["subfeatures"]:
            if sub["start_coord"]:
                current_coords[sub["feature_no"]] = {
                    "feature_name": sub["feature_name"],
                    "start_coord": sub["start_coord"],
                    "stop_coord": sub["stop_coord"],
                    "strand": sub["strand"],
                }

        # Process changes
        change_details = []
        for change in changes:
            feature_no = change.get("feature_no")
            if feature_no not in current_coords:
                continue

            current = current_coords[feature_no]
            new_start = change.get("start_coord", current["start_coord"])
            new_stop = change.get("stop_coord", current["stop_coord"])
            new_strand = change.get("strand", current["strand"])

            if (new_start != current["start_coord"] or
                new_stop != current["stop_coord"] or
                new_strand != current["strand"]):
                change_details.append({
                    "feature_no": feature_no,
                    "feature_name": current["feature_name"],
                    "old_start": current["start_coord"],
                    "old_stop": current["stop_coord"],
                    "old_strand": current["strand"],
                    "new_start": new_start,
                    "new_stop": new_stop,
                    "new_strand": new_strand,
                })

        return {
            "feature_name": feature_info["feature_name"],
            "seq_source": seq_source,
            "changes": change_details,
            "change_count": len(change_details),
        }

    def search_features(
        self,
        query: str,
        limit: int = 20,
    ) -> list[dict]:
        """
        Search for features by name.

        Args:
            query: Search term
            limit: Max results

        Returns:
            List of matching features
        """
        pattern = f"%{query}%"

        results = (
            self.db.query(Feature)
            .filter(
                or_(
                    Feature.feature_name.ilike(pattern),
                    Feature.gene_name.ilike(pattern),
                )
            )
            .order_by(Feature.feature_name)
            .limit(limit)
            .all()
        )

        return [
            {
                "feature_no": f.feature_no,
                "feature_name": f.feature_name,
                "gene_name": f.gene_name,
                "feature_type": f.feature_type,
            }
            for f in results
        ]
