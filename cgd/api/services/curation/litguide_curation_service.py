"""
Literature Guide Curation Service - Business logic for feature-centric literature curation.

Mirrors functionality from legacy LitGuideCurationPage.pm:
- Get literature for a feature (curated and uncurated)
- Add/remove topic associations between references and features
- Update curation status
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    Note,
    NoteLink,
    RefLink,
    RefProperty,
    Reference,
    RefpropFeat,
)

logger = logging.getLogger(__name__)

SOURCE = "CGD"

# Literature topics from legacy CV
LITERATURE_TOPICS = [
    "Gene Product",
    "Phenotype",
    "Expression",
    "Disease",
    "Gene Ontology",
    "Regulation",
    "Physical Interaction",
    "Genetic Interaction",
    "Localization",
    "Sequence",
    "Homology/Comparative",
    "Other",
]

# Curation status values
CURATION_STATUSES = [
    "Not Yet Curated",
    "High Priority",
    "Partially Curated",
    "Curated Todo",
    "Done: No genes",
    "Done: All genes HTP",
    "Done: Curated",
]


class LitGuideCurationError(Exception):
    """Raised when literature guide curation validation fails."""

    pass


class LitGuideCurationService:
    """Service for feature-centric literature guide curation."""

    def __init__(self, db: Session):
        self.db = db

    def get_feature_by_name(self, name: str) -> Optional[Feature]:
        """Look up feature by name or gene_name."""
        return (
            self.db.query(Feature)
            .filter(
                or_(
                    func.upper(Feature.feature_name) == name.upper(),
                    func.upper(Feature.gene_name) == name.upper(),
                )
            )
            .first()
        )

    def get_feature_by_no(self, feature_no: int) -> Optional[Feature]:
        """Get feature by feature_no."""
        return (
            self.db.query(Feature)
            .filter(Feature.feature_no == feature_no)
            .first()
        )

    def get_feature_literature(self, feature_no: int) -> dict:
        """
        Get all literature for a feature.

        Returns curated (with topics) and uncurated references.
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LitGuideCurationError(f"Feature {feature_no} not found")

        # Get curated literature (references with topics via RefpropFeat)
        curated_query = (
            self.db.query(
                Reference.reference_no,
                Reference.pubmed,
                Reference.citation,
                Reference.title,
                Reference.year,
                RefProperty.property_value.label("topic"),
                RefProperty.ref_property_no,
                RefpropFeat.refprop_feat_no,
            )
            .join(RefProperty, Reference.reference_no == RefProperty.reference_no)
            .join(RefpropFeat, RefProperty.ref_property_no == RefpropFeat.ref_property_no)
            .filter(RefpropFeat.feature_no == feature_no)
            .filter(RefProperty.property_type == "literature_topic")
            .order_by(Reference.year.desc(), Reference.pubmed)
        )

        curated_results = curated_query.all()

        # Group by reference
        curated_refs = {}
        for row in curated_results:
            ref_no = row.reference_no
            if ref_no not in curated_refs:
                curated_refs[ref_no] = {
                    "reference_no": row.reference_no,
                    "pubmed": row.pubmed,
                    "citation": row.citation,
                    "title": row.title,
                    "year": row.year,
                    "topics": [],
                }
            curated_refs[ref_no]["topics"].append({
                "topic": row.topic,
                "ref_property_no": row.ref_property_no,
                "refprop_feat_no": row.refprop_feat_no,
            })

        # Get uncurated literature (references linked via RefLink but no topics)
        # RefLink connects references to features directly
        uncurated_query = (
            self.db.query(
                Reference.reference_no,
                Reference.pubmed,
                Reference.citation,
                Reference.title,
                Reference.year,
            )
            .join(RefLink, Reference.reference_no == RefLink.reference_no)
            .filter(RefLink.tab_name == "FEATURE")
            .filter(RefLink.col_name == "FEATURE_NO")
            .filter(RefLink.primary_key == feature_no)
            .filter(~Reference.reference_no.in_(curated_refs.keys()) if curated_refs else True)
            .order_by(Reference.year.desc(), Reference.pubmed)
        )

        uncurated_results = uncurated_query.all()

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "curated": list(curated_refs.values()),
            "uncurated": [
                {
                    "reference_no": row.reference_no,
                    "pubmed": row.pubmed,
                    "citation": row.citation,
                    "title": row.title,
                    "year": row.year,
                }
                for row in uncurated_results
            ],
        }

    def add_topic_association(
        self,
        feature_no: int,
        reference_no: int,
        topic: str,
        curator_userid: str,
    ) -> int:
        """
        Add a topic association between a feature and reference.

        Returns refprop_feat_no.
        """
        # Validate topic
        if topic not in LITERATURE_TOPICS:
            raise LitGuideCurationError(
                f"Invalid topic '{topic}'. Valid topics: {', '.join(LITERATURE_TOPICS)}"
            )

        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LitGuideCurationError(f"Feature {feature_no} not found")

        reference = (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )
        if not reference:
            raise LitGuideCurationError(f"Reference {reference_no} not found")

        # Get or create ref_property for this topic
        ref_prop = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "literature_topic",
                RefProperty.property_value == topic,
            )
            .first()
        )

        if not ref_prop:
            ref_prop = RefProperty(
                reference_no=reference_no,
                source=SOURCE,
                property_type="literature_topic",
                property_value=topic,
                date_last_reviewed=datetime.now(),
                created_by=curator_userid[:12],
            )
            self.db.add(ref_prop)
            self.db.flush()

        # Check for existing link
        existing = (
            self.db.query(RefpropFeat)
            .filter(
                RefpropFeat.ref_property_no == ref_prop.ref_property_no,
                RefpropFeat.feature_no == feature_no,
            )
            .first()
        )

        if existing:
            raise LitGuideCurationError(
                f"Topic '{topic}' already associated with this feature-reference pair"
            )

        # Create link
        link = RefpropFeat(
            ref_property_no=ref_prop.ref_property_no,
            feature_no=feature_no,
            created_by=curator_userid[:12],
        )
        self.db.add(link)
        self.db.commit()

        logger.info(
            f"Added topic '{topic}' association: feature {feature_no}, "
            f"reference {reference_no} by {curator_userid}"
        )

        return link.refprop_feat_no

    def remove_topic_association(
        self,
        refprop_feat_no: int,
        curator_userid: str,
    ) -> bool:
        """Remove a topic association."""
        link = (
            self.db.query(RefpropFeat)
            .filter(RefpropFeat.refprop_feat_no == refprop_feat_no)
            .first()
        )

        if not link:
            raise LitGuideCurationError(f"Topic association {refprop_feat_no} not found")

        self.db.delete(link)
        self.db.commit()

        logger.info(f"Removed topic association {refprop_feat_no} by {curator_userid}")

        return True

    def set_reference_curation_status(
        self,
        reference_no: int,
        curation_status: str,
        curator_userid: str,
    ) -> int:
        """
        Set or update curation status for a reference.

        Returns ref_property_no.
        """
        if curation_status not in CURATION_STATUSES:
            raise LitGuideCurationError(
                f"Invalid status '{curation_status}'. "
                f"Valid statuses: {', '.join(CURATION_STATUSES)}"
            )

        reference = (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )
        if not reference:
            raise LitGuideCurationError(f"Reference {reference_no} not found")

        # Check for existing curation status property
        existing = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "curation_status",
            )
            .first()
        )

        if existing:
            existing.property_value = curation_status
            existing.date_last_reviewed = datetime.now()
            self.db.commit()
            return existing.ref_property_no

        # Create new property
        prop = RefProperty(
            reference_no=reference_no,
            source=SOURCE,
            property_type="curation_status",
            property_value=curation_status,
            date_last_reviewed=datetime.now(),
            created_by=curator_userid[:12],
        )
        self.db.add(prop)
        self.db.commit()

        logger.info(
            f"Set curation status '{curation_status}' for reference {reference_no} "
            f"by {curator_userid}"
        )

        return prop.ref_property_no

    def get_reference_curation_status(self, reference_no: int) -> Optional[str]:
        """Get curation status for a reference."""
        prop = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "curation_status",
            )
            .first()
        )

        return prop.property_value if prop else None

    def search_references(
        self,
        query: str,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[dict], int]:
        """
        Search references by pubmed, title, or citation.

        Returns (list of reference dicts, total count).
        """
        base_query = self.db.query(Reference)

        # Try as pubmed ID first
        try:
            pubmed = int(query)
            base_query = base_query.filter(Reference.pubmed == pubmed)
        except ValueError:
            # Search by title or citation
            base_query = base_query.filter(
                or_(
                    Reference.title.ilike(f"%{query}%"),
                    Reference.citation.ilike(f"%{query}%"),
                )
            )

        base_query = base_query.order_by(Reference.year.desc(), Reference.pubmed)

        total = base_query.count()
        results = base_query.offset((page - 1) * page_size).limit(page_size).all()

        return (
            [
                {
                    "reference_no": r.reference_no,
                    "pubmed": r.pubmed,
                    "citation": r.citation,
                    "title": r.title,
                    "year": r.year,
                    "curation_status": self.get_reference_curation_status(r.reference_no),
                }
                for r in results
            ],
            total,
        )

    def get_reference_literature(self, reference_no: int) -> dict:
        """
        Get reference details with all associated features and topics.

        Used for reference-centric literature guide curation.
        """
        reference = (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )
        if not reference:
            raise LitGuideCurationError(f"Reference {reference_no} not found")

        # Get curation status
        curation_status = self.get_reference_curation_status(reference_no)

        # Get all feature-topic associations for this reference
        feature_topic_query = (
            self.db.query(
                Feature.feature_no,
                Feature.feature_name,
                Feature.gene_name,
                Feature.feature_type,
                RefProperty.property_value.label("topic"),
                RefProperty.ref_property_no,
                RefpropFeat.refprop_feat_no,
            )
            .join(RefpropFeat, Feature.feature_no == RefpropFeat.feature_no)
            .join(RefProperty, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
            .filter(RefProperty.reference_no == reference_no)
            .filter(RefProperty.property_type == "literature_topic")
            .order_by(Feature.feature_name, RefProperty.property_value)
        )

        results = feature_topic_query.all()

        # Group by feature
        features_dict = {}
        for row in results:
            feat_no = row.feature_no
            if feat_no not in features_dict:
                features_dict[feat_no] = {
                    "feature_no": row.feature_no,
                    "feature_name": row.feature_name,
                    "gene_name": row.gene_name,
                    "feature_type": row.feature_type,
                    "topics": [],
                }
            features_dict[feat_no]["topics"].append({
                "topic": row.topic,
                "ref_property_no": row.ref_property_no,
                "refprop_feat_no": row.refprop_feat_no,
            })

        return {
            "reference_no": reference.reference_no,
            "pubmed": reference.pubmed,
            "citation": reference.citation,
            "title": reference.title,
            "year": reference.year,
            "curation_status": curation_status,
            "features": list(features_dict.values()),
        }

    def add_feature_to_reference(
        self,
        reference_no: int,
        feature_identifier: str,
        topic: str,
        curator_userid: str,
    ) -> dict:
        """
        Add a feature-topic association to a reference.

        feature_identifier can be feature_no (int as string) or feature/gene name.
        Returns dict with feature info and refprop_feat_no.
        """
        # Validate topic
        if topic not in LITERATURE_TOPICS:
            raise LitGuideCurationError(
                f"Invalid topic '{topic}'. Valid topics: {', '.join(LITERATURE_TOPICS)}"
            )

        # Find feature
        try:
            feature_no = int(feature_identifier)
            feature = self.get_feature_by_no(feature_no)
        except ValueError:
            feature = self.get_feature_by_name(feature_identifier)

        if not feature:
            raise LitGuideCurationError(f"Feature '{feature_identifier}' not found")

        # Use existing method to add association
        refprop_feat_no = self.add_topic_association(
            feature.feature_no,
            reference_no,
            topic,
            curator_userid,
        )

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "refprop_feat_no": refprop_feat_no,
        }

    def unlink_feature_from_reference(
        self,
        reference_no: int,
        feature_identifier: str,
        curator_userid: str,
    ) -> dict:
        """
        Unlink a feature from a reference.

        Removes the RefLink entry connecting the feature to the reference.
        Also removes any topic associations (RefpropFeat) for this feature-reference pair.

        Args:
            reference_no: Reference number
            feature_identifier: Feature name, gene name, or feature_no
            curator_userid: Curator user ID for logging

        Returns:
            Dict with feature info and status
        """
        # Find feature
        try:
            feature_no = int(feature_identifier)
            feature = self.get_feature_by_no(feature_no)
        except ValueError:
            feature = self.get_feature_by_name(feature_identifier)

        if not feature:
            raise LitGuideCurationError(f"Feature '{feature_identifier}' not found")

        # Verify reference exists
        reference = (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )
        if not reference:
            raise LitGuideCurationError(f"Reference {reference_no} not found")

        # Find the RefLink entry
        ref_link = (
            self.db.query(RefLink)
            .filter(
                RefLink.reference_no == reference_no,
                RefLink.tab_name == "FEATURE",
                RefLink.col_name == "FEATURE_NO",
                RefLink.primary_key == feature.feature_no,
            )
            .first()
        )

        if not ref_link:
            raise LitGuideCurationError(
                f"Feature '{feature.feature_name}' is not linked to reference {reference_no}"
            )

        # Remove any topic associations for this feature-reference pair
        # Find all ref_property entries for this reference with literature_topic
        topic_props = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "literature_topic",
            )
            .all()
        )

        removed_topics = 0
        for prop in topic_props:
            # Delete any RefpropFeat entries linking this property to the feature
            deleted = (
                self.db.query(RefpropFeat)
                .filter(
                    RefpropFeat.ref_property_no == prop.ref_property_no,
                    RefpropFeat.feature_no == feature.feature_no,
                )
                .delete()
            )
            removed_topics += deleted

        # Delete the RefLink entry
        self.db.delete(ref_link)
        self.db.commit()

        logger.info(
            f"Unlinked feature {feature.feature_name} (no={feature.feature_no}) "
            f"from reference {reference_no} by {curator_userid}. "
            f"Removed {removed_topics} topic associations."
        )

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "removed_topics": removed_topics,
        }

    def get_reference_notes(self, reference_no: int) -> list[dict]:
        """
        Get all curation notes associated with a reference.

        Notes can be linked to:
        1. Feature-specific topics (via REFPROP_FEAT table)
        2. Non-gene topics (via REF_PROPERTY table)

        Returns list of notes with feature, topic, note text, and note type.
        """
        notes = []

        # Get feature-specific notes (linked via REFPROP_FEAT)
        feature_notes_query = (
            self.db.query(
                Note.note,
                Note.note_type,
                RefProperty.property_value.label("topic"),
                Feature.feature_name,
                Feature.gene_name,
            )
            .join(NoteLink, Note.note_no == NoteLink.note_no)
            .join(RefpropFeat, NoteLink.primary_key == RefpropFeat.refprop_feat_no)
            .join(RefProperty, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
            .join(Feature, RefpropFeat.feature_no == Feature.feature_no)
            .filter(NoteLink.tab_name == "REFPROP_FEAT")
            .filter(RefProperty.reference_no == reference_no)
            .order_by(Feature.feature_name, RefProperty.property_value)
        )

        for row in feature_notes_query.all():
            notes.append({
                "feature_name": row.gene_name or row.feature_name,
                "topic": row.topic,
                "note": row.note,
                "note_type": row.note_type,
            })

        # Get non-gene topic notes (linked via REF_PROPERTY)
        nongene_notes_query = (
            self.db.query(
                Note.note,
                Note.note_type,
                RefProperty.property_value.label("topic"),
            )
            .join(NoteLink, Note.note_no == NoteLink.note_no)
            .join(RefProperty, NoteLink.primary_key == RefProperty.ref_property_no)
            .filter(NoteLink.tab_name == "REF_PROPERTY")
            .filter(RefProperty.reference_no == reference_no)
            .order_by(RefProperty.property_value)
        )

        for row in nongene_notes_query.all():
            notes.append({
                "feature_name": None,  # Non-gene topic
                "topic": row.topic,
                "note": row.note,
                "note_type": row.note_type,
            })

        return notes

    def get_nongene_topics(self, reference_no: int) -> dict:
        """
        Get topics linked to reference but NOT associated with any feature.

        These are ref_property entries with property_type='literature_topic'
        that have no corresponding refprop_feat entries.

        Returns dict with public_topics (literature_topic) and
        internal_topics (curation_status).
        """
        # Get all literature_topic properties for this reference
        all_topic_props = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "literature_topic",
            )
            .all()
        )

        # Find topics that have NO feature associations
        public_topics = []
        for prop in all_topic_props:
            # Check if this property has any feature links
            has_features = (
                self.db.query(RefpropFeat)
                .filter(RefpropFeat.ref_property_no == prop.ref_property_no)
                .first()
            )
            if not has_features:
                public_topics.append({
                    "topic": prop.property_value,
                    "ref_property_no": prop.ref_property_no,
                })

        # Get curation_status properties (internal topics) that have no features
        # These are stored directly on ref_property without refprop_feat links
        all_status_props = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "curation_status",
            )
            .all()
        )

        internal_topics = []
        for prop in all_status_props:
            internal_topics.append({
                "topic": prop.property_value,
                "ref_property_no": prop.ref_property_no,
            })

        return {
            "public_topics": public_topics,
            "internal_topics": internal_topics,
        }

    def add_nongene_topic(
        self,
        reference_no: int,
        topic: str,
        curator_userid: str,
    ) -> int:
        """
        Add a non-gene topic to a reference (topic not associated with any feature).

        Returns ref_property_no.
        """
        # Validate topic
        if topic not in LITERATURE_TOPICS:
            raise LitGuideCurationError(
                f"Invalid topic '{topic}'. Valid topics: {', '.join(LITERATURE_TOPICS)}"
            )

        # Verify reference exists
        reference = (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )
        if not reference:
            raise LitGuideCurationError(f"Reference {reference_no} not found")

        # Check if this topic already exists for the reference
        existing = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "literature_topic",
                RefProperty.property_value == topic,
            )
            .first()
        )

        if existing:
            # Check if it has feature associations
            has_features = (
                self.db.query(RefpropFeat)
                .filter(RefpropFeat.ref_property_no == existing.ref_property_no)
                .first()
            )
            if not has_features:
                raise LitGuideCurationError(
                    f"Non-gene topic '{topic}' already exists for this reference"
                )
            # Topic exists but is associated with features, so we can't add it as non-gene
            raise LitGuideCurationError(
                f"Topic '{topic}' already exists and is associated with features"
            )

        # Create new ref_property
        ref_prop = RefProperty(
            reference_no=reference_no,
            source=SOURCE,
            property_type="literature_topic",
            property_value=topic,
            date_last_reviewed=datetime.now(),
            created_by=curator_userid[:12],
        )
        self.db.add(ref_prop)
        self.db.commit()

        logger.info(
            f"Added non-gene topic '{topic}' to reference {reference_no} "
            f"by {curator_userid}"
        )

        return ref_prop.ref_property_no

    def remove_nongene_topic(
        self,
        ref_property_no: int,
        curator_userid: str,
    ) -> bool:
        """
        Remove a non-gene topic from a reference.

        Only removes if the topic has no feature associations.
        """
        prop = (
            self.db.query(RefProperty)
            .filter(RefProperty.ref_property_no == ref_property_no)
            .first()
        )

        if not prop:
            raise LitGuideCurationError(f"Topic property {ref_property_no} not found")

        # Check if it has feature associations
        has_features = (
            self.db.query(RefpropFeat)
            .filter(RefpropFeat.ref_property_no == ref_property_no)
            .first()
        )

        if has_features:
            raise LitGuideCurationError(
                "Cannot remove topic that has feature associations. "
                "Remove the feature associations first."
            )

        self.db.delete(prop)
        self.db.commit()

        logger.info(
            f"Removed non-gene topic property {ref_property_no} by {curator_userid}"
        )

        return True
