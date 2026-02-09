"""
Locus Curation Service - Business logic for locus/feature info updates.

Mirrors functionality from legacy UpdateLocusInfo.pm:
- Update gene name, name description
- Manage aliases
- Update description
- Update feature type/qualifier
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    FeatureAlias,
    FeatureNote,
    FeatUrl,
    Url,
    Reference,
    RefLink,
)

logger = logging.getLogger(__name__)

SOURCE = "CGD"


class LocusCurationError(Exception):
    """Raised when locus curation validation fails."""

    pass


class LocusCurationService:
    """Service for locus curation operations."""

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

    def get_feature_details(self, feature_no: int) -> dict:
        """
        Get detailed feature info for curation.

        Returns all feature fields plus aliases, notes, URLs.
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        # Get aliases
        aliases = []
        for alias in feature.feature_alias:
            alias_refs = []
            # Get references for this alias
            ref_links = (
                self.db.query(RefLink)
                .filter(
                    RefLink.tab_name == "FEATURE_ALIAS",
                    RefLink.col_name == "FEATURE_ALIAS_NO",
                    RefLink.primary_key == alias.feature_alias_no,
                )
                .all()
            )
            for ref_link in ref_links:
                ref = (
                    self.db.query(Reference)
                    .filter(Reference.reference_no == ref_link.reference_no)
                    .first()
                )
                if ref:
                    alias_refs.append({
                        "reference_no": ref.reference_no,
                        "pubmed": ref.pubmed,
                    })

            aliases.append({
                "feature_alias_no": alias.feature_alias_no,
                "alias_name": alias.alias_name,
                "alias_type": alias.alias_type,
                "source": alias.source,
                "references": alias_refs,
            })

        # Get notes
        notes = []
        for note in feature.feature_note:
            notes.append({
                "feature_note_no": note.feature_note_no,
                "note_type": note.note_type,
                "note_class": note.note_class,
                "note_text": note.note_text,
                "date_created": note.date_created.isoformat()
                if note.date_created else None,
            })

        # Get URLs
        urls = []
        for feat_url in feature.feat_url:
            url = self.db.query(Url).filter(Url.url_no == feat_url.url_no).first()
            if url:
                urls.append({
                    "feat_url_no": feat_url.feat_url_no,
                    "url_no": url.url_no,
                    "url_type": url.url_type,
                    "link": url.link,
                })

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "name_description": feature.name_description,
            "feature_type": feature.feature_type,
            "qualifier": feature.qualifier,
            "headline": feature.headline,
            "description": feature.description,
            "gene_product": feature.gene_product,
            "source": feature.source,
            "date_created": feature.date_created.isoformat()
            if feature.date_created else None,
            "created_by": feature.created_by,
            "aliases": aliases,
            "notes": notes,
            "urls": urls,
        }

    def search_features(
        self, query: str, page: int = 1, page_size: int = 50
    ) -> tuple[list[dict], int]:
        """
        Search features by name.

        Returns:
            Tuple of (list of feature dicts, total count)
        """
        base_query = self.db.query(Feature).filter(
            or_(
                Feature.feature_name.ilike(f"%{query}%"),
                Feature.gene_name.ilike(f"%{query}%"),
            )
        ).order_by(Feature.feature_name)

        total = base_query.count()
        features = base_query.offset((page - 1) * page_size).limit(page_size).all()

        return (
            [
                {
                    "feature_no": f.feature_no,
                    "feature_name": f.feature_name,
                    "gene_name": f.gene_name,
                    "feature_type": f.feature_type,
                    "headline": f.headline,
                }
                for f in features
            ],
            total,
        )

    def update_feature(
        self,
        feature_no: int,
        curator_userid: str,
        gene_name: Optional[str] = None,
        name_description: Optional[str] = None,
        headline: Optional[str] = None,
        description: Optional[str] = None,
        gene_product: Optional[str] = None,
        feature_type: Optional[str] = None,
        qualifier: Optional[str] = None,
    ) -> bool:
        """
        Update feature fields.

        Returns:
            True if successful
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        # Update fields if provided
        if gene_name is not None:
            feature.gene_name = gene_name or None
        if name_description is not None:
            feature.name_description = name_description or None
        if headline is not None:
            feature.headline = headline or None
        if description is not None:
            feature.description = description or None
        if gene_product is not None:
            feature.gene_product = gene_product or None
        if feature_type is not None:
            feature.feature_type = feature_type
        if qualifier is not None:
            feature.qualifier = qualifier or None

        self.db.commit()

        logger.info(f"Updated feature {feature_no} by {curator_userid}")

        return True

    def add_alias(
        self,
        feature_no: int,
        alias_name: str,
        alias_type: str,
        curator_userid: str,
        reference_no: Optional[int] = None,
    ) -> int:
        """
        Add alias to feature.

        Returns:
            feature_alias_no
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        # Check if alias already exists for this feature
        existing = (
            self.db.query(FeatureAlias)
            .filter(
                FeatureAlias.feature_no == feature_no,
                func.upper(FeatureAlias.alias_name) == alias_name.upper(),
            )
            .first()
        )
        if existing:
            raise LocusCurationError(
                f"Alias '{alias_name}' already exists for this feature"
            )

        alias = FeatureAlias(
            feature_no=feature_no,
            alias_name=alias_name,
            alias_type=alias_type,
            source=SOURCE,
            created_by=curator_userid[:12],
        )
        self.db.add(alias)
        self.db.flush()

        # Add reference link if provided
        if reference_no:
            ref_link = RefLink(
                reference_no=reference_no,
                tab_name="FEATURE_ALIAS",
                col_name="FEATURE_ALIAS_NO",
                primary_key=alias.feature_alias_no,
                created_by=curator_userid[:12],
            )
            self.db.add(ref_link)

        self.db.commit()

        logger.info(
            f"Added alias '{alias_name}' to feature {feature_no}"
        )

        return alias.feature_alias_no

    def remove_alias(self, feature_alias_no: int, curator_userid: str) -> bool:
        """Remove alias from feature."""
        alias = (
            self.db.query(FeatureAlias)
            .filter(FeatureAlias.feature_alias_no == feature_alias_no)
            .first()
        )
        if not alias:
            raise LocusCurationError(f"Alias {feature_alias_no} not found")

        # Remove reference links
        self.db.query(RefLink).filter(
            RefLink.tab_name == "FEATURE_ALIAS",
            RefLink.col_name == "FEATURE_ALIAS_NO",
            RefLink.primary_key == feature_alias_no,
        ).delete()

        self.db.delete(alias)
        self.db.commit()

        logger.info(f"Removed alias {feature_alias_no} by {curator_userid}")

        return True

    def add_note(
        self,
        feature_no: int,
        note_type: str,
        note_text: str,
        curator_userid: str,
        note_class: Optional[str] = None,
    ) -> int:
        """
        Add note to feature.

        Returns:
            feature_note_no
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        note = FeatureNote(
            feature_no=feature_no,
            note_type=note_type,
            note_class=note_class,
            note_text=note_text,
            source=SOURCE,
            created_by=curator_userid[:12],
        )
        self.db.add(note)
        self.db.commit()

        logger.info(f"Added note to feature {feature_no}")

        return note.feature_note_no

    def remove_note(self, feature_note_no: int, curator_userid: str) -> bool:
        """Remove note from feature."""
        note = (
            self.db.query(FeatureNote)
            .filter(FeatureNote.feature_note_no == feature_note_no)
            .first()
        )
        if not note:
            raise LocusCurationError(f"Note {feature_note_no} not found")

        self.db.delete(note)
        self.db.commit()

        logger.info(f"Removed note {feature_note_no} by {curator_userid}")

        return True

    def add_url(
        self,
        feature_no: int,
        url_type: str,
        link: str,
        curator_userid: str,
    ) -> int:
        """Add URL to feature."""
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        # Create or get URL
        url = self.db.query(Url).filter(
            Url.url_type == url_type,
            Url.link == link,
        ).first()

        if not url:
            url = Url(
                url_type=url_type,
                link=link,
                created_by=curator_userid[:12],
            )
            self.db.add(url)
            self.db.flush()

        # Create link
        feat_url = FeatUrl(
            feature_no=feature_no,
            url_no=url.url_no,
        )
        self.db.add(feat_url)
        self.db.commit()

        return feat_url.feat_url_no

    def remove_url(self, feat_url_no: int, curator_userid: str) -> bool:
        """Remove URL from feature."""
        feat_url = (
            self.db.query(FeatUrl)
            .filter(FeatUrl.feat_url_no == feat_url_no)
            .first()
        )
        if not feat_url:
            raise LocusCurationError(f"Feature URL {feat_url_no} not found")

        self.db.delete(feat_url)
        self.db.commit()

        logger.info(f"Removed feature URL {feat_url_no} by {curator_userid}")

        return True
