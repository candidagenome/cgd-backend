"""
Locus Curation Service - Business logic for locus/feature info updates.

Mirrors functionality from legacy UpdateLocusInfo.pm:
- Update gene name, name description
- Manage aliases
- Update headline
- Update feature type
"""

import logging
from typing import Optional, List, Tuple

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    Alias,
    FeatAlias,
    Note,
    NoteLink,
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

        # Get aliases through FeatAlias linking table
        aliases = []
        for feat_alias in feature.feat_alias:
            alias = feat_alias.alias
            alias_refs = []
            # Get references for this alias link
            ref_links = (
                self.db.query(RefLink)
                .filter(
                    RefLink.tab_name == "FEAT_ALIAS",
                    RefLink.col_name == "FEAT_ALIAS_NO",
                    RefLink.primary_key == feat_alias.feat_alias_no,
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
                "feat_alias_no": feat_alias.feat_alias_no,
                "alias_no": alias.alias_no,
                "alias_name": alias.alias_name,
                "alias_type": alias.alias_type,
                "references": alias_refs,
            })

        # Get notes through NoteLink table
        notes = []
        note_links = (
            self.db.query(NoteLink)
            .filter(
                NoteLink.tab_name == "FEATURE",
                NoteLink.primary_key == feature_no,
            )
            .all()
        )
        for note_link in note_links:
            note = note_link.note
            notes.append({
                "note_link_no": note_link.note_link_no,
                "note_no": note.note_no,
                "note_type": note.note_type,
                "note_text": note.note,
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
            "headline": feature.headline,
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
    ) -> Tuple[List[dict], int]:
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
        feature_type: Optional[str] = None,
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
        if feature_type is not None:
            feature.feature_type = feature_type

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
            feat_alias_no
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        # Check if alias already exists in Alias table
        alias = (
            self.db.query(Alias)
            .filter(
                func.upper(Alias.alias_name) == alias_name.upper(),
                Alias.alias_type == alias_type,
            )
            .first()
        )

        if not alias:
            # Create new alias
            alias = Alias(
                alias_name=alias_name,
                alias_type=alias_type,
                created_by=curator_userid[:12],
            )
            self.db.add(alias)
            self.db.flush()

        # Check if this feature already has this alias
        existing_link = (
            self.db.query(FeatAlias)
            .filter(
                FeatAlias.feature_no == feature_no,
                FeatAlias.alias_no == alias.alias_no,
            )
            .first()
        )
        if existing_link:
            raise LocusCurationError(
                f"Alias '{alias_name}' already exists for this feature"
            )

        # Create the link
        feat_alias = FeatAlias(
            feature_no=feature_no,
            alias_no=alias.alias_no,
        )
        self.db.add(feat_alias)
        self.db.flush()

        # Add reference link if provided
        if reference_no:
            ref_link = RefLink(
                reference_no=reference_no,
                tab_name="FEAT_ALIAS",
                col_name="FEAT_ALIAS_NO",
                primary_key=feat_alias.feat_alias_no,
                created_by=curator_userid[:12],
            )
            self.db.add(ref_link)

        self.db.commit()

        logger.info(
            f"Added alias '{alias_name}' to feature {feature_no}"
        )

        return feat_alias.feat_alias_no

    def remove_alias(self, feat_alias_no: int, curator_userid: str) -> bool:
        """Remove alias from feature."""
        feat_alias = (
            self.db.query(FeatAlias)
            .filter(FeatAlias.feat_alias_no == feat_alias_no)
            .first()
        )
        if not feat_alias:
            raise LocusCurationError(f"Alias link {feat_alias_no} not found")

        # Remove reference links
        self.db.query(RefLink).filter(
            RefLink.tab_name == "FEAT_ALIAS",
            RefLink.col_name == "FEAT_ALIAS_NO",
            RefLink.primary_key == feat_alias_no,
        ).delete()

        self.db.delete(feat_alias)
        self.db.commit()

        logger.info(f"Removed alias {feat_alias_no} by {curator_userid}")

        return True

    def add_note(
        self,
        feature_no: int,
        note_type: str,
        note_text: str,
        curator_userid: str,
    ) -> int:
        """
        Add note to feature.

        Returns:
            note_link_no
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        # Check if this exact note already exists
        existing_note = (
            self.db.query(Note)
            .filter(
                Note.note_type == note_type,
                Note.note == note_text,
            )
            .first()
        )

        if not existing_note:
            # Create new note
            note = Note(
                note_type=note_type,
                note=note_text,
                created_by=curator_userid[:12],
            )
            self.db.add(note)
            self.db.flush()
        else:
            note = existing_note

        # Check if this feature already has this note
        existing_link = (
            self.db.query(NoteLink)
            .filter(
                NoteLink.tab_name == "FEATURE",
                NoteLink.primary_key == feature_no,
                NoteLink.note_no == note.note_no,
            )
            .first()
        )
        if existing_link:
            raise LocusCurationError("This note already exists for this feature")

        # Create link
        note_link = NoteLink(
            note_no=note.note_no,
            tab_name="FEATURE",
            primary_key=feature_no,
            created_by=curator_userid[:12],
        )
        self.db.add(note_link)
        self.db.commit()

        logger.info(f"Added note to feature {feature_no}")

        return note_link.note_link_no

    def remove_note(self, note_link_no: int, curator_userid: str) -> bool:
        """Remove note from feature."""
        note_link = (
            self.db.query(NoteLink)
            .filter(NoteLink.note_link_no == note_link_no)
            .first()
        )
        if not note_link:
            raise LocusCurationError(f"Note link {note_link_no} not found")

        self.db.delete(note_link)
        self.db.commit()

        logger.info(f"Removed note link {note_link_no} by {curator_userid}")

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
            Url.url == link,
        ).first()

        if not url:
            url = Url(
                url_type=url_type,
                url=link,
                created_by=curator_userid[:12],
            )
            self.db.add(url)
            self.db.flush()

        # Check if link already exists
        existing = (
            self.db.query(FeatUrl)
            .filter(
                FeatUrl.feature_no == feature_no,
                FeatUrl.url_no == url.url_no,
            )
            .first()
        )
        if existing:
            raise LocusCurationError("This URL is already linked to this feature")

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
