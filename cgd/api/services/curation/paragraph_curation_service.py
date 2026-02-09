"""
Paragraph Curation Service - Business logic for paragraph management.

Paragraphs are text summaries that appear on locus pages and can be
shared across multiple features. Each paragraph can contain markup
for references, GO terms, and feature links.

Mirrors validation rules from legacy NewParagraph.pm.
"""

import logging
import re
from datetime import datetime
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    FeatPara,
    Organism,
    Paragraph,
    Reference,
    RefLink,
)

logger = logging.getLogger(__name__)


class ParagraphCurationError(Exception):
    """Raised when paragraph curation validation fails."""

    pass


class ParagraphCurationService:
    """Service for paragraph curation operations."""

    # Maximum paragraph text length
    MAX_PARAGRAPH_LENGTH = 4000

    def __init__(self, db: Session):
        self.db = db

    def get_feature_by_name(
        self, feature_name: str, organism_abbrev: Optional[str] = None
    ) -> Optional[Feature]:
        """Look up feature by name or gene name."""
        query = self.db.query(Feature).filter(
            func.upper(Feature.feature_name) == feature_name.upper()
        )
        if organism_abbrev:
            # Join to organism to filter by abbreviation
            query = query.join(
                Organism, Feature.organism_no == Organism.organism_no
            ).filter(Organism.organism_abbrev == organism_abbrev)

        feature = query.first()
        if feature:
            return feature

        # Try gene name
        query = self.db.query(Feature).filter(
            func.upper(Feature.gene_name) == feature_name.upper()
        )
        if organism_abbrev:
            query = query.join(
                Organism, Feature.organism_no == Organism.organism_no
            ).filter(Organism.organism_abbrev == organism_abbrev)

        return query.first()

    def get_paragraphs_for_feature(
        self, feature_name: str, organism_abbrev: Optional[str] = None
    ) -> dict:
        """
        Get all paragraphs for a feature.

        Returns feature info and list of paragraphs with their order.
        """
        feature = self.get_feature_by_name(feature_name, organism_abbrev)
        if not feature:
            raise ParagraphCurationError(
                f"Feature '{feature_name}' not found"
                + (f" for organism '{organism_abbrev}'" if organism_abbrev else "")
            )

        # Get paragraphs ordered by paragraph_order
        feat_paras = (
            self.db.query(FeatPara, Paragraph)
            .join(Paragraph, FeatPara.paragraph_no == Paragraph.paragraph_no)
            .filter(FeatPara.feature_no == feature.feature_no)
            .order_by(FeatPara.paragraph_order)
            .all()
        )

        paragraphs = []
        for fp, para in feat_paras:
            # Get linked features for this paragraph
            linked_features = self._get_features_for_paragraph(para.paragraph_no)

            paragraphs.append({
                "paragraph_no": para.paragraph_no,
                "paragraph_text": para.paragraph_text,
                "date_edited": (
                    para.date_edited.isoformat() if para.date_edited else None
                ),
                "paragraph_order": fp.paragraph_order,
                "linked_features": linked_features,
            })

        # Get organism info
        organism = (
            self.db.query(Organism)
            .filter(Organism.organism_no == feature.organism_no)
            .first()
        )

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "organism_abbrev": organism.organism_abbrev if organism else None,
            "organism_name": organism.organism_name if organism else None,
            "paragraphs": paragraphs,
        }

    def _get_features_for_paragraph(self, paragraph_no: int) -> list[dict]:
        """Get all features linked to a paragraph."""
        feat_paras = (
            self.db.query(FeatPara, Feature)
            .join(Feature, FeatPara.feature_no == Feature.feature_no)
            .filter(FeatPara.paragraph_no == paragraph_no)
            .order_by(FeatPara.paragraph_order)
            .all()
        )

        return [
            {
                "feature_no": f.feature_no,
                "feature_name": f.feature_name,
                "gene_name": f.gene_name,
                "paragraph_order": fp.paragraph_order,
            }
            for fp, f in feat_paras
        ]

    def get_paragraph_details(self, paragraph_no: int) -> dict:
        """Get full details for a paragraph including linked features."""
        paragraph = (
            self.db.query(Paragraph)
            .filter(Paragraph.paragraph_no == paragraph_no)
            .first()
        )

        if not paragraph:
            raise ParagraphCurationError(f"Paragraph {paragraph_no} not found")

        linked_features = self._get_features_for_paragraph(paragraph_no)

        # Get linked references
        ref_links = (
            self.db.query(RefLink)
            .filter(
                RefLink.tab_name == "PARAGRAPH",
                RefLink.col_name == "PARAGRAPH_NO",
                RefLink.primary_key == paragraph_no,
            )
            .all()
        )

        references = []
        for rl in ref_links:
            ref = (
                self.db.query(Reference)
                .filter(Reference.reference_no == rl.reference_no)
                .first()
            )
            if ref:
                references.append({
                    "reference_no": ref.reference_no,
                    "citation": ref.citation,
                    "dbxref_id": ref.dbxref_id,
                })

        return {
            "paragraph_no": paragraph.paragraph_no,
            "paragraph_text": paragraph.paragraph_text,
            "date_edited": (
                paragraph.date_edited.isoformat() if paragraph.date_edited else None
            ),
            "linked_features": linked_features,
            "linked_references": references,
        }

    def create_paragraph(
        self,
        paragraph_text: str,
        feature_names: list[str],
        organism_abbrev: str,
        curator_userid: str,
    ) -> dict:
        """
        Create a new paragraph and link it to features.

        Args:
            paragraph_text: The paragraph text (may contain markup)
            feature_names: List of feature names to link
            organism_abbrev: Organism abbreviation
            curator_userid: Curator's userid

        Returns:
            Dict with paragraph_no and linked features

        Raises:
            ParagraphCurationError: If validation fails
        """
        if not paragraph_text:
            raise ParagraphCurationError("Paragraph text is required")

        if len(paragraph_text) > self.MAX_PARAGRAPH_LENGTH:
            raise ParagraphCurationError(
                f"Paragraph exceeds {self.MAX_PARAGRAPH_LENGTH} character limit"
            )

        if not feature_names:
            raise ParagraphCurationError(
                "At least one feature must be linked to the paragraph"
            )

        # Check if paragraph with same text already exists
        existing = (
            self.db.query(Paragraph)
            .filter(Paragraph.paragraph_text == paragraph_text)
            .first()
        )

        if existing:
            paragraph_no = existing.paragraph_no
            logger.info(f"Using existing paragraph {paragraph_no}")
        else:
            # Create new paragraph
            paragraph = Paragraph(
                paragraph_text=paragraph_text,
                date_edited=datetime.now(),
            )
            self.db.add(paragraph)
            self.db.flush()
            paragraph_no = paragraph.paragraph_no

            logger.info(
                f"Created paragraph {paragraph_no} by {curator_userid}"
            )

        # Extract and link references
        self._update_reference_links(paragraph_no, paragraph_text)

        # Link to features
        linked_features = []
        for name in feature_names:
            feature = self.get_feature_by_name(name, organism_abbrev)
            if not feature:
                raise ParagraphCurationError(
                    f"Feature '{name}' not found for organism '{organism_abbrev}'"
                )

            # Check if already linked
            existing_link = (
                self.db.query(FeatPara)
                .filter(
                    FeatPara.feature_no == feature.feature_no,
                    FeatPara.paragraph_no == paragraph_no,
                )
                .first()
            )

            if existing_link:
                logger.info(
                    f"Paragraph {paragraph_no} already linked to feature {name}"
                )
                linked_features.append({
                    "feature_no": feature.feature_no,
                    "feature_name": feature.feature_name,
                    "gene_name": feature.gene_name,
                    "paragraph_order": existing_link.paragraph_order,
                })
                continue

            # Get next order for this feature
            next_order = self._get_next_paragraph_order(feature.feature_no)

            feat_para = FeatPara(
                feature_no=feature.feature_no,
                paragraph_no=paragraph_no,
                paragraph_order=next_order,
            )
            self.db.add(feat_para)

            linked_features.append({
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "gene_name": feature.gene_name,
                "paragraph_order": next_order,
            })

        self.db.commit()

        return {
            "paragraph_no": paragraph_no,
            "linked_features": linked_features,
        }

    def _get_next_paragraph_order(self, feature_no: int) -> int:
        """Get the next paragraph order number for a feature."""
        max_order = (
            self.db.query(func.max(FeatPara.paragraph_order))
            .filter(FeatPara.feature_no == feature_no)
            .scalar()
        )
        return (max_order or 0) + 1

    def update_paragraph(
        self,
        paragraph_no: int,
        paragraph_text: str,
        update_date: bool,
        curator_userid: str,
    ) -> bool:
        """
        Update paragraph text.

        Args:
            paragraph_no: Paragraph to update
            paragraph_text: New text
            update_date: Whether to update date_edited
            curator_userid: Curator's userid

        Returns:
            True on success

        Raises:
            ParagraphCurationError: If validation fails
        """
        paragraph = (
            self.db.query(Paragraph)
            .filter(Paragraph.paragraph_no == paragraph_no)
            .first()
        )

        if not paragraph:
            raise ParagraphCurationError(f"Paragraph {paragraph_no} not found")

        if len(paragraph_text) > self.MAX_PARAGRAPH_LENGTH:
            raise ParagraphCurationError(
                f"Paragraph exceeds {self.MAX_PARAGRAPH_LENGTH} character limit"
            )

        if paragraph.paragraph_text != paragraph_text:
            paragraph.paragraph_text = paragraph_text
            # Update reference links
            self._update_reference_links(paragraph_no, paragraph_text)

        if update_date:
            paragraph.date_edited = datetime.now()

        self.db.commit()

        logger.info(f"Updated paragraph {paragraph_no} by {curator_userid}")

        return True

    def _update_reference_links(self, paragraph_no: int, paragraph_text: str):
        """Update REF_LINK entries based on references in paragraph text."""
        # Extract reference DBIDs from markup like <reference:S000123456>
        ref_pattern = r"<reference:(S[0-9]+)>"
        found_dbids = set(re.findall(ref_pattern, paragraph_text, re.IGNORECASE))

        # Get current links
        current_links = (
            self.db.query(RefLink)
            .filter(
                RefLink.tab_name == "PARAGRAPH",
                RefLink.col_name == "PARAGRAPH_NO",
                RefLink.primary_key == paragraph_no,
            )
            .all()
        )

        current_ref_nos = {rl.reference_no for rl in current_links}

        # Get reference_no for each DBID
        new_ref_nos = set()
        for dbid in found_dbids:
            ref = (
                self.db.query(Reference)
                .filter(Reference.dbxref_id == dbid)
                .first()
            )
            if ref:
                new_ref_nos.add(ref.reference_no)

        # Add new links
        for ref_no in new_ref_nos - current_ref_nos:
            ref_link = RefLink(
                reference_no=ref_no,
                tab_name="PARAGRAPH",
                col_name="PARAGRAPH_NO",
                primary_key=paragraph_no,
            )
            self.db.add(ref_link)

        # Remove old links
        for ref_no in current_ref_nos - new_ref_nos:
            link = (
                self.db.query(RefLink)
                .filter(
                    RefLink.reference_no == ref_no,
                    RefLink.tab_name == "PARAGRAPH",
                    RefLink.col_name == "PARAGRAPH_NO",
                    RefLink.primary_key == paragraph_no,
                )
                .first()
            )
            if link:
                self.db.delete(link)

    def reorder_paragraphs(
        self,
        feature_no: int,
        paragraph_orders: list[dict],
        curator_userid: str,
    ) -> bool:
        """
        Reorder paragraphs for a feature.

        Args:
            feature_no: Feature to reorder paragraphs for
            paragraph_orders: List of {paragraph_no, order} dicts
            curator_userid: Curator's userid

        Returns:
            True on success

        Raises:
            ParagraphCurationError: If validation fails
        """
        # Validate orders are sequential starting at 1
        orders = sorted([po["order"] for po in paragraph_orders])
        expected = list(range(1, len(orders) + 1))
        if orders != expected:
            raise ParagraphCurationError(
                "Paragraph orders must be sequential starting at 1"
            )

        # Update each paragraph order
        for po in paragraph_orders:
            feat_para = (
                self.db.query(FeatPara)
                .filter(
                    FeatPara.feature_no == feature_no,
                    FeatPara.paragraph_no == po["paragraph_no"],
                )
                .first()
            )

            if not feat_para:
                raise ParagraphCurationError(
                    f"Paragraph {po['paragraph_no']} not linked to feature {feature_no}"
                )

            feat_para.paragraph_order = po["order"]

        self.db.commit()

        logger.info(
            f"Reordered paragraphs for feature {feature_no} by {curator_userid}"
        )

        return True

    def link_feature(
        self,
        paragraph_no: int,
        feature_name: str,
        organism_abbrev: str,
        curator_userid: str,
    ) -> dict:
        """
        Link a paragraph to a feature.

        Args:
            paragraph_no: Paragraph to link
            feature_name: Feature to link to
            organism_abbrev: Organism abbreviation
            curator_userid: Curator's userid

        Returns:
            Dict with link info

        Raises:
            ParagraphCurationError: If validation fails
        """
        paragraph = (
            self.db.query(Paragraph)
            .filter(Paragraph.paragraph_no == paragraph_no)
            .first()
        )

        if not paragraph:
            raise ParagraphCurationError(f"Paragraph {paragraph_no} not found")

        feature = self.get_feature_by_name(feature_name, organism_abbrev)
        if not feature:
            raise ParagraphCurationError(
                f"Feature '{feature_name}' not found for organism '{organism_abbrev}'"
            )

        # Check if already linked
        existing = (
            self.db.query(FeatPara)
            .filter(
                FeatPara.feature_no == feature.feature_no,
                FeatPara.paragraph_no == paragraph_no,
            )
            .first()
        )

        if existing:
            raise ParagraphCurationError(
                f"Paragraph {paragraph_no} already linked to {feature_name}"
            )

        # Get next order
        next_order = self._get_next_paragraph_order(feature.feature_no)

        feat_para = FeatPara(
            feature_no=feature.feature_no,
            paragraph_no=paragraph_no,
            paragraph_order=next_order,
        )
        self.db.add(feat_para)
        self.db.commit()

        logger.info(
            f"Linked paragraph {paragraph_no} to feature {feature_name} "
            f"by {curator_userid}"
        )

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "paragraph_order": next_order,
        }

    def unlink_feature(
        self,
        paragraph_no: int,
        feature_no: int,
        curator_userid: str,
    ) -> bool:
        """
        Unlink a paragraph from a feature.

        Args:
            paragraph_no: Paragraph to unlink
            feature_no: Feature to unlink from
            curator_userid: Curator's userid

        Returns:
            True on success

        Raises:
            ParagraphCurationError: If not found
        """
        feat_para = (
            self.db.query(FeatPara)
            .filter(
                FeatPara.feature_no == feature_no,
                FeatPara.paragraph_no == paragraph_no,
            )
            .first()
        )

        if not feat_para:
            raise ParagraphCurationError(
                f"Paragraph {paragraph_no} not linked to feature {feature_no}"
            )

        self.db.delete(feat_para)
        self.db.commit()

        logger.info(
            f"Unlinked paragraph {paragraph_no} from feature {feature_no} "
            f"by {curator_userid}"
        )

        return True

    def get_organisms(self) -> list[dict]:
        """Get list of all organisms for dropdown."""
        organisms = self.db.query(Organism).order_by(Organism.organism_name).all()
        return [
            {
                "organism_abbrev": o.organism_abbrev,
                "organism_name": o.organism_name,
            }
            for o in organisms
        ]
