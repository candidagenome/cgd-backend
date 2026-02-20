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
    Organism,
)
from cgd.api.services.curation.reference_curation_service import (
    ReferenceCurationService,
    ReferenceCurationError,
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

    def _get_reference_urls(self, reference: Reference) -> list:
        """
        Get URLs for a reference (Full Text, Datasets, Web Supplement, etc.).

        Args:
            reference: Reference object

        Returns:
            List of URL dicts with url_type and url
        """
        urls = []
        for ref_url in reference.ref_url:
            if ref_url.url:
                urls.append({
                    "url_type": ref_url.url.url_type,
                    "url": ref_url.url.url,
                })
        return urls

    def _get_field_references(self, feature_no: int, col_name: str) -> list:
        """
        Get references linked to a specific field of a feature.

        Args:
            feature_no: Feature number (primary_key in ref_link)
            col_name: Column name (GENE_NAME, NAME_DESCRIPTION, HEADLINE)

        Returns:
            List of reference dicts with ref_link_no, reference_no, dbxref_id,
            pubmed, citation, and urls
        """
        refs = []
        ref_links = (
            self.db.query(RefLink)
            .filter(
                RefLink.tab_name == "FEATURE",
                RefLink.col_name == col_name,
                RefLink.primary_key == feature_no,
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
                refs.append({
                    "ref_link_no": ref_link.ref_link_no,
                    "reference_no": ref.reference_no,
                    "dbxref_id": ref.dbxref_id,
                    "pubmed": ref.pubmed,
                    "citation": ref.citation,
                    "urls": self._get_reference_urls(ref),
                })
        return refs

    def get_feature_details(self, feature_no: int) -> dict:
        """
        Get detailed feature info for curation.

        Returns all feature fields plus aliases, notes, URLs, and field references.
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        # Get references for each editable field
        gene_name_refs = self._get_field_references(feature_no, "GENE_NAME")
        name_description_refs = self._get_field_references(feature_no, "NAME_DESCRIPTION")
        headline_refs = self._get_field_references(feature_no, "HEADLINE")

        # Get aliases through FeatAlias linking table
        aliases = []
        for feat_alias in feature.feat_alias:
            alias = feat_alias.alias
            if not alias:
                continue  # Skip if alias relationship is missing
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
                        "dbxref_id": ref.dbxref_id,
                        "pubmed": ref.pubmed,
                        "citation": ref.citation,
                        "urls": self._get_reference_urls(ref),
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
            if not note:
                continue  # Skip if note relationship is missing
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
                    "link": url.url,  # Field is named 'url' in the model
                })

        return {
            "feature_no": feature.feature_no,
            "feature_name": feature.feature_name,
            "gene_name": feature.gene_name,
            "gene_name_refs": gene_name_refs,
            "name_description": feature.name_description,
            "name_description_refs": name_description_refs,
            "feature_type": feature.feature_type,
            "headline": feature.headline,
            "headline_refs": headline_refs,
            "source": feature.source,
            "date_created": feature.date_created.isoformat()
            if feature.date_created else None,
            "created_by": feature.created_by,
            "aliases": aliases,
            "notes": notes,
            "urls": urls,
        }

    def search_features(
        self, query: str, page: int = 1, page_size: int = 50,
        organism_abbrev: Optional[str] = None
    ) -> Tuple[List[dict], int]:
        """
        Search features by name.

        Args:
            query: Search string for feature_name or gene_name
            page: Page number (1-indexed)
            page_size: Results per page
            organism_abbrev: Optional organism abbreviation filter

        Returns:
            Tuple of (list of feature dicts, total count)
        """
        base_query = self.db.query(Feature).filter(
            or_(
                Feature.feature_name.ilike(f"%{query}%"),
                Feature.gene_name.ilike(f"%{query}%"),
            )
        )

        # Filter by organism if specified
        if organism_abbrev:
            base_query = base_query.join(
                Organism, Feature.organism_no == Organism.organism_no
            ).filter(Organism.organism_abbrev == organism_abbrev)

        base_query = base_query.order_by(Feature.feature_name)

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

    def _link_pmids_to_field(
        self,
        feature_no: int,
        col_name: str,
        pmids_str: str,
        curator_userid: str,
    ) -> list:
        """
        Link PMIDs to a feature field via ref_link table.

        Args:
            feature_no: Feature number
            col_name: Column name (GENE_NAME, NAME_DESCRIPTION, HEADLINE)
            pmids_str: Pipe-delimited PMIDs (e.g., "12345678|23456789")
            curator_userid: Curator's userid

        Returns:
            List of created ref_link_no values
        """
        if not pmids_str or not pmids_str.strip():
            return []

        created = []
        ref_service = ReferenceCurationService(self.db)

        for pmid_str in pmids_str.split("|"):
            pmid_str = pmid_str.strip()
            if not pmid_str:
                continue

            try:
                pmid = int(pmid_str)
            except ValueError:
                logger.warning(f"Invalid PMID '{pmid_str}', skipping")
                continue

            # Look up reference by PMID
            reference = ref_service.get_reference_by_pubmed(pmid)

            if not reference:
                # Try to create reference from PubMed
                try:
                    reference_no = ref_service.create_reference_from_pubmed(
                        pmid=pmid,
                        reference_status="Published",
                        curator_userid=curator_userid,
                    )
                    reference = ref_service.get_reference_by_no(reference_no)
                    logger.info(f"Created reference from PMID:{pmid}")
                except ReferenceCurationError as e:
                    logger.warning(f"Failed to create reference for PMID:{pmid}: {e}")
                    continue

            if not reference:
                continue

            # Check if link already exists
            existing = (
                self.db.query(RefLink)
                .filter(
                    RefLink.tab_name == "FEATURE",
                    RefLink.col_name == col_name,
                    RefLink.primary_key == feature_no,
                    RefLink.reference_no == reference.reference_no,
                )
                .first()
            )

            if existing:
                logger.info(
                    f"Reference {reference.reference_no} already linked to "
                    f"{col_name} for feature {feature_no}"
                )
                continue

            # Create ref_link
            ref_link = RefLink(
                reference_no=reference.reference_no,
                tab_name="FEATURE",
                col_name=col_name,
                primary_key=feature_no,
                created_by=curator_userid[:12],
            )
            self.db.add(ref_link)
            self.db.flush()
            created.append(ref_link.ref_link_no)
            logger.info(
                f"Linked reference {reference.reference_no} (PMID:{pmid}) to "
                f"{col_name} for feature {feature_no}"
            )

        return created

    def update_feature(
        self,
        feature_no: int,
        curator_userid: str,
        gene_name: Optional[str] = None,
        gene_name_pmids: Optional[str] = None,
        name_description: Optional[str] = None,
        name_description_pmids: Optional[str] = None,
        headline: Optional[str] = None,
        headline_pmids: Optional[str] = None,
        feature_type: Optional[str] = None,
    ) -> bool:
        """
        Update feature fields.

        Args:
            feature_no: Feature number
            curator_userid: Curator's userid
            gene_name: Standard gene name
            gene_name_pmids: Pipe-delimited PMIDs for gene name references
            name_description: Name description
            name_description_pmids: Pipe-delimited PMIDs for name description references
            headline: Headline/short description (max 240 chars)
            headline_pmids: Pipe-delimited PMIDs for headline references
            feature_type: Feature type

        Returns:
            True if successful
        """
        feature = self.get_feature_by_no(feature_no)
        if not feature:
            raise LocusCurationError(f"Feature {feature_no} not found")

        # Validate headline length
        if headline is not None and len(headline) > 240:
            raise LocusCurationError(
                f"Headline exceeds maximum length of 240 characters "
                f"(got {len(headline)})"
            )

        # Update fields if provided
        if gene_name is not None:
            feature.gene_name = gene_name or None
        if name_description is not None:
            feature.name_description = name_description or None
        if headline is not None:
            feature.headline = headline or None
        if feature_type is not None:
            feature.feature_type = feature_type

        # Link PMIDs to fields
        if gene_name_pmids:
            self._link_pmids_to_field(
                feature_no, "GENE_NAME", gene_name_pmids, curator_userid
            )
        if name_description_pmids:
            self._link_pmids_to_field(
                feature_no, "NAME_DESCRIPTION", name_description_pmids, curator_userid
            )
        if headline_pmids:
            self._link_pmids_to_field(
                feature_no, "HEADLINE", headline_pmids, curator_userid
            )

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

    def unlink_field_reference(self, ref_link_no: int, curator_userid: str) -> bool:
        """
        Unlink a reference from a feature field.

        Args:
            ref_link_no: RefLink record ID
            curator_userid: Curator's userid

        Returns:
            True if successful
        """
        ref_link = (
            self.db.query(RefLink)
            .filter(RefLink.ref_link_no == ref_link_no)
            .first()
        )
        if not ref_link:
            raise LocusCurationError(f"Reference link {ref_link_no} not found")

        # Verify it's a FEATURE table link
        if ref_link.tab_name != "FEATURE":
            raise LocusCurationError(
                f"Reference link {ref_link_no} is not a feature field reference"
            )

        self.db.delete(ref_link)
        self.db.commit()

        logger.info(f"Unlinked reference {ref_link_no} by {curator_userid}")

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
                source="CGD",
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
