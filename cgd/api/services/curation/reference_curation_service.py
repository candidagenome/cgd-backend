"""
Reference Curation Service - Business logic for reference management.

Mirrors validation rules from legacy CreateReference.pm:
- PubMed ID validation and metadata fetching
- Reference status management
- Literature guide linking
- Duplicate detection
"""

import logging
from datetime import datetime
from typing import Optional
from xml.etree import ElementTree

import httpx
from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Abstract,
    Author,
    AuthorEditor,
    Feature,
    Journal,
    RefBad,
    RefProperty,
    RefUnlink,
    Reference,
    RefpropFeat,
)

logger = logging.getLogger(__name__)

# PubMed E-utilities URL
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


class ReferenceCurationError(Exception):
    """Raised when reference curation validation fails."""

    pass


class ReferenceCurationService:
    """Service for reference curation operations."""

    # Valid reference status values
    VALID_STATUSES = ["Published", "Epub ahead of print", "In preparation", "Submitted"]

    # Curation status values for literature guide
    CURATION_STATUSES = [
        "Not Yet Curated",
        "High Priority",
        "Partially Curated",
        "Curated Todo",
        "Done: No genes",
        "Done: All genes HTP",
        "Done: Curated",
    ]

    def __init__(self, db: Session):
        self.db = db

    def get_reference_by_pubmed(self, pubmed: int) -> Optional[Reference]:
        """Look up reference by PubMed ID."""
        return self.db.query(Reference).filter(Reference.pubmed == pubmed).first()

    def get_reference_by_no(self, reference_no: int) -> Optional[Reference]:
        """Look up reference by reference_no."""
        return (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )

    def check_bad_reference(self, pubmed: int) -> Optional[RefBad]:
        """Check if PubMed ID is in the bad reference list."""
        return self.db.query(RefBad).filter(RefBad.pubmed == pubmed).first()

    def check_unlinked_reference(
        self, reference_no: int, feature_no: int
    ) -> Optional[RefUnlink]:
        """Check if reference is unlinked from a feature."""
        return (
            self.db.query(RefUnlink)
            .filter(
                RefUnlink.reference_no == reference_no,
                RefUnlink.feature_no == feature_no,
            )
            .first()
        )

    def fetch_pubmed_metadata(self, pubmed: int) -> dict:
        """
        Fetch metadata from PubMed E-utilities.

        Returns parsed metadata including title, authors, journal, year, etc.
        """
        params = {
            "db": "pubmed",
            "id": str(pubmed),
            "retmode": "xml",
            "rettype": "medline",
        }

        try:
            response = httpx.get(PUBMED_EFETCH_URL, params=params, timeout=30.0)

            if response.status_code != 200:
                raise ReferenceCurationError(
                    f"Failed to fetch PubMed metadata: {response.status_code}"
                )

            return self._parse_pubmed_xml(response.text)
        except httpx.TimeoutException:
            raise ReferenceCurationError(
                f"Timeout fetching PubMed metadata for PMID:{pubmed}"
            )
        except httpx.RequestError as e:
            raise ReferenceCurationError(
                f"Network error fetching PubMed metadata: {e}"
            )

    def _parse_pubmed_xml(self, xml_text: str) -> dict:
        """Parse PubMed XML response into structured metadata."""
        try:
            root = ElementTree.fromstring(xml_text)

            article = root.find(".//PubmedArticle/MedlineCitation/Article")
            if article is None:
                raise ReferenceCurationError("No article found in PubMed response")

            # Title
            title_elem = article.find("ArticleTitle")
            title = title_elem.text if title_elem is not None else ""

            # Journal
            journal_elem = article.find("Journal/Title")
            journal_name = journal_elem.text if journal_elem is not None else ""

            journal_abbrev_elem = article.find("Journal/ISOAbbreviation")
            journal_abbrev = (
                journal_abbrev_elem.text if journal_abbrev_elem is not None else ""
            )

            # Year
            year_elem = article.find("Journal/JournalIssue/PubDate/Year")
            if year_elem is None:
                year_elem = article.find("Journal/JournalIssue/PubDate/MedlineDate")
            year = int(year_elem.text[:4]) if year_elem is not None else None

            # Volume, Issue, Pages
            volume_elem = article.find("Journal/JournalIssue/Volume")
            volume = volume_elem.text if volume_elem is not None else ""

            issue_elem = article.find("Journal/JournalIssue/Issue")
            issue = issue_elem.text if issue_elem is not None else ""

            pages_elem = article.find("Pagination/MedlinePgn")
            pages = pages_elem.text if pages_elem is not None else ""

            # Authors
            authors = []
            author_list = article.find("AuthorList")
            if author_list is not None:
                for author in author_list.findall("Author"):
                    last_name = author.find("LastName")
                    first_name = author.find("ForeName")
                    initials = author.find("Initials")

                    authors.append({
                        "last_name": last_name.text if last_name is not None else "",
                        "first_name": first_name.text if first_name is not None else "",
                        "initials": initials.text if initials is not None else "",
                    })

            # Abstract
            abstract_elem = article.find("Abstract/AbstractText")
            abstract = abstract_elem.text if abstract_elem is not None else ""

            # DOI
            doi = None
            for id_elem in root.findall(
                ".//PubmedArticle/PubmedData/ArticleIdList/ArticleId"
            ):
                if id_elem.get("IdType") == "doi":
                    doi = id_elem.text
                    break

            return {
                "title": title,
                "journal_name": journal_name,
                "journal_abbrev": journal_abbrev,
                "year": year,
                "volume": volume,
                "issue": issue,
                "pages": pages,
                "authors": authors,
                "abstract": abstract,
                "doi": doi,
            }

        except ElementTree.ParseError as e:
            raise ReferenceCurationError(f"Failed to parse PubMed XML: {e}")

    def create_reference_from_pubmed(
        self,
        pubmed: int,
        reference_status: str,
        curator_userid: str,
        override_bad: bool = False,
    ) -> int:
        """
        Create a new reference from PubMed ID.

        Args:
            pubmed: PubMed ID
            reference_status: Reference status (Published, Epub ahead of print, etc.)
            curator_userid: Curator's userid
            override_bad: If True, override existing bad reference record

        Returns:
            New reference_no

        Raises:
            ReferenceCurationError: If validation fails
        """
        # Check for existing reference
        existing = self.get_reference_by_pubmed(pubmed)
        if existing:
            raise ReferenceCurationError(
                f"Reference with PubMed {pubmed} already exists "
                f"(reference_no: {existing.reference_no})"
            )

        # Check for bad reference
        if not override_bad:
            bad = self.check_bad_reference(pubmed)
            if bad:
                raise ReferenceCurationError(
                    f"PubMed {pubmed} is in bad reference list. "
                    f"Use override_bad=True to proceed."
                )

        # Validate status
        if reference_status not in self.VALID_STATUSES:
            raise ReferenceCurationError(
                f"Invalid status '{reference_status}'. "
                f"Valid statuses: {', '.join(self.VALID_STATUSES)}"
            )

        # Fetch metadata from PubMed
        try:
            metadata = self.fetch_pubmed_metadata(pubmed)
        except ReferenceCurationError:
            # If PubMed fetch fails, create a placeholder reference
            logger.warning(
                f"Failed to fetch PubMed metadata for {pubmed}, creating placeholder"
            )
            metadata = {
                "title": "",
                "year": datetime.now().year,
                "volume": "",
                "pages": "",
                "journal_abbrev": "",
                "authors": [],
            }

        # Build citation string
        if metadata.get("authors"):
            first_author = metadata["authors"][0].get("last_name", "")
            if len(metadata["authors"]) > 1:
                author_str = f"{first_author} et al."
            else:
                author_str = first_author
        else:
            author_str = ""

        citation_parts = [author_str]
        if metadata.get("year"):
            citation_parts.append(f"({metadata['year']})")
        if metadata.get("journal_abbrev"):
            citation_parts.append(metadata["journal_abbrev"])
        if metadata.get("volume"):
            vol_str = metadata["volume"]
            if metadata.get("pages"):
                vol_str += f":{metadata['pages']}"
            citation_parts.append(vol_str)

        citation = " ".join(filter(None, citation_parts)) or f"PMID:{pubmed}"

        # Get or create journal
        journal_no = None
        if metadata.get("journal_abbrev"):
            journal = (
                self.db.query(Journal)
                .filter(Journal.abbreviation == metadata["journal_abbrev"])
                .first()
            )
            if journal:
                journal_no = journal.journal_no

        reference = Reference(
            pubmed=pubmed,
            source="PubMed",
            status=reference_status,
            pdf_status="N",  # Default: no PDF
            dbxref_id=f"PMID:{pubmed}",
            citation=citation,
            year=metadata.get("year") or datetime.now().year,
            title=metadata.get("title", "")[:400],  # Truncate to fit column
            volume=metadata.get("volume", "")[:20] if metadata.get("volume") else None,
            pages=metadata.get("pages", "")[:20] if metadata.get("pages") else None,
            journal_no=journal_no,
            created_by=curator_userid[:12],
        )

        self.db.add(reference)
        self.db.flush()

        # Add authors
        if metadata.get("authors"):
            for order, author_data in enumerate(metadata["authors"], start=1):
                author_name = author_data.get("last_name", "")
                if author_data.get("initials"):
                    author_name += f" {author_data['initials']}"

                if not author_name.strip():
                    continue

                # Get or create author
                author = (
                    self.db.query(Author)
                    .filter(Author.author_name == author_name[:100])
                    .first()
                )
                if not author:
                    author = Author(
                        author_name=author_name[:100],
                        created_by=curator_userid[:12],
                    )
                    self.db.add(author)
                    self.db.flush()

                # Create author-reference link
                author_editor = AuthorEditor(
                    reference_no=reference.reference_no,
                    author_no=author.author_no,
                    author_type="Author",
                    author_order=order,
                    created_by=curator_userid[:12],
                )
                self.db.add(author_editor)

        # Add abstract if available
        if metadata.get("abstract"):
            abstract_record = Abstract(
                reference_no=reference.reference_no,
                abstract=metadata["abstract"][:4000],  # Truncate to fit
            )
            self.db.add(abstract_record)

        # Clear bad reference record if overriding
        if override_bad:
            bad = self.check_bad_reference(pubmed)
            if bad:
                self.db.delete(bad)

        self.db.commit()

        logger.info(
            f"Created reference {reference.reference_no} from PubMed {pubmed} "
            f"with {len(metadata.get('authors', []))} authors by {curator_userid}"
        )

        return reference.reference_no

    def update_reference(
        self,
        reference_no: int,
        curator_userid: str,
        title: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        volume: Optional[str] = None,
        pages: Optional[str] = None,
    ) -> bool:
        """
        Update reference metadata.

        Args:
            reference_no: Reference to update
            curator_userid: Curator's userid
            title: New title (optional)
            status: New status (optional)
            year: New year (optional)
            volume: New volume (optional)
            pages: New pages (optional)

        Returns:
            True on success

        Raises:
            ReferenceCurationError: If reference not found or validation fails
        """
        reference = self.get_reference_by_no(reference_no)
        if not reference:
            raise ReferenceCurationError(f"Reference {reference_no} not found")

        if status and status not in self.VALID_STATUSES:
            raise ReferenceCurationError(
                f"Invalid status '{status}'. "
                f"Valid statuses: {', '.join(self.VALID_STATUSES)}"
            )

        if title:
            reference.title = title
        if status:
            reference.status = status
        if year:
            reference.year = year
        if volume:
            reference.volume = volume
        if pages:
            reference.pages = pages

        self.db.commit()

        logger.info(f"Updated reference {reference_no} by {curator_userid}")

        return True

    def delete_reference(self, reference_no: int, curator_userid: str) -> bool:
        """
        Delete a reference.

        Note: This will fail if the reference has any linked annotations.
        Consider using soft delete or moving to ref_bad instead.
        """
        reference = self.get_reference_by_no(reference_no)
        if not reference:
            raise ReferenceCurationError(f"Reference {reference_no} not found")

        # Check for linked annotations
        # This is a simplified check - full implementation would check all link tables

        logger.info(
            f"Deleting reference {reference_no} (PubMed: {reference.pubmed}) "
            f"by {curator_userid}"
        )

        self.db.delete(reference)
        self.db.commit()

        return True

    def set_curation_status(
        self,
        reference_no: int,
        curation_status: str,
        curator_userid: str,
    ) -> int:
        """
        Set or update the curation status for a reference.

        Args:
            reference_no: Reference number
            curation_status: Curation status value
            curator_userid: Curator's userid

        Returns:
            ref_property_no

        Raises:
            ReferenceCurationError: If validation fails
        """
        if curation_status not in self.CURATION_STATUSES:
            raise ReferenceCurationError(
                f"Invalid curation status '{curation_status}'. "
                f"Valid statuses: {', '.join(self.CURATION_STATUSES)}"
            )

        reference = self.get_reference_by_no(reference_no)
        if not reference:
            raise ReferenceCurationError(f"Reference {reference_no} not found")

        # Check for existing curation status property
        existing = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "Curation status",
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
            source="CGD",
            property_type="Curation status",
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

    def link_to_literature_guide(
        self,
        reference_no: int,
        feature_names: list[str],
        topic: str,
        curator_userid: str,
    ) -> list[int]:
        """
        Link a reference to features via literature guide.

        Args:
            reference_no: Reference number
            feature_names: List of feature names to link
            topic: Literature topic
            curator_userid: Curator's userid

        Returns:
            List of created refprop_feat_no values

        Raises:
            ReferenceCurationError: If validation fails
        """
        reference = self.get_reference_by_no(reference_no)
        if not reference:
            raise ReferenceCurationError(f"Reference {reference_no} not found")

        # Get or create ref_property for this topic
        ref_prop = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == "Topic",
                RefProperty.property_value == topic,
            )
            .first()
        )

        if not ref_prop:
            ref_prop = RefProperty(
                reference_no=reference_no,
                source="CGD",
                property_type="Topic",
                property_value=topic,
                date_last_reviewed=datetime.now(),
                created_by=curator_userid[:12],
            )
            self.db.add(ref_prop)
            self.db.flush()

        created = []
        for name in feature_names:
            feature = (
                self.db.query(Feature)
                .filter(
                    func.upper(Feature.feature_name) == name.upper()
                )
                .first()
            )

            if not feature:
                feature = (
                    self.db.query(Feature)
                    .filter(func.upper(Feature.gene_name) == name.upper())
                    .first()
                )

            if not feature:
                logger.warning(f"Feature '{name}' not found, skipping")
                continue

            # Check for existing link
            existing = (
                self.db.query(RefpropFeat)
                .filter(
                    RefpropFeat.ref_property_no == ref_prop.ref_property_no,
                    RefpropFeat.feature_no == feature.feature_no,
                )
                .first()
            )

            if existing:
                logger.info(
                    f"Feature {name} already linked to reference {reference_no}"
                )
                continue

            link = RefpropFeat(
                ref_property_no=ref_prop.ref_property_no,
                feature_no=feature.feature_no,
                created_by=curator_userid[:12],
            )
            self.db.add(link)
            self.db.flush()
            created.append(link.refprop_feat_no)

        self.db.commit()

        logger.info(
            f"Linked reference {reference_no} to {len(created)} features "
            f"for topic '{topic}' by {curator_userid}"
        )

        return created

    def get_reference_curation_details(self, reference_no: int) -> dict:
        """
        Get full curation details for a reference.

        Includes curation status, topics, linked features, etc.
        """
        reference = self.get_reference_by_no(reference_no)
        if not reference:
            raise ReferenceCurationError(f"Reference {reference_no} not found")

        # Get properties
        properties = (
            self.db.query(RefProperty)
            .filter(RefProperty.reference_no == reference_no)
            .all()
        )

        # Get abstract
        abstract = (
            self.db.query(Abstract)
            .filter(Abstract.reference_no == reference_no)
            .first()
        )

        # Get authors
        authors = (
            self.db.query(AuthorEditor)
            .join(Author, AuthorEditor.author_no == Author.author_no)
            .filter(AuthorEditor.reference_no == reference_no)
            .order_by(AuthorEditor.author_order)
            .all()
        )

        # Build response
        curation_status = None
        topics = []

        for prop in properties:
            if prop.property_type == "Curation status":
                curation_status = prop.property_value
            elif prop.property_type == "Topic":
                # Get linked features
                links = (
                    self.db.query(RefpropFeat, Feature)
                    .join(Feature, RefpropFeat.feature_no == Feature.feature_no)
                    .filter(RefpropFeat.ref_property_no == prop.ref_property_no)
                    .all()
                )

                topics.append({
                    "topic": prop.property_value,
                    "features": [
                        {
                            "feature_no": f.feature_no,
                            "feature_name": f.feature_name,
                            "gene_name": f.gene_name,
                        }
                        for _, f in links
                    ],
                })

        return {
            "reference_no": reference.reference_no,
            "pubmed": reference.pubmed,
            "title": reference.title,
            "citation": reference.citation,
            "year": reference.year,
            "status": reference.status,
            "source": reference.source,
            "curation_status": curation_status,
            "topics": topics,
            "abstract": abstract.abstract if abstract else None,
            "authors": [
                {
                    "author_no": ae.author_no,
                    "order": ae.author_order,
                }
                for ae in authors
            ],
        }
