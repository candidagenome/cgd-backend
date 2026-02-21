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
    Dbxref,
    DbxrefRef,
    DeleteLog,
    Feature,
    GoRef,
    Journal,
    RefBad,
    RefLink,
    RefProperty,
    RefUnlink,
    RefUrl,
    Reference,
    RefpropFeat,
    Url,
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

    # Curation status values from database REF_PROPERTY table
    CURATION_STATUSES = [
        "Not yet curated",
        "High Priority",
        "Abstract curated, full text not curated",
        "Done:Abstract curated, full text not curated",
        "Basic, lit guide, GO, Pheno curation done",
        "Dataset to load",
        "Gene model",
        "Genomic sequence not identified",
        "Pathways",
        "Related species",
        "cell biology",
        "clinical",
        "multiple",
        "not gene specific",
        "other",
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
        # Get the pubmed ID from the reference
        reference = self.get_reference_by_no(reference_no)
        if not reference or not reference.pubmed:
            return None

        return (
            self.db.query(RefUnlink)
            .filter(
                RefUnlink.pubmed == reference.pubmed,
                RefUnlink.tab_name == "FEATURE",
                RefUnlink.primary_key == feature_no,
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

    def create_manual_reference(
        self,
        title: str,
        year: int,
        reference_status: str,
        curator_userid: str,
        authors: Optional[list[str]] = None,
        journal_abbrev: Optional[str] = None,
        volume: Optional[str] = None,
        pages: Optional[str] = None,
        abstract: Optional[str] = None,
        publication_types: Optional[list[str]] = None,
    ) -> int:
        """
        Create a reference manually (without PubMed ID).

        Args:
            title: Reference title
            year: Publication year
            reference_status: Reference status (Published, Epub ahead of print, etc.)
            curator_userid: Curator's userid
            authors: List of author names (e.g., ["Smith J", "Doe JA"])
            journal_abbrev: Journal abbreviation
            volume: Volume number
            pages: Page range
            abstract: Abstract text
            publication_types: List of publication types (e.g., ["Journal Article"])

        Returns:
            New reference_no

        Raises:
            ReferenceCurationError: If validation fails
        """
        # Validate status
        if reference_status not in self.VALID_STATUSES:
            raise ReferenceCurationError(
                f"Invalid status '{reference_status}'. "
                f"Valid statuses: {', '.join(self.VALID_STATUSES)}"
            )

        if not title:
            raise ReferenceCurationError("Title is required")

        if not year:
            raise ReferenceCurationError("Year is required")

        # Build citation string
        if authors:
            first_author = authors[0].split()[0] if authors[0] else ""
            if len(authors) > 1:
                author_str = f"{first_author} et al."
            else:
                author_str = first_author
        else:
            author_str = ""

        citation_parts = [author_str]
        citation_parts.append(f"({year})")
        if journal_abbrev:
            citation_parts.append(journal_abbrev)
        if volume:
            vol_str = volume
            if pages:
                vol_str += f":{pages}"
            citation_parts.append(vol_str)

        citation = " ".join(filter(None, citation_parts)) or title[:50]

        # Check for duplicate citation
        existing = (
            self.db.query(Reference)
            .filter(Reference.citation == citation)
            .first()
        )
        if existing:
            raise ReferenceCurationError(
                f"Reference with citation '{citation}' already exists "
                f"(reference_no: {existing.reference_no})"
            )

        # Get journal_no if journal provided
        journal_no = None
        if journal_abbrev:
            journal = (
                self.db.query(Journal)
                .filter(Journal.abbreviation == journal_abbrev)
                .first()
            )
            if journal:
                journal_no = journal.journal_no
            else:
                # Create new journal if it doesn't exist
                journal = Journal(
                    abbreviation=journal_abbrev[:50],
                    full_name=journal_abbrev[:200],
                    created_by=curator_userid[:12],
                )
                self.db.add(journal)
                self.db.flush()
                journal_no = journal.journal_no

        # Determine PDF status based on reference status
        pdf_status = "N" if reference_status == "Published" else "NAP"

        reference = Reference(
            pubmed=None,
            source="Curator PubMed reference",  # Must match CODE table values
            status=reference_status,
            pdf_status=pdf_status,
            citation=citation[:500],
            year=year,
            title=title[:400],
            volume=volume[:20] if volume else None,
            page=pages[:20] if pages else None,
            journal_no=journal_no,
            created_by=curator_userid[:12],
        )

        self.db.add(reference)
        self.db.flush()

        # Add authors
        if authors:
            for order, author_name in enumerate(authors, start=1):
                if not author_name.strip():
                    continue

                # Clean author name (remove punctuation)
                clean_name = author_name.replace(",", "").replace(".", "").strip()

                # Get or create author
                author = (
                    self.db.query(Author)
                    .filter(Author.author_name == clean_name[:100])
                    .first()
                )
                if not author:
                    author = Author(
                        author_name=clean_name[:100],
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
                )
                self.db.add(author_editor)

        # Add abstract if available
        if abstract:
            # Truncate if too long
            if len(abstract) > 4000:
                abstract = abstract[:3950] + "...ABSTRACT TRUNCATED AT 3950 CHARACTERS."
            abstract_record = Abstract(
                reference_no=reference.reference_no,
                abstract=abstract,
            )
            self.db.add(abstract_record)

        self.db.commit()

        logger.info(
            f"Created manual reference {reference.reference_no} "
            f"with {len(authors) if authors else 0} authors by {curator_userid}"
        )

        return reference.reference_no

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
            source="Curator PubMed reference",  # Must match CODE table values
            status=reference_status,
            pdf_status="N",  # Default: no PDF
            dbxref_id=f"PMID:{pubmed}",
            citation=citation,
            year=metadata.get("year") or datetime.now().year,
            title=metadata.get("title", "")[:400],  # Truncate to fit column
            volume=metadata.get("volume", "")[:20] if metadata.get("volume") else None,
            page=metadata.get("pages", "")[:20] if metadata.get("pages") else None,
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

        if title is not None:
            reference.title = title or None
        if status:
            reference.status = status
        if year is not None:
            reference.year = year or None
        if volume is not None:
            reference.volume = volume or None
        if pages is not None:
            reference.page = pages or None

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
            source="CGD",
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
                RefProperty.property_type == "literature_topic",
                RefProperty.property_value == topic,
            )
            .first()
        )

        if not ref_prop:
            ref_prop = RefProperty(
                reference_no=reference_no,
                source="CGD",
                property_type="literature_topic",
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
            if prop.property_type == "curation_status":
                curation_status = prop.property_value
            elif prop.property_type == "literature_topic":
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

        # Get URLs linked to this reference
        urls = (
            self.db.query(RefUrl, Url)
            .join(Url, RefUrl.url_no == Url.url_no)
            .filter(RefUrl.reference_no == reference_no)
            .all()
        )

        return {
            "reference_no": reference.reference_no,
            "pubmed": reference.pubmed,
            "dbxref_id": reference.dbxref_id,
            "title": reference.title,
            "citation": reference.citation,
            "year": reference.year,
            "volume": reference.volume,
            "pages": reference.page,
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
            "urls": [
                {
                    "url_no": url.url_no,
                    "url": url.url,
                    "url_type": url.url_type,
                    "source": url.source,
                }
                for ref_url, url in urls
            ],
        }

    def search_references(
        self,
        pubmed: Optional[int] = None,
        reference_no: Optional[int] = None,
        dbxref_id: Optional[str] = None,
        volume: Optional[str] = None,
        page: Optional[str] = None,
        author: Optional[str] = None,
        author2: Optional[str] = None,
        keyword: Optional[str] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
        limit: int = 100,
    ) -> list[dict]:
        """
        Search for references by various criteria.

        Args:
            pubmed: PubMed ID
            reference_no: Reference number
            dbxref_id: CGDID (dbxref_id)
            volume: Journal volume
            page: Page number/range
            author: Author name (partial match)
            author2: Second author name (partial match)
            keyword: Keyword in title or abstract
            min_year: Minimum publication year
            max_year: Maximum publication year
            limit: Maximum results to return

        Returns:
            List of reference dictionaries
        """
        if pubmed:
            ref = self.get_reference_by_pubmed(pubmed)
            if ref:
                return [self._reference_to_dict(ref)]
            return []

        if reference_no:
            ref = self.get_reference_by_no(reference_no)
            if ref:
                return [self._reference_to_dict(ref)]
            return []

        if dbxref_id:
            ref = (
                self.db.query(Reference)
                .filter(Reference.dbxref_id == dbxref_id)
                .first()
            )
            if ref:
                return [self._reference_to_dict(ref)]
            return []

        # Build query for other search types
        query = self.db.query(Reference)

        if volume and page:
            query = query.filter(
                Reference.volume == volume,
                Reference.page.like(f"{page}%"),
            )

        if author or author2:
            # Join with author tables for author search
            if author:
                author_pattern = f"%{author}%"
                author_refs = (
                    self.db.query(AuthorEditor.reference_no)
                    .join(Author, AuthorEditor.author_no == Author.author_no)
                    .filter(Author.author_name.ilike(author_pattern))
                    .distinct()
                    .subquery()
                )
                query = query.filter(Reference.reference_no.in_(author_refs))

            if author2:
                author2_pattern = f"%{author2}%"
                author2_refs = (
                    self.db.query(AuthorEditor.reference_no)
                    .join(Author, AuthorEditor.author_no == Author.author_no)
                    .filter(Author.author_name.ilike(author2_pattern))
                    .distinct()
                    .subquery()
                )
                query = query.filter(Reference.reference_no.in_(author2_refs))

        if keyword:
            # Search in title and abstract
            keyword_pattern = f"%{keyword}%"
            # Get reference_nos that have matching abstracts
            abstract_refs = (
                self.db.query(Abstract.reference_no)
                .filter(Abstract.abstract.ilike(keyword_pattern))
                .subquery()
            )
            query = query.filter(
                (Reference.title.ilike(keyword_pattern))
                | (Reference.reference_no.in_(abstract_refs))
            )

        if min_year:
            query = query.filter(Reference.year >= min_year)

        if max_year:
            query = query.filter(Reference.year <= max_year)

        # Order by year descending, then reference_no
        query = query.order_by(Reference.year.desc(), Reference.reference_no.desc())

        references = query.limit(limit).all()

        return [self._reference_to_dict(ref) for ref in references]

    def _reference_to_dict(self, ref: Reference) -> dict:
        """Convert Reference model to dictionary."""
        return {
            "reference_no": ref.reference_no,
            "pubmed": ref.pubmed,
            "dbxref_id": ref.dbxref_id,
            "citation": ref.citation,
            "title": ref.title,
            "year": ref.year,
            "volume": ref.volume,
            "page": ref.page,
            "status": ref.status,
            "source": ref.source,
        }

    def is_reference_in_use(self, reference_no: int) -> dict:
        """
        Check if a reference has linked data.

        Returns dict with flags for each type of linked data.
        """
        # Check go_ref table
        go_ref_count = (
            self.db.query(func.count(GoRef.go_ref_no))
            .filter(GoRef.reference_no == reference_no)
            .scalar()
        )

        # Check ref_link table
        ref_link_count = (
            self.db.query(func.count(RefLink.ref_link_no))
            .filter(RefLink.reference_no == reference_no)
            .scalar()
        )

        # Check ref_property/refprop_feat for linked features
        refprop_feat_count = (
            self.db.query(func.count(RefpropFeat.refprop_feat_no))
            .join(RefProperty, RefpropFeat.ref_property_no == RefProperty.ref_property_no)
            .filter(RefProperty.reference_no == reference_no)
            .scalar()
        )

        in_use = (
            go_ref_count > 0
            or ref_link_count > 0
            or refprop_feat_count > 0
        )

        return {
            "in_use": in_use,
            "go_ref_count": go_ref_count,
            "ref_link_count": ref_link_count,
            "refprop_feat_count": refprop_feat_count,
        }

    def delete_reference_with_cleanup(
        self,
        reference_no: int,
        curator_userid: str,
        delete_log_comment: Optional[str] = None,
        make_secondary_for: Optional[int] = None,
    ) -> dict:
        """
        Delete a reference with proper cleanup.

        - Adds pubmed to REF_BAD if applicable
        - Deletes from REF_UNLINK
        - Handles CGDID (make secondary or mark deleted)
        - Logs deletion in DELETE_LOG

        Args:
            reference_no: Reference to delete
            curator_userid: Curator's userid
            delete_log_comment: Optional comment for delete log
            make_secondary_for: If set, make CGDID secondary for this reference_no

        Returns:
            Result dict with messages
        """
        messages = []

        reference = self.get_reference_by_no(reference_no)
        if not reference:
            raise ReferenceCurationError(f"Reference {reference_no} not found")

        # Check if in use
        usage = self.is_reference_in_use(reference_no)
        if usage["in_use"]:
            raise ReferenceCurationError(
                f"Reference {reference_no} is linked to data and cannot be deleted. "
                "Transfer or delete linked data first."
            )

        pubmed = reference.pubmed
        dbxref_id = reference.dbxref_id

        # Delete related records first (to avoid foreign key constraint errors)
        try:
            # Delete author_editor records
            self.db.query(AuthorEditor).filter(
                AuthorEditor.reference_no == reference_no
            ).delete()

            # Delete ref_url records
            self.db.query(RefUrl).filter(
                RefUrl.reference_no == reference_no
            ).delete()

            # Delete abstract
            self.db.query(Abstract).filter(
                Abstract.reference_no == reference_no
            ).delete()

            # Delete the reference
            self.db.delete(reference)
            self.db.flush()
            messages.append(f"Reference {reference_no} deleted")
        except Exception as e:
            self.db.rollback()
            raise ReferenceCurationError(f"Failed to delete reference: {e}")

        # Delete from ref_unlink if pubmed exists
        if pubmed:
            try:
                self.db.query(RefUnlink).filter(RefUnlink.pubmed == pubmed).delete()
                messages.append(f"Cleaned up ref_unlink entries for PMID:{pubmed}")
            except Exception:
                pass  # Not critical

            # Add to ref_bad
            existing_bad = self.db.query(RefBad).filter(RefBad.pubmed == pubmed).first()
            if not existing_bad:
                try:
                    ref_bad = RefBad(
                        pubmed=pubmed,
                        created_by=curator_userid[:12],
                    )
                    self.db.add(ref_bad)
                    messages.append(f"Added PMID:{pubmed} to ref_bad")
                except Exception:
                    pass  # Not critical

        # Handle CGDID
        if dbxref_id:
            dbxref = (
                self.db.query(Dbxref)
                .filter(Dbxref.dbxref_id == dbxref_id)
                .first()
            )

            if dbxref and make_secondary_for:
                # Make it secondary for another reference
                target_ref = self.get_reference_by_no(make_secondary_for)
                if target_ref:
                    dbxref.dbxref_type = "CGDID Secondary"
                    # Create link to new reference
                    dbxref_ref = DbxrefRef(
                        dbxref_no=dbxref.dbxref_no,
                        reference_no=make_secondary_for,
                    )
                    self.db.add(dbxref_ref)
                    messages.append(
                        f"Made {dbxref_id} secondary for reference {make_secondary_for}"
                    )
            elif dbxref:
                # Mark as deleted
                dbxref.dbxref_type = "CGDID Deleted"
                messages.append(f"Marked {dbxref_id} as deleted")

        # Add delete log entry
        if delete_log_comment:
            try:
                delete_log = DeleteLog(
                    tab_name="REFERENCE",
                    primary_key=reference_no,
                    description=delete_log_comment[:240],
                    created_by=curator_userid[:12],
                )
                self.db.add(delete_log)
                messages.append("Added delete log entry")
            except Exception:
                pass  # Not critical

        self.db.commit()

        logger.info(
            f"Deleted reference {reference_no} (PMID:{pubmed}) by {curator_userid}"
        )

        return {
            "success": True,
            "messages": messages,
            "dbxref_id": dbxref_id,
        }

    def get_year_range(self) -> tuple[int, int]:
        """Get min and max publication years in database."""
        result = self.db.query(
            func.min(Reference.year),
            func.max(Reference.year),
        ).first()

        min_year = result[0] or 1900
        max_year = result[1] or datetime.now().year

        return min_year, max_year

    def get_url_types(self) -> list[str]:
        """Get list of valid URL types for references."""
        # Based on legacy Database::Url->allowed_reference_url_type
        return [
            "Reference Data",
            "Reference LINKOUT",
            "Reference full text",
            "Reference full text all",
            "Reference Supplement",
        ]

    def get_url_sources(self) -> list[str]:
        """Get list of valid URL sources."""
        return ["Author", "NCBI", "Publisher"]

    def add_reference_url(
        self,
        reference_no: int,
        url: str,
        url_type: str,
        source: str,
        curator_userid: str,
    ) -> dict:
        """
        Add a URL to a reference.

        Creates a new URL entry if it doesn't exist, then links it to the reference.

        Args:
            reference_no: Reference number
            url: The URL string
            url_type: Type of URL (Full-text, Abstract, etc.)
            source: Source of URL (Author, NCBI, Publisher)
            curator_userid: Curator user ID

        Returns:
            dict with url_no, ref_url_no, and message
        """
        # Verify reference exists
        reference = self.get_reference_by_no(reference_no)
        if not reference:
            raise ReferenceCurationError(f"Reference {reference_no} not found")

        # Check if URL already exists
        existing_url = (
            self.db.query(Url)
            .filter(Url.url == url)
            .first()
        )

        if existing_url:
            url_no = existing_url.url_no
            # Check if already linked to this reference
            existing_link = (
                self.db.query(RefUrl)
                .filter(
                    RefUrl.reference_no == reference_no,
                    RefUrl.url_no == url_no,
                )
                .first()
            )
            if existing_link:
                raise ReferenceCurationError(
                    f"URL is already linked to reference {reference_no}"
                )
        else:
            # Create new URL entry
            new_url = Url(
                source=source,
                url_type=url_type,
                url=url,
                created_by=curator_userid[:12],
            )
            self.db.add(new_url)
            self.db.flush()
            url_no = new_url.url_no
            logger.info(f"Created new URL entry: url_no={url_no}, url={url}")

        # Create RefUrl link
        ref_url = RefUrl(
            reference_no=reference_no,
            url_no=url_no,
        )
        self.db.add(ref_url)
        self.db.commit()

        logger.info(
            f"Linked URL {url_no} to reference {reference_no} by {curator_userid}"
        )

        return {
            "url_no": url_no,
            "ref_url_no": ref_url.ref_url_no,
            "message": f"URL added to reference {reference_no}",
        }
