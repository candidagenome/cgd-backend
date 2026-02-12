"""
Literature Review Curation Service - Triage PubMed papers from REF_TEMP.

Allows curators to review papers gathered via PubMed searches and:
- Add to database with "Not yet curated" status
- Add with "High Priority" status and link to genes
- Discard (add to REF_BAD table)
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Abstract,
    Author,
    AuthorEditor,
    Feature,
    Journal,
    Organism,
    RefBad,
    RefProperty,
    RefTemp,
    Reference,
    RefpropFeat,
)

logger = logging.getLogger(__name__)

# Constants matching Perl module
PROPERTY_TYPE = "curation_status"
HIGH_PRIORITY = "High Priority"
NOT_YET_CURATED = "Not yet curated"
REF_SOURCE = "Curator Triage"


class LitReviewError(Exception):
    """Raised when literature review operations fail."""

    pass


class LitReviewCurationService:
    """Service for literature review and triage operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_pending_papers(
        self,
        limit: int = 200,
        offset: int = 0,
    ) -> dict:
        """
        Get papers from REF_TEMP waiting for review.

        Excludes papers that are already in the REFERENCE table.

        Args:
            limit: Maximum number of papers to return
            offset: Offset for pagination

        Returns:
            Dict with papers list and total count
        """
        # Get total count
        total = self.db.query(func.count(RefTemp.ref_temp_no)).scalar()

        # Get papers, ordered by pubmed descending (newest first)
        papers = (
            self.db.query(RefTemp)
            .order_by(desc(RefTemp.pubmed))
            .offset(offset)
            .limit(limit)
            .all()
        )

        result = []
        for paper in papers:
            # Check if already in REFERENCE table
            existing_ref = (
                self.db.query(Reference)
                .filter(Reference.pubmed == paper.pubmed)
                .first()
            )
            if existing_ref:
                # Auto-remove from ref_temp since it's already imported
                self._delete_from_ref_temp(paper.pubmed)
                continue

            result.append({
                "ref_temp_no": paper.ref_temp_no,
                "pubmed": paper.pubmed,
                "citation": paper.citation,
                "abstract": paper.abstract,
                "fulltext_url": paper.fulltext_url,
                "date_created": paper.date_created.isoformat() if paper.date_created else None,
            })

        return {
            "papers": result,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def get_paper_by_pubmed(self, pubmed: int) -> Optional[dict]:
        """Get a single paper from REF_TEMP by PubMed ID."""
        paper = (
            self.db.query(RefTemp)
            .filter(RefTemp.pubmed == pubmed)
            .first()
        )

        if not paper:
            return None

        return {
            "ref_temp_no": paper.ref_temp_no,
            "pubmed": paper.pubmed,
            "citation": paper.citation,
            "abstract": paper.abstract,
            "fulltext_url": paper.fulltext_url,
            "date_created": paper.date_created.isoformat() if paper.date_created else None,
        }

    def get_organisms(self) -> list[dict]:
        """Get list of organisms for dropdown."""
        organisms = (
            self.db.query(Organism)
            .order_by(Organism.organism_abbrev)
            .all()
        )

        return [
            {
                "organism_abbrev": org.organism_abbrev,
                "organism_name": org.organism_name,
            }
            for org in organisms
        ]

    def triage_add(
        self,
        pubmed: int,
        curator_userid: str,
    ) -> dict:
        """
        Add a paper with "Not yet curated" status.

        Args:
            pubmed: PubMed ID
            curator_userid: Curator's userid

        Returns:
            Result dict with reference_no and messages
        """
        messages = []

        # Check if already in REFERENCE table
        existing = (
            self.db.query(Reference)
            .filter(Reference.pubmed == pubmed)
            .first()
        )
        if existing:
            messages.append(f"PubMed {pubmed} already exists (reference_no: {existing.reference_no})")
            self._delete_from_ref_temp(pubmed)
            return {
                "success": False,
                "reference_no": existing.reference_no,
                "messages": messages,
            }

        # Create reference
        try:
            reference_no = self._create_reference_from_ref_temp(pubmed, curator_userid)
            messages.append(f"Created reference {reference_no} from PubMed {pubmed}")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to create reference from PubMed {pubmed}: {e}")
            # Check if it's a unique constraint violation
            error_str = str(e).lower()
            if "unique" in error_str or "duplicate" in error_str or "ora-00001" in error_str:
                # Check if reference exists by pubmed (re-query after rollback)
                existing = (
                    self.db.query(Reference)
                    .filter(Reference.pubmed == pubmed)
                    .first()
                )
                if existing:
                    messages.append(f"Reference already exists: {existing.reference_no}")
                    self._delete_from_ref_temp(pubmed)
                    return {
                        "success": True,
                        "reference_no": existing.reference_no,
                        "messages": messages,
                    }
                messages.append(f"Unique constraint violation - citation or dbxref_id may already exist")
            else:
                messages.append(f"Database error: {str(e)[:300]}")
            return {
                "success": False,
                "reference_no": None,
                "messages": messages,
            }

        # Set "Not yet curated" status
        try:
            self._set_curation_status(reference_no, NOT_YET_CURATED, curator_userid)
            messages.append(f"Set status to '{NOT_YET_CURATED}'")
        except Exception as e:
            messages.append(f"Warning: Could not set curation status: {str(e)}")

        # Delete from ref_temp
        self._delete_from_ref_temp(pubmed)
        messages.append(f"Removed PubMed {pubmed} from review queue")

        return {
            "success": True,
            "reference_no": reference_no,
            "messages": messages,
        }

    def triage_high_priority(
        self,
        pubmed: int,
        curator_userid: str,
        feature_names: Optional[list[str]] = None,
        organism_abbrev: Optional[str] = None,
    ) -> dict:
        """
        Add a paper with "High Priority" status and optionally link to genes.

        Args:
            pubmed: PubMed ID
            curator_userid: Curator's userid
            feature_names: Optional list of gene/feature names to link
            organism_abbrev: Organism abbreviation for validating features

        Returns:
            Result dict with reference_no, linked features, and messages
        """
        messages = []
        linked_features = []

        # Check if already in REFERENCE table
        existing = (
            self.db.query(Reference)
            .filter(Reference.pubmed == pubmed)
            .first()
        )
        if existing:
            messages.append(f"PubMed {pubmed} already exists (reference_no: {existing.reference_no})")
            self._delete_from_ref_temp(pubmed)
            return {
                "success": False,
                "reference_no": existing.reference_no,
                "linked_features": [],
                "messages": messages,
            }

        # Create reference
        try:
            reference_no = self._create_reference_from_ref_temp(pubmed, curator_userid)
            messages.append(f"Created reference {reference_no} from PubMed {pubmed}")
        except Exception as e:
            logger.error(f"Failed to create reference from PubMed {pubmed}: {e}")
            messages.append(f"Error creating reference: {str(e)}")
            return {
                "success": False,
                "reference_no": None,
                "linked_features": [],
                "messages": messages,
            }

        # Set "High Priority" status
        ref_property_no = None
        try:
            ref_property_no = self._set_curation_status(reference_no, HIGH_PRIORITY, curator_userid)
            messages.append(f"Set status to '{HIGH_PRIORITY}'")
        except Exception as e:
            messages.append(f"Warning: Could not set curation status: {str(e)}")

        # Link to features if provided
        if feature_names and ref_property_no:
            for name in feature_names:
                name = name.strip().upper()
                if not name:
                    continue

                result = self._link_to_feature(
                    reference_no,
                    ref_property_no,
                    name,
                    organism_abbrev,
                    curator_userid,
                )
                if result["success"]:
                    linked_features.append(result["feature_name"])
                    messages.append(f"Linked to feature {result['feature_name']}")
                else:
                    messages.append(result["message"])

        # Delete from ref_temp
        self._delete_from_ref_temp(pubmed)
        messages.append(f"Removed PubMed {pubmed} from review queue")

        return {
            "success": True,
            "reference_no": reference_no,
            "linked_features": linked_features,
            "messages": messages,
        }

    def triage_discard(
        self,
        pubmed: int,
        curator_userid: str,
    ) -> dict:
        """
        Discard a paper by adding to REF_BAD.

        Args:
            pubmed: PubMed ID
            curator_userid: Curator's userid

        Returns:
            Result dict with success status and messages
        """
        messages = []

        # Check if already in REF_BAD
        existing_bad = (
            self.db.query(RefBad)
            .filter(RefBad.pubmed == pubmed)
            .first()
        )
        if existing_bad:
            messages.append(f"PubMed {pubmed} is already in discard list")
            self._delete_from_ref_temp(pubmed)
            return {
                "success": True,
                "messages": messages,
            }

        # Check if in REFERENCE table (shouldn't discard if already imported)
        existing_ref = (
            self.db.query(Reference)
            .filter(Reference.pubmed == pubmed)
            .first()
        )
        if existing_ref:
            messages.append(f"PubMed {pubmed} exists in Reference table, cannot discard")
            self._delete_from_ref_temp(pubmed)
            return {
                "success": False,
                "messages": messages,
            }

        # Add to REF_BAD
        try:
            ref_bad = RefBad(
                pubmed=pubmed,
                created_by=curator_userid[:12],
            )
            self.db.add(ref_bad)
            self.db.commit()
            messages.append(f"Added PubMed {pubmed} to discard list")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to add PubMed {pubmed} to ref_bad: {e}")
            messages.append(f"Error discarding paper: {str(e)}")
            return {
                "success": False,
                "messages": messages,
            }

        # Delete from ref_temp
        self._delete_from_ref_temp(pubmed)
        messages.append(f"Removed PubMed {pubmed} from review queue")

        return {
            "success": True,
            "messages": messages,
        }

    def triage_batch(
        self,
        actions: list[dict],
        curator_userid: str,
    ) -> dict:
        """
        Process multiple triage actions in one request.

        Args:
            actions: List of dicts with pubmed, action, and optional parameters
                     action can be: "add", "high_priority", "discard"
            curator_userid: Curator's userid

        Returns:
            Dict with results for each action
        """
        results = []

        for action_data in actions:
            pubmed = action_data.get("pubmed")
            action = action_data.get("action")

            if not pubmed or not action:
                results.append({
                    "pubmed": pubmed,
                    "success": False,
                    "messages": ["Missing pubmed or action"],
                })
                continue

            if action == "add":
                result = self.triage_add(pubmed, curator_userid)
            elif action == "high_priority":
                result = self.triage_high_priority(
                    pubmed,
                    curator_userid,
                    feature_names=action_data.get("feature_names"),
                    organism_abbrev=action_data.get("organism_abbrev"),
                )
            elif action == "discard":
                result = self.triage_discard(pubmed, curator_userid)
            else:
                results.append({
                    "pubmed": pubmed,
                    "success": False,
                    "messages": [f"Unknown action: {action}"],
                })
                continue

            result["pubmed"] = pubmed
            result["action"] = action
            results.append(result)

        return {
            "results": results,
            "total_processed": len(results),
            "successful": sum(1 for r in results if r.get("success")),
        }

    def _create_reference_from_ref_temp(
        self,
        pubmed: int,
        curator_userid: str,
    ) -> int:
        """
        Create a reference record from REF_TEMP data.

        Uses the citation stored in REF_TEMP rather than fetching from PubMed.
        """
        # Get REF_TEMP record
        ref_temp = (
            self.db.query(RefTemp)
            .filter(RefTemp.pubmed == pubmed)
            .first()
        )

        if not ref_temp:
            raise LitReviewError(f"PubMed {pubmed} not found in review queue")

        # Parse citation for year (format: "Author et al. (YYYY) Journal ...")
        year = datetime.now().year
        citation = ref_temp.citation or f"PMID:{pubmed}"
        if "(" in citation and ")" in citation:
            try:
                year_str = citation.split("(")[1].split(")")[0]
                year = int(year_str[:4])
            except (IndexError, ValueError):
                pass

        # Create reference
        reference = Reference(
            pubmed=pubmed,
            source=REF_SOURCE,
            status="Published",
            pdf_status="N",
            dbxref_id=f"CGD_REF:{pubmed}",
            citation=citation[:500],
            year=year,
            created_by=curator_userid[:12],
        )

        self.db.add(reference)
        self.db.flush()

        # Add abstract if available
        if ref_temp.abstract:
            abstract = Abstract(
                reference_no=reference.reference_no,
                abstract=ref_temp.abstract[:4000],
            )
            self.db.add(abstract)

        self.db.commit()

        logger.info(
            f"Created reference {reference.reference_no} from REF_TEMP "
            f"PubMed {pubmed} by {curator_userid}"
        )

        return reference.reference_no

    def _set_curation_status(
        self,
        reference_no: int,
        status: str,
        curator_userid: str,
    ) -> int:
        """Set curation status in REF_PROPERTY table."""
        # Check for existing property
        existing = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type == PROPERTY_TYPE,
                RefProperty.source == "CGD",
            )
            .first()
        )

        if existing:
            existing.property_value = status
            existing.date_last_reviewed = datetime.now()
            self.db.commit()
            return existing.ref_property_no

        # Create new property
        prop = RefProperty(
            reference_no=reference_no,
            source="CGD",
            property_type=PROPERTY_TYPE,
            property_value=status,
            date_last_reviewed=datetime.now(),
            created_by=curator_userid[:12],
        )
        self.db.add(prop)
        self.db.commit()

        return prop.ref_property_no

    def _link_to_feature(
        self,
        reference_no: int,
        ref_property_no: int,
        feature_name: str,
        organism_abbrev: Optional[str],
        curator_userid: str,
    ) -> dict:
        """Link reference to a feature via REFPROP_FEAT table."""
        # Look up feature
        query = self.db.query(Feature).filter(
            func.upper(Feature.feature_name) == feature_name.upper()
        )

        if organism_abbrev:
            # Join with organism to filter by organism
            query = query.filter(Feature.organism_abbrev == organism_abbrev)

        feature = query.first()

        # Try gene_name if feature_name not found
        if not feature:
            query = self.db.query(Feature).filter(
                func.upper(Feature.gene_name) == feature_name.upper()
            )
            if organism_abbrev:
                query = query.filter(Feature.organism_abbrev == organism_abbrev)
            feature = query.first()

        if not feature:
            return {
                "success": False,
                "message": f"Feature '{feature_name}' not found" + (
                    f" for organism {organism_abbrev}" if organism_abbrev else ""
                ),
            }

        # Check for existing link
        existing = (
            self.db.query(RefpropFeat)
            .filter(
                RefpropFeat.ref_property_no == ref_property_no,
                RefpropFeat.feature_no == feature.feature_no,
            )
            .first()
        )

        if existing:
            return {
                "success": True,
                "feature_name": feature.feature_name,
                "message": f"Feature {feature.feature_name} already linked",
            }

        # Create link
        link = RefpropFeat(
            ref_property_no=ref_property_no,
            feature_no=feature.feature_no,
            created_by=curator_userid[:12],
        )
        self.db.add(link)
        self.db.commit()

        return {
            "success": True,
            "feature_name": feature.feature_name,
            "feature_no": feature.feature_no,
        }

    def _delete_from_ref_temp(self, pubmed: int) -> bool:
        """Delete a paper from REF_TEMP after processing."""
        try:
            ref_temp = (
                self.db.query(RefTemp)
                .filter(RefTemp.pubmed == pubmed)
                .first()
            )
            if ref_temp:
                self.db.delete(ref_temp)
                self.db.commit()
                return True
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to delete PubMed {pubmed} from ref_temp: {e}")
        return False
