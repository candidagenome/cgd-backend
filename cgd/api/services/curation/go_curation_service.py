"""
GO Curation Service - Business logic for GO annotation CRUD operations.

Mirrors validation rules from legacy UpdateGO.pm:
- GO ID must exist and match selected ontology aspect
- Reference must be valid and not unlinked from feature
- Evidence codes have specific requirements (IC requires "from GOid")
- Qualifiers have ontology-specific constraints
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    Go,
    GoAnnotation,
    GoQualifier,
    GoRef,
    GorefDbxref,
    Reference,
    RefUnlink,
)

logger = logging.getLogger(__name__)


class GoCurationError(Exception):
    """Raised when GO curation validation fails."""

    pass


class GoCurationService:
    """Service for GO annotation curation operations."""

    # Valid GO evidence codes
    EVIDENCE_CODES = [
        "EXP",  # Inferred from Experiment
        "IDA",  # Inferred from Direct Assay
        "IPI",  # Inferred from Physical Interaction
        "IMP",  # Inferred from Mutant Phenotype
        "IGI",  # Inferred from Genetic Interaction
        "IEP",  # Inferred from Expression Pattern
        "HTP",  # High Throughput
        "HDA",  # High Throughput Direct Assay
        "HMP",  # High Throughput Mutant Phenotype
        "HGI",  # High Throughput Genetic Interaction
        "HEP",  # High Throughput Expression Pattern
        "IBA",  # Inferred from Biological aspect of Ancestor
        "IBD",  # Inferred from Biological aspect of Descendant
        "IKR",  # Inferred from Key Residues
        "IRD",  # Inferred from Rapid Divergence
        "ISS",  # Inferred from Sequence or Structural Similarity
        "ISO",  # Inferred from Sequence Orthology
        "ISA",  # Inferred from Sequence Alignment
        "ISM",  # Inferred from Sequence Model
        "IGC",  # Inferred from Genomic Context
        "RCA",  # Reviewed Computational Analysis
        "TAS",  # Traceable Author Statement
        "NAS",  # Non-traceable Author Statement
        "IC",   # Inferred by Curator
        "ND",   # No biological Data available
        "IEA",  # Inferred from Electronic Annotation
    ]

    # Valid GO qualifiers by aspect
    QUALIFIERS = {
        "F": ["NOT", "contributes_to"],
        "P": ["NOT", "acts_upstream_of", "acts_upstream_of_positive_effect",
              "acts_upstream_of_negative_effect", "acts_upstream_of_or_within",
              "acts_upstream_of_or_within_positive_effect",
              "acts_upstream_of_or_within_negative_effect"],
        "C": ["NOT", "colocalizes_with", "part_of"],
    }

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

    def get_go_by_goid(self, goid: int) -> Optional[Go]:
        """Look up GO term by GO ID."""
        return self.db.query(Go).filter(Go.goid == goid).first()

    def validate_goid(self, goid: int, expected_aspect: Optional[str] = None) -> Go:
        """
        Validate GO ID exists and optionally matches expected aspect.

        Args:
            goid: GO ID number (without GO: prefix)
            expected_aspect: Expected aspect (F/P/C or function/process/component)

        Returns:
            Go object if valid

        Raises:
            GoCurationError: If GO ID invalid or aspect mismatch
        """
        go = self.get_go_by_goid(goid)
        if not go:
            raise GoCurationError(f"GO ID {goid} not found in database")

        if expected_aspect:
            aspect_map = {
                "F": "function",
                "P": "process",
                "C": "component",
                "function": "function",
                "process": "process",
                "component": "component",
            }
            expected = aspect_map.get(expected_aspect.upper(), expected_aspect.lower())

            if go.go_aspect.lower() != expected:
                raise GoCurationError(
                    f"GO ID {goid} is {go.go_aspect}, expected {expected_aspect}"
                )

        return go

    def validate_evidence_code(
        self, evidence: str, ic_from_goid: Optional[int] = None
    ) -> str:
        """
        Validate GO evidence code.

        Args:
            evidence: Evidence code
            ic_from_goid: Required GO ID for IC evidence

        Returns:
            Normalized evidence code

        Raises:
            GoCurationError: If evidence code invalid
        """
        evidence = evidence.upper()
        if evidence not in self.EVIDENCE_CODES:
            raise GoCurationError(f"Invalid evidence code: {evidence}")

        if evidence == "IC" and not ic_from_goid:
            raise GoCurationError("IC evidence requires 'from GO ID' value")

        return evidence

    def validate_reference(
        self, reference_no: int, feature_no: Optional[int] = None
    ) -> Reference:
        """
        Validate reference exists and is not unlinked from feature.

        Args:
            reference_no: Reference number
            feature_no: Optional feature to check for unlink

        Returns:
            Reference object if valid

        Raises:
            GoCurationError: If reference invalid or unlinked
        """
        ref = self.db.query(Reference).filter(
            Reference.reference_no == reference_no
        ).first()

        if not ref:
            raise GoCurationError(f"Reference {reference_no} not found")

        # Check for unlink if feature provided
        if feature_no and ref.pubmed:
            unlink = (
                self.db.query(RefUnlink)
                .filter(
                    RefUnlink.pubmed == ref.pubmed,
                    RefUnlink.tab_name == "FEATURE",
                    RefUnlink.primary_key == feature_no,
                )
                .first()
            )
            if unlink:
                raise GoCurationError(
                    f"Reference {reference_no} is unlinked from feature {feature_no}"
                )

        return ref

    def validate_qualifiers(self, qualifiers: list[str], aspect: str) -> list[str]:
        """
        Validate GO qualifiers for the given aspect.

        Args:
            qualifiers: List of qualifier strings
            aspect: GO aspect (F/P/C)

        Returns:
            Validated list of qualifiers

        Raises:
            GoCurationError: If invalid qualifier for aspect
        """
        aspect_key = aspect[0].upper() if len(aspect) > 1 else aspect.upper()
        valid = self.QUALIFIERS.get(aspect_key, [])

        for q in qualifiers:
            if q not in valid:
                raise GoCurationError(
                    f"Invalid qualifier '{q}' for {aspect} aspect. "
                    f"Valid qualifiers: {', '.join(valid)}"
                )

        return qualifiers

    def get_annotations_for_feature(self, feature_no: int) -> list[dict]:
        """
        Get all GO annotations for a feature.

        Returns structured data including references, qualifiers, and evidence.
        """
        annotations = (
            self.db.query(GoAnnotation)
            .filter(GoAnnotation.feature_no == feature_no)
            .all()
        )

        results = []
        for ann in annotations:
            go = self.db.query(Go).filter(Go.go_no == ann.go_no).first()

            # Get references and qualifiers
            refs = []
            for go_ref in ann.go_ref:
                ref = self.db.query(Reference).filter(
                    Reference.reference_no == go_ref.reference_no
                ).first()

                qualifiers = [q.qualifier for q in go_ref.go_qualifier]

                refs.append({
                    "go_ref_no": go_ref.go_ref_no,
                    "reference_no": go_ref.reference_no,
                    "pubmed": ref.pubmed if ref else None,
                    "citation": ref.citation if ref else None,
                    "has_qualifier": go_ref.has_qualifier,
                    "has_supporting_evidence": go_ref.has_supporting_evidence,
                    "qualifiers": qualifiers,
                })

            results.append({
                "go_annotation_no": ann.go_annotation_no,
                "go_no": ann.go_no,
                "goid": go.goid if go else None,
                "go_term": go.go_term if go else None,
                "go_aspect": go.go_aspect if go else None,
                "go_evidence": ann.go_evidence,
                "annotation_type": ann.annotation_type,
                "source": ann.source,
                "date_last_reviewed": ann.date_last_reviewed.isoformat()
                if ann.date_last_reviewed else None,
                "date_created": ann.date_created.isoformat()
                if ann.date_created else None,
                "created_by": ann.created_by,
                "references": refs,
            })

        return results

    def create_annotation(
        self,
        feature_no: int,
        goid: int,
        evidence: str,
        reference_no: int,
        curator_userid: str,
        annotation_type: str = "manually curated",
        source: str = "CGD",
        qualifiers: Optional[list[str]] = None,
        ic_from_goid: Optional[int] = None,
    ) -> int:
        """
        Create a new GO annotation.

        Args:
            feature_no: Feature number
            goid: GO ID
            evidence: Evidence code
            reference_no: Reference number
            curator_userid: Curator's userid
            annotation_type: Annotation type (default: "manually curated")
            source: Source (default: "CGD")
            qualifiers: Optional list of qualifiers
            ic_from_goid: GO ID for IC evidence "from" field

        Returns:
            New go_annotation_no

        Raises:
            GoCurationError: If validation fails
        """
        # Validate inputs
        go = self.validate_goid(goid)
        evidence = self.validate_evidence_code(evidence, ic_from_goid)
        self.validate_reference(reference_no, feature_no)

        if qualifiers:
            self.validate_qualifiers(qualifiers, go.go_aspect)

        # Check for existing annotation
        existing = (
            self.db.query(GoAnnotation)
            .filter(
                GoAnnotation.feature_no == feature_no,
                GoAnnotation.go_no == go.go_no,
                GoAnnotation.go_evidence == evidence,
                GoAnnotation.annotation_type == annotation_type,
                GoAnnotation.source == source,
            )
            .first()
        )

        if existing:
            # Add reference to existing annotation instead
            logger.info(
                f"Adding reference to existing annotation {existing.go_annotation_no}"
            )
            self._add_reference_to_annotation(
                existing.go_annotation_no, reference_no, qualifiers, curator_userid
            )
            return existing.go_annotation_no

        # Create new annotation
        annotation = GoAnnotation(
            go_no=go.go_no,
            feature_no=feature_no,
            go_evidence=evidence,
            annotation_type=annotation_type,
            source=source,
            date_last_reviewed=datetime.now(),
            created_by=curator_userid[:12],
        )
        self.db.add(annotation)
        self.db.flush()  # Get the annotation_no

        # Add reference
        self._add_reference_to_annotation(
            annotation.go_annotation_no, reference_no, qualifiers, curator_userid
        )

        self.db.commit()

        logger.info(
            f"Created GO annotation {annotation.go_annotation_no} "
            f"for feature {feature_no}, GO:{goid}"
        )

        return annotation.go_annotation_no

    def _add_reference_to_annotation(
        self,
        go_annotation_no: int,
        reference_no: int,
        qualifiers: Optional[list[str]],
        curator_userid: str,
    ) -> int:
        """Add a reference to an existing annotation."""
        # Check if reference already linked
        existing_ref = (
            self.db.query(GoRef)
            .filter(
                GoRef.go_annotation_no == go_annotation_no,
                GoRef.reference_no == reference_no,
            )
            .first()
        )

        if existing_ref:
            raise GoCurationError(
                f"Reference {reference_no} already linked to annotation {go_annotation_no}"
            )

        has_qualifier = "Y" if qualifiers else "N"

        go_ref = GoRef(
            go_annotation_no=go_annotation_no,
            reference_no=reference_no,
            has_qualifier=has_qualifier,
            has_supporting_evidence="N",  # Default
            created_by=curator_userid[:12],
        )
        self.db.add(go_ref)
        self.db.flush()

        # Add qualifiers
        if qualifiers:
            for q in qualifiers:
                qualifier = GoQualifier(
                    go_ref_no=go_ref.go_ref_no,
                    qualifier=q,
                )
                self.db.add(qualifier)

        return go_ref.go_ref_no

    def update_date_last_reviewed(
        self, go_annotation_no: int, curator_userid: str
    ) -> bool:
        """
        Update the date_last_reviewed for an annotation.

        This is called when a curator confirms they've reviewed the annotation.
        """
        annotation = (
            self.db.query(GoAnnotation)
            .filter(GoAnnotation.go_annotation_no == go_annotation_no)
            .first()
        )

        if not annotation:
            raise GoCurationError(f"Annotation {go_annotation_no} not found")

        annotation.date_last_reviewed = datetime.now()
        self.db.commit()

        logger.info(
            f"Updated date_last_reviewed for annotation {go_annotation_no} "
            f"by {curator_userid}"
        )

        return True

    def delete_annotation(self, go_annotation_no: int, curator_userid: str) -> bool:
        """
        Delete a GO annotation and all associated records.

        Cascading deletes handle go_ref, go_qualifier, goref_dbxref.
        """
        annotation = (
            self.db.query(GoAnnotation)
            .filter(GoAnnotation.go_annotation_no == go_annotation_no)
            .first()
        )

        if not annotation:
            raise GoCurationError(f"Annotation {go_annotation_no} not found")

        # Log before delete
        logger.info(
            f"Deleting GO annotation {go_annotation_no} "
            f"(feature {annotation.feature_no}, GO {annotation.go_no}) "
            f"by {curator_userid}"
        )

        self.db.delete(annotation)
        self.db.commit()

        return True

    def delete_reference_from_annotation(
        self, go_ref_no: int, curator_userid: str
    ) -> bool:
        """
        Remove a reference from an annotation.

        If this is the only reference, the annotation should be deleted instead.
        """
        go_ref = self.db.query(GoRef).filter(GoRef.go_ref_no == go_ref_no).first()

        if not go_ref:
            raise GoCurationError(f"GO reference {go_ref_no} not found")

        # Check if this is the only reference
        ref_count = (
            self.db.query(GoRef)
            .filter(GoRef.go_annotation_no == go_ref.go_annotation_no)
            .count()
        )

        if ref_count <= 1:
            raise GoCurationError(
                "Cannot remove only reference. Delete the annotation instead."
            )

        logger.info(
            f"Removing reference {go_ref.reference_no} from annotation "
            f"{go_ref.go_annotation_no} by {curator_userid}"
        )

        self.db.delete(go_ref)
        self.db.commit()

        return True
