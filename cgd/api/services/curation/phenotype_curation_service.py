"""
Phenotype Curation Service - Business logic for phenotype annotation CRUD operations.

Mirrors validation rules from legacy UpdatePhenotype.pm:
- Experiment type, mutant type, observable, qualifier must be valid CV terms
- Reference must be valid and not unlinked from feature
- Experiment properties have specific requirements (strain_background required for CGD)
- Observable determines required property types (e.g., chemical compound requires chebi)
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session

from sqlalchemy.orm import joinedload

from cgd.models.models import (
    Code,
    Cv,
    CvTerm,
    Experiment,
    ExptExptprop,
    ExptProperty,
    Feature,
    PhenoAnnotation,
    Phenotype,
    RefLink,
    RefUnlink,
    Reference,
    RefUrl,
)

logger = logging.getLogger(__name__)

# Source identifier
SOURCE = "CGD"


class PhenotypeCurationError(Exception):
    """Raised when phenotype curation validation fails."""

    pass


class PhenotypeCurationService:
    """Service for phenotype annotation curation operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_feature_by_name(
        self, name: str, organism_abbrev: Optional[str] = None
    ) -> Optional[Feature]:
        """
        Look up feature by name or gene_name.

        If organism_abbrev is provided, filter by organism.
        Returns the first matching feature (use get_features_by_name for all matches).
        """
        from cgd.models.models import Organism

        query = self.db.query(Feature).filter(
            or_(
                func.upper(Feature.feature_name) == name.upper(),
                func.upper(Feature.gene_name) == name.upper(),
            )
        )

        if organism_abbrev:
            query = query.join(Organism, Feature.organism_no == Organism.organism_no).filter(
                func.upper(Organism.organism_abbrev) == organism_abbrev.upper()
            )

        feature = query.first()

        if feature:
            logger.info(
                f"Found feature: name={name} -> feature_no={feature.feature_no}, "
                f"feature_name={feature.feature_name}, gene_name={feature.gene_name}"
            )
        else:
            logger.warning(f"Feature not found: name={name}, organism={organism_abbrev}")

        return feature

    def get_reference_no_by_pubmed(self, pubmed: int) -> Optional[int]:
        """Look up reference_no by PubMed ID."""
        ref = (
            self.db.query(Reference)
            .filter(Reference.pubmed == pubmed)
            .first()
        )
        return ref.reference_no if ref else None

    def get_features_by_name(
        self, name: str, organism_abbrev: Optional[str] = None
    ) -> list[Feature]:
        """
        Look up ALL features by name or gene_name.

        If organism_abbrev is provided, filter by organism.
        Returns all matching features.
        """
        from cgd.models.models import Organism

        query = self.db.query(Feature).filter(
            or_(
                func.upper(Feature.feature_name) == name.upper(),
                func.upper(Feature.gene_name) == name.upper(),
            )
        )

        if organism_abbrev:
            query = query.join(Organism, Feature.organism_no == Organism.organism_no).filter(
                func.upper(Organism.organism_abbrev) == organism_abbrev.upper()
            )

        features = query.all()
        logger.info(f"Found {len(features)} features for name={name}, organism={organism_abbrev}")
        return features

    def get_annotations_for_features(self, feature_nos: list[int]) -> list[dict]:
        """
        Get all phenotype annotations for multiple features.

        Returns structured data including phenotype details, experiment, and references.
        """
        if not feature_nos:
            return []

        logger.info(f"Querying phenotype annotations for feature_nos={feature_nos}")

        annotations = (
            self.db.query(PhenoAnnotation)
            .filter(PhenoAnnotation.feature_no.in_(feature_nos))
            .all()
        )

        logger.info(f"Found {len(annotations)} phenotype annotations for {len(feature_nos)} features")

        results = []
        for ann in annotations:
            phenotype = (
                self.db.query(Phenotype)
                .filter(Phenotype.phenotype_no == ann.phenotype_no)
                .first()
            )

            # Get the feature for this annotation
            feature = (
                self.db.query(Feature)
                .filter(Feature.feature_no == ann.feature_no)
                .first()
            )

            # Get experiment and properties
            experiment_data = None
            properties = []
            if ann.experiment_no:
                experiment = (
                    self.db.query(Experiment)
                    .filter(Experiment.experiment_no == ann.experiment_no)
                    .first()
                )
                if experiment:
                    experiment_data = {
                        "experiment_no": experiment.experiment_no,
                        "experiment_comment": experiment.experiment_comment,
                    }

                    # Get properties
                    prop_links = (
                        self.db.query(ExptExptprop)
                        .filter(ExptExptprop.experiment_no == ann.experiment_no)
                        .all()
                    )
                    for link in prop_links:
                        prop = (
                            self.db.query(ExptProperty)
                            .filter(
                                ExptProperty.expt_property_no == link.expt_property_no
                            )
                            .first()
                        )
                        if prop:
                            properties.append({
                                "property_type": prop.property_type,
                                "property_value": prop.property_value,
                                "property_description": prop.property_description,
                            })

            # Get references
            references = []
            ref_links = (
                self.db.query(RefLink)
                .filter(
                    RefLink.tab_name == "PHENO_ANNOTATION",
                    RefLink.col_name == "PHENO_ANNOTATION_NO",
                    RefLink.primary_key == ann.pheno_annotation_no,
                )
                .all()
            )
            for ref_link in ref_links:
                ref = (
                    self.db.query(Reference)
                    .options(joinedload(Reference.ref_url).joinedload(RefUrl.url))
                    .filter(Reference.reference_no == ref_link.reference_no)
                    .first()
                )
                if ref:
                    # Build urls list from ref_url relationship
                    urls = []
                    for ref_url in ref.ref_url:
                        if ref_url.url:
                            urls.append({
                                "url_type": ref_url.url.url_type,
                                "url": ref_url.url.url,
                            })
                    references.append({
                        "reference_no": ref.reference_no,
                        "dbxref_id": ref.dbxref_id,
                        "pubmed": ref.pubmed,
                        "citation": ref.citation,
                        "urls": urls,
                    })

            results.append({
                "pheno_annotation_no": ann.pheno_annotation_no,
                "feature_no": ann.feature_no,
                "feature_name": feature.feature_name if feature else None,
                "gene_name": feature.gene_name if feature else None,
                "phenotype_no": ann.phenotype_no,
                "experiment_type": phenotype.experiment_type if phenotype else None,
                "mutant_type": phenotype.mutant_type if phenotype else None,
                "observable": phenotype.observable if phenotype else None,
                "qualifier": phenotype.qualifier if phenotype else None,
                "experiment": experiment_data,
                "properties": properties,
                "references": references,
                "date_created": ann.date_created.isoformat()
                if ann.date_created else None,
                "created_by": ann.created_by,
            })

        return results

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
            PhenotypeCurationError: If reference invalid or unlinked
        """
        ref = (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )

        if not ref:
            raise PhenotypeCurationError(f"Reference {reference_no} not found")

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
                raise PhenotypeCurationError(
                    f"Reference {reference_no} is unlinked from feature {feature_no}"
                )

        return ref

    def get_or_create_phenotype(
        self,
        experiment_type: str,
        mutant_type: str,
        observable: str,
        qualifier: Optional[str],
        curator_userid: str,
    ) -> int:
        """
        Get existing phenotype_no or create new phenotype entry.

        Returns:
            phenotype_no
        """
        # Try to find existing phenotype
        query = self.db.query(Phenotype).filter(
            Phenotype.source == SOURCE,
            Phenotype.experiment_type == experiment_type,
            Phenotype.mutant_type == mutant_type,
            Phenotype.observable == observable,
        )

        if qualifier:
            query = query.filter(Phenotype.qualifier == qualifier)
        else:
            query = query.filter(Phenotype.qualifier.is_(None))

        existing = query.first()
        if existing:
            return existing.phenotype_no

        # Create new phenotype
        phenotype = Phenotype(
            source=SOURCE,
            experiment_type=experiment_type,
            mutant_type=mutant_type,
            observable=observable,
            qualifier=qualifier,
            created_by=curator_userid[:12],
        )
        self.db.add(phenotype)
        self.db.flush()

        logger.info(f"Created phenotype {phenotype.phenotype_no}: {observable}")

        return phenotype.phenotype_no

    def get_or_create_experiment(
        self,
        experiment_comment: Optional[str],
        curator_userid: str,
    ) -> int:
        """
        Create experiment entry.

        An experiment is needed to link properties, even without a comment.

        Returns:
            experiment_no
        """
        experiment = Experiment(
            source=SOURCE,
            experiment_comment=experiment_comment,
            created_by=curator_userid[:12],
        )
        self.db.add(experiment)
        self.db.flush()

        logger.info(f"Created experiment {experiment.experiment_no}")

        return experiment.experiment_no

    def get_or_create_expt_property(
        self,
        property_type: str,
        property_value: str,
        property_description: Optional[str],
        curator_userid: str,
    ) -> int:
        """
        Get existing expt_property_no or create new entry.

        Returns:
            expt_property_no
        """
        # Try to find existing property
        query = self.db.query(ExptProperty).filter(
            ExptProperty.property_type == property_type,
            ExptProperty.property_value == property_value,
        )

        if property_description:
            query = query.filter(
                ExptProperty.property_description == property_description
            )
        else:
            query = query.filter(ExptProperty.property_description.is_(None))

        existing = query.first()
        if existing:
            return existing.expt_property_no

        # Create new property
        prop = ExptProperty(
            property_type=property_type,
            property_value=property_value,
            property_description=property_description,
            created_by=curator_userid[:12],
        )
        self.db.add(prop)
        self.db.flush()

        logger.info(f"Created expt_property {prop.expt_property_no}: {property_type}={property_value}")

        return prop.expt_property_no

    def link_experiment_to_property(
        self, experiment_no: int, expt_property_no: int
    ) -> None:
        """Link experiment to property via expt_exptprop table."""
        # Check if link exists
        existing = (
            self.db.query(ExptExptprop)
            .filter(
                ExptExptprop.experiment_no == experiment_no,
                ExptExptprop.expt_property_no == expt_property_no,
            )
            .first()
        )

        if existing:
            return

        link = ExptExptprop(
            experiment_no=experiment_no,
            expt_property_no=expt_property_no,
        )
        self.db.add(link)
        self.db.flush()

    def get_annotations_for_feature(self, feature_no: int) -> list[dict]:
        """
        Get all phenotype annotations for a feature.

        Returns structured data including phenotype details, experiment, and references.
        """
        logger.info(f"Querying phenotype annotations for feature_no={feature_no}")

        annotations = (
            self.db.query(PhenoAnnotation)
            .filter(PhenoAnnotation.feature_no == feature_no)
            .all()
        )

        logger.info(f"Found {len(annotations)} phenotype annotations for feature_no={feature_no}")

        results = []
        for ann in annotations:
            phenotype = (
                self.db.query(Phenotype)
                .filter(Phenotype.phenotype_no == ann.phenotype_no)
                .first()
            )

            # Get experiment and properties
            experiment_data = None
            properties = []
            if ann.experiment_no:
                experiment = (
                    self.db.query(Experiment)
                    .filter(Experiment.experiment_no == ann.experiment_no)
                    .first()
                )
                if experiment:
                    experiment_data = {
                        "experiment_no": experiment.experiment_no,
                        "experiment_comment": experiment.experiment_comment,
                    }

                    # Get properties
                    prop_links = (
                        self.db.query(ExptExptprop)
                        .filter(ExptExptprop.experiment_no == ann.experiment_no)
                        .all()
                    )
                    for link in prop_links:
                        prop = (
                            self.db.query(ExptProperty)
                            .filter(
                                ExptProperty.expt_property_no == link.expt_property_no
                            )
                            .first()
                        )
                        if prop:
                            properties.append({
                                "property_type": prop.property_type,
                                "property_value": prop.property_value,
                                "property_description": prop.property_description,
                            })

            # Get references
            references = []
            ref_links = (
                self.db.query(RefLink)
                .filter(
                    RefLink.tab_name == "PHENO_ANNOTATION",
                    RefLink.col_name == "PHENO_ANNOTATION_NO",
                    RefLink.primary_key == ann.pheno_annotation_no,
                )
                .all()
            )
            for ref_link in ref_links:
                ref = (
                    self.db.query(Reference)
                    .options(joinedload(Reference.ref_url).joinedload(RefUrl.url))
                    .filter(Reference.reference_no == ref_link.reference_no)
                    .first()
                )
                if ref:
                    # Build urls list from ref_url relationship
                    urls = []
                    for ref_url in ref.ref_url:
                        if ref_url.url:
                            urls.append({
                                "url_type": ref_url.url.url_type,
                                "url": ref_url.url.url,
                            })
                    references.append({
                        "reference_no": ref.reference_no,
                        "dbxref_id": ref.dbxref_id,
                        "pubmed": ref.pubmed,
                        "citation": ref.citation,
                        "urls": urls,
                    })

            results.append({
                "pheno_annotation_no": ann.pheno_annotation_no,
                "feature_no": ann.feature_no,
                "phenotype_no": ann.phenotype_no,
                "experiment_type": phenotype.experiment_type if phenotype else None,
                "mutant_type": phenotype.mutant_type if phenotype else None,
                "observable": phenotype.observable if phenotype else None,
                "qualifier": phenotype.qualifier if phenotype else None,
                "experiment": experiment_data,
                "properties": properties,
                "references": references,
                "date_created": ann.date_created.isoformat()
                if ann.date_created else None,
                "created_by": ann.created_by,
            })

        return results

    def create_annotation(
        self,
        feature_no: int,
        experiment_type: str,
        mutant_type: str,
        observable: str,
        qualifier: Optional[str],
        reference_no: int,
        curator_userid: str,
        experiment_comment: Optional[str] = None,
        properties: Optional[list[dict]] = None,
    ) -> int:
        """
        Create a new phenotype annotation.

        Args:
            feature_no: Feature number
            experiment_type: Experiment type (CV term)
            mutant_type: Mutant type (CV term)
            observable: Observable (CV term)
            qualifier: Qualifier (CV term, optional)
            reference_no: Reference number
            curator_userid: Curator's userid
            experiment_comment: Optional experiment comment
            properties: Optional list of {property_type, property_value, property_description}

        Returns:
            New pheno_annotation_no

        Raises:
            PhenotypeCurationError: If validation fails
        """
        # Validate reference
        self.validate_reference(reference_no, feature_no)

        # Get or create phenotype
        phenotype_no = self.get_or_create_phenotype(
            experiment_type, mutant_type, observable, qualifier, curator_userid
        )

        # Get or create experiment if we have comment or properties
        experiment_no = None
        if experiment_comment or properties:
            experiment_no = self.get_or_create_experiment(
                experiment_comment, curator_userid
            )

            # Link properties to experiment
            if experiment_no and properties:
                for prop in properties:
                    prop_no = self.get_or_create_expt_property(
                        prop["property_type"],
                        prop["property_value"],
                        prop.get("property_description"),
                        curator_userid,
                    )
                    self.link_experiment_to_property(experiment_no, prop_no)

        # Check for existing annotation
        query = self.db.query(PhenoAnnotation).filter(
            PhenoAnnotation.feature_no == feature_no,
            PhenoAnnotation.phenotype_no == phenotype_no,
        )
        if experiment_no:
            query = query.filter(PhenoAnnotation.experiment_no == experiment_no)
        else:
            query = query.filter(PhenoAnnotation.experiment_no.is_(None))

        existing = query.first()
        if existing:
            # Just add reference link if not already present
            self._add_reference_link(
                existing.pheno_annotation_no, reference_no, curator_userid
            )
            self.db.commit()
            return existing.pheno_annotation_no

        # Create new annotation
        annotation = PhenoAnnotation(
            feature_no=feature_no,
            phenotype_no=phenotype_no,
            experiment_no=experiment_no,
            created_by=curator_userid[:12],
        )
        self.db.add(annotation)
        self.db.flush()

        # Add reference link
        self._add_reference_link(
            annotation.pheno_annotation_no, reference_no, curator_userid
        )

        self.db.commit()

        logger.info(
            f"Created phenotype annotation {annotation.pheno_annotation_no} "
            f"for feature {feature_no}"
        )

        return annotation.pheno_annotation_no

    def _add_reference_link(
        self, pheno_annotation_no: int, reference_no: int, curator_userid: str
    ) -> None:
        """Add reference link to pheno_annotation."""
        # Check if link exists
        existing = (
            self.db.query(RefLink)
            .filter(
                RefLink.reference_no == reference_no,
                RefLink.tab_name == "PHENO_ANNOTATION",
                RefLink.col_name == "PHENO_ANNOTATION_NO",
                RefLink.primary_key == pheno_annotation_no,
            )
            .first()
        )

        if existing:
            return

        ref_link = RefLink(
            reference_no=reference_no,
            tab_name="PHENO_ANNOTATION",
            col_name="PHENO_ANNOTATION_NO",
            primary_key=pheno_annotation_no,
            created_by=curator_userid[:12],
        )
        self.db.add(ref_link)
        self.db.flush()

    def delete_annotation(
        self, pheno_annotation_no: int, curator_userid: str
    ) -> bool:
        """
        Delete a phenotype annotation.

        Removes the annotation and its reference links.
        Experiment records are kept as they may be shared.
        """
        annotation = (
            self.db.query(PhenoAnnotation)
            .filter(PhenoAnnotation.pheno_annotation_no == pheno_annotation_no)
            .first()
        )

        if not annotation:
            raise PhenotypeCurationError(
                f"Phenotype annotation {pheno_annotation_no} not found"
            )

        # Log before delete
        logger.info(
            f"Deleting phenotype annotation {pheno_annotation_no} "
            f"(feature {annotation.feature_no}) by {curator_userid}"
        )

        # Delete reference links
        self.db.query(RefLink).filter(
            RefLink.tab_name == "PHENO_ANNOTATION",
            RefLink.col_name == "PHENO_ANNOTATION_NO",
            RefLink.primary_key == pheno_annotation_no,
        ).delete()

        # Delete annotation
        self.db.delete(annotation)
        self.db.commit()

        return True

    def get_cv_terms(self, cv_name: str) -> list[str]:
        """
        Get CV terms for a given CV name.

        Used for experiment_type, mutant_type, observable, qualifier dropdowns.
        Queries the Cv/CvTerm tables since Oracle triggers validate against cv_term.
        """
        cv_lower = cv_name.lower()

        # Query Cv/CvTerm tables - Oracle triggers validate against cv_term
        cv = (
            self.db.query(Cv)
            .filter(func.lower(Cv.cv_name) == cv_lower)
            .first()
        )
        if cv:
            result = (
                self.db.query(CvTerm.term_name)
                .filter(CvTerm.cv_no == cv.cv_no)
                .order_by(CvTerm.term_name)
                .all()
            )
            return [r[0] for r in result]

        return []

    def get_property_types(self) -> list[str]:
        """Get valid property types from database."""
        # Query unique property types
        result = (
            self.db.query(ExptProperty.property_type)
            .distinct()
            .order_by(ExptProperty.property_type)
            .all()
        )
        return [r[0] for r in result]

    def get_cv_term_tree(self, cv_name: str) -> list[dict]:
        """
        Get hierarchical CV terms for a given CV name.

        Returns tree structure with parent-child relationships.
        Used for experiment_type, mutant_type, qualifier, observable trees.
        """
        from collections import defaultdict
        from cgd.models.models import CvtermRelationship

        cv_lower = cv_name.lower()

        # Query Cv to get cv_no
        cv = (
            self.db.query(Cv)
            .filter(func.lower(Cv.cv_name) == cv_lower)
            .first()
        )
        if not cv:
            return []

        # Get all terms for this CV
        cv_terms = (
            self.db.query(CvTerm)
            .filter(CvTerm.cv_no == cv.cv_no)
            .all()
        )

        if not cv_terms:
            return []

        # Build maps
        term_map = {t.cv_term_no: t for t in cv_terms}
        term_no_map = {t.cv_term_no: t.term_name for t in cv_terms}

        # Get all relationships for these terms
        relationships = (
            self.db.query(CvtermRelationship)
            .filter(
                or_(
                    CvtermRelationship.parent_cv_term_no.in_(list(term_map.keys())),
                    CvtermRelationship.child_cv_term_no.in_(list(term_map.keys())),
                )
            )
            .all()
        )

        # Build parent -> children map
        children_map: dict[int, list[int]] = defaultdict(list)
        has_parent: set[int] = set()

        for rel in relationships:
            if rel.parent_cv_term_no in term_map and rel.child_cv_term_no in term_map:
                children_map[rel.parent_cv_term_no].append(rel.child_cv_term_no)
                has_parent.add(rel.child_cv_term_no)

        # Find root terms (terms with no parent)
        root_term_nos = [t.cv_term_no for t in cv_terms if t.cv_term_no not in has_parent]

        # Recursively build tree
        def build_tree_node(term_no: int, depth: int = 0) -> dict:
            term = term_map[term_no]
            children = []
            for child_no in sorted(children_map.get(term_no, []),
                                   key=lambda x: term_map[x].term_name.lower()):
                children.append(build_tree_node(child_no, depth + 1))

            return {
                "term": term.term_name,
                "depth": depth,
                "children": children,
            }

        # Build tree from roots
        tree = []
        for root_no in sorted(root_term_nos, key=lambda x: term_map[x].term_name.lower()):
            tree.append(build_tree_node(root_no, 0))

        # If no tree structure (flat list), return all terms as roots
        if not tree and cv_terms:
            tree = [
                {"term": t.term_name, "depth": 0, "children": []}
                for t in sorted(cv_terms, key=lambda x: x.term_name.lower())
            ]

        return tree
