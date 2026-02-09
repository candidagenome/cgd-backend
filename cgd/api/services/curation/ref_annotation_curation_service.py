"""
Reference Annotation Curation Service - Business logic for managing annotations
linked to a reference.

Mirrors functionality from legacy UpdateReferenceAnnotation.pm:
- Literature Guide management (REF_PROPERTY, REFPROP_FEAT)
- GO annotation management (GO_REF, GO_ANNOTATION)
- REF_LINK management (FEATURE, PHENO_ANNOTATION, FEAT_ALIAS, etc.)
"""

import logging
from typing import Optional

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Alias,
    FeatAlias,
    Feature,
    Go,
    GoAnnotation,
    GoQualifier,
    GoRef,
    GorefDbxref,
    PhenoAnnotation,
    Phenotype,
    RefLink,
    RefProperty,
    Reference,
    RefpropFeat,
)

logger = logging.getLogger(__name__)


class RefAnnotationCurationError(Exception):
    """Raised when reference annotation curation fails."""

    pass


class RefAnnotationCurationService:
    """Service for managing annotations linked to a reference."""

    def __init__(self, db: Session):
        self.db = db

    def get_reference_by_no(self, reference_no: int) -> Optional[Reference]:
        """Look up reference by reference_no."""
        return (
            self.db.query(Reference)
            .filter(Reference.reference_no == reference_no)
            .first()
        )

    def get_reference_annotations(self, reference_no: int) -> dict:
        """
        Get all annotations associated with a reference.

        Returns dict with:
        - lit_guide: Literature guide entries (topics and features)
        - go_annotations: GO annotation entries
        - ref_links: REF_LINK entries by table type
        """
        reference = self.get_reference_by_no(reference_no)
        if not reference:
            raise RefAnnotationCurationError(f"Reference {reference_no} not found")

        return {
            "reference": {
                "reference_no": reference.reference_no,
                "pubmed": reference.pubmed,
                "dbxref_id": reference.dbxref_id,
                "citation": reference.citation,
                "title": reference.title,
            },
            "lit_guide": self._get_literature_guide_entries(reference_no),
            "go_annotations": self._get_go_annotation_entries(reference_no),
            "ref_links": self._get_ref_link_entries(reference_no),
        }

    def _get_literature_guide_entries(self, reference_no: int) -> list[dict]:
        """Get literature guide entries for a reference."""
        # Get all ref_property entries with topic type
        properties = (
            self.db.query(RefProperty)
            .filter(
                RefProperty.reference_no == reference_no,
                RefProperty.property_type.in_(["Topic", "Curation status"]),
            )
            .all()
        )

        entries = []
        for prop in properties:
            # Get linked features
            feat_links = (
                self.db.query(RefpropFeat, Feature)
                .join(Feature, RefpropFeat.feature_no == Feature.feature_no)
                .filter(RefpropFeat.ref_property_no == prop.ref_property_no)
                .all()
            )

            if feat_links:
                for link, feature in feat_links:
                    entries.append({
                        "type": "feature",
                        "ref_property_no": prop.ref_property_no,
                        "refprop_feat_no": link.refprop_feat_no,
                        "property_type": prop.property_type,
                        "property_value": prop.property_value,
                        "feature_no": feature.feature_no,
                        "feature_name": feature.feature_name,
                        "gene_name": feature.gene_name,
                        "date_created": link.date_created.isoformat() if link.date_created else None,
                        "created_by": link.created_by,
                    })
            else:
                # Non-gene topic (no features linked)
                entries.append({
                    "type": "non_gene",
                    "ref_property_no": prop.ref_property_no,
                    "refprop_feat_no": None,
                    "property_type": prop.property_type,
                    "property_value": prop.property_value,
                    "feature_no": None,
                    "feature_name": None,
                    "gene_name": None,
                    "date_created": prop.date_created.isoformat() if prop.date_created else None,
                    "created_by": prop.created_by,
                    "date_last_reviewed": prop.date_last_reviewed.isoformat() if prop.date_last_reviewed else None,
                })

        return entries

    def _get_go_annotation_entries(self, reference_no: int) -> list[dict]:
        """Get GO annotation entries for a reference."""
        # Join go_ref -> go_annotation -> feature and go
        results = (
            self.db.query(GoRef, GoAnnotation, Feature, Go)
            .join(GoAnnotation, GoRef.go_annotation_no == GoAnnotation.go_annotation_no)
            .join(Feature, GoAnnotation.feature_no == Feature.feature_no)
            .join(Go, GoAnnotation.go_no == Go.go_no)
            .filter(GoRef.reference_no == reference_no)
            .all()
        )

        entries = []
        for go_ref, go_ann, feature, go in results:
            # Get qualifier if exists
            qualifier = None
            if go_ref.has_qualifier == "Y":
                qual = (
                    self.db.query(GoQualifier)
                    .filter(GoQualifier.go_ref_no == go_ref.go_ref_no)
                    .first()
                )
                if qual:
                    qualifier = qual.qualifier

            # Get supporting evidence if exists
            support = None
            if go_ref.has_supporting_evidence == "Y":
                support_entries = (
                    self.db.query(GorefDbxref)
                    .filter(GorefDbxref.go_ref_no == go_ref.go_ref_no)
                    .all()
                )
                if support_entries:
                    support = "|".join(s.support_type for s in support_entries)

            entries.append({
                "go_ref_no": go_ref.go_ref_no,
                "go_annotation_no": go_ann.go_annotation_no,
                "feature_no": feature.feature_no,
                "feature_name": feature.feature_name,
                "gene_name": feature.gene_name,
                "go_no": go.go_no,
                "goid": go.goid,
                "go_term": go.go_term,
                "go_aspect": go.go_aspect,
                "go_evidence": go_ann.go_evidence,
                "qualifier": qualifier,
                "support": support,
                "has_qualifier": go_ref.has_qualifier,
                "has_supporting_evidence": go_ref.has_supporting_evidence,
                "date_created": go_ref.date_created.isoformat() if go_ref.date_created else None,
                "created_by": go_ref.created_by,
            })

        return entries

    def _get_ref_link_entries(self, reference_no: int) -> dict:
        """Get REF_LINK entries grouped by table type."""
        ref_links = (
            self.db.query(RefLink)
            .filter(RefLink.reference_no == reference_no)
            .all()
        )

        result = {
            "feature": [],
            "pheno_annotation": [],
            "feat_alias": [],
            "other": [],
        }

        for link in ref_links:
            tab_name = link.tab_name.upper()

            base_entry = {
                "ref_link_no": link.ref_link_no,
                "tab_name": link.tab_name,
                "primary_key": link.primary_key,
                "col_name": link.col_name,
                "date_created": link.date_created.isoformat() if link.date_created else None,
                "created_by": link.created_by,
            }

            if tab_name == "FEATURE":
                feature = (
                    self.db.query(Feature)
                    .filter(Feature.feature_no == link.primary_key)
                    .first()
                )
                if feature:
                    base_entry.update({
                        "feature_no": feature.feature_no,
                        "feature_name": feature.feature_name,
                        "gene_name": feature.gene_name,
                        "feature_type": feature.feature_type,
                        "headline": feature.headline,
                    })
                result["feature"].append(base_entry)

            elif tab_name == "PHENO_ANNOTATION":
                pheno_ann = (
                    self.db.query(PhenoAnnotation, Feature, Phenotype)
                    .join(Feature, PhenoAnnotation.feature_no == Feature.feature_no)
                    .join(Phenotype, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
                    .filter(PhenoAnnotation.pheno_annotation_no == link.primary_key)
                    .first()
                )
                if pheno_ann:
                    pa, feature, phenotype = pheno_ann
                    base_entry.update({
                        "pheno_annotation_no": pa.pheno_annotation_no,
                        "feature_no": feature.feature_no,
                        "feature_name": feature.feature_name,
                        "gene_name": feature.gene_name,
                        "observable": phenotype.observable,
                        "qualifier": phenotype.qualifier,
                    })
                result["pheno_annotation"].append(base_entry)

            elif tab_name == "FEAT_ALIAS":
                alias_data = (
                    self.db.query(FeatAlias, Feature, Alias)
                    .join(Feature, FeatAlias.feature_no == Feature.feature_no)
                    .join(Alias, FeatAlias.alias_no == Alias.alias_no)
                    .filter(FeatAlias.feat_alias_no == link.primary_key)
                    .first()
                )
                if alias_data:
                    feat_alias, feature, alias = alias_data
                    base_entry.update({
                        "feat_alias_no": feat_alias.feat_alias_no,
                        "feature_no": feature.feature_no,
                        "feature_name": feature.feature_name,
                        "gene_name": feature.gene_name,
                        "alias_name": alias.alias_name,
                    })
                result["feat_alias"].append(base_entry)

            else:
                result["other"].append(base_entry)

        return result

    def delete_lit_guide_entry(
        self,
        refprop_feat_no: Optional[int],
        ref_property_no: int,
        curator_userid: str,
    ) -> dict:
        """
        Delete a literature guide entry.

        If refprop_feat_no is provided, deletes the feature link.
        If no more features are linked to ref_property, deletes ref_property too.
        If refprop_feat_no is None, deletes the ref_property directly (non-gene topic).
        """
        messages = []

        if refprop_feat_no:
            # Delete refprop_feat entry
            refprop_feat = (
                self.db.query(RefpropFeat)
                .filter(RefpropFeat.refprop_feat_no == refprop_feat_no)
                .first()
            )

            if not refprop_feat:
                raise RefAnnotationCurationError(
                    f"RefpropFeat {refprop_feat_no} not found"
                )

            ref_property_no = refprop_feat.ref_property_no
            self.db.delete(refprop_feat)
            messages.append(f"Deleted refprop_feat {refprop_feat_no}")

        # Check if ref_property still has features
        ref_property = (
            self.db.query(RefProperty)
            .filter(RefProperty.ref_property_no == ref_property_no)
            .first()
        )

        if ref_property:
            remaining = (
                self.db.query(func.count(RefpropFeat.refprop_feat_no))
                .filter(RefpropFeat.ref_property_no == ref_property_no)
                .scalar()
            )

            if remaining == 0:
                self.db.delete(ref_property)
                messages.append(f"Deleted ref_property {ref_property_no}")

        self.db.commit()

        logger.info(
            f"Deleted literature guide entry by {curator_userid}: {messages}"
        )

        return {"success": True, "messages": messages}

    def transfer_lit_guide_entry(
        self,
        refprop_feat_no: Optional[int],
        ref_property_no: int,
        new_reference_no: int,
        curator_userid: str,
    ) -> dict:
        """
        Transfer a literature guide entry to another reference.

        Creates new ref_property and refprop_feat for new reference,
        then deletes the old entries.
        """
        messages = []

        # Verify new reference exists
        new_ref = self.get_reference_by_no(new_reference_no)
        if not new_ref:
            raise RefAnnotationCurationError(
                f"Target reference {new_reference_no} not found"
            )

        # Get original ref_property
        old_ref_prop = (
            self.db.query(RefProperty)
            .filter(RefProperty.ref_property_no == ref_property_no)
            .first()
        )

        if not old_ref_prop:
            raise RefAnnotationCurationError(
                f"RefProperty {ref_property_no} not found"
            )

        if refprop_feat_no:
            # Transfer feature link
            old_refprop_feat = (
                self.db.query(RefpropFeat)
                .filter(RefpropFeat.refprop_feat_no == refprop_feat_no)
                .first()
            )

            if not old_refprop_feat:
                raise RefAnnotationCurationError(
                    f"RefpropFeat {refprop_feat_no} not found"
                )

            feature_no = old_refprop_feat.feature_no

            # Check if new ref already has this topic
            new_ref_prop = (
                self.db.query(RefProperty)
                .filter(
                    RefProperty.reference_no == new_reference_no,
                    RefProperty.property_type == old_ref_prop.property_type,
                    RefProperty.property_value == old_ref_prop.property_value,
                )
                .first()
            )

            if not new_ref_prop:
                # Create new ref_property
                new_ref_prop = RefProperty(
                    reference_no=new_reference_no,
                    source=old_ref_prop.source,
                    property_type=old_ref_prop.property_type,
                    property_value=old_ref_prop.property_value,
                    created_by=curator_userid[:12],
                )
                self.db.add(new_ref_prop)
                self.db.flush()
                messages.append(
                    f"Created ref_property for reference {new_reference_no}"
                )

            # Check if feature already linked to new ref_property
            existing_link = (
                self.db.query(RefpropFeat)
                .filter(
                    RefpropFeat.ref_property_no == new_ref_prop.ref_property_no,
                    RefpropFeat.feature_no == feature_no,
                )
                .first()
            )

            if not existing_link:
                # Create new refprop_feat
                new_refprop_feat = RefpropFeat(
                    ref_property_no=new_ref_prop.ref_property_no,
                    feature_no=feature_no,
                    created_by=curator_userid[:12],
                )
                self.db.add(new_refprop_feat)
                messages.append(
                    f"Linked feature {feature_no} to reference {new_reference_no}"
                )

            # Delete old refprop_feat
            self.db.delete(old_refprop_feat)
            messages.append(f"Deleted old refprop_feat {refprop_feat_no}")

            # Check if old ref_property has remaining features
            remaining = (
                self.db.query(func.count(RefpropFeat.refprop_feat_no))
                .filter(RefpropFeat.ref_property_no == ref_property_no)
                .scalar()
            )

            if remaining == 0:
                self.db.delete(old_ref_prop)
                messages.append(f"Deleted old ref_property {ref_property_no}")

        else:
            # Transfer non-gene topic (just ref_property)
            # Check if new ref already has this topic
            existing = (
                self.db.query(RefProperty)
                .filter(
                    RefProperty.reference_no == new_reference_no,
                    RefProperty.property_type == old_ref_prop.property_type,
                    RefProperty.property_value == old_ref_prop.property_value,
                )
                .first()
            )

            if not existing:
                # Create new ref_property
                new_ref_prop = RefProperty(
                    reference_no=new_reference_no,
                    source=old_ref_prop.source,
                    property_type=old_ref_prop.property_type,
                    property_value=old_ref_prop.property_value,
                    created_by=curator_userid[:12],
                )
                self.db.add(new_ref_prop)
                messages.append(
                    f"Created ref_property for reference {new_reference_no}"
                )

            # Delete old ref_property (and its children)
            children = (
                self.db.query(RefpropFeat)
                .filter(RefpropFeat.ref_property_no == ref_property_no)
                .all()
            )
            for child in children:
                self.db.delete(child)

            self.db.delete(old_ref_prop)
            messages.append(f"Deleted old ref_property {ref_property_no}")

        self.db.commit()

        logger.info(
            f"Transferred literature guide entry to ref {new_reference_no} "
            f"by {curator_userid}: {messages}"
        )

        return {"success": True, "messages": messages}

    def delete_go_ref_entry(
        self,
        go_ref_no: int,
        curator_userid: str,
    ) -> dict:
        """
        Delete a GO annotation entry.

        Deletes go_ref. If no other go_ref entries reference the go_annotation,
        deletes the go_annotation too.
        """
        messages = []

        go_ref = (
            self.db.query(GoRef)
            .filter(GoRef.go_ref_no == go_ref_no)
            .first()
        )

        if not go_ref:
            raise RefAnnotationCurationError(f"GoRef {go_ref_no} not found")

        go_annotation_no = go_ref.go_annotation_no

        # Delete go_qualifier if exists
        self.db.query(GoQualifier).filter(
            GoQualifier.go_ref_no == go_ref_no
        ).delete()

        # Delete goref_dbxref if exists
        self.db.query(GorefDbxref).filter(
            GorefDbxref.go_ref_no == go_ref_no
        ).delete()

        # Delete go_ref
        self.db.delete(go_ref)
        messages.append(f"Deleted go_ref {go_ref_no}")

        # Check if go_annotation is still in use
        remaining = (
            self.db.query(func.count(GoRef.go_ref_no))
            .filter(GoRef.go_annotation_no == go_annotation_no)
            .scalar()
        )

        if remaining == 0:
            go_annotation = (
                self.db.query(GoAnnotation)
                .filter(GoAnnotation.go_annotation_no == go_annotation_no)
                .first()
            )
            if go_annotation:
                self.db.delete(go_annotation)
                messages.append(f"Deleted go_annotation {go_annotation_no}")

        self.db.commit()

        logger.info(
            f"Deleted GO annotation entry by {curator_userid}: {messages}"
        )

        return {"success": True, "messages": messages}

    def transfer_go_ref_entry(
        self,
        go_ref_no: int,
        new_reference_no: int,
        curator_userid: str,
    ) -> dict:
        """
        Transfer a GO annotation to another reference.

        Creates new go_ref for new reference (including qualifier/support),
        then deletes the old go_ref.
        """
        messages = []

        # Verify new reference exists
        new_ref = self.get_reference_by_no(new_reference_no)
        if not new_ref:
            raise RefAnnotationCurationError(
                f"Target reference {new_reference_no} not found"
            )

        old_go_ref = (
            self.db.query(GoRef)
            .filter(GoRef.go_ref_no == go_ref_no)
            .first()
        )

        if not old_go_ref:
            raise RefAnnotationCurationError(f"GoRef {go_ref_no} not found")

        go_annotation_no = old_go_ref.go_annotation_no

        # Check if new reference already has this go_annotation
        existing = (
            self.db.query(GoRef)
            .filter(
                GoRef.reference_no == new_reference_no,
                GoRef.go_annotation_no == go_annotation_no,
            )
            .first()
        )

        if existing:
            new_go_ref_no = existing.go_ref_no
            messages.append(
                f"Go annotation already linked to reference {new_reference_no}"
            )
        else:
            # Create new go_ref
            new_go_ref = GoRef(
                reference_no=new_reference_no,
                go_annotation_no=go_annotation_no,
                has_qualifier=old_go_ref.has_qualifier,
                has_supporting_evidence=old_go_ref.has_supporting_evidence,
                created_by=curator_userid[:12],
            )
            self.db.add(new_go_ref)
            self.db.flush()
            new_go_ref_no = new_go_ref.go_ref_no
            messages.append(
                f"Created go_ref for reference {new_reference_no}"
            )

        # Transfer qualifier if exists
        if old_go_ref.has_qualifier == "Y":
            old_qualifiers = (
                self.db.query(GoQualifier)
                .filter(GoQualifier.go_ref_no == go_ref_no)
                .all()
            )
            for qual in old_qualifiers:
                # Check if already exists
                existing_qual = (
                    self.db.query(GoQualifier)
                    .filter(
                        GoQualifier.go_ref_no == new_go_ref_no,
                        GoQualifier.qualifier == qual.qualifier,
                    )
                    .first()
                )
                if not existing_qual:
                    new_qual = GoQualifier(
                        go_ref_no=new_go_ref_no,
                        qualifier=qual.qualifier,
                    )
                    self.db.add(new_qual)
            messages.append("Transferred go_qualifier entries")

        # Transfer supporting evidence if exists
        if old_go_ref.has_supporting_evidence == "Y":
            old_supports = (
                self.db.query(GorefDbxref)
                .filter(GorefDbxref.go_ref_no == go_ref_no)
                .all()
            )
            for sup in old_supports:
                existing_sup = (
                    self.db.query(GorefDbxref)
                    .filter(
                        GorefDbxref.go_ref_no == new_go_ref_no,
                        GorefDbxref.dbxref_no == sup.dbxref_no,
                        GorefDbxref.support_type == sup.support_type,
                    )
                    .first()
                )
                if not existing_sup:
                    new_sup = GorefDbxref(
                        go_ref_no=new_go_ref_no,
                        dbxref_no=sup.dbxref_no,
                        support_type=sup.support_type,
                    )
                    self.db.add(new_sup)
            messages.append("Transferred goref_dbxref entries")

        # Delete old entries
        self.db.query(GoQualifier).filter(
            GoQualifier.go_ref_no == go_ref_no
        ).delete()
        self.db.query(GorefDbxref).filter(
            GorefDbxref.go_ref_no == go_ref_no
        ).delete()
        self.db.delete(old_go_ref)
        messages.append(f"Deleted old go_ref {go_ref_no}")

        self.db.commit()

        logger.info(
            f"Transferred GO annotation to ref {new_reference_no} "
            f"by {curator_userid}: {messages}"
        )

        return {"success": True, "messages": messages}

    def delete_ref_link_entry(
        self,
        ref_link_no: int,
        curator_userid: str,
    ) -> dict:
        """
        Delete a REF_LINK entry.

        Note: This only removes the reference association, not the underlying data.
        """
        ref_link = (
            self.db.query(RefLink)
            .filter(RefLink.ref_link_no == ref_link_no)
            .first()
        )

        if not ref_link:
            raise RefAnnotationCurationError(f"RefLink {ref_link_no} not found")

        tab_name = ref_link.tab_name
        primary_key = ref_link.primary_key
        col_name = ref_link.col_name

        self.db.delete(ref_link)
        self.db.commit()

        # Check if data still has any reference association
        remaining = (
            self.db.query(func.count(RefLink.ref_link_no))
            .filter(
                RefLink.tab_name == tab_name,
                RefLink.primary_key == primary_key,
                RefLink.col_name == col_name,
            )
            .scalar()
        )

        warning = None
        if remaining == 0:
            warning = (
                f"Warning: {tab_name}.{col_name} with primary_key={primary_key} "
                "is no longer associated with any reference"
            )

        logger.info(
            f"Deleted ref_link {ref_link_no} ({tab_name}) by {curator_userid}"
        )

        return {
            "success": True,
            "message": f"Deleted ref_link {ref_link_no}",
            "warning": warning,
        }

    def transfer_ref_link_entry(
        self,
        ref_link_no: int,
        new_reference_no: int,
        curator_userid: str,
    ) -> dict:
        """
        Transfer a REF_LINK entry to another reference.

        Updates the reference_no on the existing ref_link entry.
        """
        # Verify new reference exists
        new_ref = self.get_reference_by_no(new_reference_no)
        if not new_ref:
            raise RefAnnotationCurationError(
                f"Target reference {new_reference_no} not found"
            )

        ref_link = (
            self.db.query(RefLink)
            .filter(RefLink.ref_link_no == ref_link_no)
            .first()
        )

        if not ref_link:
            raise RefAnnotationCurationError(f"RefLink {ref_link_no} not found")

        old_ref_no = ref_link.reference_no

        # Check if the same link already exists for new reference
        existing = (
            self.db.query(RefLink)
            .filter(
                RefLink.reference_no == new_reference_no,
                RefLink.tab_name == ref_link.tab_name,
                RefLink.primary_key == ref_link.primary_key,
                RefLink.col_name == ref_link.col_name,
            )
            .first()
        )

        if existing:
            # Just delete the old one since new ref already has link
            self.db.delete(ref_link)
            message = (
                f"Ref_link already exists for reference {new_reference_no}, "
                f"deleted old ref_link {ref_link_no}"
            )
        else:
            # Update reference_no
            ref_link.reference_no = new_reference_no
            message = f"Transferred ref_link {ref_link_no} to reference {new_reference_no}"

        self.db.commit()

        logger.info(
            f"Transferred ref_link {ref_link_no} from ref {old_ref_no} "
            f"to ref {new_reference_no} by {curator_userid}"
        )

        return {"success": True, "message": message}

    def bulk_delete(
        self,
        reference_no: int,
        entry_type: str,
        curator_userid: str,
    ) -> dict:
        """
        Bulk delete all entries of a given type for a reference.

        entry_type: 'lit_guide', 'go_annotation', or 'ref_link'
        """
        messages = []
        count = 0

        if entry_type == "lit_guide":
            # Get all ref_property entries
            properties = (
                self.db.query(RefProperty)
                .filter(
                    RefProperty.reference_no == reference_no,
                    RefProperty.property_type.in_(["Topic", "Curation status"]),
                )
                .all()
            )

            for prop in properties:
                # Delete children first
                children = (
                    self.db.query(RefpropFeat)
                    .filter(RefpropFeat.ref_property_no == prop.ref_property_no)
                    .all()
                )
                for child in children:
                    self.db.delete(child)
                    count += 1
                self.db.delete(prop)

            messages.append(f"Deleted {count} literature guide entries")

        elif entry_type == "go_annotation":
            go_refs = (
                self.db.query(GoRef)
                .filter(GoRef.reference_no == reference_no)
                .all()
            )

            for go_ref in go_refs:
                go_annotation_no = go_ref.go_annotation_no

                # Delete qualifiers and support
                self.db.query(GoQualifier).filter(
                    GoQualifier.go_ref_no == go_ref.go_ref_no
                ).delete()
                self.db.query(GorefDbxref).filter(
                    GorefDbxref.go_ref_no == go_ref.go_ref_no
                ).delete()

                self.db.delete(go_ref)
                count += 1

                # Check if go_annotation still in use
                remaining = (
                    self.db.query(func.count(GoRef.go_ref_no))
                    .filter(GoRef.go_annotation_no == go_annotation_no)
                    .scalar()
                )
                if remaining == 0:
                    go_ann = (
                        self.db.query(GoAnnotation)
                        .filter(GoAnnotation.go_annotation_no == go_annotation_no)
                        .first()
                    )
                    if go_ann:
                        self.db.delete(go_ann)

            messages.append(f"Deleted {count} GO annotation entries")

        elif entry_type == "ref_link":
            ref_links = (
                self.db.query(RefLink)
                .filter(RefLink.reference_no == reference_no)
                .all()
            )

            orphaned = []
            for link in ref_links:
                tab_name = link.tab_name
                primary_key = link.primary_key
                col_name = link.col_name

                self.db.delete(link)
                count += 1

                # Check if data becomes orphaned
                remaining = (
                    self.db.query(func.count(RefLink.ref_link_no))
                    .filter(
                        RefLink.tab_name == tab_name,
                        RefLink.primary_key == primary_key,
                        RefLink.col_name == col_name,
                    )
                    .scalar()
                )
                if remaining == 0:
                    orphaned.append({
                        "tab_name": tab_name,
                        "primary_key": primary_key,
                        "col_name": col_name,
                    })

            messages.append(f"Deleted {count} ref_link entries")
            if orphaned:
                messages.append(
                    f"Warning: {len(orphaned)} data entries are now orphaned"
                )

        else:
            raise RefAnnotationCurationError(f"Invalid entry type: {entry_type}")

        self.db.commit()

        logger.info(
            f"Bulk deleted {entry_type} for reference {reference_no} "
            f"by {curator_userid}: {count} entries"
        )

        return {"success": True, "messages": messages, "count": count}

    def bulk_transfer(
        self,
        reference_no: int,
        entry_type: str,
        new_reference_no: int,
        curator_userid: str,
    ) -> dict:
        """
        Bulk transfer all entries of a given type to another reference.

        entry_type: 'lit_guide', 'go_annotation', or 'ref_link'
        """
        # Verify new reference exists
        new_ref = self.get_reference_by_no(new_reference_no)
        if not new_ref:
            raise RefAnnotationCurationError(
                f"Target reference {new_reference_no} not found"
            )

        messages = []
        count = 0

        if entry_type == "lit_guide":
            entries = self._get_literature_guide_entries(reference_no)
            for entry in entries:
                try:
                    self.transfer_lit_guide_entry(
                        refprop_feat_no=entry.get("refprop_feat_no"),
                        ref_property_no=entry["ref_property_no"],
                        new_reference_no=new_reference_no,
                        curator_userid=curator_userid,
                    )
                    count += 1
                except RefAnnotationCurationError as e:
                    messages.append(f"Failed to transfer: {e}")

            messages.insert(0, f"Transferred {count} literature guide entries")

        elif entry_type == "go_annotation":
            entries = self._get_go_annotation_entries(reference_no)
            for entry in entries:
                try:
                    self.transfer_go_ref_entry(
                        go_ref_no=entry["go_ref_no"],
                        new_reference_no=new_reference_no,
                        curator_userid=curator_userid,
                    )
                    count += 1
                except RefAnnotationCurationError as e:
                    messages.append(f"Failed to transfer: {e}")

            messages.insert(0, f"Transferred {count} GO annotation entries")

        elif entry_type == "ref_link":
            ref_links = (
                self.db.query(RefLink)
                .filter(RefLink.reference_no == reference_no)
                .all()
            )

            for link in ref_links:
                try:
                    self.transfer_ref_link_entry(
                        ref_link_no=link.ref_link_no,
                        new_reference_no=new_reference_no,
                        curator_userid=curator_userid,
                    )
                    count += 1
                except RefAnnotationCurationError as e:
                    messages.append(f"Failed to transfer: {e}")

            messages.insert(0, f"Transferred {count} ref_link entries")

        else:
            raise RefAnnotationCurationError(f"Invalid entry type: {entry_type}")

        logger.info(
            f"Bulk transferred {entry_type} from ref {reference_no} "
            f"to ref {new_reference_no} by {curator_userid}: {count} entries"
        )

        return {"success": True, "messages": messages, "count": count}
