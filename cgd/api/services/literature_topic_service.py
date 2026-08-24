"""
Literature Topic Service - Business logic for literature topic search.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import List

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, distinct

from cgd.api.services.text_search_service import (
    _get_assembly21_feature_nos_to_exclude,
)
from cgd.schemas.literature_topic_schema import (
    LiteratureTopicTerm,
    LiteratureTopicTreeResponse,
    LiteratureTopicSearchResponse,
    LiteratureTopicSearchQuery,
    TopicReferenceResult,
    ReferenceForLitTopic,
    GeneForLitTopic,
    CitationLinkForLitTopic,
)
from cgd.models.models import (
    Cv,
    CvTerm,
    CvtermRelationship,
    RefProperty,
    RefpropFeat,
    Reference,
    RefUrl,
    Url,
    Feature,
    Organism,
)

logger = logging.getLogger(__name__)


def _build_citation_links(ref, ref_urls=None) -> list[CitationLinkForLitTopic]:
    """Build citation links for a reference."""
    links = []

    # CGD Paper link (always present)
    links.append(CitationLinkForLitTopic(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLinkForLitTopic(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    return links


def get_literature_topic_tree(db: Session) -> LiteratureTopicTreeResponse:
    """
    Get hierarchical tree of literature topics.

    Returns literature topics with reference counts, organized hierarchically
    using the CV term relationships.
    """
    # Get the literature_topic CV
    lit_topic_cv = (
        db.query(Cv)
        .filter(func.lower(Cv.cv_name) == 'literature_topic')
        .first()
    )

    if not lit_topic_cv:
        return LiteratureTopicTreeResponse(tree=[])

    # Get all CV terms for literature_topic
    cv_terms = (
        db.query(CvTerm)
        .filter(CvTerm.cv_no == lit_topic_cv.cv_no)
        .all()
    )

    if not cv_terms:
        return LiteratureTopicTreeResponse(tree=[])

    # Build maps
    term_map = {t.term_name: t for t in cv_terms}
    term_no_map = {t.cv_term_no: t for t in cv_terms}

    # Get reference counts per topic from ref_property
    topic_counts = (
        db.query(
            RefProperty.property_value,
            func.count(distinct(RefProperty.reference_no)).label('count')
        )
        .filter(RefProperty.property_value.in_([t.term_name for t in cv_terms]))
        .group_by(RefProperty.property_value)
        .all()
    )
    count_map = {topic: cnt for topic, cnt in topic_counts}

    # Get all relationships for these terms
    relationships = (
        db.query(CvtermRelationship)
        .filter(
            CvtermRelationship.parent_cv_term_no.in_([t.cv_term_no for t in cv_terms])
            | CvtermRelationship.child_cv_term_no.in_([t.cv_term_no for t in cv_terms])
        )
        .all()
    )

    # Build parent -> children map
    children_map: dict[int, list[int]] = defaultdict(list)
    has_parent: set[int] = set()

    for rel in relationships:
        if rel.parent_cv_term_no in term_no_map and rel.child_cv_term_no in term_no_map:
            children_map[rel.parent_cv_term_no].append(rel.child_cv_term_no)
            has_parent.add(rel.child_cv_term_no)

    # Find root terms (terms that have no parent)
    root_term_nos = [t.cv_term_no for t in cv_terms if t.cv_term_no not in has_parent]

    # Recursively build tree
    def build_tree_node(term_no: int) -> LiteratureTopicTerm:
        term = term_no_map[term_no]
        term_name = term.term_name
        count = count_map.get(term_name, 0)

        children = []
        for child_no in sorted(children_map.get(term_no, []),
                               key=lambda x: term_no_map[x].term_name):
            children.append(build_tree_node(child_no))

        return LiteratureTopicTerm(
            cv_term_no=term_no,
            term=term_name,
            count=count,
            children=children,
        )

    # Build tree from roots
    tree = []
    for root_no in sorted(root_term_nos, key=lambda x: term_no_map[x].term_name):
        tree.append(build_tree_node(root_no))

    # If tree is empty but we have terms, fall back to flat list
    if not tree and cv_terms:
        tree = [
            LiteratureTopicTerm(
                cv_term_no=t.cv_term_no,
                term=t.term_name,
                count=count_map.get(t.term_name, 0),
                children=[]
            )
            for t in sorted(cv_terms, key=lambda x: x.term_name)
        ]

    return LiteratureTopicTreeResponse(tree=tree)


def search_by_topics(
    db: Session,
    topic_cv_term_nos: List[int],
) -> LiteratureTopicSearchResponse:
    """
    Search references by literature topics.

    Args:
        db: Database session
        topic_cv_term_nos: List of cv_term_no values for topics to search

    Returns:
        LiteratureTopicSearchResponse with references and genes per topic.
        Only genes curated under the searched topic are returned for each
        reference (not all genes associated with the paper via other topics).
    """
    if not topic_cv_term_nos:
        return LiteratureTopicSearchResponse(
            query=LiteratureTopicSearchQuery(topic_cv_term_nos=[], topic_names=[]),
            total_references=0,
            total_genes=0,
            results=[],
        )

    # Get topic names for the cv_term_nos
    topics = (
        db.query(CvTerm)
        .filter(CvTerm.cv_term_no.in_(topic_cv_term_nos))
        .all()
    )
    topic_name_map = {t.cv_term_no: t.term_name for t in topics}
    topic_names = list(topic_name_map.values())

    # Query ref_property for matching topics with references, including the
    # genes curated under each (reference, topic) link
    ref_properties = (
        db.query(RefProperty)
        .options(
            joinedload(RefProperty.reference),
            joinedload(RefProperty.refprop_feat)
            .joinedload(RefpropFeat.feature)
            .joinedload(Feature.organism),
        )
        .filter(RefProperty.property_value.in_(topic_names))
        .all()
    )

    # Build genes per (topic, reference). Each RefProperty row is one
    # (reference, topic) link and its refprop_feat are the genes curated
    # under that specific topic — genes the paper carries only under OTHER
    # topics are deliberately not included (curator request).
    topic_ref_genes: dict[tuple, list[GeneForLitTopic]] = defaultdict(list)
    all_genes: set[int] = set()

    # Drop Assembly 21 (orf19.*) features that have Assembly 22 equivalents,
    # otherwise C. albicans genes appear twice per reference (C1_08450C_A
    # and orf19.390 are the same gene curated on both assemblies)
    candidate_feature_nos = {
        rpf.feature.feature_no
        for rp in ref_properties
        for rpf in rp.refprop_feat
        if rpf.feature
    }
    excluded_a21 = _get_assembly21_feature_nos_to_exclude(db, candidate_feature_nos)

    for rp in ref_properties:
        if not rp.reference:
            continue
        key = (rp.property_value, rp.reference.reference_no)
        seen_gene_nos = {g.feature_no for g in topic_ref_genes[key]}

        for rpf in rp.refprop_feat:
            feat = rpf.feature
            if (
                feat
                and feat.feature_no not in seen_gene_nos
                and feat.feature_no not in excluded_a21
            ):
                all_genes.add(feat.feature_no)
                seen_gene_nos.add(feat.feature_no)

                org = feat.organism
                organism_name = None
                if org:
                    organism_name = (
                        getattr(org, "organism_name", None)
                        or getattr(org, "display_name", None)
                    )

                gene_obj = GeneForLitTopic(
                    feature_no=feat.feature_no,
                    feature_name=feat.feature_name,
                    gene_name=feat.gene_name,
                    organism=organism_name,
                )
                topic_ref_genes[key].append(gene_obj)

    # Build results grouped by topic, then by reference
    # Structure: topic -> reference_no -> (ref_obj, links)
    results_by_topic: dict[str, dict[int, tuple]] = {}
    topic_cv_term_map: dict[str, int] = {}
    all_references: set[int] = set()

    # Cache for reference URL lookups
    ref_url_cache: dict[int, list] = {}

    for rp in ref_properties:
        topic = rp.property_value
        ref = rp.reference
        if not ref:
            continue

        # Find cv_term_no for this topic
        cv_term_no = None
        for tno, tname in topic_name_map.items():
            if tname == topic:
                cv_term_no = tno
                break

        if cv_term_no is None:
            continue

        # Track cv_term_no for topic
        topic_cv_term_map[topic] = cv_term_no

        # Initialize topic dict if not exists
        if topic not in results_by_topic:
            results_by_topic[topic] = {}

        # Track unique references
        all_references.add(ref.reference_no)

        # Initialize reference entry if not exists
        if ref.reference_no not in results_by_topic[topic]:
            # Get ref URLs if not cached
            if ref.reference_no not in ref_url_cache:
                ref_urls = (
                    db.query(RefUrl)
                    .options(joinedload(RefUrl.url))
                    .filter(RefUrl.reference_no == ref.reference_no)
                    .all()
                )
                ref_url_cache[ref.reference_no] = ref_urls

            links = _build_citation_links(ref, ref_url_cache[ref.reference_no])

            results_by_topic[topic][ref.reference_no] = (ref, links)

    # Build final results
    results = []
    for topic in sorted(results_by_topic.keys()):
        refs_dict = results_by_topic[topic]
        references = []

        for ref_no in refs_dict:
            ref, links = refs_dict[ref_no]
            # Genes curated under this specific topic for this reference
            genes = list(topic_ref_genes.get((topic, ref_no), []))
            # Sort genes
            genes.sort(key=lambda g: (g.gene_name or g.feature_name or ''))

            ref_obj = ReferenceForLitTopic(
                reference_no=ref.reference_no,
                dbxref_id=ref.dbxref_id,
                pubmed=ref.pubmed,
                citation=ref.citation,
                title=ref.title,
                year=ref.year,
                links=links,
                genes=genes,
            )
            references.append(ref_obj)

        # Sort references by year descending, then citation
        references.sort(key=lambda r: (-(r.year or 0), r.citation or ''))

        results.append(TopicReferenceResult(
            topic=topic,
            cv_term_no=topic_cv_term_map[topic],
            references=references,
        ))

    return LiteratureTopicSearchResponse(
        query=LiteratureTopicSearchQuery(
            topic_cv_term_nos=topic_cv_term_nos,
            topic_names=topic_names,
        ),
        total_references=len(all_references),
        total_genes=len(all_genes),
        results=results,
    )
