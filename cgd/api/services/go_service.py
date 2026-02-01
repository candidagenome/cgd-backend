"""
GO Service - handles GO term page data retrieval.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Optional

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_
from fastapi import HTTPException

from cgd.schemas.go_schema import (
    GoTermOut,
    GoTermResponse,
    AnnotationSummary,
    AnnotatedGene,
    ReferenceEvidence,
    CitationLinkForGO,
)
from cgd.models.models import (
    Go,
    GoAnnotation,
    GoGosyn,
    GoRef,
    GoQualifier,
    Feature,
    RefUrl,
)


# Map GO aspect codes to full names
ASPECT_NAMES = {
    "C": "Cellular Component",
    "F": "Molecular Function",
    "P": "Biological Process",
}

# Map annotation_type values from database to display labels
# Database values: 'manually curated', 'high-throughput', 'computational'
ANNOTATION_TYPE_MAP = {
    "manually curated": "manually_curated",
    "high-throughput": "high_throughput",
    "computational": "computational",
}

# Reverse map for display
ANNOTATION_TYPE_LABELS = {
    "manually_curated": "Manually Curated",
    "high_throughput": "High-Throughput",
    "computational": "Computational",
}

# Order for displaying annotation types
ANNOTATION_TYPE_ORDER = ["manually_curated", "high_throughput", "computational"]


def _format_goid(goid: int | str) -> str:
    """Format GOID as GO:XXXXXXX (7-digit padded)."""
    if isinstance(goid, str):
        if goid.startswith("GO:"):
            return goid
        goid = int(goid)
    return f"GO:{goid:07d}"


def _parse_goid(goid_str: str) -> int:
    """
    Parse GOID string to integer.
    Accepts formats: "GO:0005634", "0005634", "5634"
    """
    goid_str = goid_str.strip()
    if goid_str.upper().startswith("GO:"):
        goid_str = goid_str[3:]
    return int(goid_str)


def _get_organism_name(feature: Feature) -> str:
    """Get organism name from feature."""
    if feature.organism:
        return (
            getattr(feature.organism, "organism_name", None)
            or getattr(feature.organism, "display_name", None)
            or getattr(feature.organism, "name", None)
            or str(feature.organism_no)
        )
    return str(feature.organism_no)


def _normalize_annotation_type(db_type: str) -> str:
    """Normalize database annotation type to API format."""
    if not db_type:
        return "manually_curated"
    return ANNOTATION_TYPE_MAP.get(db_type, db_type.replace(" ", "_").replace("-", "_"))


def _build_citation_links(ref, ref_urls=None) -> list[CitationLinkForGO]:
    """
    Build citation links for a reference in GO term page context.

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: Optional list of RefUrl objects for additional links

    Returns:
        List of CitationLinkForGO objects
    """
    links = []

    # CGD Paper link (always present) - always use dbxref_id (CGDID)
    links.append(CitationLinkForGO(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLinkForGO(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Process URLs from ref_url table (if provided)
    # Match Perl behavior: show all URLs except 'Reference supplement' and 'Reference Data'
    if ref_urls:
        for ref_url in ref_urls:
            url_obj = ref_url.url
            if url_obj and url_obj.url:
                url_type = (url_obj.url_type or "").lower()

                # Skip Reference supplement (displayed separately)
                if "supplement" in url_type:
                    links.append(CitationLinkForGO(
                        name="Reference Supplement",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # Skip Reference Data (not shown as full text)
                elif "reference data" in url_type:
                    continue
                # Download Datasets
                elif any(kw in url_type for kw in ["download", "dataset"]):
                    links.append(CitationLinkForGO(
                        name="Download Datasets",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # All other URL types are shown as Full Text (matching Perl default behavior)
                else:
                    links.append(CitationLinkForGO(
                        name="Full Text",
                        url=url_obj.url,
                        link_type="external"
                    ))

    return links


def get_go_term_info(
    db: Session,
    goid_str: str,
    page: int = 1,
    limit: int = 20,
) -> GoTermResponse:
    """
    Get GO term info and all genes annotated to it.

    Args:
        db: Database session
        goid_str: GO identifier (e.g., "GO:0005634" or "5634")
        page: Page number (1-indexed)
        limit: Number of genes per page (default 20)

    Returns:
        GoTermResponse with term info and annotated genes
    """
    # Parse GOID
    try:
        goid_int = _parse_goid(goid_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid GO identifier format: {goid_str}"
        )

    # Query GO term
    go_term = db.query(Go).filter(Go.goid == goid_int).first()
    if not go_term:
        raise HTTPException(
            status_code=404,
            detail=f"GO term not found: {_format_goid(goid_int)}"
        )

    # Get synonyms via GoGosyn junction table
    synonyms = []
    go_gosyn_records = (
        db.query(GoGosyn)
        .options(joinedload(GoGosyn.go_synonym))
        .filter(GoGosyn.go_no == go_term.go_no)
        .all()
    )
    for gg in go_gosyn_records:
        if gg.go_synonym and gg.go_synonym.go_synonym:
            synonyms.append(gg.go_synonym.go_synonym)

    # Build term output
    aspect_code = go_term.go_aspect[0].upper() if go_term.go_aspect else "P"
    term_out = GoTermOut(
        goid=_format_goid(go_term.goid),
        go_term=go_term.go_term,
        go_definition=go_term.go_definition,
        go_aspect=aspect_code,
        aspect_name=ASPECT_NAMES.get(aspect_code, go_term.go_aspect),
        synonyms=synonyms,
    )

    # Query all annotations for this GO term with eager loading
    annotations = (
        db.query(GoAnnotation)
        .options(
            joinedload(GoAnnotation.feature).joinedload(Feature.organism),
            joinedload(GoAnnotation.go_ref).joinedload(GoRef.reference),
            joinedload(GoAnnotation.go_ref).joinedload(GoRef.go_qualifier),
        )
        .filter(GoAnnotation.go_no == go_term.go_no)
        .all()
    )

    # Collect all unique reference_no values to query RefUrl
    ref_nos = set()
    for ann in annotations:
        for go_ref in ann.go_ref:
            if go_ref.reference:
                ref_nos.add(go_ref.reference.reference_no)

    # Query RefUrl for all references (Full Text links, supplements, etc.)
    ref_url_map: dict[int, list] = {}  # reference_no -> list of RefUrl objects
    if ref_nos:
        ref_url_query = (
            db.query(RefUrl)
            .options(joinedload(RefUrl.url))
            .filter(RefUrl.reference_no.in_(ref_nos))
            .all()
        )
        for ref_url in ref_url_query:
            if ref_url.reference_no not in ref_url_map:
                ref_url_map[ref_url.reference_no] = []
            ref_url_map[ref_url.reference_no].append(ref_url)

    # Group annotations by type and qualifier status
    # Structure: {annotation_type: {"with_qualifier": {qualifier_key: {feature_no: gene_data}},
    #                               "without_qualifier": {feature_no: gene_data}}}
    annotations_by_type: dict[str, dict[str, dict]] = defaultdict(
        lambda: {"with_qualifier": defaultdict(dict), "without_qualifier": {}}
    )

    for ann in annotations:
        feature = ann.feature
        if not feature:
            continue

        annotation_type = _normalize_annotation_type(ann.annotation_type)
        feature_no = feature.feature_no

        # Process each go_ref to determine if it has qualifiers
        for go_ref in ann.go_ref:
            ref = go_ref.reference
            if not ref:
                continue

            # Get qualifiers for this go_ref
            qualifiers = sorted([gq.qualifier for gq in go_ref.go_qualifier if gq.qualifier])
            has_qualifier = go_ref.has_qualifier == 'Y' and len(qualifiers) > 0

            if has_qualifier:
                # Group by qualifier combination (e.g., "NOT" or "contributes to")
                qualifier_key = ",".join(qualifiers)
                gene_dict = annotations_by_type[annotation_type]["with_qualifier"][qualifier_key]
            else:
                gene_dict = annotations_by_type[annotation_type]["without_qualifier"]

            # Create or get gene entry
            if feature_no not in gene_dict:
                gene_dict[feature_no] = AnnotatedGene(
                    locus_name=feature.gene_name,
                    systematic_name=feature.feature_name,
                    species=_get_organism_name(feature),
                    references=[],
                )

            gene_entry = gene_dict[feature_no]

            # Check if we already have this reference
            ref_key = str(ref.pubmed) if ref.pubmed else ref.citation
            existing_ref = None
            for r in gene_entry.references:
                r_key = str(r.pubmed) if r.pubmed else r.citation
                if r_key == ref_key:
                    existing_ref = r
                    break

            if existing_ref:
                # Add evidence code if not already present
                if ann.go_evidence and ann.go_evidence not in existing_ref.evidence_codes:
                    existing_ref.evidence_codes.append(ann.go_evidence)
                # Add qualifiers if not already present
                for q in qualifiers:
                    if q not in existing_ref.qualifiers:
                        existing_ref.qualifiers.append(q)
            else:
                # Build citation links (CGD Paper, PubMed, Full Text, etc.)
                ref_urls = ref_url_map.get(ref.reference_no, [])
                links = _build_citation_links(ref, ref_urls)

                # Create new reference entry
                gene_entry.references.append(ReferenceEvidence(
                    citation=ref.citation,
                    pubmed=ref.pubmed,
                    dbxref_id=ref.dbxref_id,
                    evidence_codes=[ann.go_evidence] if ann.go_evidence else [],
                    qualifiers=qualifiers,
                    links=links,
                ))

    # Build annotation summaries with proper grouping
    annotation_summaries = []
    total_genes = 0

    for annotation_type in ANNOTATION_TYPE_ORDER:
        type_data = annotations_by_type.get(annotation_type)
        if not type_data:
            continue

        # Genes without qualifiers (direct annotations)
        without_qual = type_data["without_qualifier"]
        # Genes with qualifiers (NOT, contributes_to, etc.)
        with_qual = type_data["with_qualifier"]

        # Combine all genes for this annotation type
        all_genes: dict[int, AnnotatedGene] = {}

        # Add genes without qualifiers first
        for feature_no, gene in without_qual.items():
            if feature_no not in all_genes:
                all_genes[feature_no] = gene
            else:
                # Merge references
                for ref in gene.references:
                    all_genes[feature_no].references.append(ref)

        # Add genes with qualifiers
        for qualifier_key, genes_dict in with_qual.items():
            for feature_no, gene in genes_dict.items():
                if feature_no not in all_genes:
                    all_genes[feature_no] = gene
                else:
                    # Merge references
                    for ref in gene.references:
                        all_genes[feature_no].references.append(ref)

        if all_genes:
            genes_list = list(all_genes.values())
            # Sort genes by species, then by locus_name or systematic_name
            genes_list.sort(key=lambda g: (
                g.species.lower(),
                (g.locus_name or g.systematic_name).lower()
            ))

            annotation_summaries.append(AnnotationSummary(
                annotation_type=annotation_type,
                gene_count=len(genes_list),
                genes=genes_list,
            ))
            total_genes += len(genes_list)

    return GoTermResponse(
        term=term_out,
        total_genes=total_genes,
        annotations=annotation_summaries,
    )
