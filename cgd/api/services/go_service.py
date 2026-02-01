"""
GO Service - handles GO term page data retrieval.
"""
from __future__ import annotations

from collections import defaultdict

from sqlalchemy.orm import Session, joinedload
from fastapi import HTTPException

from cgd.schemas.go_schema import (
    GoTermOut,
    GoTermResponse,
    AnnotationSummary,
    AnnotatedGene,
    ReferenceEvidence,
)
from cgd.models.models import (
    Go,
    GoAnnotation,
    GoGosyn,
    GoSynonym,
    GoRef,
    GoQualifier,
    Feature,
    Reference,
)


# Map GO aspect codes to full names
ASPECT_NAMES = {
    "C": "Cellular Component",
    "F": "Molecular Function",
    "P": "Biological Process",
}


def _format_goid(goid: int | str) -> str:
    """Format GOID as GO:XXXXXXX (7-digit padded)."""
    if isinstance(goid, str):
        # If already formatted, return as is
        if goid.startswith("GO:"):
            return goid
        # Otherwise parse the numeric part
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


def get_go_term_info(db: Session, goid_str: str) -> GoTermResponse:
    """
    Get GO term info and all genes annotated to it.

    Args:
        db: Database session
        goid_str: GO identifier (e.g., "GO:0005634" or "5634")

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

    # Query all annotations for this GO term
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

    # Group annotations by type, then by gene
    # Structure: {annotation_type: {feature_no: gene_data}}
    annotations_by_type: dict[str, dict[int, AnnotatedGene]] = defaultdict(dict)

    for ann in annotations:
        feature = ann.feature
        if not feature:
            continue

        annotation_type = ann.annotation_type or "manually_curated"
        feature_no = feature.feature_no

        # Create or get gene entry
        if feature_no not in annotations_by_type[annotation_type]:
            annotations_by_type[annotation_type][feature_no] = AnnotatedGene(
                locus_name=feature.gene_name,
                systematic_name=feature.feature_name,
                species=_get_organism_name(feature),
                references=[],
            )

        gene_entry = annotations_by_type[annotation_type][feature_no]

        # Add references with evidence codes
        for go_ref in ann.go_ref:
            ref = go_ref.reference
            if not ref:
                continue

            # Get qualifiers for this go_ref
            qualifiers = [gq.qualifier for gq in go_ref.go_qualifier if gq.qualifier]

            # Check if we already have this reference for this gene
            # If so, add evidence code to existing entry
            existing_ref = None
            for r in gene_entry.references:
                if r.pmid == str(ref.pubmed) if ref.pubmed else r.citation == ref.citation:
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
                # Create new reference entry
                gene_entry.references.append(ReferenceEvidence(
                    citation=ref.citation,
                    pmid=str(ref.pubmed) if ref.pubmed else None,
                    evidence_codes=[ann.go_evidence] if ann.go_evidence else [],
                    qualifiers=qualifiers,
                ))

    # Build annotation summaries
    annotation_summaries = []
    total_genes = 0

    for annotation_type in ["manually_curated", "high_throughput", "computational"]:
        genes_dict = annotations_by_type.get(annotation_type, {})
        if genes_dict:
            genes_list = list(genes_dict.values())
            # Sort genes by locus_name or systematic_name
            genes_list.sort(key=lambda g: (g.locus_name or g.systematic_name).lower())
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
