"""
GO Service - handles GO term page data retrieval.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Optional

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, func
from fastapi import HTTPException

from cgd.schemas.go_schema import (
    GoTermOut,
    GoTermResponse,
    AnnotationSummary,
    AnnotatedGene,
    ReferenceEvidence,
    CitationLinkForGO,
    QualifierGroup,
    SpeciesCount,
    GoEvidenceCode,
    GoEvidenceResponse,
    GoHierarchyNode,
    GoHierarchyEdge,
    GoHierarchyResponse,
)
from cgd.models.models import (
    Go,
    GoAnnotation,
    GoGosyn,
    GoRef,
    GoQualifier,
    GoPath,
    Feature,
    RefUrl,
    Code,
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


def _abbreviate_species(species_name: str) -> str:
    """
    Abbreviate species name for display.
    e.g., "Candida albicans SC5314" -> "C. albicans"
    """
    if not species_name:
        return species_name
    parts = species_name.split()
    if len(parts) >= 2:
        return f"{parts[0][0]}. {parts[1]}"
    return species_name


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

    # Build annotation summaries with qualifier groups
    annotation_summaries = []
    total_genes = 0
    go_term_name = go_term.go_term

    for annotation_type in ANNOTATION_TYPE_ORDER:
        type_data = annotations_by_type.get(annotation_type)
        if not type_data:
            continue

        # Genes without qualifiers (direct annotations)
        without_qual = type_data["without_qualifier"]
        # Genes with qualifiers (NOT, contributes_to, etc.)
        with_qual = type_data["with_qualifier"]

        qualifier_groups = []
        type_gene_count = 0

        # Track all unique genes for this annotation type (to avoid double counting)
        seen_feature_nos = set()

        # First, add direct annotations (no qualifier)
        if without_qual:
            genes_list = list(without_qual.values())
            genes_list.sort(key=lambda g: (
                g.species.lower(),
                (g.locus_name or g.systematic_name).lower()
            ))

            # Calculate species counts
            species_counts_dict: dict[str, int] = defaultdict(int)
            for gene in genes_list:
                abbrev_species = _abbreviate_species(gene.species)
                species_counts_dict[abbrev_species] += 1
                seen_feature_nos.add(gene.systematic_name)

            species_counts = [
                SpeciesCount(species=sp, count=cnt)
                for sp, cnt in sorted(species_counts_dict.items())
            ]

            qualifier_groups.append(QualifierGroup(
                qualifier=None,
                display_name=go_term_name,
                species_counts=species_counts,
                genes=genes_list,
            ))
            type_gene_count += len(genes_list)

        # Then add each qualifier group
        for qualifier_key in sorted(with_qual.keys()):
            genes_dict = with_qual[qualifier_key]
            genes_list = list(genes_dict.values())
            genes_list.sort(key=lambda g: (
                g.species.lower(),
                (g.locus_name or g.systematic_name).lower()
            ))

            # Calculate species counts
            species_counts_dict: dict[str, int] = defaultdict(int)
            for gene in genes_list:
                abbrev_species = _abbreviate_species(gene.species)
                species_counts_dict[abbrev_species] += 1
                # Only count gene if not already counted in direct annotations
                if gene.systematic_name not in seen_feature_nos:
                    seen_feature_nos.add(gene.systematic_name)

            species_counts = [
                SpeciesCount(species=sp, count=cnt)
                for sp, cnt in sorted(species_counts_dict.items())
            ]

            # Build display name with qualifier prefix
            display_name = f"{qualifier_key} {go_term_name}"

            qualifier_groups.append(QualifierGroup(
                qualifier=qualifier_key,
                display_name=display_name,
                species_counts=species_counts,
                genes=genes_list,
            ))
            type_gene_count += len(genes_list)

        if qualifier_groups:
            annotation_summaries.append(AnnotationSummary(
                annotation_type=annotation_type,
                gene_count=type_gene_count,
                qualifier_groups=qualifier_groups,
            ))
            total_genes += type_gene_count

    return GoTermResponse(
        term=term_out,
        total_genes=total_genes,
        annotations=annotation_summaries,
    )


def _uppercase_first_letters(sentence: str) -> str:
    """
    Uppercase the first letter of each word (except common words).
    Matches Perl _uppercase_words logic.
    """
    if not sentence:
        return sentence

    skip_words = {'from', 'on', 'in', 'of', 'or', 'structural'}
    words = sentence.split(' ')
    result = []

    for word in words:
        if word.lower() in skip_words:
            result.append(word)
        else:
            # Capitalize first letter
            result.append(word.capitalize())

    return ' '.join(result)


def get_go_evidence_codes(db: Session) -> GoEvidenceResponse:
    """
    Get all GO evidence codes with their definitions and examples.

    Returns:
        GoEvidenceResponse with list of evidence codes
    """
    # Query Code table for GO evidence codes
    # Evidence codes are stored where tab_name='GO_ANNOTATION' and col_name='GO_EVIDENCE'
    codes = (
        db.query(Code)
        .filter(Code.tab_name == 'GO_ANNOTATION')
        .filter(Code.col_name == 'GO_EVIDENCE')
        .order_by(Code.code_value)
        .all()
    )

    evidence_codes = []

    for code in codes:
        code_value = code.code_value
        description = code.description or ''

        # Parse description - format is "definition: example1; example2; example3"
        if ': ' in description:
            parts = description.split(': ', 1)
            definition = _uppercase_first_letters(parts[0])
            examples_str = parts[1] if len(parts) > 1 else ''
            examples = [ex.strip() for ex in examples_str.split('; ') if ex.strip()]
        else:
            definition = _uppercase_first_letters(description)
            examples = []

        evidence_codes.append(GoEvidenceCode(
            code=code_value,
            definition=definition,
            examples=examples,
        ))

    return GoEvidenceResponse(evidence_codes=evidence_codes)


def get_go_hierarchy(
    db: Session,
    goid_str: str,
    max_nodes: int = 30,
    ancestor_levels: int = 5,
) -> GoHierarchyResponse:
    """
    Get GO term hierarchy (ancestors) for diagram visualization.

    Args:
        db: Database session
        goid_str: GO identifier (e.g., "GO:0005634" or "5634")
        max_nodes: Maximum number of nodes to return (default 30)
        ancestor_levels: Maximum ancestor generations to traverse (default 5)

    Returns:
        GoHierarchyResponse with nodes and edges for the hierarchy diagram
    """
    # Parse GOID
    try:
        goid_int = _parse_goid(goid_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid GO identifier format: {goid_str}"
        )

    # Query focus GO term
    focus_go = db.query(Go).filter(Go.goid == goid_int).first()
    if not focus_go:
        raise HTTPException(
            status_code=404,
            detail=f"GO term not found: {_format_goid(goid_int)}"
        )

    # Collect all go_no values we need to include (focus + ancestors)
    # Map go_no -> level (0 = focus, negative = ancestor)
    go_no_to_level: dict[int, int] = {focus_go.go_no: 0}

    # Query ancestors via GoPath table (where child_go_no = focus term's go_no)
    ancestor_paths = (
        db.query(GoPath)
        .filter(GoPath.child_go_no == focus_go.go_no)
        .filter(GoPath.generation <= ancestor_levels)
        .order_by(GoPath.generation)
        .all()
    )

    # Build set of ancestor go_no values with their levels
    for path in ancestor_paths:
        ancestor_go_no = path.ancestor_go_no
        level = -path.generation  # Negative for ancestors
        # Keep the closest level (smallest absolute value) if seen multiple times
        if ancestor_go_no not in go_no_to_level:
            go_no_to_level[ancestor_go_no] = level
        elif abs(level) < abs(go_no_to_level[ancestor_go_no]):
            go_no_to_level[ancestor_go_no] = level

    # Limit to max_nodes if we have too many
    all_go_nos = list(go_no_to_level.keys())
    if len(all_go_nos) > max_nodes:
        # Keep focus term and closest ancestors
        sorted_items = sorted(go_no_to_level.items(), key=lambda x: abs(x[1]))
        all_go_nos = [item[0] for item in sorted_items[:max_nodes]]
        go_no_to_level = {k: v for k, v in go_no_to_level.items() if k in all_go_nos}

    # Query Go records for all nodes
    go_records = db.query(Go).filter(Go.go_no.in_(all_go_nos)).all()
    go_no_to_go: dict[int, Go] = {go.go_no: go for go in go_records}

    # Query annotation counts for all nodes (direct annotations)
    # Count distinct feature_no for each go_no
    annotation_counts = (
        db.query(
            GoAnnotation.go_no,
            func.count(func.distinct(GoAnnotation.feature_no)).label('gene_count')
        )
        .filter(GoAnnotation.go_no.in_(all_go_nos))
        .group_by(GoAnnotation.go_no)
        .all()
    )
    go_no_to_gene_count: dict[int, int] = {row.go_no: row.gene_count for row in annotation_counts}

    # Build nodes list
    nodes = []
    focus_node = None
    for go_no in all_go_nos:
        go = go_no_to_go.get(go_no)
        if not go:
            continue

        level = go_no_to_level[go_no]
        direct_count = go_no_to_gene_count.get(go_no, 0)
        aspect_code = go.go_aspect[0].upper() if go.go_aspect else "P"

        node = GoHierarchyNode(
            goid=_format_goid(go.goid),
            go_term=go.go_term,
            go_aspect=aspect_code,
            direct_gene_count=direct_count,
            inherited_gene_count=0,  # Could be computed but expensive
            has_annotations=direct_count > 0,
            is_focus=(go_no == focus_go.go_no),
            level=level,
        )
        nodes.append(node)

        if go_no == focus_go.go_no:
            focus_node = node

    # Build edges - query GoPath for direct parent-child relationships (generation=1)
    # between nodes we're displaying
    edges = []
    edge_set = set()  # Track unique edges

    # For each node, find its direct parents that are also in our display set
    for path in ancestor_paths:
        if path.generation == 1:  # Direct parent-child relationship
            if path.ancestor_go_no in all_go_nos and path.child_go_no in all_go_nos:
                parent_go = go_no_to_go.get(path.ancestor_go_no)
                child_go = go_no_to_go.get(path.child_go_no)
                if parent_go and child_go:
                    edge_key = (parent_go.goid, child_go.goid)
                    if edge_key not in edge_set:
                        edge_set.add(edge_key)
                        rel_type = "is_a"
                        if path.relationship_type:
                            rel_type = path.relationship_type.replace(" ", "_")
                        edges.append(GoHierarchyEdge(
                            source=_format_goid(parent_go.goid),
                            target=_format_goid(child_go.goid),
                            relationship_type=rel_type,
                        ))

    # Also query edges between ancestors (not just to focus term)
    # Find paths where both ancestor and child are in our display set
    if len(all_go_nos) > 1:
        # Get all generation=1 paths between our nodes
        ancestor_go_nos = [gn for gn in all_go_nos if gn != focus_go.go_no]
        if ancestor_go_nos:
            inter_ancestor_paths = (
                db.query(GoPath)
                .filter(GoPath.generation == 1)
                .filter(GoPath.child_go_no.in_(ancestor_go_nos))
                .filter(GoPath.ancestor_go_no.in_(all_go_nos))
                .all()
            )
            for path in inter_ancestor_paths:
                parent_go = go_no_to_go.get(path.ancestor_go_no)
                child_go = go_no_to_go.get(path.child_go_no)
                if parent_go and child_go:
                    edge_key = (parent_go.goid, child_go.goid)
                    if edge_key not in edge_set:
                        edge_set.add(edge_key)
                        rel_type = "is_a"
                        if path.relationship_type:
                            rel_type = path.relationship_type.replace(" ", "_")
                        edges.append(GoHierarchyEdge(
                            source=_format_goid(parent_go.goid),
                            target=_format_goid(child_go.goid),
                            relationship_type=rel_type,
                        ))

    # Determine if focus term has parents (can_go_up)
    can_go_up = any(path.generation == 1 for path in ancestor_paths)

    return GoHierarchyResponse(
        focus_term=focus_node,
        nodes=nodes,
        edges=edges,
        can_go_up=can_go_up,
    )
