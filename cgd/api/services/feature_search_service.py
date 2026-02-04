"""
Feature Search (Advanced Search) Service.

Handles complex feature searching with multiple filter criteria including:
- Organism/strain selection
- Feature types (ORF, tRNA, etc.)
- Feature qualifiers (Verified, Uncharacterized, Dubious, Deleted)
- Chromosome selection
- Intron presence
- GO Slim terms with evidence codes and annotation methods
"""
from __future__ import annotations

import math
from typing import Optional, List, Dict, Set, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, text

from cgd.models.models import (
    Feature, Organism, FeatProperty, FeatLocation, Seq,
    Go, GoSet, GoPath, GoAnnotation, FeatRelationship,
)
from cgd.schemas.feature_search_schema import (
    FeatureSearchRequest,
    FeatureSearchResponse,
    FeatureSearchResult,
    FeatureSearchConfigResponse,
    QuerySummary,
    FilterCount,
    PaginationInfo,
    OrganismInfo,
    GoSlimTerms,
    GoSlimTerm,
    GoTermBrief,
)


# Application name for web_metadata queries
APPLICATION_NAME = "Chromosomal Feature Search"

# Default GO Slim set name
GO_SLIM_SET_NAME = "CGD_GO_Slim"

# Default annotation methods to use if GO search is activated
DEFAULT_ANNOTATION_METHODS = ["manually curated", "high-throughput"]


def get_feature_search_config(
    db: Session,
    organism: Optional[str] = None,
) -> FeatureSearchConfigResponse:
    """
    Get configuration for the feature search form.

    Args:
        db: Database session
        organism: Optional organism abbreviation to get chromosomes for

    Returns:
        Configuration with organisms, feature types, qualifiers, chromosomes, GO terms
    """
    organisms = _get_organisms(db)
    feature_types = _get_feature_types(db)
    qualifiers = _get_qualifiers(db)
    chromosomes = _get_all_chromosomes(db, organisms)
    go_slim_terms = _get_go_slim_terms(db)
    evidence_codes = _get_evidence_codes(db)
    annotation_methods = _get_annotation_methods(db)

    return FeatureSearchConfigResponse(
        organisms=organisms,
        feature_types=feature_types,
        qualifiers=qualifiers,
        chromosomes=chromosomes,
        go_slim_terms=go_slim_terms,
        evidence_codes=evidence_codes,
        annotation_methods=annotation_methods,
    )


def search_features(
    db: Session,
    request: FeatureSearchRequest,
) -> FeatureSearchResponse:
    """
    Execute feature search with all filters.

    Args:
        db: Database session
        request: Search request with all filter criteria

    Returns:
        Search results with pagination
    """
    # Validate required fields
    if not request.organism:
        return FeatureSearchResponse(
            success=False,
            error="Organism is required"
        )

    if not request.feature_types and not request.include_all_types:
        return FeatureSearchResponse(
            success=False,
            error="At least one feature type must be selected"
        )

    # Get organism info
    organism_obj = (
        db.query(Organism)
        .filter(Organism.organism_abbrev == request.organism)
        .first()
    )
    if not organism_obj:
        return FeatureSearchResponse(
            success=False,
            error=f"Organism '{request.organism}' not found"
        )

    # Determine if we need to show position/GO columns
    show_position = bool(
        request.chromosomes or
        request.has_introns is not None
    )
    do_go_search = bool(
        request.process_goids or
        request.function_goids or
        request.component_goids or
        request.additional_goids
    )

    # Build base query for features
    base_query = (
        db.query(Feature)
        .filter(Feature.organism_no == organism_obj.organism_no)
    )

    # Filter by feature types
    if not request.include_all_types and request.feature_types:
        # Also check qualifiers that match feature type names (Verified, Uncharacterized, etc.)
        type_conditions = [Feature.feature_type.in_(request.feature_types)]

        # Get features with matching qualifiers
        qual_subquery = (
            db.query(FeatProperty.feature_no)
            .filter(
                FeatProperty.property_type == "qualifier",
                FeatProperty.property_value.in_(request.feature_types)
            )
        )
        type_conditions.append(Feature.feature_no.in_(qual_subquery))

        base_query = base_query.filter(or_(*type_conditions))

    # Get all matching feature numbers for filtering
    feature_nos = set(f.feature_no for f in base_query.all())

    # Apply filters and track counts
    filter_counts = []

    # Qualifier filter
    if request.qualifiers:
        feature_nos, count = _filter_by_qualifiers(db, feature_nos, request.qualifiers)
        filter_counts.append(FilterCount(
            description=f"Qualifier is {' or '.join(request.qualifiers)}",
            count=count
        ))
    else:
        # By default, exclude Deleted features
        feature_nos, _ = _exclude_deleted_features(db, feature_nos)

    # Chromosome filter
    if request.chromosomes:
        feature_nos, count = _filter_by_chromosomes(db, feature_nos, request.chromosomes)
        filter_counts.append(FilterCount(
            description=f"Located on chromosome {', '.join(request.chromosomes)}",
            count=count
        ))

    # Intron filter
    if request.has_introns is not None:
        feature_nos, count = _filter_by_introns(db, feature_nos, request.has_introns)
        intron_desc = "Has introns" if request.has_introns else "Does not have introns"
        filter_counts.append(FilterCount(
            description=intron_desc,
            count=count
        ))

    # GO term filter
    go_annotations: Dict[int, Dict[str, List[GoTermBrief]]] = {}
    if do_go_search:
        # Combine all GOIDs
        all_goids = (
            request.process_goids +
            request.function_goids +
            request.component_goids +
            request.additional_goids
        )

        # Use default annotation methods if not specified
        annotation_methods = request.annotation_methods or DEFAULT_ANNOTATION_METHODS
        evidence_codes = request.evidence_codes or []

        feature_nos, count, go_annotations = _filter_by_go_terms(
            db, feature_nos, all_goids, annotation_methods, evidence_codes
        )
        filter_counts.append(FilterCount(
            description="Annotated to selected GO terms",
            count=count
        ))

    # Get total count
    total_results = len(feature_nos)

    if total_results == 0:
        return FeatureSearchResponse(
            success=True,
            query_summary=QuerySummary(
                organism_name=organism_obj.organism_name,
                feature_types=request.feature_types if not request.include_all_types else ["All"],
                filter_counts=filter_counts,
                total_results=0,
            ),
            results=[],
            pagination=PaginationInfo(
                page=1,
                page_size=request.page_size,
                total_items=0,
                total_pages=0,
                has_next=False,
                has_prev=False,
            ),
            show_position=show_position,
            show_go_terms=do_go_search,
        )

    # Build final query with sorting
    sorted_feature_nos = _sort_features(db, feature_nos, request.sort_by)

    # Paginate
    total_pages = math.ceil(total_results / request.page_size)
    start_idx = (request.page - 1) * request.page_size
    end_idx = start_idx + request.page_size
    page_feature_nos = sorted_feature_nos[start_idx:end_idx]

    # Get feature details
    results = _get_feature_details(
        db, page_feature_nos, show_position, do_go_search, go_annotations
    )

    return FeatureSearchResponse(
        success=True,
        query_summary=QuerySummary(
            organism_name=organism_obj.organism_name,
            feature_types=request.feature_types if not request.include_all_types else ["All"],
            filter_counts=filter_counts,
            total_results=total_results,
        ),
        features=results,
        total_count=total_results,
        total_pages=total_pages,
        pagination=PaginationInfo(
            page=request.page,
            page_size=request.page_size,
            total_items=total_results,
            total_pages=total_pages,
            has_next=request.page < total_pages,
            has_prev=request.page > 1,
        ),
        show_position=show_position,
        show_go_terms=do_go_search,
    )


def _get_organisms(db: Session) -> List[OrganismInfo]:
    """Get all available organisms/strains."""
    organisms = (
        db.query(Organism)
        .filter(Organism.taxonomic_rank == "Strain")
        .order_by(Organism.organism_order)
        .all()
    )

    return [
        OrganismInfo(
            organism_abbrev=org.organism_abbrev,
            organism_name=org.organism_name,
        )
        for org in organisms
    ]


def _get_feature_types(db: Session) -> List[str]:
    """Get feature types available for this application."""
    # Query distinct feature types that exist in the database
    feature_types = (
        db.query(Feature.feature_type)
        .distinct()
        .order_by(Feature.feature_type)
        .all()
    )

    # Put ORF first if present
    types = [ft[0] for ft in feature_types]
    if "ORF" in types:
        types.remove("ORF")
        types.insert(0, "ORF")

    return types


def _get_qualifiers(db: Session) -> List[str]:
    """Get distinct feature qualifiers for ORFs."""
    qualifiers = (
        db.query(FeatProperty.property_value)
        .join(Feature, FeatProperty.feature_no == Feature.feature_no)
        .filter(
            Feature.feature_type == "ORF",
            FeatProperty.property_type == "qualifier"
        )
        .distinct()
        .all()
    )

    # Order: regular qualifiers, then Merged, then Deleted
    regular = []
    merged = []
    deleted = []

    for (qual,) in qualifiers:
        if "Merged" in qual:
            merged.append(qual)
        elif "Deleted" in qual:
            deleted.append(qual)
        else:
            regular.append(qual)

    return sorted(regular) + sorted(merged) + sorted(deleted)


def _get_all_chromosomes(
    db: Session,
    organisms: List[OrganismInfo],
) -> Dict[str, List[str]]:
    """Get chromosomes grouped by organism."""
    result = {}

    for org in organisms:
        chromosomes = _get_chromosomes_for_organism(db, org.organism_abbrev)
        result[org.organism_abbrev] = chromosomes

    return result


def _get_chromosomes_for_organism(db: Session, organism_abbrev: str) -> List[str]:
    """Get chromosomes for a specific organism."""
    # Get chromosome features for this organism
    chromosomes = (
        db.query(Feature.feature_name)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            Organism.organism_abbrev == organism_abbrev,
            Feature.feature_type == "chromosome"
        )
        .order_by(Feature.feature_name)
        .all()
    )

    return [chr[0] for chr in chromosomes]


def _get_go_slim_terms(db: Session) -> GoSlimTerms:
    """Get GO Slim terms grouped by aspect."""
    go_slim_query = (
        db.query(GoSet.go_no, Go.goid, Go.go_term, Go.go_aspect)
        .join(Go, GoSet.go_no == Go.go_no)
        .filter(GoSet.go_set_name == GO_SLIM_SET_NAME)
        .order_by(Go.go_term)
        .all()
    )

    process = []
    function = []
    component = []

    for go_no, goid, go_term, go_aspect in go_slim_query:
        term = GoSlimTerm(
            goid=goid,
            goid_formatted=f"GO:{goid:07d}",
            term=go_term,
        )
        if go_aspect == "P":
            process.append(term)
        elif go_aspect == "F":
            function.append(term)
        elif go_aspect == "C":
            component.append(term)

    return GoSlimTerms(
        process=process,
        function=function,
        component=component,
    )


def _get_evidence_codes(db: Session) -> List[str]:
    """Get GO evidence codes."""
    # Query distinct evidence codes from go_annotation
    codes = (
        db.query(GoAnnotation.go_evidence)
        .distinct()
        .order_by(GoAnnotation.go_evidence)
        .all()
    )
    return [code[0] for code in codes]


def _get_annotation_methods(db: Session) -> List[str]:
    """Get GO annotation methods."""
    # Query distinct annotation types from go_annotation
    methods = (
        db.query(GoAnnotation.annotation_type)
        .distinct()
        .order_by(GoAnnotation.annotation_type)
        .all()
    )
    return [m[0] for m in methods]


def _filter_by_qualifiers(
    db: Session,
    feature_nos: Set[int],
    qualifiers: List[str],
) -> Tuple[Set[int], int]:
    """Filter features by qualifiers."""
    if not feature_nos:
        return set(), 0

    # Convert set to list for .in_() query
    feature_nos_list = list(feature_nos)

    matching = (
        db.query(FeatProperty.feature_no)
        .filter(
            FeatProperty.feature_no.in_(feature_nos_list),
            FeatProperty.property_type == "qualifier",
            FeatProperty.property_value.in_(qualifiers)
        )
        .distinct()
        .all()
    )

    result = set(f[0] for f in matching)
    return result, len(result)


def _exclude_deleted_features(
    db: Session,
    feature_nos: Set[int],
) -> Tuple[Set[int], int]:
    """Exclude deleted features (default behavior)."""
    if not feature_nos:
        return set(), 0

    # Convert set to list for .in_() query
    feature_nos_list = list(feature_nos)

    deleted = (
        db.query(FeatProperty.feature_no)
        .filter(
            FeatProperty.feature_no.in_(feature_nos_list),
            FeatProperty.property_type == "qualifier",
            FeatProperty.property_value.like("%Deleted%")
        )
        .distinct()
        .all()
    )

    deleted_nos = set(f[0] for f in deleted)
    result = feature_nos - deleted_nos
    return result, len(result)


def _filter_by_chromosomes(
    db: Session,
    feature_nos: Set[int],
    chromosomes: List[str],
) -> Tuple[Set[int], int]:
    """Filter features by chromosome location."""
    if not feature_nos:
        return set(), 0

    # Get chromosome feature numbers
    chr_feature_nos = (
        db.query(Feature.feature_no)
        .filter(Feature.feature_name.in_(chromosomes))
        .all()
    )
    chr_feature_no_list = [f[0] for f in chr_feature_nos]

    if not chr_feature_no_list:
        return set(), 0

    # Get seq_no for chromosomes
    chr_seq_nos = (
        db.query(Seq.seq_no)
        .filter(Seq.feature_no.in_(chr_feature_no_list))
        .all()
    )
    chr_seq_no_list = [s[0] for s in chr_seq_nos]

    if not chr_seq_no_list:
        return set(), 0

    # Get features located on these chromosomes
    # Convert set to list for .in_() query
    feature_nos_list = list(feature_nos)
    matching = (
        db.query(FeatLocation.feature_no)
        .filter(
            FeatLocation.feature_no.in_(feature_nos_list),
            FeatLocation.root_seq_no.in_(chr_seq_no_list),
            FeatLocation.is_loc_current == "Y"
        )
        .distinct()
        .all()
    )

    result = set(f[0] for f in matching)
    return result, len(result)


def _filter_by_introns(
    db: Session,
    feature_nos: Set[int],
    has_introns: bool,
) -> Tuple[Set[int], int]:
    """Filter features by intron presence."""
    if not feature_nos:
        return set(), 0

    # Convert set to list for .in_() query
    feature_nos_list = list(feature_nos)

    # Find features with intron subfeatures (rank=2 for subfeature relationship)
    # Feature types containing 'intron' (case-insensitive): intron, Intron, five_prime_UTR_intron, etc.
    features_with_introns = (
        db.query(FeatRelationship.parent_feature_no)
        .join(Feature, FeatRelationship.child_feature_no == Feature.feature_no)
        .filter(
            FeatRelationship.parent_feature_no.in_(feature_nos_list),
            FeatRelationship.rank == 2,
            func.lower(Feature.feature_type).like("%intron%")
        )
        .distinct()
        .all()
    )

    intron_nos = set(f[0] for f in features_with_introns)

    if has_introns:
        result = feature_nos & intron_nos
    else:
        result = feature_nos - intron_nos

    return result, len(result)


def _filter_by_go_terms(
    db: Session,
    feature_nos: Set[int],
    goids: List[int],
    annotation_methods: List[str],
    evidence_codes: List[str],
) -> Tuple[Set[int], int, Dict[int, Dict[str, List[GoTermBrief]]]]:
    """
    Filter features by GO term annotations.

    Returns:
        Tuple of (filtered feature_nos, count, go_annotations dict)
    """
    if not feature_nos or not goids:
        return feature_nos, len(feature_nos), {}

    # Get all descendant GOIDs for the selected GO Slim terms
    descendant_goids = _get_descendant_goids(db, goids)
    all_goids_to_search = set(goids) | descendant_goids

    # Get go_no for these GOIDs
    go_no_mapping = dict(
        db.query(Go.goid, Go.go_no)
        .filter(Go.goid.in_(all_goids_to_search))
        .all()
    )
    go_nos_to_search = set(go_no_mapping.values())

    # Build annotation query
    # Convert sets to lists for .in_() query
    feature_nos_list = list(feature_nos)
    go_nos_list = list(go_nos_to_search)

    ann_query = (
        db.query(GoAnnotation.feature_no, GoAnnotation.go_no, Go.goid, Go.go_term, Go.go_aspect)
        .join(Go, GoAnnotation.go_no == Go.go_no)
        .filter(
            GoAnnotation.feature_no.in_(feature_nos_list),
            GoAnnotation.go_no.in_(go_nos_list)
        )
    )

    if annotation_methods:
        ann_query = ann_query.filter(GoAnnotation.annotation_type.in_(annotation_methods))

    if evidence_codes:
        ann_query = ann_query.filter(GoAnnotation.go_evidence.in_(evidence_codes))

    annotations = ann_query.all()

    # Group annotations by feature
    feature_go_terms: Dict[int, Dict[str, List[GoTermBrief]]] = {}
    matching_features = set()

    for feature_no, go_no, goid, go_term, go_aspect in annotations:
        matching_features.add(feature_no)

        if feature_no not in feature_go_terms:
            feature_go_terms[feature_no] = {"P": [], "F": [], "C": []}

        term = GoTermBrief(goid=f"GO:{goid:07d}", term=go_term)

        # Avoid duplicates
        if term not in feature_go_terms[feature_no][go_aspect]:
            feature_go_terms[feature_no][go_aspect].append(term)

    # For multiple GO terms, features must be annotated to ALL terms (AND logic)
    if len(goids) > 1:
        # Check each feature has annotations matching all selected GO terms
        # This is a simplification - full implementation would check each original GOID
        pass  # For now, accept features with any matching annotation

    result = feature_nos & matching_features
    return result, len(result), feature_go_terms


def _get_descendant_goids(db: Session, ancestor_goids: List[int]) -> Set[int]:
    """Get all descendant GOIDs for given ancestor GOIDs."""
    # Query go_path for descendants
    descendants = (
        db.query(Go.goid)
        .join(GoPath, Go.go_no == GoPath.child_go_no)
        .join(Go, GoPath.ancestor_go_no == Go.go_no, isouter=True)
        .filter(
            db.query(Go.go_no)
            .filter(Go.goid.in_(ancestor_goids))
            .filter(GoPath.ancestor_go_no == Go.go_no)
            .exists()
        )
        .distinct()
        .all()
    )

    # Simpler approach: query directly
    ancestor_go_nos = (
        db.query(Go.go_no)
        .filter(Go.goid.in_(ancestor_goids))
        .all()
    )
    ancestor_go_no_set = set(g[0] for g in ancestor_go_nos)

    descendant_query = (
        db.query(Go.goid)
        .join(GoPath, Go.go_no == GoPath.child_go_no)
        .filter(GoPath.ancestor_go_no.in_(ancestor_go_no_set))
        .distinct()
        .all()
    )

    return set(d[0] for d in descendant_query)


def _sort_features(
    db: Session,
    feature_nos: Set[int],
    sort_by: str,
) -> List[int]:
    """Sort features by the specified field."""
    if not feature_nos:
        return []

    # Convert set to list for .in_() query
    feature_nos_list = list(feature_nos)

    if sort_by == "gene":
        # Sort by gene name (nulls last), then feature name
        sorted_features = (
            db.query(Feature.feature_no)
            .filter(Feature.feature_no.in_(feature_nos_list))
            .order_by(
                func.coalesce(Feature.gene_name, "ZZZZZ"),
                Feature.feature_name
            )
            .all()
        )
    elif sort_by == "feature_type":
        # Sort by feature type, then feature name
        sorted_features = (
            db.query(Feature.feature_no)
            .filter(Feature.feature_no.in_(feature_nos_list))
            .order_by(Feature.feature_type, Feature.feature_name)
            .all()
        )
    else:
        # Default: sort by feature name (ORF)
        sorted_features = (
            db.query(Feature.feature_no)
            .filter(Feature.feature_no.in_(feature_nos_list))
            .order_by(Feature.feature_name)
            .all()
        )

    return [f[0] for f in sorted_features]


def _get_feature_details(
    db: Session,
    feature_nos: List[int],
    show_position: bool,
    show_go_terms: bool,
    go_annotations: Dict[int, Dict[str, List[GoTermBrief]]],
) -> List[FeatureSearchResult]:
    """Get detailed feature information for results."""
    if not feature_nos:
        return []

    # Get basic feature info
    features = (
        db.query(Feature)
        .filter(Feature.feature_no.in_(feature_nos))
        .all()
    )

    # Create lookup by feature_no
    feature_lookup = {f.feature_no: f for f in features}

    # Get qualifiers
    qualifiers = (
        db.query(FeatProperty.feature_no, FeatProperty.property_value)
        .filter(
            FeatProperty.feature_no.in_(feature_nos),
            FeatProperty.property_type == "qualifier"
        )
        .all()
    )
    qualifier_lookup = {}
    for fno, qual in qualifiers:
        if fno not in qualifier_lookup:
            qualifier_lookup[fno] = []
        qualifier_lookup[fno].append(qual)

    # Get position info if needed
    position_lookup = {}
    if show_position:
        locations = (
            db.query(
                FeatLocation.feature_no,
                FeatLocation.strand,
                FeatLocation.start_coord,
                FeatLocation.stop_coord,
                FeatLocation.root_seq_no
            )
            .filter(
                FeatLocation.feature_no.in_(feature_nos),
                FeatLocation.is_loc_current == "Y"
            )
            .all()
        )

        # Get chromosome names for root_seq_nos
        root_seq_nos = set(loc[4] for loc in locations)
        chr_names = (
            db.query(Seq.seq_no, Feature.feature_name)
            .join(Feature, Seq.feature_no == Feature.feature_no)
            .filter(Seq.seq_no.in_(root_seq_nos))
            .all()
        )
        chr_lookup = {seq_no: name for seq_no, name in chr_names}

        for fno, strand, start, stop, root_seq_no in locations:
            position_lookup[fno] = {
                "strand": strand,
                "start": start,
                "stop": stop,
                "chromosome": chr_lookup.get(root_seq_no),
            }

    # Build results in the order of feature_nos
    results = []
    for fno in feature_nos:
        feature = feature_lookup.get(fno)
        if not feature:
            continue

        quals = qualifier_lookup.get(fno, [])
        qualifier_str = "|".join(quals) if quals else None

        pos = position_lookup.get(fno, {}) if show_position else {}
        go_terms = go_annotations.get(fno, {"P": [], "F": [], "C": []}) if show_go_terms else {"P": [], "F": [], "C": []}

        results.append(FeatureSearchResult(
            feature_id=feature.feature_no,
            orf=feature.feature_name,
            gene=feature.gene_name,
            feature_type=feature.feature_type,
            qualifier=qualifier_str,
            description=feature.headline,
            chromosome=pos.get("chromosome"),
            strand=pos.get("strand"),
            start_coord=pos.get("start"),
            stop_coord=pos.get("stop"),
            go_process_terms=go_terms.get("P", []),
            go_function_terms=go_terms.get("F", []),
            go_component_terms=go_terms.get("C", []),
        ))

    return results


def generate_download_tsv(
    db: Session,
    request: FeatureSearchRequest,
) -> str:
    """Generate TSV content for download."""
    # Execute search without pagination
    request.page = 1
    request.page_size = 10000  # Large limit for download

    response = search_features(db, request)

    if not response.success or not response.features:
        return "# No results found\n"

    # Build TSV
    lines = []

    # Header
    headers = ["Systematic_Name", "Gene_Name", "Feature_Type", "Qualifier", "Description"]
    if response.show_position:
        headers.extend(["Chromosome", "Strand", "Start", "Stop"])
    if response.show_go_terms:
        headers.extend(["GO_Process", "GO_Function", "GO_Component"])

    lines.append("\t".join(headers))

    # Data rows
    for result in response.features:
        row = [
            result.orf,
            result.gene or "",
            result.feature_type,
            result.qualifier or "",
            result.description or "",
        ]
        if response.show_position:
            row.extend([
                result.chromosome or "",
                result.strand or "",
                str(result.start_coord) if result.start_coord else "",
                str(result.stop_coord) if result.stop_coord else "",
            ])
        if response.show_go_terms:
            row.extend([
                "; ".join(t.term for t in result.go_process_terms[:5]),
                "; ".join(t.term for t in result.go_function_terms[:5]),
                "; ".join(t.term for t in result.go_component_terms[:5]),
            ])

        lines.append("\t".join(row))

    return "\n".join(lines)
