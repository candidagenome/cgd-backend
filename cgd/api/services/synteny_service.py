"""Service for synteny viewer endpoint."""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from cgd.models.models import (
    Feature,
    FeatLocation,
    FeatHomology,
    HomologyGroup,
    Seq,
    FeatAlias,
    Alias,
)
from cgd.schemas.synteny_schema import (
    SyntenyGene,
    SyntenyRegion,
    OrthologConnection,
    QueryGene,
    SyntenyResponse,
)

logger = logging.getLogger(__name__)

# The Candida species in CGD with ortholog data for synteny comparison
CGD_SPECIES = [
    'Candida albicans SC5314',
    'Candida dubliniensis CD36',
    'Candida tropicalis MYA-3404',
    'Candida parapsilosis CDC317',
    'Candida auris B8441',
    'Candida glabrata CBS138',
]


def _get_organism_info(f) -> tuple[str, int]:
    """Extract organism name and taxon_id from a feature."""
    org = f.organism
    organism_name = None
    taxon_id = 0
    if org is not None:
        organism_name = (
            getattr(org, "organism_name", None)
            or getattr(org, "display_name", None)
            or getattr(org, "name", None)
        )
        taxon_id = getattr(org, "taxon_id", 0) or 0
    if not organism_name:
        organism_name = str(f.organism_no)
    return organism_name, taxon_id


def _get_chromosome_name(db: Session, root_seq_no: int) -> Optional[str]:
    """Get chromosome/contig name from root_seq_no via Seq -> Feature join."""
    result = (
        db.query(Feature.feature_name)
        .join(Seq, Seq.feature_no == Feature.feature_no)
        .filter(Seq.seq_no == root_seq_no)
        .first()
    )
    return result[0] if result else None


def _get_flanking_genes(
    db: Session,
    root_seq_no: int,
    center_start: int,
    flanking_count: int,
) -> list[tuple]:
    """
    Get flanking genes on the same chromosome.

    Returns tuples of (feature_no, feature_name, gene_name, start, stop, strand).
    Ordered by start coordinate.
    """
    # Get all ORFs on this chromosome ordered by start coord
    genes = (
        db.query(
            Feature.feature_no,
            Feature.feature_name,
            Feature.gene_name,
            FeatLocation.start_coord,
            FeatLocation.stop_coord,
            FeatLocation.strand,
        )
        .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
        .filter(
            FeatLocation.root_seq_no == root_seq_no,
            FeatLocation.is_loc_current == 'Y',
            func.lower(Feature.feature_type) == 'orf',
        )
        .order_by(FeatLocation.start_coord)
        .all()
    )

    # Find index of center gene
    center_idx = None
    for i, gene in enumerate(genes):
        if gene.start_coord == center_start:
            center_idx = i
            break

    if center_idx is None:
        # Center gene not found, try to find closest
        for i, gene in enumerate(genes):
            if gene.start_coord >= center_start:
                center_idx = i
                break
        if center_idx is None:
            center_idx = len(genes) - 1 if genes else 0

    # Get flanking genes
    start_idx = max(0, center_idx - flanking_count)
    end_idx = min(len(genes), center_idx + flanking_count + 1)

    return genes[start_idx:end_idx]


def _get_cgob_cluster_for_feature(
    db: Session,
    feature_no: int,
) -> Optional[tuple[str, int]]:
    """
    Get CGOB ortholog cluster info for a feature.

    Returns (homology_group_id, homology_group_no) or None.
    """
    result = (
        db.query(
            HomologyGroup.homology_group_id,
            HomologyGroup.homology_group_no,
        )
        .join(FeatHomology, FeatHomology.homology_group_no == HomologyGroup.homology_group_no)
        .filter(
            FeatHomology.feature_no == feature_no,
            HomologyGroup.homology_group_type == 'ortholog',
            HomologyGroup.method == 'CGOB',
        )
        .first()
    )
    return result if result else None


def _get_ortholog_members(
    db: Session,
    homology_group_no: int,
) -> list[str]:
    """Get all feature_names in a homology group."""
    results = (
        db.query(Feature.feature_name)
        .join(FeatHomology, FeatHomology.feature_no == Feature.feature_no)
        .filter(FeatHomology.homology_group_no == homology_group_no)
        .all()
    )
    return [r[0] for r in results]


def _find_cgob_cluster_for_gene(
    db: Session,
    query_feature: Feature,
) -> Optional[HomologyGroup]:
    """
    Find CGOB ortholog cluster for a gene.

    First checks the query feature's feat_homology. If not found, searches for
    other ORF features with the same gene_name in the same organism that have
    CGOB links. This handles cases where CGOB data was loaded with Assembly 22
    names (e.g., C1_13700W_A) but the query uses Assembly 19 names (e.g., orf19.5007).

    Args:
        db: Database session
        query_feature: The feature to find CGOB cluster for

    Returns:
        HomologyGroup if found, None otherwise
    """
    # First, check the query feature's own feat_homology
    for fh in query_feature.feat_homology:
        hg = fh.homology_group
        if hg and hg.homology_group_type == 'ortholog' and hg.method == 'CGOB':
            return hg

    # If not found and we have a gene_name, search for alternate assembly versions
    if not query_feature.gene_name:
        return None

    # Find other ORFs with the same gene_name in the same organism
    alternate_features = (
        db.query(Feature)
        .options(
            joinedload(Feature.feat_homology)
            .joinedload(FeatHomology.homology_group)
        )
        .filter(
            func.upper(Feature.gene_name) == func.upper(query_feature.gene_name),
            Feature.organism_no == query_feature.organism_no,
            func.lower(Feature.feature_type) == 'orf',
            Feature.feature_no != query_feature.feature_no,  # Exclude the query feature
        )
        .all()
    )

    for alt_feat in alternate_features:
        for fh in alt_feat.feat_homology:
            hg = fh.homology_group
            if hg and hg.homology_group_type == 'ortholog' and hg.method == 'CGOB':
                logger.debug(
                    f"Found CGOB cluster via alternate feature: "
                    f"{query_feature.feature_name} -> {alt_feat.feature_name}"
                )
                return hg

    return None


def _select_preferred_feature(features: list) -> Optional[Feature]:
    """
    Select the preferred feature from a list of candidates.

    Preference order:
    1. Features with current location (is_loc_current == 'Y')
    2. Features with feature_name starting with 'C' (Assembly 22 naming)
    3. Features with gene_name set
    4. First feature alphabetically by feature_name
    """
    if not features:
        return None
    if len(features) == 1:
        return features[0]

    # Filter to features with current location
    with_location = [
        f for f in features
        if any(fl.is_loc_current == 'Y' for fl in f.feat_location)
    ]
    if with_location:
        features = with_location

    if len(features) == 1:
        return features[0]

    # Prefer Assembly 22 features (feature_name starts with 'C')
    a22_features = [
        f for f in features
        if f.feature_name and f.feature_name.startswith('C')
    ]
    if a22_features:
        features = a22_features

    if len(features) == 1:
        return features[0]

    # Prefer features with gene_name set
    with_gene_name = [f for f in features if f.gene_name]
    if with_gene_name:
        features = with_gene_name

    # Sort by feature_name for deterministic selection
    features.sort(key=lambda f: f.feature_name or '')
    return features[0]


def get_synteny_data(
    db: Session,
    name: str,
    flanking_count: int = 10,
) -> SyntenyResponse:
    """
    Get synteny data for a locus across all CGD species.

    Args:
        db: Database session
        name: Locus name (gene_name, feature_name, dbxref_id, or alias)
        flanking_count: Number of genes upstream/downstream to include

    Returns:
        SyntenyResponse with query gene, synteny regions, and ortholog connections
    """
    n = name.strip()
    upper_n = func.upper(n)

    # Define common query options
    feature_options = [
        joinedload(Feature.organism),
        joinedload(Feature.feat_location),
        joinedload(Feature.feat_homology)
        .joinedload(FeatHomology.homology_group)
        .joinedload(HomologyGroup.feat_homology)
        .joinedload(FeatHomology.feature)
        .joinedload(Feature.organism),
    ]

    # Query for direct matches (gene_name, feature_name, dbxref_id)
    direct_features = (
        db.query(Feature)
        .options(*feature_options)
        .filter(
            or_(
                func.upper(Feature.gene_name) == upper_n,
                func.upper(Feature.feature_name) == upper_n,
                func.upper(Feature.dbxref_id) == upper_n,
            ),
            func.lower(Feature.feature_type) == 'orf',
        )
        .all()
    )

    # Prefer direct matches over alias matches
    # This prevents aliases (e.g., "MDR1" as alias for GYP2) from being selected
    # when there's an actual gene with that name (e.g., MDR1)
    query_feature = _select_preferred_feature(direct_features)

    if not query_feature:
        # No direct match found - try alias matches (e.g., CDC60B -> CDC60)
        alias_features = (
            db.query(Feature)
            .options(*feature_options)
            .join(FeatAlias, Feature.feature_no == FeatAlias.feature_no)
            .join(Alias, FeatAlias.alias_no == Alias.alias_no)
            .filter(
                func.upper(Alias.alias_name) == upper_n,
                func.lower(Feature.feature_type) == 'orf',
            )
            .all()
        )
        query_feature = _select_preferred_feature(alias_features)

    if not query_feature:
        raise ValueError(f"Locus not found: {name}")

    # Get query gene location
    query_loc = None
    query_root_seq_no = None
    for fl in query_feature.feat_location:
        if fl.is_loc_current == 'Y':
            query_loc = fl
            query_root_seq_no = fl.root_seq_no
            break

    if not query_loc:
        raise ValueError(f"No location found for locus: {name}")

    query_organism, _ = _get_organism_info(query_feature)
    query_chromosome = _get_chromosome_name(db, query_root_seq_no)

    # Build query gene info
    query_gene = QueryGene(
        feature_name=query_feature.feature_name,
        gene_name=query_feature.gene_name,
        organism=query_organism,
        chromosome=query_chromosome or '',
        start=query_loc.start_coord,
        stop=query_loc.stop_coord,
        strand=query_loc.strand,
    )

    # Find CGOB ortholog cluster for query gene
    # This also checks alternate assembly versions (e.g., Assembly 22 vs Assembly 19)
    cgob_cluster = _find_cgob_cluster_for_gene(db, query_feature)

    # Build synteny regions for each species
    synteny_regions: dict[str, SyntenyRegion] = {}
    ortholog_connections: dict[str, set] = {}  # ortholog_id -> set of feature_names
    feature_to_ortholog: dict[int, str] = {}  # feature_no -> ortholog_id

    # If we have a CGOB cluster, get all orthologs and their locations
    orthologs_by_species: dict[str, list] = {sp: [] for sp in CGD_SPECIES}

    if cgob_cluster:
        for fh in cgob_cluster.feat_homology:
            other_feat = fh.feature
            if other_feat:
                other_org, _ = _get_organism_info(other_feat)
                if other_org in orthologs_by_species:
                    orthologs_by_species[other_org].append(other_feat)
                    # Track ortholog connection
                    cluster_id = cgob_cluster.homology_group_id or f"CGOB_{cgob_cluster.homology_group_no}"
                    feature_to_ortholog[other_feat.feature_no] = cluster_id
                    if cluster_id not in ortholog_connections:
                        ortholog_connections[cluster_id] = set()
                    ortholog_connections[cluster_id].add(other_feat.feature_name)

    # For each species, get synteny region
    for species in CGD_SPECIES:
        orthologs = orthologs_by_species.get(species, [])

        if not orthologs:
            # No ortholog in this species - skip
            continue

        # Use first ortholog to center the view
        center_feat = orthologs[0]

        # Get location of center feature
        center_loc = (
            db.query(FeatLocation)
            .filter(
                FeatLocation.feature_no == center_feat.feature_no,
                FeatLocation.is_loc_current == 'Y',
            )
            .first()
        )

        if not center_loc:
            continue

        chromosome = _get_chromosome_name(db, center_loc.root_seq_no)
        if not chromosome:
            continue

        # Get flanking genes
        flanking = _get_flanking_genes(
            db,
            center_loc.root_seq_no,
            center_loc.start_coord,
            flanking_count,
        )

        # Build gene list
        genes = []
        for feat_no, feat_name, gene_name, start, stop, strand in flanking:
            # Check if this gene is the query ortholog
            is_query = feat_no == center_feat.feature_no

            # Check for ortholog group membership
            ortholog_id = feature_to_ortholog.get(feat_no)
            if not ortholog_id:
                # Check if this flanking gene has its own CGOB cluster
                cluster_info = _get_cgob_cluster_for_feature(db, feat_no)
                if cluster_info:
                    hg_id, hg_no = cluster_info
                    ortholog_id = hg_id or f"CGOB_{hg_no}"
                    # Get all members for this cluster
                    members = _get_ortholog_members(db, hg_no)
                    if ortholog_id not in ortholog_connections:
                        ortholog_connections[ortholog_id] = set()
                    ortholog_connections[ortholog_id].update(members)

            genes.append(SyntenyGene(
                feature_name=feat_name,
                gene_name=gene_name,
                start=start,
                stop=stop,
                strand=strand,
                is_query=is_query,
                ortholog_id=ortholog_id,
            ))

        synteny_regions[species] = SyntenyRegion(
            organism_name=species,
            chromosome=chromosome,
            genes=genes,
        )

    # Build ortholog connections list (only include groups with genes in multiple species)
    connections = []
    for orth_id, gene_set in ortholog_connections.items():
        if len(gene_set) >= 2:  # Only include if there are connections
            connections.append(OrthologConnection(
                ortholog_id=orth_id,
                genes=list(gene_set),
            ))

    return SyntenyResponse(
        query_gene=query_gene,
        synteny_regions=synteny_regions,
        ortholog_connections=connections,
    )
