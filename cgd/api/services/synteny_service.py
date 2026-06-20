"""Service for synteny viewer endpoint."""
from __future__ import annotations

import logging
from typing import Optional

import httpx
from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from cgd.core.settings import settings
from cgd.models.models import (
    Feature,
    FeatLocation,
    FeatHomology,
    FeatRelationship,
    HomologyGroup,
    Seq,
    FeatAlias,
    Alias,
    DbxrefHomology,
    Dbxref,
    DbxrefFeat,
)
from cgd.schemas.synteny_schema import (
    Exon,
    SyntenyGene,
    SyntenyRegion,
    OrthologConnection,
    QueryGene,
    SyntenyResponse,
    SyntenyResolveCandidate,
    SyntenyResolveResponse,
)
# Reuse the SGD entity resolver so ORF/systematic-name links resolve the same
# way the ortholog converter does (CGD stores no S. cerevisiae ORF names).
from cgd.api.services.ortholog_converter_service import _resolve_sgd_ids

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

# External reference species shown above the Candida group in the synteny viewer.
# S. cerevisiae gene neighborhoods are fetched live from the SGD backend; only
# the ortholog mapping (which SGD gene corresponds to a Candida gene) is local.
SC_REFERENCE_ORGANISM = 'Saccharomyces cerevisiae'
SGD_SYNTENY_TIMEOUT = 8.0

# Process-lifetime cache of successful SGD neighborhood lookups, keyed by
# (sgdid, flanking_count). Only successes are cached so a transient SGD outage
# does not poison later requests. SGD genome coordinates change rarely, and the
# cache is cleared on every deploy/restart.
_sgd_synteny_cache: dict[tuple[str, int], dict] = {}


def _fetch_sgd_synteny(sgdid: str, flanking_count: int) -> Optional[dict]:
    """Fetch a gene's neighborhood from the SGD backend. Best-effort: returns
    None if SGD is unavailable or the gene is not found."""
    key = (sgdid.upper(), flanking_count)
    if key in _sgd_synteny_cache:
        return _sgd_synteny_cache[key]

    url = f"{settings.sgd_backend_url.rstrip('/')}/locus/{sgdid}/synteny_neighbors"
    try:
        resp = httpx.get(
            url,
            params={"flanking": flanking_count},
            timeout=SGD_SYNTENY_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning(
                "SGD synteny lookup for %s returned status %s", sgdid, resp.status_code
            )
            return None
        data = resp.json()
    except (httpx.RequestError, ValueError) as exc:
        logger.warning("SGD synteny lookup for %s failed: %s", sgdid, exc)
        return None

    if data and data.get("neighbors"):
        _sgd_synteny_cache[key] = data
        return data
    return None


def _normalize_strand(strand: Optional[str]) -> str:
    """Normalize SGD '+'/'-' strands to CGD's 'W'/'C' for consistent display."""
    if strand == '+':
        return 'W'
    if strand == '-':
        return 'C'
    return strand or 'W'


def _sgd_orthologs_in_cluster(cluster: HomologyGroup) -> list[str]:
    """Return S. cerevisiae ortholog identifiers stored on a cluster.

    CGD records these as DbxrefHomology rows whose ``name`` is the organism
    ("Saccharomyces cerevisiae S288C") and whose ``dbxref.source`` is the CGOB
    pillar source (not 'SGD'), so match on the organism name like the ortholog
    converter does. The ``dbxref_id`` is the CGOB identifier, typically the
    systematic/ORF name (e.g. YLR113W) rather than an SGDID; the SGD endpoint
    resolves either form."""
    ids: list[str] = []
    for dh in cluster.dbxref_homology:
        dbx = dh.dbxref
        if dbx and dbx.dbxref_id and 'saccharomyces cerevisiae' in (dh.name or '').lower():
            ids.append(dbx.dbxref_id)
    return ids


def _build_sc_reference_region(
    query_clusters: list[HomologyGroup],
    sgdid_to_ortholog_id: dict[str, str],
    ortholog_connections: dict[str, set],
    flanking_count: int,
) -> Optional[SyntenyRegion]:
    """
    Build the S. cerevisiae external-reference region for the synteny viewer.

    The query gene's S. cerevisiae ortholog (SGDID) is resolved from local
    ortholog clusters; its gene neighborhood is then fetched live from the SGD
    backend. Each SGD neighbor is connected back to the Candida genes via the
    shared ortholog_id when one exists. Best-effort: returns None when the query
    gene has no S. cerevisiae ortholog or SGD is unavailable.
    """
    # Resolve the query gene's S. cerevisiae ortholog to center the SGD view.
    # This identifier (and the connection map keys) are CGOB ids, typically
    # systematic/ORF names (e.g. YLR113W) rather than SGDIDs.
    query_sc_id = None
    for cluster in query_clusters:
        sc_ids = _sgd_orthologs_in_cluster(cluster)
        if sc_ids:
            query_sc_id = sc_ids[0]
            break
    if not query_sc_id:
        return None

    data = _fetch_sgd_synteny(query_sc_id, flanking_count)
    if not data:
        return None

    query_sc_id_upper = query_sc_id.upper()

    genes: list[SyntenyGene] = []
    for n in data.get("neighbors", []):
        sgdid = (n.get("sgdid") or "").strip()
        systematic = (n.get("systematic_name") or "").strip()
        feature_name = systematic or sgdid
        if not feature_name:
            continue

        # CGOB stores S. cerevisiae orthologs by systematic name, but fall back
        # to SGDID so connections resolve regardless of which form was recorded.
        ortholog_id = None
        for key in (systematic, sgdid):
            if key and key.upper() in sgdid_to_ortholog_id:
                ortholog_id = sgdid_to_ortholog_id[key.upper()]
                break

        is_query = bool(n.get("is_query")) or (
            query_sc_id_upper in {systematic.upper(), sgdid.upper()}
        )

        if ortholog_id:
            ortholog_connections.setdefault(ortholog_id, set()).add(feature_name)

        exons = [
            Exon(start=e["start"], stop=e["stop"])
            for e in n.get("exons", [])
            if e.get("start") is not None and e.get("stop") is not None
        ]

        genes.append(SyntenyGene(
            feature_name=feature_name,
            gene_name=n.get("gene_name"),
            start=n.get("start"),
            stop=n.get("stop"),
            strand=_normalize_strand(n.get("strand")),
            is_query=is_query,
            ortholog_id=ortholog_id,
            exons=exons,
            external_url=f"https://www.yeastgenome.org/locus/{sgdid}" if sgdid else None,
        ))

    if not genes:
        return None

    return SyntenyRegion(
        organism_name=SC_REFERENCE_ORGANISM,
        chromosome=data.get("chromosome") or "",
        genes=genes,
        is_reference=True,
    )


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


def _get_exons_for_features(
    db: Session,
    feature_nos: list[int],
) -> dict[int, list[Exon]]:
    """
    Get exon coordinates for a list of features.

    Exons are stored as child features via FeatRelationship (rank=2).
    Returns a dict mapping feature_no to list of Exon objects with chromosome coordinates.
    """
    if not feature_nos:
        return {}

    # Query exons (CDS regions) for all features at once
    # Exons are child features linked via FeatRelationship with rank=2
    # and have feature_type 'CDS' (Coding Sequence)
    exon_rows = (
        db.query(
            FeatRelationship.parent_feature_no,
            FeatLocation.start_coord,
            FeatLocation.stop_coord,
        )
        .join(Feature, Feature.feature_no == FeatRelationship.child_feature_no)
        .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
        .filter(
            FeatRelationship.parent_feature_no.in_(feature_nos),
            FeatRelationship.rank == 2,  # rank 2 = subfeature
            func.upper(Feature.feature_type) == 'CDS',
            FeatLocation.is_loc_current == 'Y',
        )
        .order_by(
            FeatRelationship.parent_feature_no,
            FeatLocation.start_coord,
        )
        .all()
    )

    # Group by parent feature
    exons_by_feature: dict[int, list[Exon]] = {}
    for parent_no, start, stop in exon_rows:
        if parent_no not in exons_by_feature:
            exons_by_feature[parent_no] = []
        exons_by_feature[parent_no].append(Exon(start=start, stop=stop))

    return exons_by_feature


def _get_cgob_cluster_for_feature(
    db: Session,
    feature_no: int,
) -> Optional[tuple[str, int]]:
    """
    Get ortholog cluster info for a feature (CGOB or BLAST RBH).

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
            HomologyGroup.method.in_(['CGOB', 'BLAST RBH']),
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


def _get_all_ortholog_clusters_for_feature(
    db: Session,
    feature_no: int,
) -> list[HomologyGroup]:
    """
    Get all ortholog clusters (CGOB or BLAST RBH) that a feature belongs to.

    Returns list of HomologyGroup objects with their feat_homology eagerly loaded.
    """
    return (
        db.query(HomologyGroup)
        .options(
            joinedload(HomologyGroup.feat_homology)
            .joinedload(FeatHomology.feature)
            .joinedload(Feature.organism)
        )
        .join(FeatHomology, FeatHomology.homology_group_no == HomologyGroup.homology_group_no)
        .filter(
            FeatHomology.feature_no == feature_no,
            HomologyGroup.homology_group_type == 'ortholog',
            HomologyGroup.method.in_(['CGOB', 'BLAST RBH']),
        )
        .all()
    )


def _find_cgob_cluster_for_gene(
    db: Session,
    query_feature: Feature,
) -> Optional[HomologyGroup]:
    """
    Find ortholog cluster for a gene (CGOB or BLAST RBH).

    First checks the query feature's feat_homology. If not found, searches for
    other ORF features with the same gene_name in the same organism that have
    ortholog links. This handles cases where ortholog data was loaded with Assembly 22
    names (e.g., C1_13700W_A) but the query uses Assembly 19 names (e.g., orf19.5007).

    Args:
        db: Database session
        query_feature: The feature to find ortholog cluster for

    Returns:
        HomologyGroup if found, None otherwise
    """
    # First, check the query feature's own feat_homology
    for fh in query_feature.feat_homology:
        hg = fh.homology_group
        if hg and hg.homology_group_type == 'ortholog' and hg.method in ('CGOB', 'BLAST RBH'):
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
            if hg and hg.homology_group_type == 'ortholog' and hg.method in ('CGOB', 'BLAST RBH'):
                logger.debug(
                    f"Found ortholog cluster via alternate feature: "
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

    # Build synteny regions for each species
    synteny_regions: dict[str, SyntenyRegion] = {}
    ortholog_connections: dict[str, set] = {}  # ortholog_id -> set of feature_names
    feature_to_ortholog: dict[int, str] = {}  # feature_no -> ortholog_id
    sgdid_to_ortholog_id: dict[str, str] = {}  # S. cerevisiae CGOB id (e.g. YLR113W) -> ortholog_id

    # If we have a CGOB cluster, get all orthologs and their locations
    orthologs_by_species: dict[str, list] = {sp: [] for sp in CGD_SPECIES}
    processed_clusters: set[int] = set()  # Track processed cluster IDs to avoid duplicates

    # Use the primary cluster's ID as the unified ortholog_id for all query orthologs
    # This ensures all transitively connected genes share the same ortholog_id
    primary_ortholog_id: Optional[str] = None

    def add_orthologs_from_cluster(cluster: HomologyGroup, use_unified_id: bool = False):
        """Add orthologs from a cluster to the species dict."""
        nonlocal primary_ortholog_id

        if cluster.homology_group_no in processed_clusters:
            return
        processed_clusters.add(cluster.homology_group_no)

        cluster_id = cluster.homology_group_id or f"CGOB_{cluster.homology_group_no}"

        # Set the primary ortholog_id from the first (query gene's) cluster
        if primary_ortholog_id is None:
            primary_ortholog_id = cluster_id

        # Use unified ID for query orthologs, or cluster's own ID otherwise
        effective_id = primary_ortholog_id if use_unified_id else cluster_id

        for fh in cluster.feat_homology:
            other_feat = fh.feature
            if other_feat:
                other_org, _ = _get_organism_info(other_feat)
                if other_org in orthologs_by_species:
                    # Only add if not already in the list
                    if other_feat not in orthologs_by_species[other_org]:
                        orthologs_by_species[other_org].append(other_feat)
                    # Track ortholog connection with the effective ID
                    feature_to_ortholog[other_feat.feature_no] = effective_id
                    if effective_id not in ortholog_connections:
                        ortholog_connections[effective_id] = set()
                    ortholog_connections[effective_id].add(other_feat.feature_name)

        # Map this cluster's S. cerevisiae orthologs to the same ortholog_id so
        # the SGD reference row can be connected back to these Candida genes.
        for sgdid in _sgd_orthologs_in_cluster(cluster):
            sgdid_to_ortholog_id.setdefault(sgdid.upper(), effective_id)

    # Get ALL ortholog clusters for the query gene (not just the first one)
    # This is important for C. tropicalis which has separate pairwise BLAST RBH
    # clusters for each species instead of one CGOB cluster containing all orthologs
    all_query_clusters = _get_all_ortholog_clusters_for_feature(db, query_feature.feature_no)

    if all_query_clusters:
        # First, add orthologs from ALL of the query gene's direct clusters
        # This sets the primary_ortholog_id from the first cluster
        for cluster in all_query_clusters:
            add_orthologs_from_cluster(cluster, use_unified_id=True)

        # Transitive lookup: for each ortholog found, check if they belong to
        # additional clusters (e.g., C. tropicalis -> C. albicans via BLAST RBH,
        # then C. albicans -> other species via CGOB)
        # Use the unified ID so all query orthologs share the same ortholog_id
        for species, orthologs in list(orthologs_by_species.items()):
            for ortholog_feat in orthologs:
                # Get all ortholog clusters this feature belongs to
                additional_clusters = _get_all_ortholog_clusters_for_feature(
                    db, ortholog_feat.feature_no
                )
                for add_cluster in additional_clusters:
                    add_orthologs_from_cluster(add_cluster, use_unified_id=True)

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

        # Fetch exon data for all flanking genes at once
        flanking_feature_nos = [f[0] for f in flanking]
        exons_by_feature = _get_exons_for_features(db, flanking_feature_nos)

        # Build gene list
        genes = []
        for feat_no, feat_name, gene_name, start, stop, strand in flanking:
            # Check if this gene is the query ortholog
            is_query = feat_no == center_feat.feature_no

            # Check for ortholog group membership
            ortholog_id = feature_to_ortholog.get(feat_no)
            if not ortholog_id:
                # Get ALL ortholog clusters for this flanking gene (not just the first one)
                # This is important for C. tropicalis which has pairwise BLAST RBH clusters
                flanking_clusters = _get_all_ortholog_clusters_for_feature(db, feat_no)
                if flanking_clusters:
                    # First, check if any member of any cluster already has an ortholog_id
                    # This ensures we unify ortholog groups that are connected transitively
                    existing_id = None
                    for cluster in flanking_clusters:
                        for fh in cluster.feat_homology:
                            if fh.feature and fh.feature.feature_no in feature_to_ortholog:
                                existing_id = feature_to_ortholog[fh.feature.feature_no]
                                break
                        if existing_id:
                            break

                    # Use existing ID if found, otherwise create new one from first cluster
                    if existing_id:
                        ortholog_id = existing_id
                    else:
                        first_cluster = flanking_clusters[0]
                        ortholog_id = first_cluster.homology_group_id or f"CGOB_{first_cluster.homology_group_no}"

                    if ortholog_id not in ortholog_connections:
                        ortholog_connections[ortholog_id] = set()

                    # Collect members from ALL clusters and unify under the same ortholog_id
                    for cluster in flanking_clusters:
                        for fh in cluster.feat_homology:
                            if fh.feature:
                                ortholog_connections[ortholog_id].add(fh.feature.feature_name)
                                feature_to_ortholog[fh.feature.feature_no] = ortholog_id
                        # Connect this flanking gene's S. cerevisiae orthologs too
                        for sgdid in _sgd_orthologs_in_cluster(cluster):
                            sgdid_to_ortholog_id.setdefault(sgdid.upper(), ortholog_id)

            # Get exons for this gene (empty list if no introns)
            gene_exons = exons_by_feature.get(feat_no, [])

            genes.append(SyntenyGene(
                feature_name=feat_name,
                gene_name=gene_name,
                start=start,
                stop=stop,
                strand=strand,
                is_query=is_query,
                ortholog_id=ortholog_id,
                exons=gene_exons,
            ))

        synteny_regions[species] = SyntenyRegion(
            organism_name=species,
            chromosome=chromosome,
            genes=genes,
        )

    # Add the S. cerevisiae external-reference region (best-effort, live from SGD).
    # Must run before the connections list is finalized so SGD genes are linked.
    try:
        sc_region = _build_sc_reference_region(
            all_query_clusters,
            sgdid_to_ortholog_id,
            ortholog_connections,
            flanking_count,
        )
        if sc_region is not None:
            synteny_regions[SC_REFERENCE_ORGANISM] = sc_region
    except Exception as exc:  # never let the reference row break core synteny
        logger.warning("Failed to build S. cerevisiae reference region: %s", exc)

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


# C. albicans is CGD's reference organism; when an SGD gene maps to one ortholog
# per Candida species (the common case), the synteny view is centered there.
_PREFERRED_ANCHOR_ORGANISM = 'Candida albicans SC5314'


def _find_candida_features_for_sgd_gene(
    db: Session,
    tokens: set[str],
) -> tuple[list[Feature], dict]:
    """Find Candida ORF features cross-referenced to an S. cerevisiae gene.

    SGD orthologs are stored as ``Dbxref`` rows with ``source='SGD'`` where
    ``dbxref_id`` is the SGDID and ``description`` is the standard gene name; the
    link to Candida features is through ``DbxrefFeat``. Systematic/ORF names are
    not stored, so ``tokens`` should hold SGDID and/or gene-name forms.

    Returns ``(features, sgd_info)`` where ``sgd_info`` is
    ``{'sgdid': ..., 'gene_name': ...}`` taken from a matched dbxref (lets the
    happy path populate display fields without an SGD API call).
    """
    upper = {t.strip().upper() for t in tokens if t and t.strip()}
    if not upper:
        return [], {}

    rows = (
        db.query(Feature, Dbxref)
        .select_from(Dbxref)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
        .options(joinedload(Feature.organism))
        .filter(
            func.upper(Dbxref.source) == 'SGD',
            or_(
                func.upper(Dbxref.dbxref_id).in_(upper),
                func.upper(Dbxref.description).in_(upper),
            ),
            func.lower(Feature.feature_type) == 'orf',
        )
        .all()
    )

    seen: set[int] = set()
    features: list[Feature] = []
    sgd_info: dict = {}
    for feat, dbx in rows:
        if not sgd_info and dbx is not None:
            sgd_info = {"sgdid": dbx.dbxref_id, "gene_name": dbx.description}
        if feat.feature_no not in seen:
            seen.add(feat.feature_no)
            features.append(feat)
    return features, sgd_info


def resolve_synteny_target(
    db: Session,
    identifier: str,
    source: str = 'SGD',
) -> SyntenyResolveResponse:
    """Resolve an external gene identifier to the Candida ortholog(s) to view.

    Used by the SGD -> CGD synteny cross-link (e.g.
    ``/synteny-browser?gene=HOG1&source=SGD``). Gene names and SGDIDs match CGD's
    stored SGD dbxrefs directly; ORF/systematic names are resolved to their SGDID
    and gene name via the SGD API first. CGD remains the source of truth for the
    ortholog relationship.
    """
    identifier = (identifier or "").strip()
    if not identifier:
        raise ValueError("No gene identifier supplied")
    src = (source or "").strip().upper() or 'SGD'

    # 1) Direct match against CGD's stored SGD dbxrefs (gene name or SGDID).
    #    No external call; ORF/systematic names won't match here.
    features, sgd_info = _find_candida_features_for_sgd_gene(db, {identifier})

    input_gene_name: Optional[str] = None
    input_systematic_name: Optional[str] = None
    input_sgdid: Optional[str] = None

    # 2) Fall back to the SGD API for ORF names / aliases, then retry the match
    #    with the canonical SGDID and gene name CGD does store.
    if not features:
        resolved = _resolve_sgd_ids([identifier]).get(identifier.upper())
        if resolved:
            input_gene_name = resolved.get("gene_name")
            input_systematic_name = resolved.get("systematic_name")
            input_sgdid = resolved.get("sgdid")
            retry_tokens = {t for t in (input_sgdid, input_gene_name) if t}
            features, sgd_info = _find_candida_features_for_sgd_gene(db, retry_tokens)

    # Prefer SGD-API values for display; otherwise use the matched dbxref.
    input_sgdid = input_sgdid or sgd_info.get("sgdid")
    input_gene_name = input_gene_name or sgd_info.get("gene_name")

    if not features:
        label = input_gene_name or identifier
        return SyntenyResolveResponse(
            status="none",
            source=src,
            input_id=identifier,
            input_gene_name=input_gene_name,
            input_systematic_name=input_systematic_name,
            input_sgdid=input_sgdid,
            message=f"No Candida ortholog found in CGD for {src} gene {label}.",
        )

    def _candidate(f: Feature) -> SyntenyResolveCandidate:
        return SyntenyResolveCandidate(
            feature_name=f.feature_name,
            gene_name=f.gene_name,
            organism=_get_organism_info(f)[0],
            headline=getattr(f, "headline", None),
        )

    def _anchor_sort_key(f: Feature):
        org = _get_organism_info(f)[0]
        return (
            0 if org == _PREFERRED_ANCHOR_ORGANISM else 1,
            0 if f.gene_name else 1,
            f.feature_name or "",
        )

    features_sorted = sorted(features, key=_anchor_sort_key)

    # The SGD dbxref links one Candida ortholog per species (a single ortholog
    # set) -> one choice. A genuine choice only arises when a species has more
    # than one paralog mapped to the same SGD gene.
    per_org: dict[str, list[Feature]] = {}
    for f in features:
        per_org.setdefault(_get_organism_info(f)[0], []).append(f)
    multiple_loci = any(len(v) > 1 for v in per_org.values())

    if multiple_loci:
        return SyntenyResolveResponse(
            status="many",
            source=src,
            input_id=identifier,
            input_gene_name=input_gene_name,
            input_systematic_name=input_systematic_name,
            input_sgdid=input_sgdid,
            candidates=[_candidate(f) for f in features_sorted],
        )

    return SyntenyResolveResponse(
        status="one",
        source=src,
        input_id=identifier,
        input_gene_name=input_gene_name,
        input_systematic_name=input_systematic_name,
        input_sgdid=input_sgdid,
        target=_candidate(features_sorted[0]),
    )
