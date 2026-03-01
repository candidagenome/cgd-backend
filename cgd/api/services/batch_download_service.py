"""
Batch Download Service - handles bulk data download requests.
"""
from __future__ import annotations

import gzip
import io
from typing import List, Tuple, Dict

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_

from cgd.models.models import (
    Feature, Seq, FeatLocation, GoAnnotation,
    PhenoAnnotation, FeatHomology,
    DbxrefFeat, Dbxref, Organism,
)
from cgd.schemas.batch_download_schema import (
    DataType,
    BatchDownloadRequest,
    ResolvedFeature,
    FeatureNotFound,
)
from cgd.api.services.sequence_service import (
    get_sequence_by_feature,
    get_sequence_by_coordinates,
    format_as_fasta,
)
from cgd.schemas.sequence_schema import SeqType
from cgd.schemas.batch_download_schema import ChromosomalRegion


# GO aspect mapping
GO_ASPECT_MAP = {
    'function': 'F',
    'process': 'P',
    'component': 'C',
    'molecular_function': 'F',
    'biological_process': 'P',
    'cellular_component': 'C',
}

# Non-CGD ortholog sources
NON_CGD_ORTHOLOG_SOURCES = {
    'SGD': 'S. cerevisiae',
    'POMBASE': 'S. pombe',
    'AspGD': 'A. nidulans',
    'BROAD_NEUROSPORA': 'N. crassa',
}


def _chunk_list(lst: list, chunk_size: int = 900) -> list[list]:
    """Split a list into chunks of specified size (default 900 for Oracle's 1000 limit)."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def resolve_features(
    db: Session,
    queries: List[str],
    organism: str = None,
) -> Tuple[List[ResolvedFeature], List[FeatureNotFound]]:
    """
    Resolve feature names/identifiers to Feature objects.

    Uses batch queries for efficiency with large gene lists.

    Args:
        db: Database session
        queries: List of feature names/identifiers
        organism: Optional organism abbreviation to filter by (e.g., 'C_albicans_SC5314')

    Returns:
        Tuple of (found features, not found queries)
    """
    # Clean and dedupe queries
    query_map = {}  # upper -> original
    for q in queries:
        q_clean = q.strip()
        if q_clean:
            query_map[q_clean.upper()] = q_clean

    if not query_map:
        return [], []

    queries_upper = list(query_map.keys())

    # Batch query features by gene_name, feature_name, or dbxref_id
    features_by_query: Dict[str, Feature] = {}

    for chunk in _chunk_list(queries_upper):
        query = (
            db.query(Feature)
            .options(joinedload(Feature.organism))
            .filter(
                or_(
                    func.upper(Feature.gene_name).in_(chunk),
                    func.upper(Feature.feature_name).in_(chunk),
                    func.upper(Feature.dbxref_id).in_(chunk),
                )
            )
        )

        # Filter by organism if specified
        if organism:
            query = query.join(Organism, Feature.organism_no == Organism.organism_no)
            query = query.filter(Organism.organism_abbrev == organism)

        chunk_features = query.all()

        for feature in chunk_features:
            # Map by all possible identifiers
            if feature.gene_name:
                gn_upper = feature.gene_name.upper()
                if gn_upper in query_map:
                    features_by_query[gn_upper] = feature
            if feature.feature_name:
                fn_upper = feature.feature_name.upper()
                if fn_upper in query_map:
                    features_by_query[fn_upper] = feature
            if feature.dbxref_id:
                dx_upper = feature.dbxref_id.upper()
                if dx_upper in query_map:
                    features_by_query[dx_upper] = feature

    # Get unique features
    unique_features = {f.feature_no: f for f in features_by_query.values()}
    feature_nos = list(unique_features.keys())

    # Batch query locations
    locations_by_feature: Dict[int, FeatLocation] = {}
    if feature_nos:
        for chunk in _chunk_list(feature_nos):
            chunk_locations = (
                db.query(FeatLocation)
                .filter(
                    FeatLocation.feature_no.in_(chunk),
                    FeatLocation.is_loc_current == "Y"
                )
                .all()
            )
            for loc in chunk_locations:
                locations_by_feature[loc.feature_no] = loc

    # Get unique root_seq_nos for chromosome lookup
    root_seq_nos = list(set(
        loc.root_seq_no for loc in locations_by_feature.values()
        if loc.root_seq_no
    ))

    # Batch query chromosome names
    chromosome_by_seq: Dict[int, str] = {}
    if root_seq_nos:
        for chunk in _chunk_list(root_seq_nos):
            chunk_seqs = (
                db.query(Seq)
                .join(Feature, Seq.feature_no == Feature.feature_no)
                .filter(Seq.seq_no.in_(chunk))
                .all()
            )
            for seq in chunk_seqs:
                if seq.feature:
                    chromosome_by_seq[seq.seq_no] = seq.feature.feature_name

    # Build results
    found: List[ResolvedFeature] = []
    not_found: List[FeatureNotFound] = []

    # Track which queries were found
    found_queries = set()

    for feature in unique_features.values():
        location = locations_by_feature.get(feature.feature_no)

        chromosome = None
        start = None
        end = None
        strand = None

        if location:
            chromosome = chromosome_by_seq.get(location.root_seq_no)
            start = location.start_coord
            end = location.stop_coord
            strand = location.strand

        organism_name = feature.organism.organism_name if feature.organism else None

        found.append(ResolvedFeature(
            feature_no=feature.feature_no,
            feature_name=feature.feature_name,
            gene_name=feature.gene_name,
            dbxref_id=feature.dbxref_id,
            feature_type=feature.feature_type,
            organism_name=organism_name,
            chromosome=chromosome,
            start=start,
            end=end,
            strand=strand,
        ))

        # Mark queries as found
        if feature.gene_name:
            found_queries.add(feature.gene_name.upper())
        if feature.feature_name:
            found_queries.add(feature.feature_name.upper())
        if feature.dbxref_id:
            found_queries.add(feature.dbxref_id.upper())

    # Find not found queries
    for q_upper, q_original in query_map.items():
        if q_upper not in found_queries:
            not_found.append(FeatureNotFound(query=q_original, reason="not found"))

    return found, not_found


def generate_genomic_fasta(
    db: Session,
    features: List[ResolvedFeature],
    flank_left: int = 0,
    flank_right: int = 0,
) -> str:
    """Generate FASTA content for genomic sequences."""
    lines = []

    for feat in features:
        result = get_sequence_by_feature(
            db=db,
            query=feat.feature_name,
            seq_type=SeqType.GENOMIC,
            flank_left=flank_left,
            flank_right=flank_right,
        )
        if result:
            fasta = format_as_fasta(result.fasta_header, result.sequence)
            lines.append(fasta)

    return "\n".join(lines)


def generate_protein_fasta(
    db: Session,
    features: List[ResolvedFeature],
) -> str:
    """Generate FASTA content for protein sequences."""
    lines = []

    for feat in features:
        result = get_sequence_by_feature(
            db=db,
            query=feat.feature_name,
            seq_type=SeqType.PROTEIN,
        )
        if result:
            fasta = format_as_fasta(result.fasta_header, result.sequence)
            lines.append(fasta)

    return "\n".join(lines)


def generate_coding_fasta(
    db: Session,
    features: List[ResolvedFeature],
) -> str:
    """Generate FASTA content for coding sequences (CDS)."""
    lines = []

    for feat in features:
        result = get_sequence_by_feature(
            db=db,
            query=feat.feature_name,
            seq_type=SeqType.CODING,
        )
        if result:
            fasta = format_as_fasta(result.fasta_header, result.sequence)
            lines.append(fasta)

    return "\n".join(lines)


def generate_coords_tsv(
    db: Session,
    features: List[ResolvedFeature],
) -> str:
    """
    Generate tab-delimited coordinate information.

    Columns: feature_name, gene_name, dbxref_id, chromosome, start, end, strand, feature_type
    """
    lines = [
        "feature_name\tgene_name\tdbxref_id\tchromosome\tstart\tend\tstrand\tfeature_type"
    ]

    for feat in features:
        strand_str = "+" if feat.strand == "W" else "-" if feat.strand == "C" else ""
        lines.append(
            f"{feat.feature_name}\t"
            f"{feat.gene_name or ''}\t"
            f"{feat.dbxref_id}\t"
            f"{feat.chromosome or ''}\t"
            f"{feat.start or ''}\t"
            f"{feat.end or ''}\t"
            f"{strand_str}\t"
            f"{feat.feature_type}"
        )

    return "\n".join(lines)


def generate_go_gaf(
    db: Session,
    features: List[ResolvedFeature],
) -> str:
    """
    Generate GO annotations in GAF 2.2 format.

    GAF 2.2 columns:
    1. DB
    2. DB Object ID
    3. DB Object Symbol
    4. Qualifier
    5. GO ID
    6. DB:Reference
    7. Evidence Code
    8. With/From
    9. Aspect
    10. DB Object Name
    11. DB Object Synonym
    12. DB Object Type
    13. Taxon
    14. Date
    15. Assigned By
    16. Annotation Extension
    17. Gene Product Form ID
    """
    lines = [
        "!gaf-version: 2.2",
        "!Generated by CGD Batch Download",
    ]

    feature_nos = [f.feature_no for f in features]
    feat_map = {f.feature_no: f for f in features}

    # Query GO annotations (chunked to avoid Oracle 1000 limit)
    annotations = []
    for chunk in _chunk_list(feature_nos):
        chunk_results = (
            db.query(GoAnnotation)
            .options(joinedload(GoAnnotation.go))
            .filter(GoAnnotation.feature_no.in_(chunk))
            .all()
        )
        annotations.extend(chunk_results)

    for ga in annotations:
        feat = feat_map.get(ga.feature_no)
        if not feat or not ga.go:
            continue

        go = ga.go
        goid = f"GO:{ga.go.goid:07d}"
        aspect = GO_ASPECT_MAP.get(go.go_aspect.lower(), '')

        # Build GAF line
        cols = [
            "CGD",  # DB
            feat.dbxref_id,  # DB Object ID
            feat.gene_name or feat.feature_name,  # DB Object Symbol
            "",  # Qualifier
            goid,  # GO ID
            "CGD_REF:unspecified",  # DB:Reference
            ga.go_evidence,  # Evidence Code
            "",  # With/From
            aspect,  # Aspect
            feat.feature_name,  # DB Object Name
            "",  # DB Object Synonym
            feat.feature_type or "gene",  # DB Object Type
            "taxon:5476" if "albicans" in (feat.organism_name or "") else "taxon:0",  # Taxon
            ga.date_created.strftime("%Y%m%d") if ga.date_created else "",  # Date
            ga.source,  # Assigned By
            "",  # Annotation Extension
            "",  # Gene Product Form ID
        ]
        lines.append("\t".join(cols))

    return "\n".join(lines)


def generate_phenotype_tsv(
    db: Session,
    features: List[ResolvedFeature],
) -> str:
    """
    Generate phenotype data in tab-delimited format.

    Columns: feature_name, gene_name, observable, qualifier, experiment_type, mutant_type, source
    """
    lines = [
        "feature_name\tgene_name\tobservable\tqualifier\texperiment_type\tmutant_type\tsource"
    ]

    feature_nos = [f.feature_no for f in features]
    feat_map = {f.feature_no: f for f in features}

    # Query phenotype annotations (chunked to avoid Oracle 1000 limit)
    annotations = []
    for chunk in _chunk_list(feature_nos):
        chunk_results = (
            db.query(PhenoAnnotation)
            .options(joinedload(PhenoAnnotation.phenotype))
            .filter(PhenoAnnotation.feature_no.in_(chunk))
            .all()
        )
        annotations.extend(chunk_results)

    for pa in annotations:
        feat = feat_map.get(pa.feature_no)
        if not feat or not pa.phenotype:
            continue

        pheno = pa.phenotype
        lines.append(
            f"{feat.feature_name}\t"
            f"{feat.gene_name or ''}\t"
            f"{pheno.observable}\t"
            f"{pheno.qualifier or ''}\t"
            f"{pheno.experiment_type}\t"
            f"{pheno.mutant_type}\t"
            f"{pheno.source}"
        )

    return "\n".join(lines)


def generate_ortholog_tsv(
    db: Session,
    features: List[ResolvedFeature],
) -> str:
    """
    Generate ortholog data in tab-delimited format.

    Columns: feature_name, gene_name, ortholog_feature, ortholog_gene, ortholog_organism,
             ortholog_source, method
    """
    lines = [
        "feature_name\tgene_name\tortholog_feature\tortholog_gene\t"
        "ortholog_organism\tortholog_source\tmethod"
    ]

    for feat in features:
        # Get CGD orthologs (CGOB method)
        feat_homologies = (
            db.query(FeatHomology)
            .options(joinedload(FeatHomology.homology_group))
            .filter(FeatHomology.feature_no == feat.feature_no)
            .all()
        )

        for fh in feat_homologies:
            hg = fh.homology_group
            if not hg or hg.homology_group_type != 'ortholog':
                continue

            # Get other members in the group
            other_members = (
                db.query(FeatHomology)
                .filter(
                    FeatHomology.homology_group_no == hg.homology_group_no,
                    FeatHomology.feature_no != feat.feature_no,
                )
                .all()
            )

            for om in other_members:
                other_feat = (
                    db.query(Feature)
                    .options(joinedload(Feature.organism))
                    .filter(Feature.feature_no == om.feature_no)
                    .first()
                )
                if other_feat:
                    org_name = other_feat.organism.organism_name if other_feat.organism else ""
                    lines.append(
                        f"{feat.feature_name}\t"
                        f"{feat.gene_name or ''}\t"
                        f"{other_feat.feature_name}\t"
                        f"{other_feat.gene_name or ''}\t"
                        f"{org_name}\t"
                        f"CGD\t"
                        f"{hg.method}"
                    )

        # Get external orthologs
        dbxref_feats = (
            db.query(DbxrefFeat)
            .filter(DbxrefFeat.feature_no == feat.feature_no)
            .all()
        )

        for df in dbxref_feats:
            dbxref = (
                db.query(Dbxref)
                .filter(Dbxref.dbxref_no == df.dbxref_no)
                .first()
            )
            if dbxref and dbxref.source in NON_CGD_ORTHOLOG_SOURCES:
                species = NON_CGD_ORTHOLOG_SOURCES.get(dbxref.source, '')
                lines.append(
                    f"{feat.feature_name}\t"
                    f"{feat.gene_name or ''}\t"
                    f"{dbxref.dbxref_id}\t"
                    f"{dbxref.description or ''}\t"
                    f"{species}\t"
                    f"{dbxref.source}\t"
                    f"external"
                )

    return "\n".join(lines)


def compress_content(content: str) -> bytes:
    """Gzip compress content."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as f:
        f.write(content.encode('utf-8'))
    return buf.getvalue()


def generate_region_fasta(
    db: Session,
    regions: List[ChromosomalRegion],
) -> str:
    """Generate FASTA content for chromosomal regions."""
    lines = []

    for region in regions:
        result = get_sequence_by_coordinates(
            db=db,
            chromosome=region.chromosome,
            start=region.start,
            end=region.end,
            strand=region.strand,
        )
        if result:
            fasta = format_as_fasta(result.fasta_header, result.sequence)
            lines.append(fasta)

    return "\n".join(lines)


def process_batch_download(
    db: Session,
    request: BatchDownloadRequest,
) -> Tuple[Dict[DataType, Tuple[str, bytes]], List[ResolvedFeature], List[FeatureNotFound]]:
    """
    Process a batch download request.

    Returns:
        Tuple of:
        - Dict mapping data type to (filename, content bytes)
        - List of resolved features
        - List of not found queries
    """
    # Resolve features from gene names
    features = []
    not_found = []

    if request.genes:
        features, not_found = resolve_features(db, request.genes, request.organism)

    # Generate content for each data type
    results: Dict[DataType, Tuple[str, bytes]] = {}

    # Handle region-based requests separately for sequence data types
    has_regions = bool(request.regions and len(request.regions) > 0)

    for data_type in request.data_types:
        content = ""
        filename_base = "batch_download"

        if data_type == DataType.GENOMIC:
            # For regions, generate coordinate-based sequence
            if has_regions:
                content = generate_region_fasta(db, request.regions)
            else:
                content = generate_genomic_fasta(db, features)
            filename_base = "genomic_sequences"

        elif data_type == DataType.GENOMIC_FLANKING:
            # For regions, generate coordinate-based sequence (flanking not applicable)
            if has_regions:
                content = generate_region_fasta(db, request.regions)
            else:
                content = generate_genomic_fasta(
                    db, features,
                    flank_left=request.flank_left,
                    flank_right=request.flank_right
                )
            filename_base = "genomic_flanking_sequences"

        elif data_type == DataType.CODING:
            # Coding sequences only available for gene-based queries
            if not has_regions:
                content = generate_coding_fasta(db, features)
            filename_base = "coding_sequences"

        elif data_type == DataType.PROTEIN:
            # Protein sequences only available for gene-based queries
            if not has_regions:
                content = generate_protein_fasta(db, features)
            filename_base = "protein_sequences"

        elif data_type == DataType.COORDS:
            # Coordinates only for gene-based queries
            if not has_regions:
                content = generate_coords_tsv(db, features)
            filename_base = "coordinates"

        elif data_type == DataType.GO:
            # GO annotations only for gene-based queries
            if not has_regions:
                content = generate_go_gaf(db, features)
            filename_base = "go_annotations"

        elif data_type == DataType.PHENOTYPE:
            # Phenotypes only for gene-based queries
            if not has_regions:
                content = generate_phenotype_tsv(db, features)
            filename_base = "phenotypes"

        elif data_type == DataType.ORTHOLOG:
            # Orthologs only for gene-based queries
            if not has_regions:
                content = generate_ortholog_tsv(db, features)
            filename_base = "orthologs"

        if content:
            # Determine extension
            if data_type in (DataType.GENOMIC, DataType.GENOMIC_FLANKING,
                             DataType.CODING, DataType.PROTEIN):
                ext = ".fasta"
            elif data_type == DataType.GO:
                ext = ".gaf"
            else:
                ext = ".tsv"

            if request.compress:
                content_bytes = compress_content(content)
                filename = f"{filename_base}{ext}.gz"
            else:
                content_bytes = content.encode('utf-8')
                filename = f"{filename_base}{ext}"

            results[data_type] = (filename, content_bytes)

    return results, features, not_found
