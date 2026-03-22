"""Service for genome-wide synteny browser endpoints."""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from cgd.models.models import (
    Feature,
    FeatLocation,
    FeatHomology,
    HomologyGroup,
    Seq,
    Organism,
)
from cgd.schemas.synteny_schema import (
    ChromosomeInfo,
    ChromosomeListResponse,
    GenomeGene,
    ChromosomeGenesResponse,
)

logger = logging.getLogger(__name__)

# The 5 Candida species in CGD with CGOB ortholog data for synteny comparison
CGD_SPECIES = [
    'Candida albicans SC5314',
    'Candida dubliniensis CD36',
    'Candida parapsilosis CDC317',
    'Candida auris B8441',
    'Candida glabrata CBS138',
]


def _get_chromosome_length(db: Session, chromosome_feature_no: int) -> int:
    """Get the sequence length of a chromosome from its Seq record."""
    result = (
        db.query(Seq.seq_length)
        .filter(
            Seq.feature_no == chromosome_feature_no,
            Seq.is_seq_current == 'Y',
            func.lower(Seq.seq_type) == 'genomic',
        )
        .first()
    )
    return result[0] if result else 0


def _count_genes_on_chromosome(db: Session, root_seq_no: int) -> int:
    """Count the number of ORFs on a chromosome."""
    count = (
        db.query(func.count(Feature.feature_no))
        .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
        .filter(
            FeatLocation.root_seq_no == root_seq_no,
            FeatLocation.is_loc_current == 'Y',
            func.lower(Feature.feature_type) == 'orf',
        )
        .scalar()
    )
    return count or 0


def _get_seq_no_for_chromosome(db: Session, chromosome_feature_no: int) -> Optional[int]:
    """Get the seq_no for a chromosome feature."""
    result = (
        db.query(Seq.seq_no)
        .filter(
            Seq.feature_no == chromosome_feature_no,
            Seq.is_seq_current == 'Y',
            func.lower(Seq.seq_type) == 'genomic',
        )
        .first()
    )
    return result[0] if result else None


def get_all_chromosomes(db: Session) -> ChromosomeListResponse:
    """
    Get list of all chromosomes for each CGD species.

    Returns chromosome name, length, and gene count for each.
    """
    chromosomes_by_organism: dict[str, list[ChromosomeInfo]] = {}

    for species in CGD_SPECIES:
        # Get organism record
        organism = (
            db.query(Organism)
            .filter(Organism.organism_name == species)
            .first()
        )

        if not organism:
            logger.warning(f"Organism not found: {species}")
            continue

        # Get all chromosome features for this organism
        chr_features = (
            db.query(Feature)
            .filter(
                Feature.organism_no == organism.organism_no,
                func.lower(Feature.feature_type) == 'chromosome',
            )
            .order_by(Feature.feature_name)
            .all()
        )

        chr_list = []
        for chr_feat in chr_features:
            # Get sequence length
            length = _get_chromosome_length(db, chr_feat.feature_no)
            if length == 0:
                continue  # Skip chromosomes without sequence data

            # Get seq_no for counting genes
            seq_no = _get_seq_no_for_chromosome(db, chr_feat.feature_no)
            if not seq_no:
                continue

            # Count genes on this chromosome
            gene_count = _count_genes_on_chromosome(db, seq_no)

            chr_list.append(ChromosomeInfo(
                organism_name=species,
                chromosome=chr_feat.feature_name,
                length=length,
                gene_count=gene_count,
            ))

        if chr_list:
            chromosomes_by_organism[species] = chr_list

    return ChromosomeListResponse(chromosomes=chromosomes_by_organism)


def _bulk_get_cgob_clusters(
    db: Session,
    feature_nos: list[int],
) -> dict[int, str]:
    """
    Efficiently get CGOB ortholog cluster IDs for multiple features.

    Returns dict mapping feature_no -> ortholog_id (CGOB cluster ID).
    """
    if not feature_nos:
        return {}

    results = (
        db.query(
            FeatHomology.feature_no,
            HomologyGroup.homology_group_id,
            HomologyGroup.homology_group_no,
        )
        .join(HomologyGroup, FeatHomology.homology_group_no == HomologyGroup.homology_group_no)
        .filter(
            FeatHomology.feature_no.in_(feature_nos),
            HomologyGroup.homology_group_type == 'ortholog',
            HomologyGroup.method == 'CGOB',
        )
        .all()
    )

    feature_to_ortholog = {}
    for feat_no, hg_id, hg_no in results:
        ortholog_id = hg_id or f"CGOB_{hg_no}"
        feature_to_ortholog[feat_no] = ortholog_id

    return feature_to_ortholog


def get_chromosome_genes(
    db: Session,
    chromosome_name: str,
    window_start: Optional[int] = None,
    window_end: Optional[int] = None,
) -> ChromosomeGenesResponse:
    """
    Get genes on a chromosome, optionally filtered by coordinate window.

    Args:
        db: Database session
        chromosome_name: Feature name of the chromosome
        window_start: Optional start coordinate for filtering
        window_end: Optional end coordinate for filtering

    Returns:
        ChromosomeGenesResponse with genes and their ortholog information
    """
    # Find the chromosome feature
    chr_feature = (
        db.query(Feature)
        .filter(
            func.upper(Feature.feature_name) == func.upper(chromosome_name),
            func.lower(Feature.feature_type) == 'chromosome',
        )
        .first()
    )

    if not chr_feature:
        raise ValueError(f"Chromosome not found: {chromosome_name}")

    # Get organism name
    organism = (
        db.query(Organism)
        .filter(Organism.organism_no == chr_feature.organism_no)
        .first()
    )
    organism_name = organism.organism_name if organism else str(chr_feature.organism_no)

    # Get chromosome sequence info
    chr_length = _get_chromosome_length(db, chr_feature.feature_no)
    seq_no = _get_seq_no_for_chromosome(db, chr_feature.feature_no)

    if not seq_no:
        raise ValueError(f"No sequence found for chromosome: {chromosome_name}")

    # Build query for genes on this chromosome
    query = (
        db.query(
            Feature.feature_no,
            Feature.feature_name,
            Feature.gene_name,
            Feature.headline,
            FeatLocation.start_coord,
            FeatLocation.stop_coord,
            FeatLocation.strand,
        )
        .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
        .filter(
            FeatLocation.root_seq_no == seq_no,
            FeatLocation.is_loc_current == 'Y',
            func.lower(Feature.feature_type) == 'orf',
        )
    )

    # Apply coordinate window filter if specified
    if window_start is not None:
        query = query.filter(FeatLocation.stop_coord >= window_start)
    if window_end is not None:
        query = query.filter(FeatLocation.start_coord <= window_end)

    # Order by start coordinate
    query = query.order_by(FeatLocation.start_coord)

    genes_data = query.all()

    # Get all feature_nos for bulk ortholog lookup
    feature_nos = [g.feature_no for g in genes_data]
    feature_to_ortholog = _bulk_get_cgob_clusters(db, feature_nos)

    # Build gene list
    genes = []
    for g in genes_data:
        ortholog_id = feature_to_ortholog.get(g.feature_no)

        genes.append(GenomeGene(
            feature_name=g.feature_name,
            gene_name=g.gene_name,
            start=g.start_coord,
            stop=g.stop_coord,
            strand=g.strand,
            ortholog_id=ortholog_id,
            headline=g.headline,
        ))

    return ChromosomeGenesResponse(
        organism_name=organism_name,
        chromosome=chr_feature.feature_name,
        chromosome_length=chr_length,
        genes=genes,
        window_start=window_start,
        window_end=window_end,
    )
