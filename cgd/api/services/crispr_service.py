"""
CRISPR Guide RNA Designer Service.

Handles guide RNA design, off-target analysis, and efficiency prediction
for Candida species.
"""
from __future__ import annotations

import logging
import re
import subprocess
import tempfile
import os
from typing import Optional, List, Dict, Tuple
from collections import defaultdict
from urllib.parse import quote
from sqlalchemy.orm import Session, aliased
from sqlalchemy import func

from cgd.core.settings import settings
from cgd.models.models import (
    Feature, Seq, FeatLocation, Organism, Phenotype, PhenoAnnotation,
    HomologyGroup, FeatHomology
)
from cgd.api.services.virulence_service import (
    _is_housekeeping_gene,
    _get_ortholog_count,
)
from cgd.schemas.virulence_schema import VIRULENCE_CATEGORIES
from cgd.schemas.crispr_schema import (
    PAMType,
    TargetRegion,
    OffTargetMethod,
    CrisprDesignRequest,
    CrisprDesignResponse,
    CrisprConfigResponse,
    GuideResult,
    GeneInfo,
    OffTargetHit,
    RestrictionSite,
    CloningPrimers,
    HomologyArms,
)
from cgd.api.services.azimuth_minimal import (
    predict_efficiency as azimuth_predict,
    is_model_available as azimuth_available,
)

logger = logging.getLogger(__name__)


def _map_organism_tag_to_abbrev(organism_tag: str) -> str:
    """
    Map an organism tag to database organism_abbrev.

    E.g., "C_albicans_SC5314_A22" -> "C_albicans_SC5314" (strip assembly suffix)
         "C_tropicalis_MYA-3404" -> "C_tropicalis" (strip strain suffix)
    """
    # Strip assembly suffixes like _A19, _A21, _A22
    result = re.sub(r"_A\d+$", "", organism_tag)
    # Strip strain suffixes like _MYA-3404
    result = re.sub(r"_MYA-\d+$", "", result)
    return result


# ============================================================================
# Configuration and Limits
# ============================================================================

# Request limits to prevent server overload
MAX_GUIDES_PER_REQUEST = 100
MAX_SEQUENCE_LENGTH = 50000  # 50kb max input sequence
MAX_OFFTARGET_GENOMES = 3
CRISPR_TIMEOUT = 300  # 5 minutes max for off-target search
UPSTREAM_REGION_LENGTH = 500  # bp upstream of CDS for 5' upstream targeting

# PAM patterns (5' to 3', where N = any nucleotide)
PAM_PATTERNS: Dict[PAMType, dict] = {
    PAMType.NGG: {
        "pattern": r"[ACGT]GG",
        "length": 3,
        "position": "3prime",  # PAM is 3' of guide
        "display": "NGG (SpCas9)",
    },
    PAMType.NAG: {
        "pattern": r"[ACGT]AG",
        "length": 3,
        "position": "3prime",
        "display": "NAG (SpCas9, lower efficiency)",
    },
    PAMType.NNGRRT: {
        "pattern": r"[ACGT][ACGT]G[AG][AG]T",
        "length": 6,
        "position": "3prime",
        "display": "NNGRRT (SaCas9)",
    },
    PAMType.TTTV: {
        "pattern": r"TTT[ACG]",
        "length": 4,
        "position": "5prime",  # PAM is 5' of guide for Cas12a
        "display": "TTTV (Cas12a/Cpf1)",
    },
}

# Cloning vector systems with their overhang sequences
CLONING_SYSTEMS = {
    "pX330": {
        "name": "pX330 (BbsI)",
        "enzyme": "BbsI",
        "forward_prefix": "CACCG",
        "reverse_prefix": "AAAC",
        "reverse_suffix": "C",
        "description": "Addgene #42230, human codon-optimized SpCas9",
    },
    "pV1093": {
        "name": "pV1093 (BsmBI)",
        "enzyme": "BsmBI",
        "forward_prefix": "CGTCTC",
        "reverse_prefix": "CGTCTC",
        "reverse_suffix": "",
        "description": "Candida-optimized CRISPR vector",
    },
}

# Common restriction enzymes to check for
RESTRICTION_ENZYMES = {
    "BbsI": "GAAGAC",
    "BsaI": "GGTCTC",
    "BsmBI": "CGTCTC",
    "EcoRI": "GAATTC",
    "BamHI": "GGATCC",
    "HindIII": "AAGCTT",
    "NotI": "GCGGCCGC",
}

# Rule Set 2 position-dependent nucleotide scores (Doench et al. 2016)
# Simplified version - full implementation would use the complete scoring matrix
POSITION_SCORES = {
    # Position 1 (5' end of guide)
    1: {"A": 0, "C": 0, "G": -0.2, "T": 0},
    # Position 20 (3' end, PAM-proximal)
    20: {"A": 0.1, "C": 0, "G": 0.1, "T": -0.1},
}


# ============================================================================
# Helper Functions
# ============================================================================

def _reverse_complement(seq: str) -> str:
    """Return reverse complement of DNA sequence."""
    complement = {"A": "T", "T": "A", "G": "C", "C": "G", "N": "N"}
    return "".join(complement.get(base.upper(), "N") for base in reversed(seq))


def _calculate_gc_content(seq: str) -> float:
    """Calculate GC content as percentage."""
    seq = seq.upper()
    gc_count = seq.count("G") + seq.count("C")
    return (gc_count / len(seq)) * 100 if seq else 0


def _has_poly_t(seq: str) -> bool:
    """Check if sequence contains TTTT (Pol III terminator signal)."""
    return "TTTT" in seq.upper()


def _calculate_self_complementarity(seq: str, stem_length: int = 4) -> int:
    """
    Count potential guide self-complementarity stems.

    CHOPCHOP penalizes guides with self-complementarity longer than 3nt.
    We count non-overlapping 4nt windows that can pair with another 4nt
    window in the same guide.
    """
    seq = seq.upper()
    if len(seq) < stem_length * 2:
        return 0

    stems = set()
    for i in range(0, len(seq) - stem_length + 1):
        left = seq[i:i + stem_length]
        if not re.match(r"^[ACGT]+$", left):
            continue

        for j in range(i + stem_length, len(seq) - stem_length + 1):
            right = seq[j:j + stem_length]
            if left == _reverse_complement(right):
                stems.add((i, j))

    return len(stems)


def _find_restriction_sites(seq: str) -> List[RestrictionSite]:
    """Find restriction enzyme sites within a sequence."""
    sites = []
    seq_upper = seq.upper()

    for enzyme, recognition in RESTRICTION_ENZYMES.items():
        for match in re.finditer(recognition, seq_upper):
            sites.append(RestrictionSite(
                enzyme=enzyme,
                position=match.start(),
                sequence=recognition,
            ))

    return sites


def _calculate_efficiency_score_heuristic(guide: str) -> float:
    """
    Calculate predicted efficiency score using simplified heuristics.

    This is a fallback method when Azimuth model is unavailable or when
    the guide context is insufficient (non-NGG PAM, missing flanking sequence).

    Returns score from 0-100, where higher is better.
    """
    score = 50.0  # baseline
    guide = guide.upper()

    # GC content penalty (optimal is 40-70%)
    gc = _calculate_gc_content(guide)
    if gc < 40:
        score -= (40 - gc) * 0.5
    elif gc > 70:
        score -= (gc - 70) * 0.5

    # Poly-T penalty (strong)
    if _has_poly_t(guide):
        score -= 20

    # Position-specific nucleotide preferences
    # Favor G at position 20 (PAM-proximal)
    if len(guide) >= 20:
        if guide[-1] == "G":
            score += 5
        elif guide[-1] == "C":
            score += 2

    # Penalize G at position 1
    if guide[0] == "G":
        score -= 3

    # Favor C at position 3
    if len(guide) >= 3 and guide[2] == "C":
        score += 3

    # Clamp to 0-100
    return max(0, min(100, score))


def _get_30mer_context(
    target_sequence: str,
    guide_position: int,
    guide_length: int = 20,
    pam_length: int = 3,
    strand: str = "+"
) -> Optional[str]:
    """
    Extract 30-mer context for Azimuth efficiency prediction.

    The 30-mer format required by Azimuth/Rule Set 2:
        - Positions 1-4: 4bp upstream of guide
        - Positions 5-24: 20bp guide sequence
        - Positions 25-27: 3bp PAM (NGG)
        - Positions 28-30: 3bp downstream of PAM

    Args:
        target_sequence: The full target sequence
        guide_position: 1-based position of the guide in target_sequence
        guide_length: Length of the guide (default 20)
        pam_length: Length of the PAM (default 3)
        strand: Strand of the guide ("+" or "-")

    Returns:
        30-mer sequence or None if context cannot be extracted
    """
    seq = target_sequence.upper()
    pos = guide_position - 1  # Convert to 0-based

    # For 3' PAM (NGG): guide is at [pos:pos+guide_length], PAM at [pos+guide_length:pos+guide_length+pam_length]
    # We need 4bp upstream + 20bp guide + 3bp PAM + 3bp downstream = 30bp total

    if strand == "+":
        # Forward strand: guide-PAM orientation
        upstream_start = pos - 4
        downstream_end = pos + guide_length + pam_length + 3

        if upstream_start < 0 or downstream_end > len(seq):
            return None

        context_30mer = seq[upstream_start:downstream_end]
    else:
        # Reverse strand: the guide is stored in reverse complement orientation
        # The guide_position points to the leftmost coordinate in the original sequence
        # For reverse strand, we need to extract and reverse complement

        # Calculate positions for reverse strand
        # Guide spans [pos:pos+guide_length], PAM is upstream in genomic coords
        # For reverse complement: PAM is at [pos-pam_length:pos], downstream is [pos-pam_length-3:pos-pam_length]
        downstream_start = pos - pam_length - 3
        upstream_end = pos + guide_length + 4

        if downstream_start < 0 or upstream_end > len(seq):
            return None

        # Extract and reverse complement
        context_region = seq[downstream_start:upstream_end]
        context_30mer = _reverse_complement(context_region)

    # Validate length
    if len(context_30mer) != 30:
        return None

    # Validate contains only ACGT
    if not re.match(r'^[ACGT]+$', context_30mer):
        return None

    return context_30mer


def _calculate_efficiency_score(
    guide: str,
    context_30mer: Optional[str] = None,
    pam_type: PAMType = PAMType.NGG
) -> Tuple[float, str]:
    """
    Calculate predicted efficiency score using Azimuth (Rule Set 2) or fallback heuristic.

    Attempts to use the Azimuth model if:
    - context_30mer is provided and valid
    - PAM type is NGG (Azimuth is optimized for SpCas9)
    - Model is available

    Falls back to heuristic scoring otherwise.

    Args:
        guide: 20bp guide sequence
        context_30mer: Optional 30-mer context for Azimuth prediction
        pam_type: PAM type (Azimuth only supports NGG)

    Returns:
        Tuple of (score from 0-100, method used: "azimuth" or "heuristic")
    """
    # Only use Azimuth for NGG PAM (SpCas9) - it's not trained for other PAMs
    if pam_type != PAMType.NGG:
        return _calculate_efficiency_score_heuristic(guide), "heuristic"

    # Try Azimuth if we have context
    if context_30mer and azimuth_available():
        azimuth_score = azimuth_predict(context_30mer)
        if azimuth_score is not None:
            # Convert from 0-1 to 0-100
            return round(azimuth_score * 100, 1), "azimuth"

    # Fallback to heuristic
    return _calculate_efficiency_score_heuristic(guide), "heuristic"


def _calculate_cfd_score(guide: str, offtarget: str) -> float:
    """
    Calculate CFD (Cutting Frequency Determination) score.

    Returns score from 0-1, where 1 = perfect match, 0 = no cutting expected.
    This is a simplified version; production would use the full CFD matrix.
    """
    if len(guide) != len(offtarget):
        return 0.0

    guide = guide.upper()
    offtarget = offtarget.upper()

    score = 1.0
    for i, (g, o) in enumerate(zip(guide, offtarget)):
        if g != o:
            # Position-dependent mismatch penalty
            # Mismatches in seed region (last 12bp) are more costly
            if i >= len(guide) - 12:
                score *= 0.3  # Seed region mismatch
            else:
                score *= 0.7  # Non-seed mismatch

    return score


CHOPCHOP_OFFTARGET_PENALTIES = {
    0: 1000,
    1: 800,
    2: 600,
    3: 400,
}


def _calculate_chopchop_penalty(
    guide: str,
    efficiency_score: float,
    offtargets: List[OffTargetHit],
    gc_content: Optional[float] = None,
    position: Optional[int] = None,
    cds_length: Optional[int] = None,
    target_region: Optional[str] = None,
) -> float:
    """
    Calculate a CHOPCHOP-style rank penalty.

    CHOPCHOP ranks guides by a penalty model where:
    - Off-target burden dominates (fewer/weaker off-targets = lower penalty)
    - GC outside 40-70% range is penalized
    - Self-complementarity is penalized
    - Higher efficiency lowers the penalty
    - For knockout targeting, 5' positions are favored

    Lower values are better.
    """
    guide = guide.upper()
    gc = _calculate_gc_content(guide) if gc_content is None else gc_content

    penalty = 0.0

    # Off-target penalties dominate the ranking
    if len(offtargets) > 100:
        penalty += 20000

    for offtarget in offtargets:
        penalty += CHOPCHOP_OFFTARGET_PENALTIES.get(offtarget.mismatches, 0)

    # GC penalty for guides outside optimal 40-70% range
    if gc < 40 or gc > 70:
        penalty += 500

    # Self-complementarity can cause hairpin formation
    penalty += _calculate_self_complementarity(guide)

    # Efficiency lowers the penalty (higher efficiency = lower penalty)
    # Scale by 100 to match CHOPCHOP's actual weighting where efficiency
    # competes meaningfully with off-target penalties (which are 400-1000 each)
    penalty -= efficiency_score * 100

    # Position bonus for 5' targeting (knockout mode)
    # For gene knockouts, guides in the 5' region of the CDS are strongly preferred
    # because they disrupt the protein earlier, creating truncated/non-functional products.
    # CHOPCHOP weighs position significantly for 5' targeting - guides in the first 10-15%
    # of CDS should rank much higher than those at 40-50%, even with slightly lower efficiency.
    # These bonuses are scaled to compete meaningfully with efficiency differences (0-10000).
    if (
        position is not None
        and cds_length is not None
        and cds_length > 0
        and target_region in ("5_prime", "five_prime", TargetRegion.FIVE_PRIME)
    ):
        pct = position / cds_length
        if pct <= 0.10:
            # First 10% of CDS: strong bonus to prioritize early knockouts
            penalty -= 2000
        elif pct <= 0.15:
            # 10-15% of CDS: significant bonus
            penalty -= 1500
        elif pct <= 0.25:
            # 15-25% of CDS: moderate bonus
            penalty -= 1000
        elif pct <= 0.35:
            # 25-35% of CDS: small bonus
            penalty -= 500
        # 35-50% of CDS: no bonus (still within target region but not preferred)

    return round(penalty, 3)


def _chopchop_penalty_to_display_score(penalty: float) -> float:
    """
    Convert lower-is-better CHOPCHOP penalty to the existing 0-100 UI score.

    With efficiency scaled by 100, penalties typically range from:
    - Excellent guides: -10,000 to -5,000 (high efficiency, few off-targets)
    - Good guides: -5,000 to 0
    - Poor guides: 0 to +5,000 (low efficiency or many off-targets)
    - Very poor: +5,000 to +20,000+

    We map this to 0-100 where:
    - Penalty <= -8000: score = 100
    - Penalty >= +2000: score = 0
    - Linear interpolation between
    """
    # Map penalty range [-8000, +2000] to score range [100, 0]
    # Score = 100 - (penalty + 8000) / 100
    score = 100.0 - (penalty + 8000) / 100.0
    return round(max(0.0, min(100.0, score)), 1)


def _generate_cloning_primers(
    guide: str,
    system: str = "pX330"
) -> CloningPrimers:
    """Generate cloning primers for the guide."""
    config = CLONING_SYSTEMS.get(system, CLONING_SYSTEMS["pX330"])

    # Forward: 5'-overhang + G (if not present) + guide-3'
    forward = config["forward_prefix"]
    if not guide.upper().startswith("G"):
        forward += "G"
    forward += guide.upper()

    # Reverse: 5'-overhang + reverse complement + suffix-3'
    rev_comp = _reverse_complement(guide)
    if not guide.upper().startswith("G"):
        rev_comp = rev_comp[:-1]  # Remove extra C from rev comp
    reverse = config["reverse_prefix"] + rev_comp + config["reverse_suffix"]

    return CloningPrimers(
        forward=forward,
        reverse=reverse,
        vector_system=config["name"],
    )


def _generate_jbrowse_url(
    organism_tag: str,
    chromosome: str,
    start: int,
    end: int,
    flank: int = 500,
) -> Optional[str]:
    """
    Generate JBrowse2 URL for viewing a genomic region.

    Args:
        organism_tag: Organism identifier (e.g., "C_albicans_SC5314_A22")
        chromosome: Chromosome/contig name
        start: Start coordinate
        end: End coordinate
        flank: Flanking region to include (default 500bp)

    Returns:
        JBrowse2 URL or None if not possible
    """
    if not chromosome or not start or not end:
        return None

    # JBrowse2 only has certain assemblies/chromosomes available
    # For C. albicans, only A22 (Ca22*) chromosomes are in JBrowse
    # Skip if chromosome doesn't match JBrowse-available format
    if "albicans" in organism_tag.lower():
        if not chromosome.startswith("Ca22"):
            return None

    # Map organism tag to JBrowse assembly name
    # Strip assembly suffix (e.g., _A22) for JBrowse2
    assembly = _map_organism_tag_to_abbrev(organism_tag)

    # Get the correct gene features track name for this assembly
    # C. albicans uses "TranscribedFeatures", others use "{assembly}_features.sorted.gff"
    if "albicans" in organism_tag.lower():
        tracks = "TranscribedFeatures"
    else:
        tracks = f"{assembly}_features.sorted.gff"

    # Calculate coordinates with flanking
    low = max(1, min(start, end) - flank)
    high = max(start, end) + flank

    # URL-encode the location
    loc_encoded = quote(f"{chromosome}:{low}..{high}", safe='')

    url = f"{settings.jbrowse_base_url}?assembly={assembly}&loc={loc_encoded}&tracks={tracks}"
    return url


def _generate_guide_jbrowse_url(
    organism_tag: str,
    chromosome: str,
    guide_start: int,
    guide_end: int,
) -> Optional[str]:
    """
    Generate JBrowse2 URL for viewing a specific guide location.

    Uses a smaller flanking region (100bp) to zoom in on the guide.
    """
    return _generate_jbrowse_url(organism_tag, chromosome, guide_start, guide_end, flank=100)


def _guide_genomic_strand(gene_strand: Optional[str], guide_strand: str) -> str:
    """
    Convert guide strand from target-sequence orientation to genomic strand.

    Gene target sequences are in coding/transcript orientation. For genes on
    the minus genomic strand, guide strands are therefore inverted relative to
    chromosome coordinates.
    """
    if gene_strand != "-":
        return guide_strand
    return "-" if guide_strand == "+" else "+"


# ============================================================================
# Database Functions
# ============================================================================

def _get_gene_info(
    db: Session,
    gene_name: str,
    organism_tag: str
) -> Optional[Tuple[GeneInfo, str, Feature]]:
    """
    Get gene information and coding sequence from database.

    Returns (GeneInfo, sequence, feature) tuple or None if not found.
    """
    query_upper = gene_name.strip().upper()

    # Map organism tag to organism_abbrev (strip assembly suffix)
    org_abbrev = _map_organism_tag_to_abbrev(organism_tag)

    # Determine target chromosome prefix based on organism/assembly
    # For C_albicans_SC5314_A22, prefer Ca22 chromosomes
    chr_prefix = None
    if "_A22" in organism_tag and "albicans" in organism_tag.lower():
        chr_prefix = "Ca22%"

    # Build base query with organism filter
    def build_base_query(filter_expr):
        query = db.query(Feature).filter(filter_expr)
        query = query.join(Organism, Feature.organism_no == Organism.organism_no)
        query = query.filter(Organism.organism_abbrev == org_abbrev)
        return query

    # If we have a chromosome prefix, try to find a feature with location on that chromosome
    feature = None
    if chr_prefix:
        # Alias for the chromosome feature to avoid conflicts
        ChromFeature = aliased(Feature)
        for filter_expr in [
            func.upper(Feature.gene_name) == query_upper,
            func.upper(Feature.feature_name) == query_upper,
            func.upper(Feature.dbxref_id) == query_upper,
        ]:
            feature = (
                db.query(Feature)
                .join(Organism, Feature.organism_no == Organism.organism_no)
                .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
                .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
                .join(ChromFeature, Seq.feature_no == ChromFeature.feature_no)
                .filter(
                    filter_expr,
                    Organism.organism_abbrev == org_abbrev,
                    FeatLocation.is_loc_current == "Y",
                    ChromFeature.feature_name.like(chr_prefix),
                    ChromFeature.feature_type == "chromosome"
                )
                .first()
            )
            if feature:
                break

    # Fall back to standard query if no Ca22 feature found
    if not feature:
        feature = build_base_query(func.upper(Feature.gene_name) == query_upper).first()
    if not feature:
        feature = build_base_query(func.upper(Feature.feature_name) == query_upper).first()
    if not feature:
        feature = build_base_query(func.upper(Feature.dbxref_id) == query_upper).first()

    if not feature:
        return None

    # Get coding sequence (prefer coding over genomic)
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == feature.feature_no,
            Seq.seq_type == "coding",
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    if not seq_record:
        # Fall back to genomic
        seq_record = (
            db.query(Seq)
            .filter(
                Seq.feature_no == feature.feature_no,
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y"
            )
            .first()
        )

    if not seq_record:
        return None

    # Get location info - prefer locations on Ca22 chromosomes for JBrowse compatibility
    # First try to find a location on a Ca22 chromosome
    location = (
        db.query(FeatLocation)
        .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
        .join(Feature, Seq.feature_no == Feature.feature_no)
        .filter(
            FeatLocation.feature_no == feature.feature_no,
            FeatLocation.is_loc_current == "Y",
            Feature.feature_name.like("Ca22%"),
            Feature.feature_type == "chromosome"
        )
        .first()
    )

    # Fall back to any current location if no Ca22 chromosome location found
    if not location:
        location = (
            db.query(FeatLocation)
            .filter(
                FeatLocation.feature_no == feature.feature_no,
                FeatLocation.is_loc_current == "Y"
            )
            .first()
        )

    # Get chromosome name if location exists
    chromosome = None
    if location and location.root_seq_no:
        root_seq = db.query(Seq).filter(Seq.seq_no == location.root_seq_no).first()
        if root_seq:
            root_feature = db.query(Feature).filter(
                Feature.feature_no == root_seq.feature_no
            ).first()
            if root_feature:
                chromosome = root_feature.feature_name

    # Generate JBrowse URL for the gene
    jbrowse_url = None
    if chromosome and location:
        jbrowse_url = _generate_jbrowse_url(
            organism_tag,
            chromosome,
            location.start_coord,
            location.stop_coord,
        )

    # Get CGD data integration (phenotypes, essentiality, virulence)
    # Wrap in try/except for graceful degradation - these are optional enrichments
    phenotype_count = 0
    phenotype_observables = []
    has_virulence_phenotype = False
    is_essential = False
    essential_reason = None
    ortholog_count = 0
    virulence_categories = []
    virulence_score = None

    try:
        phenotype_count, phenotype_observables, has_virulence_phenotype = (
            _get_phenotype_summary(db, feature.feature_no)
        )
    except Exception as e:
        logger.warning(f"Failed to get phenotype summary: {e}")

    try:
        is_essential, essential_reason, ortholog_count = (
            _get_essentiality_info(db, feature)
        )
    except Exception as e:
        logger.warning(f"Failed to get essentiality info: {e}")

    try:
        virulence_categories, virulence_score = (
            _get_virulence_summary(db, feature)
        )
    except Exception as e:
        logger.warning(f"Failed to get virulence summary: {e}")

    # Build gene info
    gene_info = GeneInfo(
        gene_name=feature.gene_name,
        feature_name=feature.feature_name,
        dbxref_id=feature.dbxref_id,
        organism=org_abbrev,
        description=feature.headline,
        chromosome=chromosome,
        start=location.start_coord if location else None,
        end=location.stop_coord if location else None,
        strand="+" if location and location.strand == "W" else "-" if location else None,
        sequence_length=len(seq_record.residues),
        cgd_url=f"/locus/{feature.feature_name}",
        jbrowse_url=jbrowse_url,
        # CGD data integration fields
        phenotype_count=phenotype_count,
        phenotype_observables=phenotype_observables,
        has_virulence_phenotype=has_virulence_phenotype,
        is_essential=is_essential,
        essential_reason=essential_reason,
        ortholog_count=ortholog_count,
        virulence_categories=virulence_categories,
        virulence_score=virulence_score,
    )

    return gene_info, seq_record.residues.upper(), feature


def _get_genomic_context(
    db: Session,
    feature_no: int,
    upstream: int = 50,
    downstream: int = 50
) -> Optional[Tuple[str, str, str]]:
    """
    Get genomic sequence with flanking regions for homology arm design.

    Returns (upstream_seq, target_seq, downstream_seq) or None.
    """
    # Get feature location
    location = (
        db.query(FeatLocation)
        .filter(
            FeatLocation.feature_no == feature_no,
            FeatLocation.is_loc_current == "Y"
        )
        .first()
    )

    if not location:
        return None

    # Get chromosome sequence
    root_seq = db.query(Seq).filter(
        Seq.seq_no == location.root_seq_no,
        Seq.is_seq_current == "Y"
    ).first()

    if not root_seq:
        return None

    chrom_seq = root_seq.residues.upper()
    start = location.start_coord - 1  # Convert to 0-based
    end = location.stop_coord

    # Extract sequences (handle strand)
    if location.strand == "W":
        upstream_seq = chrom_seq[max(0, start - upstream):start]
        target_seq = chrom_seq[start:end]
        downstream_seq = chrom_seq[end:end + downstream]
    else:
        # Crick strand - reverse complement everything
        downstream_seq = _reverse_complement(chrom_seq[max(0, start - downstream):start])
        target_seq = _reverse_complement(chrom_seq[start:end])
        upstream_seq = _reverse_complement(chrom_seq[end:end + upstream])

    return upstream_seq, target_seq, downstream_seq


# ============================================================================
# CGD Data Integration Helper Functions
# ============================================================================

def _get_phenotype_summary(
    db: Session,
    feature_no: int
) -> Tuple[int, List[str], bool]:
    """
    Get phenotype summary for a gene.

    Args:
        db: Database session
        feature_no: Feature number to query

    Returns:
        Tuple of (phenotype_count, top_observables, has_virulence_phenotype)
    """
    try:
        # Query phenotype annotations grouped by observable
        pheno_query = (
            db.query(
                Phenotype.observable,
                func.count(PhenoAnnotation.pheno_annotation_no).label('count')
            )
            .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
            .filter(PhenoAnnotation.feature_no == feature_no)
            .group_by(Phenotype.observable)
            .order_by(func.count(PhenoAnnotation.pheno_annotation_no).desc())
            .all()
        )

        if not pheno_query:
            return 0, [], False

        # Total count of phenotype annotations
        total_count = sum(count for _, count in pheno_query)

        # Top 5 observables
        top_observables = [observable for observable, _ in pheno_query[:5]]

        # Check for virulence-related phenotypes
        virulence_patterns = ['virulence', 'pathogen', 'host', 'infection', 'colonization']
        has_virulence = any(
            any(pattern in obs.lower() for pattern in virulence_patterns)
            for obs, _ in pheno_query
        )

        return total_count, top_observables, has_virulence

    except Exception as e:
        logger.warning(f"Error getting phenotype summary for feature {feature_no}: {e}")
        return 0, [], False


def _get_essentiality_info(
    db: Session,
    feature: Feature
) -> Tuple[bool, Optional[str], int]:
    """
    Get essentiality information for a gene.

    Reuses logic from virulence_service for housekeeping gene detection.

    Args:
        db: Database session
        feature: Feature object to check

    Returns:
        Tuple of (is_essential, essential_reason, ortholog_count)
    """
    try:
        # Use existing housekeeping gene detection from virulence_service
        is_housekeeping, reason = _is_housekeeping_gene(db, feature)
        ortholog_count = _get_ortholog_count(db, feature)

        return is_housekeeping, reason, ortholog_count

    except Exception as e:
        logger.warning(f"Error getting essentiality info for feature {feature.feature_no}: {e}")
        return False, None, 0


def _get_virulence_summary(
    db: Session,
    feature: Feature
) -> Tuple[List[str], Optional[int]]:
    """
    Get virulence category summary for a gene.

    Args:
        db: Database session
        feature: Feature object to check

    Returns:
        Tuple of (virulence_categories, virulence_score)
    """
    try:
        matched_categories = set()

        # Check each virulence category to see if this gene matches
        for cat_key, cat_config in VIRULENCE_CATEGORIES.items():
            rules = cat_config.get("rules", {})

            # Check gene patterns
            if "gene_patterns" in rules and feature.gene_name:
                for pattern in rules["gene_patterns"]:
                    sql_pattern = pattern.replace("%", "")
                    if feature.gene_name.upper().startswith(sql_pattern.upper()):
                        matched_categories.add(cat_config["name"])
                        break

            # Check phenotype observables
            if "phenotype_observables" in rules:
                pheno_matches = (
                    db.query(Phenotype.observable)
                    .join(PhenoAnnotation, PhenoAnnotation.phenotype_no == Phenotype.phenotype_no)
                    .filter(PhenoAnnotation.feature_no == feature.feature_no)
                    .distinct()
                    .all()
                )
                for (observable,) in pheno_matches:
                    for obs_pattern in rules["phenotype_observables"]:
                        regex_pattern = obs_pattern.replace("%", ".*")
                        if re.search(regex_pattern, observable, re.IGNORECASE):
                            matched_categories.add(cat_config["name"])
                            break

        # Calculate a simple virulence score based on number of categories matched
        # Each category match adds 3 points, with a max of 20
        virulence_score = min(len(matched_categories) * 3, 20) if matched_categories else None

        return sorted(matched_categories), virulence_score

    except Exception as e:
        logger.warning(f"Error getting virulence summary for feature {feature.feature_no}: {e}")
        return [], None


def _get_related_genes(
    db: Session,
    feature: Feature
) -> Dict[str, str]:
    """
    Get paralogs and orthologs of a gene.

    Args:
        db: Database session
        feature: The target gene feature

    Returns:
        Dictionary mapping gene_name (uppercase) -> relationship type ('paralog' or 'ortholog')
    """
    related_genes = {}

    try:
        # Find all homology groups this feature belongs to
        homology_memberships = (
            db.query(FeatHomology.homology_group_no, HomologyGroup.homology_group_type)
            .join(HomologyGroup, FeatHomology.homology_group_no == HomologyGroup.homology_group_no)
            .filter(FeatHomology.feature_no == feature.feature_no)
            .all()
        )

        if not homology_memberships:
            return related_genes

        # Get all features in these homology groups (excluding the target gene)
        for hg_no, hg_type in homology_memberships:
            related_features = (
                db.query(Feature.gene_name, Feature.feature_name)
                .join(FeatHomology, FeatHomology.feature_no == Feature.feature_no)
                .filter(
                    FeatHomology.homology_group_no == hg_no,
                    Feature.feature_no != feature.feature_no
                )
                .all()
            )

            # Map gene names to relationship type
            relationship = hg_type.lower() if hg_type else 'homolog'
            for gene_name, feature_name in related_features:
                # Use gene_name if available, otherwise feature_name
                name = (gene_name or feature_name or '').upper()
                if name:
                    # Only store if not already present, or if current is more specific
                    if name not in related_genes:
                        related_genes[name] = relationship

        logger.debug(
            f"Found {len(related_genes)} related genes for {feature.gene_name or feature.feature_name}: "
            f"paralogs={sum(1 for r in related_genes.values() if r == 'paralog')}, "
            f"orthologs={sum(1 for r in related_genes.values() if r == 'ortholog')}"
        )

    except Exception as e:
        logger.warning(f"Error getting related genes for feature {feature.feature_no}: {e}")

    return related_genes


# ============================================================================
# Guide Finding Functions
# ============================================================================

def _find_pam_sites(
    sequence: str,
    pam_type: PAMType,
    guide_length: int = 20
) -> List[Tuple[str, str, int, str]]:
    """
    Find all PAM sites in a sequence and extract guide sequences.

    Returns list of (guide_sequence, pam_sequence, position, strand) tuples.
    Position is the 1-based leftmost coordinate of the guide/protospacer in
    the input sequence. For reverse-strand guides, this is still reported in
    input-sequence coordinates, not reverse-complement coordinates.
    """
    pam_config = PAM_PATTERNS[pam_type]
    pattern = pam_config["pattern"]
    pam_len = pam_config["length"]
    is_3prime = pam_config["position"] == "3prime"

    guides = []
    sequence = sequence.upper()

    # Use lookahead pattern to find overlapping PAM sites (e.g., "TGGG" contains both TGG and GGG)
    lookahead_pattern = f"(?=({pattern}))"

    # Search forward strand
    for match in re.finditer(lookahead_pattern, sequence):
        pam_start = match.start()
        pam_seq = match.group(1)  # The actual PAM is in capture group 1
        pam_end = pam_start + len(pam_seq)

        if is_3prime:
            # PAM is 3' of guide (SpCas9 style)
            # Guide is upstream of PAM
            guide_start = pam_start - guide_length
            guide_end = pam_start
            if guide_start >= 0:
                guide_seq = sequence[guide_start:guide_end]
                position = guide_start + 1  # 1-based
                guides.append((guide_seq, pam_seq, position, "+"))
        else:
            # PAM is 5' of guide (Cas12a style)
            # Guide is downstream of PAM
            guide_start = pam_end
            guide_end = pam_end + guide_length
            if guide_end <= len(sequence):
                guide_seq = sequence[guide_start:guide_end]
                position = guide_start + 1  # 1-based
                guides.append((guide_seq, pam_seq, position, "+"))

    # Search reverse strand
    rev_sequence = _reverse_complement(sequence)
    seq_len = len(sequence)

    for match in re.finditer(lookahead_pattern, rev_sequence):
        pam_start = match.start()
        pam_seq = match.group(1)  # The actual PAM is in capture group 1
        pam_end = pam_start + len(pam_seq)

        if is_3prime:
            guide_start = pam_start - guide_length
            guide_end = pam_start
            if guide_start >= 0:
                guide_seq = rev_sequence[guide_start:guide_end]
                # Convert guide start back to input-sequence coordinates.
                fwd_position = seq_len - guide_end + 1  # 1-based
                guides.append((guide_seq, pam_seq, fwd_position, "-"))
        else:
            guide_start = pam_end
            guide_end = pam_end + guide_length
            if guide_end <= len(rev_sequence):
                guide_seq = rev_sequence[guide_start:guide_end]
                fwd_position = seq_len - guide_end + 1
                guides.append((guide_seq, pam_seq, fwd_position, "-"))

    return guides


def _filter_target_region(
    guides: List[Tuple[str, str, int, str]],
    sequence_length: int,
    target_region: TargetRegion,
    upstream_length: int = 0
) -> List[Tuple[str, str, int, str]]:
    """
    Filter guides to those within the target region.

    Args:
        guides: List of (guide_seq, pam_seq, position, strand) tuples
        sequence_length: Total length of the target sequence
        target_region: Target region type
        upstream_length: Length of upstream sequence prepended (for FIVE_PRIME_UPSTREAM)
                        Positions <= upstream_length are in the upstream region

    Returns:
        Filtered list of guides
    """
    if target_region == TargetRegion.FULL_CDS or target_region == TargetRegion.CUSTOM:
        return guides

    # For FIVE_PRIME_UPSTREAM, the sequence includes upstream + CDS
    # upstream_length indicates where CDS starts
    cds_length = sequence_length - upstream_length

    # Calculate region boundaries (50% of CDS)
    # CHOPCHOP's "5' region" includes guides in approximately the first half
    # of the CDS, not just the first 20%
    region_size = int(cds_length * 0.5)

    if target_region == TargetRegion.FIVE_PRIME:
        # First 20% of CDS (positions 1 to region_size, no upstream)
        return [(g, pam, p, s) for g, pam, p, s in guides if p <= region_size]
    elif target_region == TargetRegion.FIVE_PRIME_UPSTREAM:
        # Upstream region (positions 1 to upstream_length) + first 20% of CDS
        # The upstream region positions are: 1 to upstream_length
        # The first 20% of CDS positions are: upstream_length+1 to upstream_length+region_size
        max_position = upstream_length + region_size
        return [(g, pam, p, s) for g, pam, p, s in guides if p <= max_position]
    elif target_region == TargetRegion.THREE_PRIME:
        # Last 20% of CDS
        start = sequence_length - region_size
        return [(g, pam, p, s) for g, pam, p, s in guides if p >= start]

    return guides


# ============================================================================
# Off-target Analysis
# ============================================================================

# Limit number of guides for off-target search (performance)
MAX_GUIDES_FOR_OFFTARGET = 14

# Pattern to extract chromosome/ORF base name (without A/B allele suffix)
# Matches two naming conventions:
#   1. Chromosome names: "Ca22chr1A_C_albicans_SC5314" -> "Ca22chr1"
#   2. ORF names: "C1_06980C_A" -> "C1_06980C" (allele suffix is _A or _B)
CHROMOSOME_ALLELE_PATTERN = re.compile(r'^(.*chr[R\d]+)[AB](_.*)?$', re.IGNORECASE)
ORF_ALLELE_PATTERN = re.compile(r'^(.+)_([AB])$', re.IGNORECASE)

# Diploid organisms where we expect allelic pairs (A/B chromosomes)
DIPLOID_ORGANISMS = {
    "C_albicans_SC5314",
    "C_albicans_SC5314_A22",
    "C_albicans_SC5314_A21",
    "C_albicans_SC5314_A19",
}


def _get_chromosome_base(chromosome: str) -> Optional[str]:
    """
    Extract chromosome/ORF base name from BLAST sequence name.

    Handles two naming conventions:
    - Chromosome: "Ca22chr1A_C_albicans_SC5314" -> "CA22CHR1"
    - ORF: "C1_06980C_A" -> "C1_06980C"

    Returns None if neither pattern matches.
    """
    # Try chromosome pattern first
    match = CHROMOSOME_ALLELE_PATTERN.match(chromosome)
    if match:
        return match.group(1).upper()

    # Try ORF pattern (ends with _A or _B)
    match = ORF_ALLELE_PATTERN.match(chromosome)
    if match:
        return match.group(1).upper()

    return None


def _are_allelic_chromosomes(chr1: str, chr2: str) -> bool:
    """
    Check if two chromosome/ORF names are allelic variants (A vs B allele).

    C. albicans has diploid chromosomes/ORFs named like:
    - Chromosomes: Ca22chr1A_C_albicans_SC5314 vs Ca22chr1B_C_albicans_SC5314
    - ORFs: C1_06980C_A vs C1_06980C_B

    These represent the same genomic region on different alleles.
    """
    if chr1 == chr2:
        return True

    # Use _get_chromosome_base which handles both chromosome and ORF patterns
    base1 = _get_chromosome_base(chr1)
    base2 = _get_chromosome_base(chr2)

    if base1 and base2:
        is_allelic = base1 == base2
        if is_allelic:
            logger.debug(f"Sequences are allelic: {chr1} <-> {chr2} (base: {base1})")
        return is_allelic

    return False


# PAM patterns for off-target validation (regex patterns)
PAM_PATTERNS_FOR_OFFTARGET: Dict[PAMType, str] = {
    PAMType.NGG: r"[ACGT]GG",
    PAMType.NAG: r"[ACGT]AG",
    PAMType.NNGRRT: r"[ACGT][ACGT]G[AG][AG]T",
    PAMType.TTTV: r"TTT[ACG]",
}


def _count_mismatches(seq1: str, seq2: str) -> Tuple[int, List[int]]:
    """
    Count mismatches between two sequences and return positions.

    Returns (mismatch_count, [positions]) where positions are 0-indexed.
    """
    if len(seq1) != len(seq2):
        return max(len(seq1), len(seq2)), []

    mismatches = 0
    positions = []
    for i, (a, b) in enumerate(zip(seq1.upper(), seq2.upper())):
        if a != b:
            mismatches += 1
            positions.append(i)
    return mismatches, positions


def _validate_pam_at_position(
    chromosome_seq: str,
    hit_start: int,
    hit_end: int,
    strand: str,
    pam_type: PAMType,
    guide_length: int = 20
) -> Optional[str]:
    """
    Check if a valid PAM exists adjacent to the off-target hit.

    Coordinates are 0-based half-open genomic coordinates for the guide
    protospacer on the forward chromosome sequence. For 3' PAM systems
    (NGG, NAG, NNGRRT), PAM is downstream of the guide on plus-strand hits
    and upstream of the guide on minus-strand hits. 5' PAM systems are the
    opposite.

    Returns the PAM sequence if valid, None otherwise.
    """
    pam_config = PAM_PATTERNS.get(pam_type)
    if not pam_config:
        return None

    pam_pattern = PAM_PATTERNS_FOR_OFFTARGET.get(pam_type)
    if not pam_pattern:
        return None

    pam_len = pam_config["length"]
    is_3prime = pam_config["position"] == "3prime"

    try:
        if strand == "+":
            if is_3prime:
                # PAM is 3' of guide (after hit_end)
                pam_start = hit_end
                pam_end = hit_end + pam_len
            else:
                # PAM is 5' of guide (before hit_start)
                pam_end = hit_start
                pam_start = pam_end - pam_len

            if pam_start < 0 or pam_end > len(chromosome_seq):
                return None

            pam_seq = chromosome_seq[pam_start:pam_end]
        else:
            # Minus strand - need reverse complement
            if is_3prime:
                # For minus strand with 3' PAM, PAM is upstream in genomic coords
                pam_end = hit_start
                pam_start = pam_end - pam_len
            else:
                # For minus strand with 5' PAM, PAM is downstream in genomic coords
                pam_start = hit_end
                pam_end = hit_end + pam_len

            if pam_start < 0 or pam_end > len(chromosome_seq):
                return None

            pam_seq = _reverse_complement(chromosome_seq[pam_start:pam_end])

        # Validate PAM matches expected pattern
        if re.match(f"^{pam_pattern}$", pam_seq.upper()):
            return pam_seq.upper()

    except (IndexError, ValueError):
        pass

    return None


def _get_chromosome_seq_no(
    db: Session,
    chromosome_name: str,
    organism_tag: str
) -> Optional[int]:
    """Get the seq_no (root_seq_no) for a chromosome by name."""
    # Map organism tag to organism_abbrev (strip assembly suffix)
    org_abbrev = _map_organism_tag_to_abbrev(organism_tag)

    # Find chromosome feature and its sequence
    chromosome = (
        db.query(Feature)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            Organism.organism_abbrev == org_abbrev,
            Feature.feature_name == chromosome_name
        )
        .first()
    )

    if not chromosome:
        return None

    # Get the genomic sequence for this chromosome
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == chromosome.feature_no,
            Seq.seq_type == "genomic",
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    return seq_record.seq_no if seq_record else None


def _get_chromosome_sequence(
    db: Session,
    chromosome_name: str,
    organism_tag: str
) -> Optional[str]:
    """Get current genomic sequence residues for a chromosome name."""
    root_seq_no = _get_chromosome_seq_no(db, chromosome_name, organism_tag)
    if not root_seq_no:
        return None

    seq_record = db.query(Seq).filter(
        Seq.seq_no == root_seq_no,
        Seq.is_seq_current == "Y"
    ).first()

    return seq_record.residues.upper() if seq_record and seq_record.residues else None


def _map_position_to_gene(
    db: Session,
    chromosome_name: str,
    position: int,
    strand: str,
    organism_tag: str,
    promoter_distance: int = 1000
) -> Tuple[Optional[str], Optional[str]]:
    """
    Map a genomic position to a gene and region type.

    Returns (gene_name, region_type) where region_type is:
    - "exon": within a gene's ORF
    - "promoter": within promoter_distance bp upstream of a gene
    - "intergenic": not near any gene

    Note: This is a simplified implementation. A full implementation would
    check for introns vs exons using exon coordinates.
    """
    # Map organism tag to organism_abbrev
    org_abbrev = _map_organism_tag_to_abbrev(organism_tag)

    # Get the root_seq_no for this chromosome
    root_seq_no = _get_chromosome_seq_no(db, chromosome_name, organism_tag)
    if not root_seq_no:
        return None, "intergenic"

    # Query for features at this position
    # First check for direct overlap (within gene body)
    gene_hit = (
        db.query(Feature, FeatLocation)
        .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            Organism.organism_abbrev == org_abbrev,
            FeatLocation.root_seq_no == root_seq_no,
            FeatLocation.is_loc_current == "Y",
            FeatLocation.start_coord <= position,
            FeatLocation.stop_coord >= position,
            Feature.feature_type == "ORF"
        )
        .first()
    )

    if gene_hit:
        feature, _ = gene_hit
        gene_name = feature.gene_name or feature.feature_name
        return gene_name, "exon"

    # Check for promoter region (upstream of gene start)
    # For Watson strand genes, promoter is before start_coord
    # For Crick strand genes, promoter is after stop_coord
    promoter_hit = (
        db.query(Feature, FeatLocation)
        .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            Organism.organism_abbrev == org_abbrev,
            FeatLocation.root_seq_no == root_seq_no,
            FeatLocation.is_loc_current == "Y",
            Feature.feature_type == "ORF"
        )
        .all()
    )

    for feature, location in promoter_hit:
        if location.strand == "W":
            # Watson strand: promoter is upstream (lower coords)
            promoter_start = location.start_coord - promoter_distance
            promoter_end = location.start_coord - 1
            if promoter_start <= position <= promoter_end:
                gene_name = feature.gene_name or feature.feature_name
                return gene_name, "promoter"
        else:
            # Crick strand: promoter is downstream (higher coords)
            promoter_start = location.stop_coord + 1
            promoter_end = location.stop_coord + promoter_distance
            if promoter_start <= position <= promoter_end:
                gene_name = feature.gene_name or feature.feature_name
                return gene_name, "promoter"

    return None, "intergenic"


def _get_all_chromosome_sequences(
    db: Session,
    organism_tag: str
) -> Dict[str, str]:
    """
    Get all chromosome/contig sequences for an organism.

    Returns dict mapping chromosome_name -> sequence (uppercase).
    """
    org_abbrev = _map_organism_tag_to_abbrev(organism_tag)

    # Query all chromosome features for this organism
    chromosomes = (
        db.query(Feature)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            Organism.organism_abbrev == org_abbrev,
            Feature.feature_type == "chromosome"
        )
        .all()
    )

    result = {}
    for chrom in chromosomes:
        # Get genomic sequence for this chromosome
        seq_record = (
            db.query(Seq)
            .filter(
                Seq.feature_no == chrom.feature_no,
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y"
            )
            .first()
        )
        if seq_record and seq_record.residues:
            result[chrom.feature_name] = seq_record.residues.upper()

    logger.info(
        f"Loaded {len(result)} chromosomes for {organism_tag}, "
        f"total {sum(len(s) for s in result.values()):,} bp"
    )
    return result


def _search_offtargets_bruteforce(
    db: Session,
    guide: str,
    pam_type: PAMType,
    organism_tag: str,
    max_mismatches: int = 3,
    exclude_position: Optional[Tuple[str, int, str]] = None,
    guide_cds_position: Optional[int] = None,
    warnings: Optional[List[str]] = None,
    related_genes: Optional[Dict[str, str]] = None,
    status: Optional[Dict[str, bool]] = None,
    chromosome_cache: Optional[Dict[str, str]] = None,
) -> List[OffTargetHit]:
    """
    Search for off-targets using brute-force genome-wide scan.

    This method guarantees finding ALL off-targets with up to max_mismatches,
    unlike BLAST which may miss some due to seed-based heuristics.

    Algorithm:
    1. Load all chromosome sequences
    2. Find all PAM sites in each chromosome (both strands)
    3. Extract protospacer sequence adjacent to each PAM
    4. Compare to guide, counting mismatches
    5. Return all hits with <= max_mismatches

    Args:
        db: Database session for loading sequences and gene mapping
        guide: Guide RNA sequence (typically 20bp)
        pam_type: PAM type (NGG, NAG, etc.)
        organism_tag: Organism tag (e.g., "C_albicans_SC5314_A22")
        max_mismatches: Maximum allowed mismatches (0-4)
        exclude_position: (chromosome, position, strand) to exclude (on-target)
        guide_cds_position: Position within CDS for on-target exclusion
        warnings: Optional list to append warning messages
        related_genes: Dict mapping gene_name -> relationship for flagging
        status: Optional dict to set {"performed": True/False}
        chromosome_cache: Optional pre-loaded chromosome sequences

    Returns:
        List of OffTargetHit objects sorted by mismatches (ascending)
    """
    import time
    start_time = time.time()

    if status is not None:
        status["performed"] = False

    guide = guide.upper()
    guide_length = len(guide)
    offtargets = []

    # Get PAM configuration
    pam_config = PAM_PATTERNS.get(pam_type)
    if not pam_config:
        if warnings is not None:
            warnings.append(f"Unknown PAM type: {pam_type}")
        return []

    pam_pattern = pam_config["pattern"]
    pam_len = pam_config["length"]
    is_3prime = pam_config["position"] == "3prime"

    # Load chromosome sequences (use cache if provided)
    if chromosome_cache is not None:
        chromosomes = chromosome_cache
    else:
        chromosomes = _get_all_chromosome_sequences(db, organism_tag)

    if not chromosomes:
        msg = f"No chromosome sequences found for {organism_tag}"
        logger.warning(msg)
        if warnings is not None:
            warnings.append(msg)
        return []

    # Pre-compile PAM regex with lookahead for overlapping matches
    pam_regex = re.compile(f"(?=({pam_pattern}))", re.IGNORECASE)

    # Track statistics
    total_pam_sites = 0
    total_candidates = 0

    # Check if this is a diploid organism (for allelic pair handling)
    is_diploid = any(org in organism_tag for org in DIPLOID_ORGANISMS)

    # Process each chromosome
    for chrom_name, chrom_seq in chromosomes.items():
        chrom_len = len(chrom_seq)

        # Search forward strand
        for match in pam_regex.finditer(chrom_seq):
            pam_start = match.start()
            pam_seq = match.group(1).upper()
            pam_end = pam_start + len(pam_seq)
            total_pam_sites += 1

            if is_3prime:
                # PAM is 3' of guide (SpCas9 style)
                # Protospacer is upstream of PAM
                proto_start = pam_start - guide_length
                proto_end = pam_start
                if proto_start < 0:
                    continue
            else:
                # PAM is 5' of guide (Cas12a style)
                # Protospacer is downstream of PAM
                proto_start = pam_end
                proto_end = pam_end + guide_length
                if proto_end > chrom_len:
                    continue

            protospacer = chrom_seq[proto_start:proto_end].upper()
            if len(protospacer) != guide_length:
                continue

            # Count mismatches
            mm_count, mm_positions = _count_mismatches(guide, protospacer)
            if mm_count > max_mismatches:
                continue

            total_candidates += 1
            position = proto_start + 1  # 1-based

            # Check exclusions (on-target site)
            if _should_exclude_offtarget(
                chrom_name, position, "+", mm_count,
                exclude_position, guide_cds_position, is_diploid
            ):
                continue

            # Calculate CFD score
            cfd = _calculate_cfd_score(guide, protospacer)

            # Map to gene (expensive - only do for valid hits)
            gene_name, gene_region = _map_position_to_gene(
                db, chrom_name, position, "+", organism_tag
            )

            # Check for paralog/ortholog
            is_paralog, is_ortholog, homology_rel = _check_related_gene(
                gene_name, related_genes
            )

            offtargets.append(OffTargetHit(
                chromosome=chrom_name,
                position=position,
                strand="+",
                sequence=protospacer,
                mismatches=mm_count,
                mismatch_positions=mm_positions,
                gene_name=gene_name,
                gene_region=gene_region,
                cfd_score=cfd,
                is_paralog=is_paralog,
                is_ortholog=is_ortholog,
                homology_relationship=homology_rel,
            ))

        # Search reverse strand (reverse complement the chromosome)
        rev_chrom = _reverse_complement(chrom_seq)

        for match in pam_regex.finditer(rev_chrom):
            pam_start = match.start()
            pam_seq = match.group(1).upper()
            pam_end = pam_start + len(pam_seq)
            total_pam_sites += 1

            if is_3prime:
                proto_start = pam_start - guide_length
                proto_end = pam_start
                if proto_start < 0:
                    continue
            else:
                proto_start = pam_end
                proto_end = pam_end + guide_length
                if proto_end > chrom_len:
                    continue

            protospacer = rev_chrom[proto_start:proto_end].upper()
            if len(protospacer) != guide_length:
                continue

            mm_count, mm_positions = _count_mismatches(guide, protospacer)
            if mm_count > max_mismatches:
                continue

            total_candidates += 1

            # Convert position back to forward strand coordinates
            # Position on reverse = chrom_len - proto_end (for the 5' end of protospacer)
            fwd_position = chrom_len - proto_end + 1  # 1-based

            if _should_exclude_offtarget(
                chrom_name, fwd_position, "-", mm_count,
                exclude_position, guide_cds_position, is_diploid
            ):
                continue

            cfd = _calculate_cfd_score(guide, protospacer)

            gene_name, gene_region = _map_position_to_gene(
                db, chrom_name, fwd_position, "-", organism_tag
            )

            is_paralog, is_ortholog, homology_rel = _check_related_gene(
                gene_name, related_genes
            )

            offtargets.append(OffTargetHit(
                chromosome=chrom_name,
                position=fwd_position,
                strand="-",
                sequence=protospacer,
                mismatches=mm_count,
                mismatch_positions=mm_positions,
                gene_name=gene_name,
                gene_region=gene_region,
                cfd_score=cfd,
                is_paralog=is_paralog,
                is_ortholog=is_ortholog,
                homology_relationship=homology_rel,
            ))

    # Mark as performed
    if status is not None:
        status["performed"] = True

    # For diploid organisms, exclude allelic pairs
    if is_diploid:
        offtargets = _exclude_allelic_pairs(offtargets)

    # Sort by mismatches, then CFD score (descending)
    offtargets.sort(key=lambda x: (x.mismatches, -x.cfd_score))

    elapsed = time.time() - start_time
    logger.info(
        f"Brute-force off-target search: {total_pam_sites:,} PAM sites, "
        f"{total_candidates:,} candidates, {len(offtargets)} off-targets "
        f"in {elapsed:.2f}s"
    )

    return offtargets


def _should_exclude_offtarget(
    chromosome: str,
    position: int,
    strand: str,
    mismatches: int,
    exclude_position: Optional[Tuple[str, int, str]],
    guide_cds_position: Optional[int],
    is_diploid: bool,
) -> bool:
    """
    Check if an off-target hit should be excluded (is the on-target site).

    Returns True if the hit should be excluded.
    """
    # Strategy 1: CDS position matching for exact matches
    if guide_cds_position and mismatches == 0:
        if abs(position - guide_cds_position) <= 5:
            logger.debug(
                f"Excluding on-target by CDS position: {chromosome}:{position}"
            )
            return True

    # Strategy 2: Explicit exclude position
    if exclude_position:
        exc_chr, exc_pos, exc_strand = exclude_position

        # For diploid organisms with exact matches, handle specially
        if mismatches == 0 and is_diploid:
            # Check if this is an allelic match (same base chromosome)
            if _are_allelic_chromosomes(chromosome, exc_chr):
                if abs(position - exc_pos) < 100 and strand == exc_strand:
                    logger.debug(
                        f"Excluding allelic on-target: {chromosome}:{position}"
                    )
                    return True

        is_same_or_allelic = _are_allelic_chromosomes(chromosome, exc_chr)
        is_similar_position = abs(position - exc_pos) < 100
        is_same_strand = strand == exc_strand

        # Exact match at similar position
        if mismatches == 0 and is_similar_position and is_same_strand:
            logger.debug(
                f"Excluding exact match at similar position: {chromosome}:{position}"
            )
            return True

        # Mismatched hit on same/allelic chromosome
        if is_same_or_allelic and is_similar_position and is_same_strand:
            logger.debug(
                f"Excluding on-target/allelic hit: {chromosome}:{position}"
            )
            return True

    return False


def _check_related_gene(
    gene_name: Optional[str],
    related_genes: Optional[Dict[str, str]]
) -> Tuple[bool, bool, Optional[str]]:
    """
    Check if a gene is a paralog/ortholog of the target gene.

    Returns (is_paralog, is_ortholog, homology_relationship).
    """
    if not gene_name or not related_genes:
        return False, False, None

    gene_name_upper = gene_name.upper()
    if gene_name_upper not in related_genes:
        return False, False, None

    relationship = related_genes[gene_name_upper]
    is_paralog = relationship == 'paralog'
    is_ortholog = relationship == 'ortholog'

    return is_paralog, is_ortholog, relationship


def _search_offtargets_blast(
    db: Session,
    guide: str,
    pam_type: PAMType,
    organism_tag: str,
    max_mismatches: int = 3,
    exclude_position: Optional[Tuple[str, int, str]] = None,
    guide_cds_position: Optional[int] = None,
    warnings: Optional[List[str]] = None,
    related_genes: Optional[Dict[str, str]] = None,
    status: Optional[Dict[str, bool]] = None,
) -> List[OffTargetHit]:
    """
    Search for off-targets using BLAST.

    Uses blastn-short with parameters optimized for 20bp guide sequences.

    Args:
        db: Database session for gene mapping
        guide: Guide RNA sequence (20bp)
        pam_type: PAM type for validation
        organism_tag: Organism tag (e.g., "C_albicans_SC5314_A22")
        max_mismatches: Maximum allowed mismatches (0-3)
        exclude_position: (chromosome, position, strand) to exclude (the on-target site)
        guide_cds_position: Position of guide within CDS (1-based) for on-target exclusion
        warnings: Optional list to append warning messages to
        related_genes: Dict mapping gene_name -> relationship ('paralog'/'ortholog')
            for flagging off-targets in related genes

    Returns:
        List of OffTargetHit objects sorted by mismatches (ascending)
    """
    from cgd.core.settings import settings

    if status is not None:
        status["performed"] = False

    offtargets = []
    chromosome_cache: Dict[str, Optional[str]] = {}

    # Build genome database name for this organism
    # Try multiple naming conventions (A22 uses "default_genomic_", older assemblies use "genomic_")
    db_path = None
    genome_db = None

    # Try naming conventions in order of preference
    naming_patterns = [
        f"default_genomic_{organism_tag}",  # A22 convention
        f"genomic_{organism_tag}",           # A21/A19 convention
    ]

    # Log the BLAST database path being checked
    logger.info(f"BLAST DB path from settings: {settings.blast_db_path}")

    for pattern in naming_patterns:
        test_path = os.path.join(settings.blast_db_path, pattern)
        logger.info(f"Checking for BLAST database: {test_path}.nsq")
        if os.path.exists(test_path + ".nsq"):
            db_path = test_path
            genome_db = pattern
            logger.info(f"Found BLAST database: {genome_db}")
            break

    if not db_path:
        msg = (
            f"Off-target search unavailable: BLAST database not found for {organism_tag}. "
            f"Checked: {settings.blast_db_path}"
        )
        logger.warning(msg)
        if warnings is not None:
            warnings.append(msg)
        return []

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            query_file = os.path.join(tmpdir, "guide.fasta")
            output_file = os.path.join(tmpdir, "blast_output.txt")

            # Write guide sequence to FASTA file
            with open(query_file, "w") as f:
                f.write(f">guide\n{guide}\n")

            # Build BLAST command for short sequence search
            # -task blastn-short: optimized for short queries
            # -word_size 7: shorter words for sensitive search
            # -evalue 1000: permissive e-value for short sequences
            # -dust no: disable low complexity filtering
            # -ungapped: for exact mismatch counting
            # -outfmt 6: tabular output with custom fields
            blast_cmd = [
                os.path.join(settings.blast_bin_path, "blastn"),
                "-task", "blastn-short",
                "-word_size", "7",
                "-evalue", "1000",
                "-dust", "no",
                "-db", db_path,
                "-query", query_file,
                "-outfmt", "6 sseqid sstart send sstrand qseq sseq",
                "-max_target_seqs", "1000",
            ]

            logger.debug(f"Running BLAST for off-targets: {' '.join(blast_cmd)}")

            result = subprocess.run(
                blast_cmd,
                capture_output=True,
                text=True,
                timeout=CRISPR_TIMEOUT,
            )

            if result.returncode != 0:
                msg = f"BLAST off-target search failed: {result.stderr}"
                logger.warning(msg)
                if warnings is not None:
                    warnings.append(msg)
                return []

            if status is not None:
                status["performed"] = True

            # Parse BLAST tabular output
            # Fields: sseqid sstart send sstrand qseq sseq
            for line in result.stdout.strip().split("\n"):
                if not line:
                    continue

                fields = line.split("\t")
                if len(fields) < 6:
                    continue

                chromosome = fields[0]
                start = int(fields[1])
                end = int(fields[2])
                strand_str = fields[3]  # "plus" or "minus"
                query_aln = fields[4]  # aligned query sequence
                subject_aln = fields[5]  # aligned subject sequence

                # Convert strand
                strand = "+" if strand_str == "plus" else "-"
                hit_start = min(start, end)
                hit_end = max(start, end)
                hit_start_0 = hit_start - 1
                hit_end_0 = hit_end

                # Remove gaps for mismatch counting first (needed for exclusion check)
                query_ungapped = query_aln.replace("-", "")
                subject_ungapped = subject_aln.replace("-", "")

                # Skip if alignment is too short (partial match)
                if len(subject_ungapped) < len(guide) - max_mismatches:
                    continue

                # Pad shorter sequences if needed for comparison
                if len(subject_ungapped) < len(guide):
                    # Partial alignment - count missing bases as mismatches
                    missing = len(guide) - len(subject_ungapped)
                    mm_count, mm_positions = _count_mismatches(
                        guide[:len(subject_ungapped)],
                        subject_ungapped
                    )
                    mm_count += missing
                    mm_positions.extend(range(len(subject_ungapped), len(guide)))
                else:
                    mm_count, mm_positions = _count_mismatches(guide, subject_ungapped[:len(guide)])

                # Filter by max mismatches
                if mm_count > max_mismatches:
                    continue

                # Skip if this is the on-target position or its allelic variant
                # C. albicans is diploid, so guides will match both A and B alleles
                #
                # Strategy 1: Use CDS position matching
                # If BLAST hit position matches the guide's position within the CDS,
                # it's very likely the on-target (or allelic copy at same position)
                if guide_cds_position and mm_count == 0:
                    # Allow small tolerance for position matching (e.g., +/- 5bp)
                    if abs(hit_start - guide_cds_position) <= 5:
                        logger.debug(
                            f"Excluding on-target by CDS position: {chromosome}:{hit_start} "
                            f"(guide at CDS pos {guide_cds_position})"
                        )
                        continue

                # Strategy 2: For diploid organisms, collect exact matches for allelic pairing
                is_diploid = any(org in organism_tag for org in DIPLOID_ORGANISMS)

                if exclude_position and not (mm_count == 0 and is_diploid):
                    exc_chr, exc_pos, exc_strand = exclude_position

                    # Check if chromosome names match (including allelic A/B variants)
                    is_same_or_allelic = _are_allelic_chromosomes(chromosome, exc_chr)

                    # Check if position is similar (within tolerance)
                    is_similar_position = abs(hit_start - exc_pos) < 100
                    is_same_strand = strand == exc_strand

                    # For exact matches (0 mismatches), check by position alone
                    # This handles chromosome naming mismatches between DB and BLAST
                    if mm_count == 0 and is_similar_position and is_same_strand:
                        logger.debug(
                            f"Excluding exact match at similar position: {chromosome}:{hit_start} "
                            f"(exclude: {exc_chr}:{exc_pos})"
                        )
                        continue

                    # For hits with mismatches, require chromosome match
                    if is_same_or_allelic and is_similar_position and is_same_strand:
                        logger.debug(
                            f"Excluding on-target/allelic hit: {chromosome}:{hit_start} "
                            f"(exclude: {exc_chr}:{exc_pos})"
                        )
                        continue

                # Validate that a compatible PAM is adjacent to the BLAST hit.
                # If chromosome sequence cannot be resolved from the DB name,
                # keep the hit rather than hiding potential off-targets.
                if chromosome not in chromosome_cache:
                    chromosome_cache[chromosome] = _get_chromosome_sequence(
                        db, chromosome, organism_tag
                    )
                chromosome_seq = chromosome_cache[chromosome]
                if chromosome_seq:
                    pam_seq = _validate_pam_at_position(
                        chromosome_seq,
                        hit_start_0,
                        hit_end_0,
                        strand,
                        pam_type,
                        guide_length=len(guide),
                    )
                    if not pam_seq:
                        continue

                # Calculate CFD score
                cfd = _calculate_cfd_score(guide, subject_ungapped[:len(guide)])

                # Map position to gene
                gene_name, gene_region = _map_position_to_gene(
                    db, chromosome, hit_start, strand, organism_tag
                )

                # Check if hit is in a related gene (paralog/ortholog)
                is_paralog = False
                is_ortholog = False
                homology_relationship = None
                if gene_name and related_genes:
                    gene_name_upper = gene_name.upper()
                    if gene_name_upper in related_genes:
                        relationship = related_genes[gene_name_upper]
                        homology_relationship = relationship
                        if relationship == 'paralog':
                            is_paralog = True
                        elif relationship == 'ortholog':
                            is_ortholog = True

                offtargets.append(OffTargetHit(
                    chromosome=chromosome,
                    position=hit_start,
                    strand=strand,
                    sequence=subject_ungapped[:len(guide)],
                    mismatches=mm_count,
                    mismatch_positions=mm_positions,
                    gene_name=gene_name,
                    gene_region=gene_region,
                    cfd_score=cfd,
                    is_paralog=is_paralog,
                    is_ortholog=is_ortholog,
                    homology_relationship=homology_relationship,
                ))

    except subprocess.TimeoutExpired:
        logger.warning(f"BLAST off-target search timed out for guide: {guide[:10]}...")
        return []
    except Exception as e:
        logger.error(f"Error in off-target search: {e}")
        return []

    # For diploid organisms, exclude allelic pairs (A/B chromosome hits at same position)
    # This handles the case where chromosome naming in DB doesn't match BLAST output
    if any(org in organism_tag for org in DIPLOID_ORGANISMS):
        offtargets = _exclude_allelic_pairs(offtargets)

    # Sort by mismatches (ascending), then by CFD score (descending - higher CFD = more likely to cut)
    offtargets.sort(key=lambda x: (x.mismatches, -x.cfd_score))

    return offtargets


def _search_offtargets_bowtie(
    db: Session,
    guide: str,
    pam_type: PAMType,
    organism_tag: str,
    max_mismatches: int = 3,
    exclude_position: Optional[Tuple[str, int, str]] = None,
    guide_cds_position: Optional[int] = None,
    warnings: Optional[List[str]] = None,
    related_genes: Optional[Dict[str, str]] = None,
    status: Optional[Dict[str, bool]] = None,
) -> List[OffTargetHit]:
    """
    Search for off-targets using Bowtie short-read aligner.

    Bowtie is well-suited for aligning short sequences (20bp guides) and offers
    a good balance between speed and sensitivity.

    Args:
        db: Database session for gene mapping
        guide: Guide RNA sequence (20bp)
        pam_type: PAM type for validation
        organism_tag: Organism tag (e.g., "C_albicans_SC5314_A22")
        max_mismatches: Maximum allowed mismatches (0-3)
        exclude_position: (chromosome, position, strand) to exclude (the on-target site)
        guide_cds_position: Position of guide within CDS (1-based) for on-target exclusion
        warnings: Optional list to append warning messages to
        related_genes: Dict mapping gene_name -> relationship ('paralog'/'ortholog')
            for flagging off-targets in related genes
        status: Optional dict to set {"performed": True/False}

    Returns:
        List of OffTargetHit objects sorted by mismatches (ascending)
    """
    from cgd.core.settings import settings

    if status is not None:
        status["performed"] = False

    offtargets = []
    chromosome_cache: Dict[str, Optional[str]] = {}

    # Determine bowtie index path for this organism
    index_path = os.path.join(settings.bowtie_index_path, organism_tag)

    # Check if bowtie index exists (look for .1.ebwt or .1.ebwtl file)
    index_exists = (
        os.path.exists(index_path + ".1.ebwt") or
        os.path.exists(index_path + ".1.ebwtl")
    )

    if not index_exists:
        msg = (
            f"Off-target search unavailable: Bowtie index not found for {organism_tag}. "
            f"Checked: {index_path}"
        )
        logger.warning(msg)
        if warnings is not None:
            warnings.append(msg)
        return []

    # Check bowtie binary exists
    bowtie_bin = os.path.join(settings.bowtie_bin_path, "bowtie")
    if not os.path.exists(bowtie_bin):
        msg = f"Bowtie binary not found at {bowtie_bin}"
        logger.warning(msg)
        if warnings is not None:
            warnings.append(msg)
        return []

    try:
        # Build bowtie command
        # -x: Index base name
        # -c: Read sequence directly from command line
        # -v: Allow up to N mismatches (in entire read, not just seed)
        # -a: Report all alignments (not just best)
        # -S: Output in SAM format (required for --sam-nohead)
        # --sam-nohead: Output SAM without header for easier parsing
        # -p 1: Use 1 thread (for stability)
        bowtie_cmd = [
            bowtie_bin,
            "-x", index_path,
            "-c", guide,
            "-v", str(max_mismatches),
            "-a",
            "-S",
            "--sam-nohead",
            "-p", "1",
        ]

        logger.debug(f"Running Bowtie for off-targets: {' '.join(bowtie_cmd)}")

        result = subprocess.run(
            bowtie_cmd,
            capture_output=True,
            text=True,
            timeout=CRISPR_TIMEOUT,
        )

        if result.returncode != 0:
            # Bowtie returns non-zero if no alignments found, check stderr
            if "No alignments" in result.stderr or not result.stderr.strip():
                # No alignments is OK, just means no off-targets
                if status is not None:
                    status["performed"] = True
                return []
            msg = f"Bowtie off-target search failed: {result.stderr}"
            logger.warning(msg)
            if warnings is not None:
                warnings.append(msg)
            return []

        if status is not None:
            status["performed"] = True

        # Parse SAM output
        # SAM columns: QNAME FLAG RNAME POS MAPQ CIGAR RNEXT PNEXT TLEN SEQ QUAL [TAGS...]
        # Relevant columns:
        #   - RNAME (col 2, 0-indexed): chromosome
        #   - POS (col 3): 1-based position
        #   - FLAG (col 1): strand info (bit 16 = reverse)
        #   - Tags: NM:i:N for edit distance, MD:Z for mismatch details
        is_diploid = any(org in organism_tag for org in DIPLOID_ORGANISMS)
        guide_len = len(guide)

        for line in result.stdout.strip().split("\n"):
            if not line or line.startswith("@"):
                continue

            fields = line.split("\t")
            if len(fields) < 11:
                continue

            flag = int(fields[1])
            chromosome = fields[2]
            position = int(fields[3])  # 1-based
            seq = fields[9]

            # Skip unmapped reads
            if chromosome == "*" or flag & 4:
                continue

            # Determine strand from FLAG
            strand = "-" if flag & 16 else "+"

            # Extract mismatch count from NM tag
            mm_count = 0
            for tag in fields[11:]:
                if tag.startswith("NM:i:"):
                    mm_count = int(tag[5:])
                    break

            # Skip if exceeds max mismatches
            if mm_count > max_mismatches:
                continue

            # Calculate mismatch positions from MD tag or sequence comparison
            mm_positions = []
            md_tag = None
            for tag in fields[11:]:
                if tag.startswith("MD:Z:"):
                    md_tag = tag[5:]
                    break

            if md_tag:
                mm_positions = _parse_md_tag_mismatches(md_tag)
            else:
                # Fall back to sequence comparison
                if strand == "+":
                    mm_count, mm_positions = _count_mismatches(guide, seq)
                else:
                    # For reverse strand, bowtie reports reverse complement
                    mm_count, mm_positions = _count_mismatches(guide, seq)

            # Check exclusions (on-target site)
            if _should_exclude_offtarget(
                chromosome, position, strand, mm_count,
                exclude_position, guide_cds_position, is_diploid
            ):
                continue

            # Validate PAM at position
            hit_start_0 = position - 1  # 0-based
            hit_end_0 = hit_start_0 + guide_len

            if chromosome not in chromosome_cache:
                chromosome_cache[chromosome] = _get_chromosome_sequence(
                    db, chromosome, organism_tag
                )
            chromosome_seq = chromosome_cache[chromosome]

            if chromosome_seq:
                pam_seq = _validate_pam_at_position(
                    chromosome_seq,
                    hit_start_0,
                    hit_end_0,
                    strand,
                    pam_type,
                    guide_length=guide_len,
                )
                if not pam_seq:
                    continue

            # Calculate CFD score
            target_seq = seq if len(seq) == guide_len else seq[:guide_len]
            cfd = _calculate_cfd_score(guide, target_seq)

            # Map position to gene
            gene_name, gene_region = _map_position_to_gene(
                db, chromosome, position, strand, organism_tag
            )

            # Check if hit is in a related gene
            is_paralog, is_ortholog, homology_rel = _check_related_gene(
                gene_name, related_genes
            )

            offtargets.append(OffTargetHit(
                chromosome=chromosome,
                position=position,
                strand=strand,
                sequence=target_seq,
                mismatches=mm_count,
                mismatch_positions=mm_positions,
                gene_name=gene_name,
                gene_region=gene_region,
                cfd_score=cfd,
                is_paralog=is_paralog,
                is_ortholog=is_ortholog,
                homology_relationship=homology_rel,
            ))

    except subprocess.TimeoutExpired:
        logger.warning(f"Bowtie off-target search timed out for guide: {guide[:10]}...")
        return []
    except Exception as e:
        logger.error(f"Error in Bowtie off-target search: {e}")
        return []

    # For diploid organisms, exclude allelic pairs
    if any(org in organism_tag for org in DIPLOID_ORGANISMS):
        offtargets = _exclude_allelic_pairs(offtargets)

    # Sort by mismatches (ascending), then by CFD score (descending)
    offtargets.sort(key=lambda x: (x.mismatches, -x.cfd_score))

    logger.info(
        f"Bowtie off-target search: {len(offtargets)} off-targets found "
        f"for guide {guide[:10]}..."
    )

    return offtargets


def _parse_md_tag_mismatches(md_tag: str) -> List[int]:
    """
    Parse MD tag from SAM format to extract mismatch positions.

    MD tag format: numbers indicate matching bases, letters indicate mismatches.
    Example: "5A10T3" means 5 matches, mismatch at pos 5, 10 matches, mismatch at pos 16, 3 matches.

    Returns list of 0-indexed mismatch positions.
    """
    positions = []
    current_pos = 0

    i = 0
    while i < len(md_tag):
        # Check for number (matching bases)
        if md_tag[i].isdigit():
            num_str = ""
            while i < len(md_tag) and md_tag[i].isdigit():
                num_str += md_tag[i]
                i += 1
            current_pos += int(num_str)
        # Check for deletion (^) - skip
        elif md_tag[i] == "^":
            i += 1
            while i < len(md_tag) and md_tag[i].isalpha():
                i += 1
        # Letter indicates mismatch
        elif md_tag[i].isalpha():
            positions.append(current_pos)
            current_pos += 1
            i += 1
        else:
            i += 1

    return positions


def _exclude_allelic_pairs(offtargets: List[OffTargetHit]) -> List[OffTargetHit]:
    """
    Exclude allelic pairs from off-target list for diploid organisms.

    For C. albicans, each guide will match both A and B alleles at the same position.
    Both are "on-target" hits, not off-targets. This function identifies and removes
    these allelic pairs.

    Strategy:
    - Group exact matches (0 mismatches) by chromosome base (e.g., "Ca22chr1")
    - If we find hits on both A and B alleles at similar positions, exclude both
    - Keep only one hit if there's no matching allelic partner (true off-target)
    """
    if not offtargets:
        return offtargets

    # Separate exact matches from hits with mismatches
    exact_matches = [ot for ot in offtargets if ot.mismatches == 0]
    other_hits = [ot for ot in offtargets if ot.mismatches > 0]

    if not exact_matches:
        return offtargets

    # Group exact matches by chromosome base
    by_chr_base: Dict[str, List[OffTargetHit]] = defaultdict(list)

    for ot in exact_matches:
        chr_base = _get_chromosome_base(ot.chromosome)
        if chr_base:
            by_chr_base[chr_base].append(ot)
        else:
            # Can't determine chromosome base, keep as potential off-target
            other_hits.append(ot)

    # For each chromosome base, check for allelic pairs
    filtered_exact = []
    for chr_base, hits in by_chr_base.items():
        if len(hits) == 1:
            # Only one hit on this chromosome - could be true off-target
            # But for diploid genomes, a single exact match is likely
            # the on-target (the other allele wasn't found for some reason)
            # We'll exclude it if it's the only exact match overall
            filtered_exact.append(hits[0])
        elif len(hits) == 2:
            # Two hits - check if they're at similar positions (allelic pair)
            h1, h2 = hits
            # Allelic pairs should be at very similar positions and same strand
            if abs(h1.position - h2.position) < 100 and h1.strand == h2.strand:
                # This is an allelic pair (A and B allele) - exclude both
                logger.debug(
                    f"Excluding allelic pair: {h1.chromosome}:{h1.position} "
                    f"and {h2.chromosome}:{h2.position}"
                )
            else:
                # Different positions - these are true off-targets
                filtered_exact.extend(hits)
        else:
            # More than 2 hits on same chromosome base - unusual
            # Try to find and exclude allelic pairs, keep the rest
            # Group by position (within 100bp tolerance)
            position_groups: Dict[int, List[OffTargetHit]] = defaultdict(list)
            for h in hits:
                # Round position to nearest 100 for grouping
                pos_key = h.position // 100 * 100
                position_groups[pos_key].append(h)

            for pos_key, group in position_groups.items():
                if len(group) == 2:
                    # Potential allelic pair
                    h1, h2 = group
                    if h1.strand == h2.strand:
                        logger.debug(
                            f"Excluding allelic pair: {h1.chromosome}:{h1.position} "
                            f"and {h2.chromosome}:{h2.position}"
                        )
                        continue
                # Keep non-paired hits
                filtered_exact.extend(group)

    # If we have exactly 2 exact matches total and they form an allelic pair,
    # both were excluded, which is correct (on-target + its allele)
    # If we have 1 exact match, it might be a true off-target or the on-target
    # We keep it for now and let the calling code decide

    return filtered_exact + other_hits


def _calculate_specificity_score(offtargets: List[OffTargetHit]) -> float:
    """
    Calculate specificity score based on off-targets.

    Returns 0-100, where 100 = no off-targets (most specific).
    """
    if not offtargets:
        return 100.0

    # Weight off-targets by mismatch count
    total_penalty = 0.0
    for ot in offtargets:
        if ot.mismatches == 0:
            total_penalty += 50  # Exact match is very bad
        elif ot.mismatches == 1:
            total_penalty += 20
        elif ot.mismatches == 2:
            total_penalty += 5
        else:
            total_penalty += 1

    # Convert to score (diminishing returns)
    score = 100.0 / (1 + total_penalty / 10)
    return max(0, min(100, score))


# ============================================================================
# Main API Functions
# ============================================================================

def get_crispr_config() -> CrisprConfigResponse:
    """Get CRISPR tool configuration options."""
    # Curated list of organisms supported for CRISPR guide design
    # Each tuple: (tag, display_name)
    CRISPR_ORGANISMS = [
        ("C_albicans_SC5314_A22", "Candida albicans SC5314 (Assembly 22)"),
        ("C_albicans_SC5314_A21", "Candida albicans SC5314 (Assembly 21)"),
        ("C_albicans_SC5314_A19", "Candida albicans SC5314 (Assembly 19)"),
        ("C_dubliniensis_CD36", "Candida dubliniensis CD36"),
        ("C_parapsilosis_CDC317", "Candida parapsilosis CDC317"),
        ("C_auris_B8441", "Candida auris B8441"),
        ("C_glabrata_CBS138", "Candida glabrata CBS138"),
    ]

    organisms = [
        {"tag": tag, "name": name}
        for tag, name in CRISPR_ORGANISMS
    ]

    # PAM options
    pam_options = [
        {"value": pam.value, "label": config["display"]}
        for pam, config in PAM_PATTERNS.items()
    ]

    # Cloning systems
    cloning_systems = [
        {"value": key, "label": config["name"], "description": config["description"]}
        for key, config in CLONING_SYSTEMS.items()
    ]

    return CrisprConfigResponse(
        pam_options=pam_options,
        organisms=organisms,
        default_guide_length=20,
        max_guides=MAX_GUIDES_PER_REQUEST,
        cloning_systems=cloning_systems,
    )


def design_guides(
    db: Session,
    request: CrisprDesignRequest
) -> CrisprDesignResponse:
    """
    Design CRISPR guide RNAs for a gene or sequence.

    This is the main entry point for guide design.
    """
    warnings = []

    # Validate request
    if not request.gene_name and not request.sequence:
        return CrisprDesignResponse(
            success=False,
            error="Either gene_name or sequence must be provided",
            organism=request.organism,
            pam=request.pam.value,
            guide_length=request.guide_length,
        )

    if request.pam not in PAM_PATTERNS:
        return CrisprDesignResponse(
            success=False,
            error=f"Unsupported PAM sequence: {request.pam.value}",
            organism=request.organism,
            pam=request.pam.value,
            guide_length=request.guide_length,
        )

    # Enforce limits
    max_guides = min(request.max_guides, MAX_GUIDES_PER_REQUEST)
    if request.max_guides > MAX_GUIDES_PER_REQUEST:
        warnings.append(f"max_guides limited to {MAX_GUIDES_PER_REQUEST}")

    if len(request.offtarget_genomes) > MAX_OFFTARGET_GENOMES:
        warnings.append(f"offtarget_genomes limited to {MAX_OFFTARGET_GENOMES}")
        request.offtarget_genomes = request.offtarget_genomes[:MAX_OFFTARGET_GENOMES]

    if request.offtarget_genomes:
        warnings.append(
            "Additional off-target genomes are not implemented yet; "
            "checking the selected organism only"
        )

    if request.include_homology_arms:
        warnings.append(
            "Homology arm design is not implemented yet; returning guide results only"
        )

    # Get target sequence
    gene_info = None
    target_sequence = None
    target_feature = None  # For paralog/ortholog lookup
    related_genes = {}  # Dict mapping gene_name -> relationship type

    # Track upstream sequence length (for FIVE_PRIME_UPSTREAM option)
    upstream_length = 0

    if request.gene_name:
        # Look up gene in database
        result = _get_gene_info(db, request.gene_name, request.organism)
        if not result:
            return CrisprDesignResponse(
                success=False,
                error=f"Gene '{request.gene_name}' not found in {request.organism}",
                organism=request.organism,
                pam=request.pam.value,
                guide_length=request.guide_length,
            )
        gene_info, target_sequence, target_feature = result

        # Get related genes (paralogs/orthologs) for off-target flagging
        if target_feature:
            related_genes = _get_related_genes(db, target_feature)

        # For FIVE_PRIME_UPSTREAM, prepend upstream genomic sequence to CDS
        if request.target_region == TargetRegion.FIVE_PRIME_UPSTREAM and target_feature:
            genomic_context = _get_genomic_context(
                db,
                target_feature.feature_no,
                upstream=UPSTREAM_REGION_LENGTH,
                downstream=0
            )
            if genomic_context:
                upstream_seq, _, _ = genomic_context
                upstream_length = len(upstream_seq)
                target_sequence = upstream_seq + target_sequence
                logger.info(
                    f"Added {upstream_length}bp upstream for FIVE_PRIME_UPSTREAM targeting "
                    f"(total sequence: {len(target_sequence)}bp)"
                )
            else:
                warnings.append(
                    "Could not fetch upstream sequence; using CDS only"
                )
    else:
        # Use provided sequence
        target_sequence = request.sequence.upper()
        # Clean sequence
        target_sequence = re.sub(r"[^ACGTN]", "", target_sequence)

    # Validate sequence length
    if len(target_sequence) > MAX_SEQUENCE_LENGTH:
        return CrisprDesignResponse(
            success=False,
            error=f"Sequence too long ({len(target_sequence)} bp). Maximum is {MAX_SEQUENCE_LENGTH} bp.",
            organism=request.organism,
            pam=request.pam.value,
            guide_length=request.guide_length,
        )

    if len(target_sequence) < request.guide_length + 3:  # guide + PAM
        return CrisprDesignResponse(
            success=False,
            error=f"Sequence too short ({len(target_sequence)} bp). Minimum is {request.guide_length + 3} bp.",
            organism=request.organism,
            pam=request.pam.value,
            guide_length=request.guide_length,
        )

    # Find all PAM sites
    all_guides = _find_pam_sites(target_sequence, request.pam, request.guide_length)

    # Filter by target region
    if request.gene_name:
        all_guides = _filter_target_region(
            all_guides,
            len(target_sequence),
            request.target_region,
            upstream_length=upstream_length
        )

    if not all_guides:
        return CrisprDesignResponse(
            success=True,
            gene_info=gene_info,
            target_sequence=target_sequence[:100] + "..." if len(target_sequence) > 100 else target_sequence,
            target_length=len(target_sequence),
            organism=request.organism,
            pam=request.pam.value,
            guide_length=request.guide_length,
            total_guides_found=0,
            guides=[],
            warnings=warnings + [f"No {request.pam.value} PAM sites found in target region"],
        )

    # Score and rank guides with a CHOPCHOP-style penalty. Before
    # off-target analysis, the penalty uses sequence-intrinsic features only.
    guide_results = []
    pam_config = PAM_PATTERNS[request.pam]

    # Track efficiency scoring method for reporting
    efficiency_methods_used = set()

    for guide_seq, pam_seq, position, strand in all_guides:
        # Skip guides with non-ACGT characters
        if not re.match(r"^[ACGT]+$", guide_seq):
            continue

        # Calculate efficiency scores with Azimuth if possible
        gc_content = _calculate_gc_content(guide_seq)

        # Extract 30-mer context for Azimuth prediction
        context_30mer = _get_30mer_context(
            target_sequence,
            position,
            request.guide_length,
            pam_length=len(pam_seq),
            strand=strand
        )

        efficiency_score, efficiency_method = _calculate_efficiency_score(
            guide_seq,
            context_30mer=context_30mer,
            pam_type=request.pam
        )
        efficiency_methods_used.add(efficiency_method)

        # Initial specificity score (will be updated after off-target search)
        specificity_score = 100.0
        offtargets = []

        chopchop_penalty = _calculate_chopchop_penalty(
            guide_seq,
            efficiency_score,
            offtargets,
            gc_content,
            position=position,
            cds_length=len(target_sequence) - upstream_length,
            target_region=request.target_region,
        )
        combined_score = _chopchop_penalty_to_display_score(chopchop_penalty)
        self_complementarity = _calculate_self_complementarity(guide_seq)

        # Build full target sequence (PAM is already captured from _find_pam_sites)
        if pam_config["position"] == "3prime":
            full_target = guide_seq + pam_seq
        else:
            full_target = pam_seq + guide_seq

        # Check for restriction sites
        restriction_sites = _find_restriction_sites(guide_seq)

        # Generate cloning primers
        primers = _generate_cloning_primers(guide_seq)

        # Count off-targets by mismatch
        ot_0mm = sum(1 for ot in offtargets if ot.mismatches == 0)
        ot_1mm = sum(1 for ot in offtargets if ot.mismatches == 1)
        ot_2mm = sum(1 for ot in offtargets if ot.mismatches == 2)
        ot_3mm = sum(1 for ot in offtargets if ot.mismatches == 3)
        ot_4mm = sum(1 for ot in offtargets if ot.mismatches == 4)

        # Calculate genomic coordinates if we have gene info
        # Account for upstream_length when FIVE_PRIME_UPSTREAM is used
        # cds_relative_position: position relative to CDS start (can be negative for upstream)
        genomic_start = None
        genomic_end = None
        chromosome = None
        if gene_info and gene_info.start:
            # Convert position in combined sequence to position relative to CDS start
            # Position 1 in combined seq = -upstream_length relative to CDS start
            # Position upstream_length+1 = 1 relative to CDS start
            cds_relative_position = position - upstream_length

            if gene_info.strand == "+":
                # For + strand: upstream is at lower genomic coords
                # CDS position 1 = gene_info.start
                genomic_start = gene_info.start + cds_relative_position - 1
                genomic_end = genomic_start + request.guide_length - 1
            else:
                # For - strand: upstream is at higher genomic coords
                # CDS position 1 = gene_info.end
                genomic_end = gene_info.end - cds_relative_position + 1
                genomic_start = genomic_end - request.guide_length + 1
            chromosome = gene_info.chromosome

        guide_results.append(GuideResult(
            rank=0,  # Will be set after sorting
            sequence=guide_seq,
            pam=pam_seq,
            full_target=full_target,
            position=position,
            strand=strand,
            genomic_start=genomic_start,
            genomic_end=genomic_end,
            chromosome=chromosome,
            gc_content=round(gc_content, 1),
            efficiency_score=round(efficiency_score, 1),
            specificity_score=round(specificity_score, 1),
            combined_score=round(combined_score, 1),
            chopchop_penalty=round(chopchop_penalty, 3),
            offtarget_checked=False,
            offtarget_count=len(offtargets),
            offtarget_0mm=ot_0mm,
            offtarget_1mm=ot_1mm,
            offtarget_2mm=ot_2mm,
            offtarget_3mm=ot_3mm,
            offtarget_4mm=ot_4mm,
            offtargets=offtargets[:10],  # Limit for response size
            has_poly_t=_has_poly_t(guide_seq),
            self_complementarity=self_complementarity,
            restriction_sites=restriction_sites,
            primers=primers,
            homology_arms=None,  # TODO: Implement if requested
            jbrowse_url=_generate_guide_jbrowse_url(
                request.organism, chromosome, genomic_start, genomic_end
            ),
        ))

    # CHOPCHOP ranks lower penalties first. Position is used as a stable
    # tie-breaker so equally scoring knockout guides favor the 5' end.
    guide_results.sort(key=lambda g: (g.chopchop_penalty, g.position))

    # Log efficiency method used
    if "azimuth" in efficiency_methods_used:
        if "heuristic" in efficiency_methods_used:
            logger.info("Efficiency scoring: mixed (Azimuth + heuristic fallback)")
        else:
            logger.info("Efficiency scoring: Azimuth (Rule Set 2)")
    else:
        logger.info("Efficiency scoring: heuristic only")
        if request.pam == PAMType.NGG and azimuth_available():
            # Azimuth available but not used - likely context extraction failed
            warnings.append(
                "Efficiency scores used heuristic method; "
                "30-mer context unavailable for Azimuth prediction"
            )

    # Perform off-target search for top guides (limited for performance)
    if request.check_offtargets:
        # Limit off-target search to top N guides
        guides_for_offtarget = guide_results[:MAX_GUIDES_FOR_OFFTARGET]

        # Determine which search method to use
        # Brute-force is accurate but slow for large genomes
        # Limit: 15 Mb (C. glabrata ~12Mb, C. albicans ~30Mb)
        MAX_BRUTEFORCE_GENOME_SIZE = 15_000_000

        # Check for explicit method selection
        use_bowtie = request.offtarget_method == OffTargetMethod.BOWTIE
        use_bruteforce = request.offtarget_method == OffTargetMethod.BRUTEFORCE
        if request.offtarget_method == OffTargetMethod.AUTO:
            use_bruteforce = True  # Default to brute-force, will check size below

        # Pre-load chromosome sequences for brute-force (reused across guides)
        chromosome_cache = None
        if use_bruteforce:
            chromosome_cache = _get_all_chromosome_sequences(db, request.organism)
            if not chromosome_cache:
                use_bruteforce = False
                warnings.append(
                    "Brute-force search unavailable: no chromosome sequences found. "
                    "Falling back to BLAST."
                )
            else:
                # Check genome size
                genome_size = sum(len(seq) for seq in chromosome_cache.values())
                if genome_size > MAX_BRUTEFORCE_GENOME_SIZE:
                    if request.offtarget_method == OffTargetMethod.AUTO:
                        # Auto mode: fall back to BLAST for large genomes
                        use_bruteforce = False
                        warnings.append(
                            f"Genome size ({genome_size:,} bp) exceeds brute-force limit "
                            f"({MAX_BRUTEFORCE_GENOME_SIZE:,} bp). Using BLAST instead."
                        )
                        chromosome_cache = None  # Free memory
                    else:
                        # Explicit brute-force requested: warn but proceed
                        warnings.append(
                            f"Brute-force search on large genome ({genome_size:,} bp) "
                            "may be slow. Consider using 'blast' or 'auto' method."
                        )

        method_name = "bowtie" if use_bowtie else ("brute-force" if use_bruteforce else "BLAST")
        logger.info(
            f"Running {method_name} off-target search for "
            f"{len(guides_for_offtarget)} guides"
        )

        for guide in guides_for_offtarget:
            # Build exclude position to skip the on-target site
            exclude_pos = None
            if guide.chromosome and guide.genomic_start:
                genomic_strand = _guide_genomic_strand(
                    gene_info.strand if gene_info else None,
                    guide.strand,
                )
                exclude_pos = (guide.chromosome, guide.genomic_start, genomic_strand)
                logger.debug(
                    f"Guide {guide.rank} exclude_pos: {exclude_pos}"
                )
            else:
                logger.warning(
                    f"Guide {guide.rank} has no genomic coords: "
                    f"chromosome={guide.chromosome}, genomic_start={guide.genomic_start}"
                )

            # Search for off-targets using selected method
            offtarget_warnings = [] if guide == guides_for_offtarget[0] else None
            offtarget_status = {}

            if use_bowtie:
                offtargets = _search_offtargets_bowtie(
                    db=db,
                    guide=guide.sequence,
                    pam_type=request.pam,
                    organism_tag=request.organism,
                    max_mismatches=request.max_offtarget_mismatches,
                    exclude_position=exclude_pos,
                    guide_cds_position=guide.position,
                    warnings=offtarget_warnings,
                    related_genes=related_genes,
                    status=offtarget_status,
                )
            elif use_bruteforce:
                offtargets = _search_offtargets_bruteforce(
                    db=db,
                    guide=guide.sequence,
                    pam_type=request.pam,
                    organism_tag=request.organism,
                    max_mismatches=request.max_offtarget_mismatches,
                    exclude_position=exclude_pos,
                    guide_cds_position=guide.position,
                    warnings=offtarget_warnings,
                    related_genes=related_genes,
                    status=offtarget_status,
                    chromosome_cache=chromosome_cache,
                )
            else:
                offtargets = _search_offtargets_blast(
                    db=db,
                    guide=guide.sequence,
                    pam_type=request.pam,
                    organism_tag=request.organism,
                    max_mismatches=request.max_offtarget_mismatches,
                    exclude_position=exclude_pos,
                    guide_cds_position=guide.position,
                    warnings=offtarget_warnings,
                    related_genes=related_genes,
                    status=offtarget_status,
                )

            if offtarget_warnings:
                warnings.extend(offtarget_warnings)

            # Update guide with off-target information
            guide.offtarget_checked = bool(offtarget_status.get("performed"))
            guide.offtargets = offtargets[:10]  # Limit stored hits
            guide.offtarget_count = len(offtargets)
            guide.offtarget_0mm = sum(1 for ot in offtargets if ot.mismatches == 0)
            guide.offtarget_1mm = sum(1 for ot in offtargets if ot.mismatches == 1)
            guide.offtarget_2mm = sum(1 for ot in offtargets if ot.mismatches == 2)
            guide.offtarget_3mm = sum(1 for ot in offtargets if ot.mismatches == 3)
            guide.offtarget_4mm = sum(1 for ot in offtargets if ot.mismatches == 4)

            # Count off-targets in paralogs and orthologs
            guide.offtarget_in_paralogs = sum(1 for ot in offtargets if ot.is_paralog)
            guide.offtarget_in_orthologs = sum(1 for ot in offtargets if ot.is_ortholog)
            guide.has_related_gene_offtargets = (
                guide.offtarget_in_paralogs > 0 or guide.offtarget_in_orthologs > 0
            )

            # Recalculate specificity score based on actual off-targets only
            # when the search completed. If BLAST is unavailable, leave the
            # optimistic default in place but expose offtarget_checked=False.
            if guide.offtarget_checked:
                guide.specificity_score = round(_calculate_specificity_score(offtargets), 1)

            # Recalculate CHOPCHOP-style penalty after off-target analysis.
            guide.chopchop_penalty = _calculate_chopchop_penalty(
                guide.sequence,
                guide.efficiency_score,
                offtargets,
                guide.gc_content,
                position=guide.position,
                cds_length=len(target_sequence) - upstream_length,
                target_region=request.target_region,
            )
            guide.combined_score = _chopchop_penalty_to_display_score(
                guide.chopchop_penalty
            )

        # Re-sort after updating scores. Checked guides sort ahead of
        # unchecked guides because their off-target penalty is evidence-based.
        guide_results.sort(
            key=lambda g: (
                not g.offtarget_checked,
                g.chopchop_penalty,
                g.position,
            )
        )

        if len(guides_for_offtarget) < len(guide_results):
            warnings.append(
                f"Off-target search performed for top {MAX_GUIDES_FOR_OFFTARGET} guides only"
            )

    # Assign ranks and limit results
    for i, guide in enumerate(guide_results[:max_guides]):
        guide.rank = i + 1

    guide_results = guide_results[:max_guides]

    return CrisprDesignResponse(
        success=True,
        gene_info=gene_info,
        target_sequence=target_sequence[:100] + "..." if len(target_sequence) > 100 else target_sequence,
        target_length=len(target_sequence),
        organism=request.organism,
        pam=request.pam.value,
        guide_length=request.guide_length,
        total_guides_found=len(all_guides),
        guides=guide_results,
        warnings=warnings,
    )


def get_gene_sequence(
    db: Session,
    gene_name: str,
    organism: str = "C_albicans_SC5314_A22"
) -> Optional[Tuple[GeneInfo, str]]:
    """
    Get gene information and sequence.

    Convenience function for the /gene/{name} endpoint.
    """
    result = _get_gene_info(db, gene_name, organism)
    if result is None:
        return None
    # Return only gene_info and sequence (not the feature object)
    gene_info, sequence, _feature = result
    return gene_info, sequence


def generate_download(
    guides: List[GuideResult],
    gene_info: Optional[GeneInfo],
    format: str = "tsv",
    include_offtargets: bool = False,
    include_primers: bool = True
) -> str:
    """
    Generate downloadable content from guide results.

    Returns formatted string (TSV, CSV, or FASTA).
    """
    if format == "fasta":
        lines = []
        for guide in guides:
            header = f">guide_{guide.rank}|pos={guide.position}|strand={guide.strand}|score={guide.combined_score}"
            if gene_info:
                header += f"|gene={gene_info.gene_name or gene_info.feature_name}"
            lines.append(header)
            lines.append(guide.sequence)
        return "\n".join(lines)

    # TSV/CSV format
    sep = "\t" if format == "tsv" else ","

    headers = [
        "Rank", "Guide_Sequence", "PAM", "Position", "Strand",
        "GC%", "Efficiency", "Specificity", "Combined_Score", "CHOPCHOP_Penalty",
        "Off-targets", "Poly-T", "Restriction_Sites"
    ]
    if include_primers:
        headers.extend(["Forward_Primer", "Reverse_Primer"])
    if gene_info:
        headers.extend(["Gene", "Chromosome", "Genomic_Start", "Genomic_End"])

    rows = [sep.join(headers)]

    for guide in guides:
        row = [
            str(guide.rank),
            guide.sequence,
            guide.pam,
            str(guide.position),
            guide.strand,
            f"{guide.gc_content:.1f}",
            f"{guide.efficiency_score:.1f}",
            f"{guide.specificity_score:.1f}",
            f"{guide.combined_score:.1f}",
            f"{guide.chopchop_penalty:.3f}",
            str(guide.offtarget_count),
            "Yes" if guide.has_poly_t else "No",
            ";".join(rs.enzyme for rs in guide.restriction_sites) or "None",
        ]
        if include_primers and guide.primers:
            row.extend([guide.primers.forward, guide.primers.reverse])
        elif include_primers:
            row.extend(["", ""])
        if gene_info:
            row.extend([
                gene_info.gene_name or gene_info.feature_name,
                guide.chromosome or "",
                str(guide.genomic_start) if guide.genomic_start else "",
                str(guide.genomic_end) if guide.genomic_end else "",
            ])

        rows.append(sep.join(row))

    return "\n".join(rows)
