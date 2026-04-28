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
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.core.settings import settings
from cgd.models.models import Feature, Seq, FeatLocation, Organism
from cgd.schemas.crispr_schema import (
    PAMType,
    TargetRegion,
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

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration and Limits
# ============================================================================

# Request limits to prevent server overload
MAX_GUIDES_PER_REQUEST = 100
MAX_SEQUENCE_LENGTH = 50000  # 50kb max input sequence
MAX_OFFTARGET_GENOMES = 3
CRISPR_TIMEOUT = 300  # 5 minutes max for off-target search

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


def _calculate_efficiency_score(guide: str) -> float:
    """
    Calculate predicted efficiency score using simplified Rule Set 2.

    Returns score from 0-100, where higher is better.
    This is a simplified implementation; production would use the full
    Doench 2016 model or a trained model.
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

    # GG motif at positions 19-20 is favorable
    if len(guide) >= 20 and guide[-2:] == "GG":
        score += 5

    # Clamp to 0-100
    return max(0, min(100, score))


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


# ============================================================================
# Database Functions
# ============================================================================

def _get_gene_info(
    db: Session,
    gene_name: str,
    organism_tag: str
) -> Optional[Tuple[GeneInfo, str]]:
    """
    Get gene information and coding sequence from database.

    Returns (GeneInfo, sequence) tuple or None if not found.
    """
    query_upper = gene_name.strip().upper()

    # Map organism tag to organism_abbrev (strip assembly suffix)
    org_abbrev = re.sub(r"_A\d+$", "", organism_tag)

    # Build query with organism filter
    def build_query(filter_expr):
        query = db.query(Feature).filter(filter_expr)
        query = query.join(Organism, Feature.organism_no == Organism.organism_no)
        query = query.filter(Organism.organism_abbrev == org_abbrev)
        return query

    # Try gene_name, then feature_name, then dbxref_id
    feature = build_query(func.upper(Feature.gene_name) == query_upper).first()
    if not feature:
        feature = build_query(func.upper(Feature.feature_name) == query_upper).first()
    if not feature:
        feature = build_query(func.upper(Feature.dbxref_id) == query_upper).first()

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

    # Get location info
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
    )

    return gene_info, seq_record.residues.upper()


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
    Position is 1-based, relative to the input sequence.
    """
    pam_config = PAM_PATTERNS[pam_type]
    pattern = pam_config["pattern"]
    pam_len = pam_config["length"]
    is_3prime = pam_config["position"] == "3prime"

    guides = []
    sequence = sequence.upper()

    # Search forward strand
    for match in re.finditer(pattern, sequence):
        pam_start = match.start()
        pam_end = match.end()
        pam_seq = sequence[pam_start:pam_end]

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
                position = pam_start + 1  # 1-based
                guides.append((guide_seq, pam_seq, position, "+"))

    # Search reverse strand
    rev_sequence = _reverse_complement(sequence)
    seq_len = len(sequence)

    for match in re.finditer(pattern, rev_sequence):
        pam_start = match.start()
        pam_end = match.end()
        pam_seq = rev_sequence[pam_start:pam_end]  # PAM as it appears on reverse strand

        if is_3prime:
            guide_start = pam_start - guide_length
            guide_end = pam_start
            if guide_start >= 0:
                guide_seq = rev_sequence[guide_start:guide_end]
                # Convert position back to forward strand coordinates
                fwd_position = seq_len - pam_end + 1  # 1-based
                guides.append((guide_seq, pam_seq, fwd_position, "-"))
        else:
            guide_start = pam_end
            guide_end = pam_end + guide_length
            if guide_end <= len(rev_sequence):
                guide_seq = rev_sequence[guide_start:guide_end]
                fwd_position = seq_len - pam_end + 1
                guides.append((guide_seq, pam_seq, fwd_position, "-"))

    return guides


def _filter_target_region(
    guides: List[Tuple[str, str, int, str]],
    sequence_length: int,
    target_region: TargetRegion
) -> List[Tuple[str, str, int, str]]:
    """Filter guides to those within the target region."""
    if target_region == TargetRegion.FULL_CDS or target_region == TargetRegion.CUSTOM:
        return guides

    # Calculate region boundaries (20% of sequence)
    region_size = int(sequence_length * 0.2)

    if target_region == TargetRegion.FIVE_PRIME:
        # First 20%
        return [(g, pam, p, s) for g, pam, p, s in guides if p <= region_size]
    elif target_region == TargetRegion.THREE_PRIME:
        # Last 20%
        start = sequence_length - region_size
        return [(g, pam, p, s) for g, pam, p, s in guides if p >= start]

    return guides


# ============================================================================
# Off-target Analysis
# ============================================================================

# Limit number of guides for off-target search (performance)
MAX_GUIDES_FOR_OFFTARGET = 14

# Pattern to extract chromosome base name (without A/B allele suffix)
# Matches patterns like:
#   "Ca22chr1A_C_albicans_SC5314" -> "Ca22chr1"
#   "Ca22chrRA_C_albicans_SC5314" -> "Ca22chrR"
# The pattern captures everything up to and including the chromosome ID (number or R),
# then expects A or B allele suffix
CHROMOSOME_ALLELE_PATTERN = re.compile(r'^(.*chr[R\d]+)[AB](_.*)?$', re.IGNORECASE)

# Diploid organisms where we expect allelic pairs (A/B chromosomes)
DIPLOID_ORGANISMS = {
    "C_albicans_SC5314",
    "C_albicans_SC5314_A22",
    "C_albicans_SC5314_A21",
    "C_albicans_SC5314_A19",
}


def _get_chromosome_base(chromosome: str) -> Optional[str]:
    """
    Extract chromosome base name from BLAST chromosome name.

    E.g., "Ca22chr1A_C_albicans_SC5314" -> "Ca22chr1"
    Returns None if pattern doesn't match.
    """
    match = CHROMOSOME_ALLELE_PATTERN.match(chromosome)
    if match:
        return match.group(1).upper()
    return None


def _are_allelic_chromosomes(chr1: str, chr2: str) -> bool:
    """
    Check if two chromosome names are allelic variants (A vs B allele).

    C. albicans has diploid chromosomes named like:
    - Ca22chr1A_C_albicans_SC5314
    - Ca22chr1B_C_albicans_SC5314

    These represent the same genomic region on different alleles.
    """
    if chr1 == chr2:
        return True

    # Extract base chromosome name (without A/B suffix)
    match1 = CHROMOSOME_ALLELE_PATTERN.match(chr1)
    match2 = CHROMOSOME_ALLELE_PATTERN.match(chr2)

    if match1 and match2:
        base1 = match1.group(1).upper()
        base2 = match2.group(1).upper()
        is_allelic = base1 == base2
        if is_allelic:
            logger.debug(f"Chromosomes are allelic: {chr1} <-> {chr2} (base: {base1})")
        return is_allelic

    # Fallback: check if names differ only in A/B before underscore
    # This handles edge cases the regex might miss
    parts1 = chr1.split('_')
    parts2 = chr2.split('_')
    if parts1 and parts2:
        prefix1 = parts1[0]
        prefix2 = parts2[0]
        # Check if they differ only in last character (A vs B)
        if len(prefix1) == len(prefix2) and len(prefix1) > 1:
            if prefix1[:-1] == prefix2[:-1] and prefix1[-1] in 'AB' and prefix2[-1] in 'AB':
                logger.debug(f"Chromosomes are allelic (fallback): {chr1} <-> {chr2}")
                return True

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
    hit_end: int,
    strand: str,
    pam_type: PAMType,
    guide_length: int = 20
) -> Optional[str]:
    """
    Check if a valid PAM exists adjacent to the off-target hit.

    For 3' PAM systems (NGG, NAG, NNGRRT): PAM is immediately after guide.
    For 5' PAM systems (TTTV): PAM is immediately before guide.

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
                pam_end = hit_end - guide_length
                pam_start = pam_end - pam_len

            if pam_start < 0 or pam_end > len(chromosome_seq):
                return None

            pam_seq = chromosome_seq[pam_start:pam_end]
        else:
            # Minus strand - need reverse complement
            if is_3prime:
                # For minus strand with 3' PAM, PAM is upstream in genomic coords
                pam_end = hit_end - guide_length
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
    org_abbrev = re.sub(r"_A\d+$", "", organism_tag)

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
    org_abbrev = re.sub(r"_A\d+$", "", organism_tag)

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


def _search_offtargets_blast(
    db: Session,
    guide: str,
    pam_type: PAMType,
    organism_tag: str,
    max_mismatches: int = 3,
    exclude_position: Optional[Tuple[str, int, str]] = None,
    warnings: Optional[List[str]] = None,
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
        warnings: Optional list to append warning messages to

    Returns:
        List of OffTargetHit objects sorted by mismatches (ascending)
    """
    from cgd.core.settings import settings

    offtargets = []

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
                # Strategy: For exact matches (0 mismatches), we're more lenient with exclusion
                # because the on-target and its allele should be exact matches. For hits with
                # mismatches, we're stricter to avoid excluding real off-targets.
                if exclude_position:
                    exc_chr, exc_pos, exc_strand = exclude_position

                    # Check if chromosome names match (including allelic A/B variants)
                    is_same_or_allelic = _are_allelic_chromosomes(chromosome, exc_chr)

                    # Check if position is similar (within tolerance)
                    is_similar_position = abs(start - exc_pos) < 100
                    is_same_strand = strand == exc_strand

                    # For exact matches (0 mismatches), also check by position alone
                    # This handles chromosome naming mismatches between DB and BLAST
                    # An exact match at the same position is almost certainly on-target
                    if mm_count == 0 and is_similar_position and is_same_strand:
                        logger.debug(
                            f"Excluding exact match at similar position: {chromosome}:{start} "
                            f"(exclude: {exc_chr}:{exc_pos})"
                        )
                        continue

                    # For hits with mismatches, require chromosome match
                    if is_same_or_allelic and is_similar_position and is_same_strand:
                        logger.debug(
                            f"Excluding on-target/allelic hit: {chromosome}:{start} "
                            f"(exclude: {exc_chr}:{exc_pos})"
                        )
                        continue

                # Get chromosome sequence for PAM validation
                # For now, skip PAM validation if we can't get the sequence
                # (This would require loading chromosome sequences)
                # In production, we'd validate PAM here

                # Calculate CFD score
                cfd = _calculate_cfd_score(guide, subject_ungapped[:len(guide)])

                # Map position to gene
                gene_name, gene_region = _map_position_to_gene(
                    db, chromosome, start, strand, organism_tag
                )

                offtargets.append(OffTargetHit(
                    chromosome=chromosome,
                    position=start,
                    strand=strand,
                    sequence=subject_ungapped[:len(guide)],
                    mismatches=mm_count,
                    mismatch_positions=mm_positions,
                    gene_name=gene_name,
                    gene_region=gene_region,
                    cfd_score=cfd,
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
    from cgd.core.blast_config import get_all_blast_organisms

    # Get organisms from BLAST config
    organisms_config = get_all_blast_organisms(settings.blast_clade_conf)
    organisms = [
        {"tag": tag, "name": config.get("full_name", tag)}
        for tag, config in organisms_config.items()
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

    # Enforce limits
    max_guides = min(request.max_guides, MAX_GUIDES_PER_REQUEST)
    if request.max_guides > MAX_GUIDES_PER_REQUEST:
        warnings.append(f"max_guides limited to {MAX_GUIDES_PER_REQUEST}")

    if len(request.offtarget_genomes) > MAX_OFFTARGET_GENOMES:
        warnings.append(f"offtarget_genomes limited to {MAX_OFFTARGET_GENOMES}")
        request.offtarget_genomes = request.offtarget_genomes[:MAX_OFFTARGET_GENOMES]

    # Get target sequence
    gene_info = None
    target_sequence = None

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
        gene_info, target_sequence = result
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
            request.target_region
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

    # Score and rank guides (first pass - efficiency only)
    guide_results = []
    pam_config = PAM_PATTERNS[request.pam]

    for guide_seq, pam_seq, position, strand in all_guides:
        # Skip guides with non-ACGT characters
        if not re.match(r"^[ACGT]+$", guide_seq):
            continue

        # Calculate efficiency scores
        gc_content = _calculate_gc_content(guide_seq)
        efficiency_score = _calculate_efficiency_score(guide_seq)

        # Initial specificity score (will be updated after off-target search)
        specificity_score = 100.0
        offtargets = []

        # Combined score (weighted average) - initially based on efficiency only
        combined_score = (efficiency_score * 0.5) + (specificity_score * 0.5)

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

        # Calculate genomic coordinates if we have gene info
        genomic_start = None
        genomic_end = None
        chromosome = None
        if gene_info and gene_info.start:
            if gene_info.strand == "+":
                genomic_start = gene_info.start + position - 1
                genomic_end = genomic_start + request.guide_length - 1
            else:
                genomic_end = gene_info.end - position + 1
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
            offtarget_count=len(offtargets),
            offtarget_0mm=ot_0mm,
            offtarget_1mm=ot_1mm,
            offtarget_2mm=ot_2mm,
            offtarget_3mm=ot_3mm,
            offtargets=offtargets[:10],  # Limit for response size
            has_poly_t=_has_poly_t(guide_seq),
            restriction_sites=restriction_sites,
            primers=primers,
            homology_arms=None,  # TODO: Implement if requested
        ))

    # Sort by combined score (descending) for initial ranking
    guide_results.sort(key=lambda g: g.combined_score, reverse=True)

    # Perform off-target search for top guides (limited for performance)
    if request.check_offtargets:
        # Limit off-target search to top N guides
        guides_for_offtarget = guide_results[:MAX_GUIDES_FOR_OFFTARGET]

        logger.info(f"Running off-target search for {len(guides_for_offtarget)} guides")

        for guide in guides_for_offtarget:
            # Build exclude position to skip the on-target site
            exclude_pos = None
            if guide.chromosome and guide.genomic_start:
                exclude_pos = (guide.chromosome, guide.genomic_start, guide.strand)
                logger.debug(
                    f"Guide {guide.rank} exclude_pos: {exclude_pos}"
                )
            else:
                logger.warning(
                    f"Guide {guide.rank} has no genomic coords: "
                    f"chromosome={guide.chromosome}, genomic_start={guide.genomic_start}"
                )

            # Search for off-targets (pass warnings only for first guide to avoid duplicates)
            offtarget_warnings = [] if guide == guides_for_offtarget[0] else None
            offtargets = _search_offtargets_blast(
                db=db,
                guide=guide.sequence,
                pam_type=request.pam,
                organism_tag=request.organism,
                max_mismatches=request.max_offtarget_mismatches,
                exclude_position=exclude_pos,
                warnings=offtarget_warnings,
            )
            if offtarget_warnings:
                warnings.extend(offtarget_warnings)

            # Update guide with off-target information
            guide.offtargets = offtargets[:10]  # Limit stored hits
            guide.offtarget_count = len(offtargets)
            guide.offtarget_0mm = sum(1 for ot in offtargets if ot.mismatches == 0)
            guide.offtarget_1mm = sum(1 for ot in offtargets if ot.mismatches == 1)
            guide.offtarget_2mm = sum(1 for ot in offtargets if ot.mismatches == 2)
            guide.offtarget_3mm = sum(1 for ot in offtargets if ot.mismatches == 3)

            # Recalculate specificity score based on actual off-targets
            guide.specificity_score = round(_calculate_specificity_score(offtargets), 1)

            # Recalculate combined score
            guide.combined_score = round(
                (guide.efficiency_score * 0.5) + (guide.specificity_score * 0.5),
                1
            )

        # Re-sort after updating scores
        guide_results.sort(key=lambda g: g.combined_score, reverse=True)

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
    return _get_gene_info(db, gene_name, organism)


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
        "GC%", "Efficiency", "Specificity", "Combined_Score",
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
