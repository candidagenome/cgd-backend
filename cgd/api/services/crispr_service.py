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
) -> List[Tuple[str, int, str]]:
    """
    Find all PAM sites in a sequence and extract guide sequences.

    Returns list of (guide_sequence, position, strand) tuples.
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

        if is_3prime:
            # PAM is 3' of guide (SpCas9 style)
            # Guide is upstream of PAM
            guide_start = pam_start - guide_length
            guide_end = pam_start
            if guide_start >= 0:
                guide_seq = sequence[guide_start:guide_end]
                position = guide_start + 1  # 1-based
                guides.append((guide_seq, position, "+"))
        else:
            # PAM is 5' of guide (Cas12a style)
            # Guide is downstream of PAM
            guide_start = pam_end
            guide_end = pam_end + guide_length
            if guide_end <= len(sequence):
                guide_seq = sequence[guide_start:guide_end]
                position = pam_start + 1  # 1-based
                guides.append((guide_seq, position, "+"))

    # Search reverse strand
    rev_sequence = _reverse_complement(sequence)
    seq_len = len(sequence)

    for match in re.finditer(pattern, rev_sequence):
        pam_start = match.start()
        pam_end = match.end()

        if is_3prime:
            guide_start = pam_start - guide_length
            guide_end = pam_start
            if guide_start >= 0:
                guide_seq = rev_sequence[guide_start:guide_end]
                # Convert position back to forward strand coordinates
                fwd_position = seq_len - pam_end + 1  # 1-based
                guides.append((guide_seq, fwd_position, "-"))
        else:
            guide_start = pam_end
            guide_end = pam_end + guide_length
            if guide_end <= len(rev_sequence):
                guide_seq = rev_sequence[guide_start:guide_end]
                fwd_position = seq_len - pam_end + 1
                guides.append((guide_seq, fwd_position, "-"))

    return guides


def _filter_target_region(
    guides: List[Tuple[str, int, str]],
    sequence_length: int,
    target_region: TargetRegion
) -> List[Tuple[str, int, str]]:
    """Filter guides to those within the target region."""
    if target_region == TargetRegion.FULL_CDS or target_region == TargetRegion.CUSTOM:
        return guides

    # Calculate region boundaries (20% of sequence)
    region_size = int(sequence_length * 0.2)

    if target_region == TargetRegion.FIVE_PRIME:
        # First 20%
        return [(g, p, s) for g, p, s in guides if p <= region_size]
    elif target_region == TargetRegion.THREE_PRIME:
        # Last 20%
        start = sequence_length - region_size
        return [(g, p, s) for g, p, s in guides if p >= start]

    return guides


# ============================================================================
# Off-target Analysis
# ============================================================================

def _search_offtargets_blast(
    guide: str,
    genome_db: str,
    max_mismatches: int = 3
) -> List[OffTargetHit]:
    """
    Search for off-targets using BLAST.

    Uses short word size for sensitive search.
    """
    # This is a placeholder - actual implementation would run BLAST
    # with parameters optimized for short exact/near-exact matches
    #
    # blast_cmd = [
    #     "blastn",
    #     "-task", "blastn-short",
    #     "-word_size", "7",
    #     "-evalue", "1000",
    #     "-dust", "no",
    #     "-db", genome_db,
    #     "-query", query_file,
    #     "-outfmt", "6 sseqid sstart send sstrand qseq sseq mismatch",
    # ]
    #
    # For now, return empty list - off-target search to be implemented
    # when BLAST databases are confirmed available
    return []


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

    # Score and rank guides
    guide_results = []
    pam_config = PAM_PATTERNS[request.pam]

    for guide_seq, position, strand in all_guides:
        # Skip guides with non-ACGT characters
        if not re.match(r"^[ACGT]+$", guide_seq):
            continue

        # Calculate scores
        gc_content = _calculate_gc_content(guide_seq)
        efficiency_score = _calculate_efficiency_score(guide_seq)

        # Off-target analysis (placeholder - returns empty for now)
        offtargets = []
        if request.check_offtargets:
            # TODO: Implement actual off-target search
            pass

        specificity_score = _calculate_specificity_score(offtargets)

        # Combined score (weighted average)
        combined_score = (efficiency_score * 0.5) + (specificity_score * 0.5)

        # Get PAM sequence from target
        if strand == "+":
            if pam_config["position"] == "3prime":
                pam_start = position - 1 + request.guide_length
                pam_seq = target_sequence[pam_start:pam_start + pam_config["length"]]
            else:
                pam_start = position - 1 - pam_config["length"]
                pam_seq = target_sequence[pam_start:pam_start + pam_config["length"]]
        else:
            # For reverse strand, extract PAM from forward strand and reverse complement
            # Position is where the PAM region starts on forward strand (1-based)
            if pam_config["position"] == "3prime":
                # PAM is at position to position + pam_length on forward strand
                pam_seq = _reverse_complement(
                    target_sequence[position - 1:position - 1 + pam_config["length"]]
                )
            else:
                # For 5' PAM (Cas12a), PAM is before the guide
                pam_seq = _reverse_complement(
                    target_sequence[position - 1 - pam_config["length"]:position - 1]
                )

        # Build full target sequence
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

    # Sort by combined score (descending)
    guide_results.sort(key=lambda g: g.combined_score, reverse=True)

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
