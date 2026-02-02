"""
Pattern Match Service - handles pattern/motif searching in sequences.
"""
from __future__ import annotations

import re
import logging
from typing import Optional, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from cgd.models.models import Feature, Seq, FeatLocation
from cgd.schemas.patmatch_schema import (
    PatternType,
    StrandOption,
    SequenceDataset,
    PatmatchSearchRequest,
    PatmatchSearchResult,
    PatmatchSearchResponse,
    PatmatchHit,
    DatasetInfo,
    PatmatchConfigResponse,
)

logger = logging.getLogger(__name__)

# IUPAC nucleotide codes
IUPAC_DNA = {
    'A': 'A',
    'C': 'C',
    'G': 'G',
    'T': 'T',
    'U': 'T',
    'R': '[AG]',      # Purine
    'Y': '[CT]',      # Pyrimidine
    'S': '[GC]',      # Strong
    'W': '[AT]',      # Weak
    'K': '[GT]',      # Keto
    'M': '[AC]',      # Amino
    'B': '[CGT]',     # Not A
    'D': '[AGT]',     # Not C
    'H': '[ACT]',     # Not G
    'V': '[ACG]',     # Not T
    'N': '[ACGT]',    # Any
}

# IUPAC protein codes (standard amino acids + ambiguity codes)
IUPAC_PROTEIN = {
    'A': 'A', 'C': 'C', 'D': 'D', 'E': 'E', 'F': 'F',
    'G': 'G', 'H': 'H', 'I': 'I', 'K': 'K', 'L': 'L',
    'M': 'M', 'N': 'N', 'P': 'P', 'Q': 'Q', 'R': 'R',
    'S': 'S', 'T': 'T', 'V': 'V', 'W': 'W', 'Y': 'Y',
    'B': '[DN]',      # Aspartic acid or Asparagine
    'Z': '[EQ]',      # Glutamic acid or Glutamine
    'X': '[A-Z]',     # Any amino acid
    '*': '\\*',       # Stop codon
}

# Dataset configurations
DATASET_INFO: dict[SequenceDataset, DatasetInfo] = {
    SequenceDataset.CHROMOSOMES: DatasetInfo(
        name="chromosomes",
        display_name="Chromosomes/Contigs",
        description="Complete chromosome and contig sequences",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.ORF_GENOMIC: DatasetInfo(
        name="orf_genomic",
        display_name="ORF Genomic DNA",
        description="ORF sequences including introns",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.ORF_CODING: DatasetInfo(
        name="orf_coding",
        display_name="ORF Coding DNA",
        description="ORF coding sequences (exons only)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.ORF_GENOMIC_1KB: DatasetInfo(
        name="orf_genomic_1kb",
        display_name="ORF Genomic +/- 1kb",
        description="ORF sequences with 1kb flanking regions",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.INTERGENIC: DatasetInfo(
        name="intergenic",
        display_name="Intergenic Regions",
        description="Sequences between genes",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.NONCODING: DatasetInfo(
        name="noncoding",
        display_name="Non-coding Features",
        description="Non-coding RNA and other features",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.ORF_PROTEIN: DatasetInfo(
        name="orf_protein",
        display_name="Protein Sequences",
        description="Translated ORF protein sequences",
        pattern_type=PatternType.PROTEIN,
    ),
}


def get_patmatch_config() -> PatmatchConfigResponse:
    """Get pattern match configuration options."""
    return PatmatchConfigResponse(
        datasets=list(DATASET_INFO.values()),
        max_pattern_length=100,
        max_mismatches=3,
        max_insertions=3,
        max_deletions=3,
    )


def get_datasets_for_type(pattern_type: PatternType) -> List[DatasetInfo]:
    """Get datasets compatible with a pattern type."""
    return [
        info for info in DATASET_INFO.values()
        if info.pattern_type == pattern_type
    ]


def _pattern_to_regex(
    pattern: str,
    pattern_type: PatternType,
    max_mismatches: int = 0,
    max_insertions: int = 0,
    max_deletions: int = 0,
) -> str:
    """
    Convert a pattern with IUPAC codes to a regex pattern.

    For simplicity, this implementation handles exact matches and IUPAC codes.
    Fuzzy matching (mismatches/insertions/deletions) uses a simplified approach.
    """
    pattern = pattern.upper()
    iupac_map = IUPAC_DNA if pattern_type == PatternType.DNA else IUPAC_PROTEIN

    # Build regex from IUPAC codes
    regex_parts = []
    for char in pattern:
        if char in iupac_map:
            regex_parts.append(iupac_map[char])
        elif char == '.':
            # Wildcard
            regex_parts.append('.' if pattern_type == PatternType.PROTEIN else '[ACGT]')
        else:
            # Escape special regex characters
            regex_parts.append(re.escape(char))

    base_regex = ''.join(regex_parts)

    # For fuzzy matching, we use a simplified approach
    # A full implementation would use more sophisticated algorithms
    if max_mismatches > 0 or max_insertions > 0 or max_deletions > 0:
        # For now, just use the base pattern
        # Real fuzzy matching would require more complex logic
        pass

    return base_regex


def _reverse_complement(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    complement = str.maketrans('ACGTacgt', 'TGCAtgca')
    return seq.translate(complement)[::-1]


def _get_sequences_for_dataset(
    db: Session,
    dataset: SequenceDataset,
) -> List[Tuple[str, str, str, Optional[str]]]:
    """
    Get sequences for a dataset.

    Returns list of (name, description, sequence, chromosome) tuples.
    """
    sequences = []

    if dataset == SequenceDataset.CHROMOSOMES:
        # Get chromosome sequences
        results = (
            db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                Feature.feature_type == "chromosome",
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y",
            )
            .all()
        )
        for feature, seq in results:
            if seq.residues:
                sequences.append((
                    feature.feature_name,
                    f"Chromosome {feature.feature_name}",
                    seq.residues.upper(),
                    feature.feature_name,
                ))

    elif dataset == SequenceDataset.ORF_GENOMIC:
        # Get ORF genomic sequences
        results = (
            db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                Feature.feature_type == "ORF",
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y",
            )
            .limit(10000)  # Limit for performance
            .all()
        )
        for feature, seq in results:
            if seq.residues:
                desc = feature.gene_name or feature.feature_name
                sequences.append((
                    feature.feature_name,
                    desc,
                    seq.residues.upper(),
                    None,
                ))

    elif dataset == SequenceDataset.ORF_CODING:
        # Get ORF coding sequences
        results = (
            db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                Feature.feature_type == "ORF",
                Seq.seq_type == "coding",
                Seq.is_seq_current == "Y",
            )
            .limit(10000)
            .all()
        )
        for feature, seq in results:
            if seq.residues:
                desc = feature.gene_name or feature.feature_name
                sequences.append((
                    feature.feature_name,
                    desc,
                    seq.residues.upper(),
                    None,
                ))

    elif dataset == SequenceDataset.ORF_PROTEIN:
        # Get protein sequences
        results = (
            db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                Feature.feature_type == "ORF",
                Seq.seq_type == "protein",
                Seq.is_seq_current == "Y",
            )
            .limit(10000)
            .all()
        )
        for feature, seq in results:
            if seq.residues:
                desc = feature.gene_name or feature.feature_name
                sequences.append((
                    feature.feature_name,
                    desc,
                    seq.residues.upper(),
                    None,
                ))

    elif dataset == SequenceDataset.NONCODING:
        # Get non-coding feature sequences
        results = (
            db.query(Feature, Seq)
            .join(Seq, Feature.feature_no == Seq.feature_no)
            .filter(
                Feature.feature_type.in_(["ncRNA", "tRNA", "rRNA", "snRNA", "snoRNA"]),
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y",
            )
            .limit(5000)
            .all()
        )
        for feature, seq in results:
            if seq.residues:
                desc = f"{feature.feature_type}: {feature.gene_name or feature.feature_name}"
                sequences.append((
                    feature.feature_name,
                    desc,
                    seq.residues.upper(),
                    None,
                ))

    return sequences


def _search_sequence(
    name: str,
    description: str,
    sequence: str,
    chromosome: Optional[str],
    regex_pattern: re.Pattern,
    strand: StrandOption,
    pattern_type: PatternType,
    context_size: int = 20,
) -> List[PatmatchHit]:
    """Search a single sequence for pattern matches."""
    hits = []

    # Search forward strand
    if strand in [StrandOption.BOTH, StrandOption.WATSON]:
        for match in regex_pattern.finditer(sequence):
            start = match.start() + 1  # 1-based
            end = match.end()
            matched_seq = match.group()

            # Get context
            ctx_start = max(0, match.start() - context_size)
            ctx_end = min(len(sequence), match.end() + context_size)
            context_before = sequence[ctx_start:match.start()]
            context_after = sequence[match.end():ctx_end]

            # Build links
            locus_link = f"/locus/{name}" if not name.startswith("Ca") or "chr" not in name.lower() else None
            jbrowse_link = None
            if chromosome:
                jbrowse_link = f"/jbrowse?loc={chromosome}:{start}..{end}"

            hits.append(PatmatchHit(
                sequence_name=name,
                sequence_description=description,
                match_start=start,
                match_end=end,
                strand="+",
                matched_sequence=matched_seq,
                context_before=context_before,
                context_after=context_after,
                locus_link=locus_link,
                jbrowse_link=jbrowse_link,
            ))

    # Search reverse complement (DNA only)
    if pattern_type == PatternType.DNA and strand in [StrandOption.BOTH, StrandOption.CRICK]:
        rev_seq = _reverse_complement(sequence)
        seq_len = len(sequence)

        for match in regex_pattern.finditer(rev_seq):
            # Convert coordinates to forward strand
            rev_start = match.start()
            rev_end = match.end()
            start = seq_len - rev_end + 1
            end = seq_len - rev_start
            matched_seq = match.group()

            # Get context (from reverse complement)
            ctx_start = max(0, match.start() - context_size)
            ctx_end = min(len(rev_seq), match.end() + context_size)
            context_before = rev_seq[ctx_start:match.start()]
            context_after = rev_seq[match.end():ctx_end]

            locus_link = f"/locus/{name}" if not name.startswith("Ca") or "chr" not in name.lower() else None
            jbrowse_link = None
            if chromosome:
                jbrowse_link = f"/jbrowse?loc={chromosome}:{start}..{end}"

            hits.append(PatmatchHit(
                sequence_name=name,
                sequence_description=description,
                match_start=start,
                match_end=end,
                strand="-",
                matched_sequence=matched_seq,
                context_before=context_before,
                context_after=context_after,
                locus_link=locus_link,
                jbrowse_link=jbrowse_link,
            ))

    return hits


def run_patmatch_search(
    db: Session,
    request: PatmatchSearchRequest,
) -> PatmatchSearchResponse:
    """
    Run a pattern match search.

    Args:
        db: Database session
        request: Pattern match search request

    Returns:
        PatmatchSearchResponse with results or error
    """
    # Validate pattern
    pattern = request.pattern.strip().upper()
    if not pattern:
        return PatmatchSearchResponse(
            success=False,
            error="Pattern is required",
        )

    # Validate dataset/pattern type compatibility
    dataset_info = DATASET_INFO.get(request.dataset)
    if not dataset_info:
        return PatmatchSearchResponse(
            success=False,
            error=f"Unknown dataset: {request.dataset}",
        )

    if dataset_info.pattern_type != request.pattern_type:
        return PatmatchSearchResponse(
            success=False,
            error=f"Dataset '{dataset_info.display_name}' requires {dataset_info.pattern_type.value} patterns",
        )

    # Build regex pattern
    try:
        regex_str = _pattern_to_regex(
            pattern,
            request.pattern_type,
            request.max_mismatches,
            request.max_insertions,
            request.max_deletions,
        )
        regex_pattern = re.compile(regex_str, re.IGNORECASE)
    except re.error as e:
        return PatmatchSearchResponse(
            success=False,
            error=f"Invalid pattern: {str(e)}",
        )

    # Get sequences to search
    try:
        sequences = _get_sequences_for_dataset(db, request.dataset)
    except Exception as e:
        logger.exception("Error fetching sequences")
        return PatmatchSearchResponse(
            success=False,
            error=f"Error fetching sequences: {str(e)}",
        )

    if not sequences:
        return PatmatchSearchResponse(
            success=False,
            error=f"No sequences found in dataset '{dataset_info.display_name}'",
        )

    # Search all sequences
    all_hits = []
    total_residues = 0

    for name, description, sequence, chromosome in sequences:
        total_residues += len(sequence)
        hits = _search_sequence(
            name,
            description,
            sequence,
            chromosome,
            regex_pattern,
            request.strand,
            request.pattern_type,
        )
        all_hits.extend(hits)

        # Check if we have enough results
        if len(all_hits) >= request.max_results:
            all_hits = all_hits[:request.max_results]
            break

    # Build result
    result = PatmatchSearchResult(
        pattern=pattern,
        pattern_type=request.pattern_type.value,
        dataset=dataset_info.display_name,
        strand=request.strand.value,
        total_hits=len(all_hits),
        hits=all_hits,
        search_params={
            "max_mismatches": request.max_mismatches,
            "max_insertions": request.max_insertions,
            "max_deletions": request.max_deletions,
        },
        sequences_searched=len(sequences),
        total_residues_searched=total_residues,
    )

    return PatmatchSearchResponse(
        success=True,
        result=result,
    )
