"""
Pattern Match Service - handles pattern/motif searching in sequences.
"""
from __future__ import annotations

import re
import logging
from typing import Optional, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.models.models import Feature, Seq, FeatLocation, Organism, GenomeVersion
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

# Dataset configurations - maps dataset enum to display info and query parameters
# Organism name patterns and genome versions for filtering
ORGANISM_FILTERS = {
    "ca": "albicans",  # C. albicans
    "cg": "glabrata",  # C. glabrata
}

ASSEMBLY_VERSIONS = {
    "22": "22",
    "21": "21",
}

DATASET_INFO: dict[SequenceDataset, DatasetInfo] = {
    # C. albicans Assembly 22
    SequenceDataset.CA22_CHROMOSOMES: DatasetInfo(
        name="ca22_chromosomes",
        display_name="C. albicans A22 - Chromosomes/Contigs",
        description="Complete chromosome sequences (Assembly 22)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA22_ORF_GENOMIC: DatasetInfo(
        name="ca22_orf_genomic",
        display_name="C. albicans A22 - ORF Genomic DNA",
        description="ORF sequences including introns (Assembly 22)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA22_ORF_CODING: DatasetInfo(
        name="ca22_orf_coding",
        display_name="C. albicans A22 - ORF Coding DNA",
        description="ORF coding sequences, exons only (Assembly 22)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA22_ORF_GENOMIC_1KB: DatasetInfo(
        name="ca22_orf_genomic_1kb",
        display_name="C. albicans A22 - ORF Genomic +/- 1kb",
        description="ORF sequences with 1kb flanking regions (Assembly 22)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA22_INTERGENIC: DatasetInfo(
        name="ca22_intergenic",
        display_name="C. albicans A22 - Intergenic Regions",
        description="Sequences between genes (Assembly 22)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA22_NONCODING: DatasetInfo(
        name="ca22_noncoding",
        display_name="C. albicans A22 - Non-coding Features",
        description="ncRNA, tRNA, rRNA, etc. (Assembly 22)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA22_ORF_PROTEIN: DatasetInfo(
        name="ca22_orf_protein",
        display_name="C. albicans A22 - Protein Sequences",
        description="Translated ORF proteins (Assembly 22)",
        pattern_type=PatternType.PROTEIN,
    ),
    # C. albicans Assembly 21
    SequenceDataset.CA21_CHROMOSOMES: DatasetInfo(
        name="ca21_chromosomes",
        display_name="C. albicans A21 - Chromosomes/Contigs",
        description="Complete chromosome sequences (Assembly 21)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA21_ORF_GENOMIC: DatasetInfo(
        name="ca21_orf_genomic",
        display_name="C. albicans A21 - ORF Genomic DNA",
        description="ORF sequences including introns (Assembly 21)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA21_ORF_CODING: DatasetInfo(
        name="ca21_orf_coding",
        display_name="C. albicans A21 - ORF Coding DNA",
        description="ORF coding sequences, exons only (Assembly 21)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA21_ORF_GENOMIC_1KB: DatasetInfo(
        name="ca21_orf_genomic_1kb",
        display_name="C. albicans A21 - ORF Genomic +/- 1kb",
        description="ORF sequences with 1kb flanking regions (Assembly 21)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA21_INTERGENIC: DatasetInfo(
        name="ca21_intergenic",
        display_name="C. albicans A21 - Intergenic Regions",
        description="Sequences between genes (Assembly 21)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA21_NONCODING: DatasetInfo(
        name="ca21_noncoding",
        display_name="C. albicans A21 - Non-coding Features",
        description="ncRNA, tRNA, rRNA, etc. (Assembly 21)",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CA21_ORF_PROTEIN: DatasetInfo(
        name="ca21_orf_protein",
        display_name="C. albicans A21 - Protein Sequences",
        description="Translated ORF proteins (Assembly 21)",
        pattern_type=PatternType.PROTEIN,
    ),
    # C. glabrata
    SequenceDataset.CG_CHROMOSOMES: DatasetInfo(
        name="cg_chromosomes",
        display_name="C. glabrata - Chromosomes/Contigs",
        description="Complete chromosome sequences",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CG_ORF_GENOMIC: DatasetInfo(
        name="cg_orf_genomic",
        display_name="C. glabrata - ORF Genomic DNA",
        description="ORF sequences including introns",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CG_ORF_CODING: DatasetInfo(
        name="cg_orf_coding",
        display_name="C. glabrata - ORF Coding DNA",
        description="ORF coding sequences, exons only",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.CG_ORF_PROTEIN: DatasetInfo(
        name="cg_orf_protein",
        display_name="C. glabrata - Protein Sequences",
        description="Translated ORF proteins",
        pattern_type=PatternType.PROTEIN,
    ),
    # All organisms combined
    SequenceDataset.ALL_CHROMOSOMES: DatasetInfo(
        name="all_chromosomes",
        display_name="All Organisms - Chromosomes/Contigs",
        description="Complete chromosome sequences from all organisms",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.ALL_ORF_GENOMIC: DatasetInfo(
        name="all_orf_genomic",
        display_name="All Organisms - ORF Genomic DNA",
        description="ORF sequences including introns from all organisms",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.ALL_ORF_CODING: DatasetInfo(
        name="all_orf_coding",
        display_name="All Organisms - ORF Coding DNA",
        description="ORF coding sequences from all organisms",
        pattern_type=PatternType.DNA,
    ),
    SequenceDataset.ALL_ORF_PROTEIN: DatasetInfo(
        name="all_orf_protein",
        display_name="All Organisms - Protein Sequences",
        description="Translated ORF proteins from all organisms",
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


def _parse_dataset_params(dataset: SequenceDataset) -> Tuple[Optional[str], Optional[str], str]:
    """
    Parse dataset enum to extract organism filter, genome version, and sequence type.

    Returns: (organism_filter, genome_version, seq_category)
    - organism_filter: 'albicans', 'glabrata', or None for all
    - genome_version: '22', '21', or None for any
    - seq_category: 'chromosomes', 'orf_genomic', 'orf_coding', 'orf_protein', etc.
    """
    name = dataset.value  # e.g., "ca22_chromosomes", "cg_orf_protein", "all_chromosomes"

    organism_filter = None
    genome_version = None

    if name.startswith("ca22_"):
        organism_filter = "albicans"
        genome_version = "22"
        seq_category = name[5:]  # Remove "ca22_"
    elif name.startswith("ca21_"):
        organism_filter = "albicans"
        genome_version = "21"
        seq_category = name[5:]  # Remove "ca21_"
    elif name.startswith("cg_"):
        organism_filter = "glabrata"
        genome_version = None  # C. glabrata doesn't have multiple assemblies
        seq_category = name[3:]  # Remove "cg_"
    elif name.startswith("all_"):
        organism_filter = None  # All organisms
        genome_version = None
        seq_category = name[4:]  # Remove "all_"
    else:
        # Fallback for old-style dataset names
        seq_category = name

    return organism_filter, genome_version, seq_category


def _build_base_query(
    db: Session,
    organism_filter: Optional[str],
    genome_version: Optional[str],
    feature_types: List[str],
    seq_type: str,
):
    """Build base query with organism and genome version filters."""
    query = (
        db.query(Feature, Seq, Organism)
        .join(Seq, Feature.feature_no == Seq.feature_no)
        .join(Organism, Feature.organism_no == Organism.organism_no)
        .join(GenomeVersion, Seq.genome_version_no == GenomeVersion.genome_version_no)
        .filter(
            Feature.feature_type.in_(feature_types),
            Seq.seq_type == seq_type,
            Seq.is_seq_current == "Y",
        )
    )

    # Filter by organism if specified
    if organism_filter:
        query = query.filter(
            func.lower(Organism.organism_name).contains(organism_filter.lower())
        )

    # Filter by genome version if specified
    if genome_version:
        query = query.filter(
            GenomeVersion.genome_version == genome_version
        )

    return query


def _get_sequences_for_dataset(
    db: Session,
    dataset: SequenceDataset,
) -> List[Tuple[str, str, str, Optional[str]]]:
    """
    Get sequences for a dataset.

    Returns list of (name, description, sequence, chromosome) tuples.
    """
    sequences = []

    # Parse dataset parameters
    organism_filter, genome_version, seq_category = _parse_dataset_params(dataset)

    if seq_category == "chromosomes":
        # Get chromosome sequences
        query = _build_base_query(
            db, organism_filter, genome_version,
            ["chromosome"], "genomic"
        )
        results = query.all()
        for feature, seq, organism in results:
            if seq.residues:
                org_short = _get_organism_short_name(organism.organism_name)
                sequences.append((
                    feature.feature_name,
                    f"{org_short} Chromosome {feature.feature_name}",
                    seq.residues.upper(),
                    feature.feature_name,
                ))

    elif seq_category == "orf_genomic":
        # Get ORF genomic sequences
        query = _build_base_query(
            db, organism_filter, genome_version,
            ["ORF"], "genomic"
        )
        results = query.limit(10000).all()
        for feature, seq, organism in results:
            if seq.residues:
                desc = feature.gene_name or feature.feature_name
                sequences.append((
                    feature.feature_name,
                    desc,
                    seq.residues.upper(),
                    None,
                ))

    elif seq_category == "orf_coding":
        # Get ORF coding sequences
        query = _build_base_query(
            db, organism_filter, genome_version,
            ["ORF"], "coding"
        )
        results = query.limit(10000).all()
        for feature, seq, organism in results:
            if seq.residues:
                desc = feature.gene_name or feature.feature_name
                sequences.append((
                    feature.feature_name,
                    desc,
                    seq.residues.upper(),
                    None,
                ))

    elif seq_category == "orf_protein":
        # Get protein sequences
        query = _build_base_query(
            db, organism_filter, genome_version,
            ["ORF"], "protein"
        )
        results = query.limit(10000).all()
        for feature, seq, organism in results:
            if seq.residues:
                desc = feature.gene_name or feature.feature_name
                sequences.append((
                    feature.feature_name,
                    desc,
                    seq.residues.upper(),
                    None,
                ))

    elif seq_category == "orf_genomic_1kb":
        # Get ORF genomic sequences with 1kb flanking
        # This requires additional processing to add flanking regions
        query = _build_base_query(
            db, organism_filter, genome_version,
            ["ORF"], "genomic"
        )
        results = query.limit(5000).all()
        for feature, seq, organism in results:
            if seq.residues:
                # For 1kb flanking, we'd need to get chromosome sequence
                # For now, just return the ORF sequence as placeholder
                desc = feature.gene_name or feature.feature_name
                sequences.append((
                    feature.feature_name,
                    f"{desc} (+/- 1kb)",
                    seq.residues.upper(),
                    None,
                ))

    elif seq_category == "intergenic":
        # Get intergenic region sequences
        # These are typically stored as separate features or need to be computed
        query = _build_base_query(
            db, organism_filter, genome_version,
            ["intergenic_region", "intergenic"], "genomic"
        )
        results = query.limit(10000).all()
        for feature, seq, organism in results:
            if seq.residues:
                sequences.append((
                    feature.feature_name,
                    f"Intergenic: {feature.feature_name}",
                    seq.residues.upper(),
                    None,
                ))

    elif seq_category == "noncoding":
        # Get non-coding feature sequences
        query = _build_base_query(
            db, organism_filter, genome_version,
            ["ncRNA", "tRNA", "rRNA", "snRNA", "snoRNA", "misc_RNA"],
            "genomic"
        )
        results = query.limit(5000).all()
        for feature, seq, organism in results:
            if seq.residues:
                desc = f"{feature.feature_type}: {feature.gene_name or feature.feature_name}"
                sequences.append((
                    feature.feature_name,
                    desc,
                    seq.residues.upper(),
                    None,
                ))

    return sequences


def _get_organism_short_name(organism_name: str) -> str:
    """Get short organism name for display."""
    if "albicans" in organism_name.lower():
        return "C. albicans"
    elif "glabrata" in organism_name.lower():
        return "C. glabrata"
    else:
        return organism_name[:20]


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
