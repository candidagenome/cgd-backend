"""
Pattern Match Service - handles pattern/motif searching in sequences.

Uses nrgrep_coords binary for efficient fuzzy pattern matching,
with Python regex fallback when binary is unavailable.
"""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
import logging
from typing import Optional, List, Dict, Tuple

from cgd.core.patmatch_config import (
    NRGREP_BINARY,
    PATMATCH_DATASETS,
    PatternType,
    DatasetConfig,
    get_available_datasets,
    get_dataset_config,
    convert_pattern_for_nrgrep,
    get_reverse_complement,
    IUPAC_DNA,
    IUPAC_PROTEIN,
)
from cgd.schemas.patmatch_schema import (
    PatternType as SchemaPatternType,
    StrandOption,
    PatmatchSearchRequest,
    PatmatchSearchResult,
    PatmatchSearchResponse,
    PatmatchHit,
    DatasetInfo,
    PatmatchConfigResponse,
)

logger = logging.getLogger(__name__)


def _check_binary_available() -> bool:
    """Check if nrgrep_coords binary is available and executable."""
    return os.path.isfile(NRGREP_BINARY) and os.access(NRGREP_BINARY, os.X_OK)


def _build_mismatch_option(mismatches: int, insertions: int, deletions: int) -> str:
    """
    Build the mismatch option string for nrgrep_coords.

    nrgrep_coords uses -k option for fuzzy matching:
    - -k 0 = exact match
    - -k N = allow up to N total errors (substitutions + insertions + deletions)
    """
    # nrgrep_coords -k option allows total errors
    total_errors = mismatches + insertions + deletions
    return str(total_errors)


def _parse_nrgrep_output(
    output: str,
    fasta_index: Dict[int, str],
    file_offsets: List[int],
) -> List[Tuple[str, int, int, str, str]]:
    """
    Parse nrgrep_coords output.

    nrgrep_coords output format: [start, end: matched_sequence]

    Returns list of (seq_name, start, end, strand, matched_seq) tuples.
    """
    hits = []

    for line in output.strip().split('\n'):
        if not line or not line.startswith('['):
            continue

        # Parse: [start, end: matched_sequence]
        # Remove brackets and parse
        line = line.strip()
        line = re.sub(r'[\[\]:,]', ' ', line)
        parts = line.split()

        if len(parts) < 3:
            continue

        try:
            global_start = int(parts[0])
            global_end = int(parts[1])
            matched_seq = parts[2] if len(parts) > 2 else ""

            # Find which sequence this hit is in using file offsets
            seq_offset = _find_sequence_offset(global_start, file_offsets)
            seq_name = fasta_index.get(seq_offset, "unknown")

            # Convert to local coordinates
            local_start = global_start - seq_offset + 1
            local_end = global_end - seq_offset

            hits.append((seq_name, local_start, local_end, "W", matched_seq))
        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse nrgrep output line: {line}, error: {e}")
            continue

    return hits


def _find_sequence_offset(position: int, offsets: List[int]) -> int:
    """Find the sequence offset for a given global position."""
    result = 0
    for offset in offsets:
        if offset <= position:
            result = offset
        else:
            break
    return result


def _generate_fasta_index(fasta_file: str) -> Tuple[Dict[int, str], List[int]]:
    """
    Generate an index of sequence names and their file offsets.

    Returns: (offset_to_name dict, sorted list of offsets)
    """
    index = {}
    offsets = []
    current_offset = 0
    current_name = None

    try:
        with open(fasta_file, 'r') as f:
            for line in f:
                if line.startswith('>'):
                    # New sequence
                    name = line[1:].split()[0].strip()
                    index[current_offset] = name
                    offsets.append(current_offset)
                    current_name = name
                else:
                    # Sequence data - add to offset
                    current_offset += len(line.strip())
    except IOError as e:
        logger.error(f"Failed to read FASTA file {fasta_file}: {e}")

    return index, sorted(offsets)


def _run_nrgrep_search(
    pattern: str,
    fasta_file: str,
    mismatches: int = 0,
    insertions: int = 0,
    deletions: int = 0,
    max_results: int = 1000,
) -> Tuple[List[Tuple[str, int, int, str, str]], int]:
    """
    Run pattern search using nrgrep_coords binary.

    Returns: (list of (seq_name, start, end, strand, matched_seq) tuples, total_count)
    """
    if not _check_binary_available():
        raise RuntimeError("nrgrep_coords binary not available")

    if not os.path.exists(fasta_file):
        raise FileNotFoundError(f"FASTA file not found: {fasta_file}")

    # Generate index for the FASTA file
    fasta_index, file_offsets = _generate_fasta_index(fasta_file)

    # Build mismatch option
    k_option = _build_mismatch_option(mismatches, insertions, deletions)

    # Build command
    # nrgrep_coords options:
    # -i: case insensitive
    # -k N: allow up to N errors
    # -b SIZE: buffer size
    cmd = [
        NRGREP_BINARY,
        '-i',  # case insensitive
        '-k', k_option,
        '-b', '1600000',  # buffer size
        pattern,
        fasta_file
    ]

    logger.debug(f"Running nrgrep: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )

        if result.returncode != 0 and result.stderr:
            logger.warning(f"nrgrep stderr: {result.stderr}")

        # Parse output
        hits = _parse_nrgrep_output(result.stdout, fasta_index, file_offsets)

        # Track actual total before limiting
        actual_total = len(hits)

        # Limit results
        if len(hits) > max_results:
            hits = hits[:max_results]

        return hits, actual_total

    except subprocess.TimeoutExpired:
        raise RuntimeError("Pattern search timed out")
    except Exception as e:
        raise RuntimeError(f"Pattern search failed: {e}")


def _run_python_search(
    pattern: str,
    fasta_file: str,
    pattern_type: PatternType,
    strand: StrandOption,
    max_results: int = 1000,
) -> Tuple[List[Tuple[str, int, int, str, str]], int, int, int]:
    """
    Run pattern search using Python regex (fallback).

    Returns: (hits, sequences_searched, total_residues, actual_total_hits)
    """
    hits = []
    sequences_searched = 0
    total_residues = 0
    actual_total_hits = 0

    # Build regex pattern
    iupac_map = IUPAC_DNA if pattern_type == PatternType.DNA else IUPAC_PROTEIN
    regex_parts = []
    for char in pattern.upper():
        if char in iupac_map:
            regex_parts.append(iupac_map[char])
        elif char == '.':
            regex_parts.append('.' if pattern_type == PatternType.PROTEIN else '[ACGT]')
        else:
            regex_parts.append(re.escape(char))

    try:
        regex_pattern = re.compile(''.join(regex_parts), re.IGNORECASE)
    except re.error as e:
        raise ValueError(f"Invalid pattern: {e}")

    # Read and search FASTA file
    try:
        with open(fasta_file, 'r') as f:
            current_name = None
            current_seq = []

            for line in f:
                line = line.strip()
                if line.startswith('>'):
                    # Process previous sequence
                    if current_name and current_seq:
                        seq = ''.join(current_seq)
                        sequences_searched += 1
                        total_residues += len(seq)
                        seq_hits = _search_sequence_regex(
                            current_name, seq, regex_pattern,
                            strand, pattern_type
                        )
                        actual_total_hits += len(seq_hits)
                        # Only add to hits list if under max_results
                        if len(hits) < max_results:
                            remaining = max_results - len(hits)
                            hits.extend(seq_hits[:remaining])

                    # Start new sequence
                    current_name = line[1:].split()[0]
                    current_seq = []
                else:
                    current_seq.append(line)

            # Process last sequence
            if current_name and current_seq:
                seq = ''.join(current_seq)
                sequences_searched += 1
                total_residues += len(seq)
                seq_hits = _search_sequence_regex(
                    current_name, seq, regex_pattern,
                    strand, pattern_type
                )
                actual_total_hits += len(seq_hits)
                if len(hits) < max_results:
                    remaining = max_results - len(hits)
                    hits.extend(seq_hits[:remaining])

    except IOError as e:
        raise RuntimeError(f"Failed to read FASTA file: {e}")

    return hits[:max_results], sequences_searched, total_residues, actual_total_hits


def _search_sequence_regex(
    seq_name: str,
    sequence: str,
    regex_pattern: re.Pattern,
    strand: StrandOption,
    pattern_type: PatternType,
) -> List[Tuple[str, int, int, str, str]]:
    """Search a single sequence with regex pattern."""
    hits = []

    # Search Watson strand
    if strand in [StrandOption.BOTH, StrandOption.WATSON]:
        for match in regex_pattern.finditer(sequence):
            start = match.start() + 1  # 1-based
            end = match.end()
            matched_seq = match.group()
            hits.append((seq_name, start, end, "W", matched_seq))

    # Search Crick strand (DNA only)
    if pattern_type == PatternType.DNA and strand in [StrandOption.BOTH, StrandOption.CRICK]:
        rev_seq = get_reverse_complement(sequence)
        seq_len = len(sequence)

        for match in regex_pattern.finditer(rev_seq):
            # Convert to Watson coordinates
            rev_start = match.start()
            rev_end = match.end()
            start = seq_len - rev_end + 1
            end = seq_len - rev_start
            matched_seq = match.group()
            hits.append((seq_name, start, end, "C", matched_seq))

    return hits


def _load_sequences_for_context(
    fasta_file: str,
    seq_names: set,
) -> Dict[str, str]:
    """
    Load sequences from FASTA file for the given sequence names.

    Only loads sequences that are needed, reading the file once.
    Returns: dict mapping seq_name to sequence string
    """
    sequences = {}
    try:
        with open(fasta_file, 'r') as f:
            current_name = None
            current_seq = []

            for line in f:
                line = line.strip()
                if line.startswith('>'):
                    # Save previous sequence if it was needed
                    if current_name and current_name in seq_names and current_seq:
                        sequences[current_name] = ''.join(current_seq)
                        # Early exit if we have all sequences
                        if len(sequences) == len(seq_names):
                            return sequences

                    current_name = line[1:].split()[0]
                    current_seq = [] if current_name in seq_names else None
                elif current_seq is not None:
                    current_seq.append(line)

            # Don't forget the last sequence
            if current_name and current_name in seq_names and current_seq:
                sequences[current_name] = ''.join(current_seq)

    except IOError as e:
        logger.warning(f"Failed to read FASTA for context: {e}")

    return sequences


def _get_context_from_sequence(
    sequence: str,
    start: int,
    end: int,
    context_size: int = 20,
) -> Tuple[str, str]:
    """Extract context before and after a match from a sequence."""
    ctx_start = max(0, start - 1 - context_size)
    ctx_end = min(len(sequence), end + context_size)
    return (
        sequence[ctx_start:start - 1],
        sequence[end:ctx_end]
    )


def _get_hit_context(
    fasta_file: str,
    seq_name: str,
    start: int,
    end: int,
    context_size: int = 20,
) -> Tuple[str, str]:
    """
    Get context around a hit (for display).
    Note: For batch operations, use _load_sequences_for_context instead.

    Returns: (context_before, context_after)
    """
    sequences = _load_sequences_for_context(fasta_file, {seq_name})
    if seq_name in sequences:
        return _get_context_from_sequence(sequences[seq_name], start, end, context_size)
    return ("", "")


def get_patmatch_config() -> PatmatchConfigResponse:
    """Get pattern match configuration options."""
    datasets = []

    for config in get_available_datasets():
        datasets.append(DatasetInfo(
            name=config.name,
            display_name=config.display_name,
            description=config.description,
            pattern_type=SchemaPatternType(config.pattern_type.value),
        ))

    return PatmatchConfigResponse(
        datasets=datasets,
        max_pattern_length=100,
        max_mismatches=3,
        max_insertions=3,
        max_deletions=3,
    )


def get_datasets_for_type(pattern_type: SchemaPatternType) -> List[DatasetInfo]:
    """Get datasets compatible with a pattern type."""
    config_type = PatternType(pattern_type.value)
    datasets = []

    for config in get_available_datasets(config_type):
        datasets.append(DatasetInfo(
            name=config.name,
            display_name=config.display_name,
            description=config.description,
            pattern_type=pattern_type,
        ))

    return datasets


def run_patmatch_search(
    db,  # Not used when reading from files, kept for API compatibility
    request: PatmatchSearchRequest,
) -> PatmatchSearchResponse:
    """
    Run a pattern match search.

    Uses nrgrep_coords binary if available, otherwise falls back to Python regex.
    """
    # Validate pattern
    pattern = request.pattern.strip().upper()
    if not pattern:
        return PatmatchSearchResponse(
            success=False,
            error="Pattern is required",
        )

    # Get dataset configuration
    # Dataset is now a string - either a config key or legacy enum value
    dataset_key = _map_dataset_to_config_key(request.dataset)
    dataset_config = get_dataset_config(dataset_key)

    if not dataset_config:
        return PatmatchSearchResponse(
            success=False,
            error=f"Unknown dataset: {request.dataset}",
        )

    # Check if FASTA file exists
    if not os.path.exists(dataset_config.fasta_file):
        return PatmatchSearchResponse(
            success=False,
            error=f"Dataset file not available: {dataset_config.display_name}",
        )

    # Validate pattern type matches dataset
    expected_type = SchemaPatternType(dataset_config.pattern_type.value)
    if expected_type != request.pattern_type:
        return PatmatchSearchResponse(
            success=False,
            error=f"Dataset '{dataset_config.display_name}' requires {expected_type.value} patterns",
        )

    # Convert pattern type
    config_pattern_type = PatternType(request.pattern_type.value)

    # Try binary search first, fall back to Python
    use_binary = _check_binary_available() and (
        request.max_mismatches > 0 or
        request.max_insertions > 0 or
        request.max_deletions > 0
    )

    actual_total_hits = 0

    try:
        if use_binary:
            # Convert pattern for nrgrep
            nrgrep_pattern = convert_pattern_for_nrgrep(
                pattern, config_pattern_type,
                request.max_mismatches,
                request.max_insertions,
                request.max_deletions,
            )

            # Run Watson strand search
            hits, watson_total = _run_nrgrep_search(
                nrgrep_pattern,
                dataset_config.fasta_file,
                request.max_mismatches,
                request.max_insertions,
                request.max_deletions,
                request.max_results,
            )
            actual_total_hits = watson_total

            # For Crick strand, run with reverse complement pattern
            if (config_pattern_type == PatternType.DNA and
                    request.strand in [StrandOption.BOTH, StrandOption.CRICK]):
                rc_pattern = get_reverse_complement(nrgrep_pattern)
                crick_hits, crick_total = _run_nrgrep_search(
                    rc_pattern,
                    dataset_config.fasta_file,
                    request.max_mismatches,
                    request.max_insertions,
                    request.max_deletions,
                    request.max_results,
                )
                actual_total_hits += crick_total
                # Mark as Crick strand hits
                crick_hits = [(n, s, e, "C", m) for n, s, e, _, m in crick_hits]
                hits.extend(crick_hits)

            # Filter by strand if needed and adjust actual_total_hits
            if request.strand == StrandOption.WATSON:
                hits = [(n, s, e, st, m) for n, s, e, st, m in hits if st == "W"]
                actual_total_hits = watson_total
            elif request.strand == StrandOption.CRICK:
                hits = [(n, s, e, st, m) for n, s, e, st, m in hits if st == "C"]
                # actual_total_hits was set to watson + crick, but we only want crick
                actual_total_hits = actual_total_hits - watson_total

            sequences_searched = 0  # Not tracked with binary
            total_residues = 0

        else:
            # Use Python regex (no fuzzy matching support)
            hits, sequences_searched, total_residues, actual_total_hits = _run_python_search(
                pattern,
                dataset_config.fasta_file,
                config_pattern_type,
                request.strand,
                request.max_results,
            )

    except Exception as e:
        logger.exception("Pattern search failed")
        return PatmatchSearchResponse(
            success=False,
            error=f"Search failed: {str(e)}",
        )

    # Limit results
    hits = hits[:request.max_results]

    # Batch load sequences for context (much faster than loading per-hit)
    unique_seq_names = {h[0] for h in hits}
    logger.debug(f"Loading {len(unique_seq_names)} sequences for context")
    sequences_cache = _load_sequences_for_context(
        dataset_config.fasta_file, unique_seq_names
    )

    # Convert to PatmatchHit objects
    patmatch_hits = []
    for seq_name, start, end, strand, matched_seq in hits:
        # Get context from cached sequences
        if seq_name in sequences_cache:
            ctx_before, ctx_after = _get_context_from_sequence(
                sequences_cache[seq_name], start, end
            )
        else:
            ctx_before, ctx_after = "", ""

        # Build links
        locus_link = f"/locus/{seq_name}"
        jbrowse_link = None  # Could add JBrowse link based on coordinates

        patmatch_hits.append(PatmatchHit(
            sequence_name=seq_name,
            sequence_description=seq_name,
            match_start=start,
            match_end=end,
            strand="+" if strand == "W" else "-",
            matched_sequence=matched_seq,
            context_before=ctx_before,
            context_after=ctx_after,
            locus_link=locus_link,
            jbrowse_link=jbrowse_link,
        ))

    # Build result
    result = PatmatchSearchResult(
        pattern=pattern,
        pattern_type=request.pattern_type.value,
        dataset=dataset_config.display_name,
        strand=request.strand.value,
        total_hits=actual_total_hits,
        hits=patmatch_hits,
        search_params={
            "max_mismatches": request.max_mismatches,
            "max_insertions": request.max_insertions,
            "max_deletions": request.max_deletions,
            "max_results": request.max_results,
        },
        sequences_searched=sequences_searched,
        total_residues_searched=total_residues,
    )

    return PatmatchSearchResponse(
        success=True,
        result=result,
    )


def _map_dataset_to_config_key(dataset_value: str) -> str:
    """
    Map schema dataset enum value to config dataset key.

    Schema uses short names like 'ca22_chromosomes',
    config uses full names like 'genomic_C_albicans_SC5314_A22'.
    """
    # Mapping from schema dataset values to config keys
    mapping = {
        # C. albicans A22
        "ca22_chromosomes": "genomic_C_albicans_SC5314_A22",
        "ca22_orf_genomic": "orf_genomic_C_albicans_SC5314_A22",
        "ca22_orf_coding": "orf_coding_C_albicans_SC5314_A22",
        "ca22_orf_genomic_1kb": "orf_genomic_1000_C_albicans_SC5314_A22",
        "ca22_intergenic": "not_feature_C_albicans_SC5314_A22",
        "ca22_noncoding": "other_features_genomic_C_albicans_SC5314_A22",
        "ca22_orf_protein": "orf_trans_all_C_albicans_SC5314_A22",
        # C. albicans A21
        "ca21_chromosomes": "genomic_C_albicans_SC5314_A21",
        "ca21_orf_genomic": "orf_genomic_C_albicans_SC5314_A21",
        "ca21_orf_coding": "orf_coding_C_albicans_SC5314_A21",
        "ca21_orf_genomic_1kb": "orf_genomic_1000_C_albicans_SC5314_A21",
        "ca21_intergenic": "not_feature_C_albicans_SC5314_A21",
        "ca21_noncoding": "other_features_genomic_C_albicans_SC5314_A21",
        "ca21_orf_protein": "orf_trans_all_C_albicans_SC5314_A21",
        # C. glabrata
        "cg_chromosomes": "genomic_C_glabrata_CBS138_",
        "cg_orf_genomic": "orf_genomic_C_glabrata_CBS138_",
        "cg_orf_coding": "orf_coding_C_glabrata_CBS138_",
        "cg_orf_protein": "orf_trans_all_C_glabrata_CBS138_",
    }

    return mapping.get(dataset_value, dataset_value)


def format_results_tsv(result) -> str:
    """Format pattern match results as TSV for download."""
    lines = []

    # Header comments
    lines.append(f"# Pattern Match Results")
    lines.append(f"# Pattern: {result.pattern}")
    lines.append(f"# Type: {result.pattern_type}")
    lines.append(f"# Dataset: {result.dataset}")
    lines.append(f"# Strand: {result.strand}")
    lines.append(f"# Total Hits: {result.total_hits}")
    lines.append(f"# Sequences Searched: {result.sequences_searched}")
    lines.append(f"# Total Residues: {result.total_residues_searched}")
    lines.append("")

    # Column headers
    lines.append("Sequence\tDescription\tStart\tEnd\tStrand\tMatched_Sequence\tContext_Before\tContext_After")

    # Data rows
    for hit in result.hits:
        row = [
            hit.sequence_name,
            hit.sequence_description or "",
            str(hit.match_start),
            str(hit.match_end),
            hit.strand,
            hit.matched_sequence,
            hit.context_before,
            hit.context_after,
        ]
        lines.append("\t".join(row))

    return "\n".join(lines)
