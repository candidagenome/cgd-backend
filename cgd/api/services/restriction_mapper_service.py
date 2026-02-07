"""
Restriction Mapper Service - handles restriction enzyme mapping on DNA sequences.

This service uses the scan_for_matches binary for restriction site detection
when available, with a Python regex fallback for portability.
"""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
from typing import Optional, List, Tuple, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.models.models import Feature, Seq, FeatLocation, Organism
from cgd.schemas.restriction_mapper_schema import (
    EnzymeFilterType,
    EnzymeType,
    EnzymeCutSite,
    RestrictionMapResult,
    RestrictionMapperResponse,
    EnzymeFilterInfo,
    RestrictionMapperConfigResponse,
)
from cgd.core.restriction_config import (
    SCAN_FOR_MATCHES_BINARY,
    EnzymeFilterType as ConfigEnzymeFilterType,
    EnzymeInfo,
    load_enzymes,
    get_enzyme_file,
    IUPAC_TO_REGEX,
    get_reverse_complement,
)


# Complement mapping for Python fallback
COMPLEMENT = {"A": "T", "T": "A", "C": "G", "G": "C"}


def _check_binary_available() -> bool:
    """Check if the scan_for_matches binary is available."""
    return os.path.isfile(SCAN_FOR_MATCHES_BINARY) and os.access(SCAN_FOR_MATCHES_BINARY, os.X_OK)


def _iupac_to_regex(pattern: str) -> str:
    """Convert IUPAC DNA pattern to regex."""
    regex_parts = []
    for char in pattern.upper():
        if char in IUPAC_TO_REGEX:
            regex_parts.append(IUPAC_TO_REGEX[char])
        else:
            # Unknown character, escape it
            regex_parts.append(re.escape(char))
    return "".join(regex_parts)


def _reverse_complement(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    complement_map = str.maketrans("ACGTacgtRYSWKMBDHVNrysswkmbdhvn",
                                    "TGCAtgcaYRSWMKVHDBNyrssmkvhdbnn")
    return seq.translate(complement_map)[::-1]


def _get_sequence_for_locus(
    db: Session,
    locus: str,
) -> Optional[Tuple[str, str, str, Optional[str]]]:
    """
    Fetch DNA sequence for a locus from the database.

    Returns:
        Tuple of (sequence, feature_name, display_name, coordinates) or None if not found
    """
    query_upper = locus.strip().upper()

    # Find feature by gene_name, feature_name, or dbxref_id
    feature = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(func.upper(Feature.gene_name) == query_upper)
        .first()
    )

    if not feature:
        feature = (
            db.query(Feature)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(func.upper(Feature.feature_name) == query_upper)
            .first()
        )

    if not feature:
        feature = (
            db.query(Feature)
            .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
            .filter(func.upper(Feature.dbxref_id) == query_upper)
            .first()
        )

    if not feature:
        return None

    # Get current genomic sequence
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == feature.feature_no,
            Seq.seq_type == "genomic",
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    if not seq_record or not seq_record.residues:
        return None

    # Get location info for coordinates
    location = (
        db.query(FeatLocation)
        .filter(
            FeatLocation.feature_no == feature.feature_no,
            FeatLocation.is_loc_current == "Y"
        )
        .first()
    )

    coordinates = None
    if location:
        # Get chromosome name
        root_seq = (
            db.query(Seq)
            .join(Feature, Seq.feature_no == Feature.feature_no)
            .filter(Seq.seq_no == location.root_seq_no)
            .first()
        )
        if root_seq and root_seq.feature:
            chr_name = root_seq.feature.feature_name
            strand = "+" if location.strand == "W" else "-"
            coordinates = f"{chr_name}:{location.start_coord}-{location.stop_coord}({strand})"

    # Display name
    display_name = feature.gene_name if feature.gene_name else feature.feature_name

    return (
        seq_record.residues.upper(),
        feature.feature_name,
        display_name,
        coordinates
    )


def _run_scan_for_matches(
    sequence: str,
    seq_name: str,
    enzymes: List[EnzymeInfo],
) -> Dict[str, List[Tuple[int, int, str]]]:
    """
    Run scan_for_matches binary to find restriction sites.

    Args:
        sequence: DNA sequence
        seq_name: Name for the sequence
        enzymes: List of enzyme info

    Returns:
        Dictionary mapping enzyme names to list of (start, end, matched_seq) tuples
    """
    results: Dict[str, List[Tuple[int, int, str]]] = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        # Write sequence to temp file
        seq_file = os.path.join(tmpdir, "sequence.fasta")
        with open(seq_file, "w") as f:
            f.write(f">{seq_name}\n{sequence}\n")

        # Process each enzyme
        for enzyme in enzymes:
            results[enzyme.name] = []

            # Write pattern to temp file
            pat_file = os.path.join(tmpdir, "pattern.pat")
            with open(pat_file, "w") as f:
                f.write(enzyme.pattern)

            try:
                # Run scan_for_matches with -c flag for complement search
                cmd = [SCAN_FOR_MATCHES_BINARY, "-c", pat_file]

                with open(seq_file, "r") as seq_input:
                    result = subprocess.run(
                        cmd,
                        stdin=seq_input,
                        capture_output=True,
                        text=True,
                        timeout=60
                    )

                if result.returncode == 0:
                    # Parse output
                    # Format: >seq_name:[start,end]
                    #         matched_sequence
                    for line in result.stdout.split("\n"):
                        line = line.strip()
                        if not line:
                            continue

                        # Match pattern like >seq_name[123,128] or >seq_name:[123,128]
                        match = re.match(r">.+[\[:](\d+),(\d+)\]?", line)
                        if match:
                            start = int(match.group(1))
                            end = int(match.group(2))
                            results[enzyme.name].append((start, end, ""))

            except subprocess.TimeoutExpired:
                # Timeout - continue with other enzymes
                pass
            except Exception:
                # Error running binary - continue with other enzymes
                pass

    return results


def _find_cut_sites_python(
    sequence: str,
    enzyme: EnzymeInfo,
) -> EnzymeCutSite:
    """
    Find all cut sites for an enzyme using Python regex (fallback).

    Args:
        sequence: DNA sequence (uppercase)
        enzyme: EnzymeInfo object

    Returns:
        EnzymeCutSite with all cut positions and fragment sizes
    """
    regex_pattern = _iupac_to_regex(enzyme.pattern)
    watson_cuts = []
    crick_cuts = []

    # Search Watson strand (+ strand)
    for match in re.finditer(regex_pattern, sequence, re.IGNORECASE):
        # Cut position is 1-based, after the offset position
        cut_pos = match.start() + enzyme.offset + 1
        watson_cuts.append(cut_pos)

    # Search Crick strand (- strand)
    # Reverse complement the sequence and search
    rc_sequence = _reverse_complement(sequence)
    seq_len = len(sequence)

    for match in re.finditer(regex_pattern, rc_sequence, re.IGNORECASE):
        # Convert position back to Watson coordinates
        rc_cut_pos = match.start() + enzyme.offset + 1
        # Convert to Watson strand position
        watson_pos = seq_len - rc_cut_pos + 1
        crick_cuts.append(watson_pos)

    # Sort cut positions
    watson_cuts.sort()
    crick_cuts.sort()

    # Calculate fragment sizes based on unique cut positions
    all_cuts = sorted(set(watson_cuts + crick_cuts))
    fragment_sizes = []
    if all_cuts:
        # First fragment (from start to first cut)
        fragment_sizes.append(all_cuts[0])
        # Middle fragments
        for i in range(1, len(all_cuts)):
            fragment_sizes.append(all_cuts[i] - all_cuts[i - 1])
        # Last fragment (from last cut to end)
        fragment_sizes.append(seq_len - all_cuts[-1])

    total_cuts = len(all_cuts)

    # Map enzyme type from config to schema
    if enzyme.enzyme_type.value == "blunt":
        enz_type = EnzymeType.BLUNT
    elif enzyme.enzyme_type.value == "5_prime":
        enz_type = EnzymeType.FIVE_PRIME
    else:
        enz_type = EnzymeType.THREE_PRIME

    return EnzymeCutSite(
        enzyme_name=enzyme.name,
        recognition_seq=enzyme.pattern,
        enzyme_type=enz_type,
        cut_positions_watson=watson_cuts,
        cut_positions_crick=crick_cuts,
        total_cuts=total_cuts,
        fragment_sizes=sorted(fragment_sizes) if fragment_sizes else [],
    )


def _find_cut_sites_binary(
    sequence: str,
    enzyme: EnzymeInfo,
    binary_results: Dict[str, List[Tuple[int, int, str]]],
) -> EnzymeCutSite:
    """
    Create EnzymeCutSite from scan_for_matches binary results.

    Args:
        sequence: DNA sequence
        enzyme: EnzymeInfo object
        binary_results: Results from scan_for_matches

    Returns:
        EnzymeCutSite with cut positions and fragment sizes
    """
    seq_len = len(sequence)
    matches = binary_results.get(enzyme.name, [])

    # scan_for_matches returns positions on both strands
    # We need to separate them
    watson_cuts = []
    crick_cuts = []

    # Process matches - the binary with -c flag searches both strands
    # We need to determine which strand each match is on
    regex_pattern = _iupac_to_regex(enzyme.pattern)

    for start, end, _ in matches:
        # Check if this is a Watson strand match
        subseq = sequence[start - 1:end] if start > 0 and end <= seq_len else ""
        if re.match(regex_pattern, subseq, re.IGNORECASE):
            # Watson strand match
            cut_pos = start + enzyme.offset
            watson_cuts.append(cut_pos)
        else:
            # Crick strand match
            cut_pos = end - enzyme.offset + 1
            crick_cuts.append(cut_pos)

    # Sort cut positions
    watson_cuts.sort()
    crick_cuts.sort()

    # Calculate fragment sizes based on unique cut positions
    all_cuts = sorted(set(watson_cuts + crick_cuts))
    fragment_sizes = []
    if all_cuts:
        # First fragment (from start to first cut)
        fragment_sizes.append(all_cuts[0])
        # Middle fragments
        for i in range(1, len(all_cuts)):
            fragment_sizes.append(all_cuts[i] - all_cuts[i - 1])
        # Last fragment (from last cut to end)
        fragment_sizes.append(seq_len - all_cuts[-1])

    total_cuts = len(all_cuts)

    # Map enzyme type from config to schema
    if enzyme.enzyme_type.value == "blunt":
        enz_type = EnzymeType.BLUNT
    elif enzyme.enzyme_type.value == "5_prime":
        enz_type = EnzymeType.FIVE_PRIME
    else:
        enz_type = EnzymeType.THREE_PRIME

    return EnzymeCutSite(
        enzyme_name=enzyme.name,
        recognition_seq=enzyme.pattern,
        enzyme_type=enz_type,
        cut_positions_watson=watson_cuts,
        cut_positions_crick=crick_cuts,
        total_cuts=total_cuts,
        fragment_sizes=sorted(fragment_sizes) if fragment_sizes else [],
    )


def _filter_enzymes(
    enzyme_filter: EnzymeFilterType,
    cut_sites: List[EnzymeCutSite],
) -> Tuple[List[EnzymeCutSite], List[str]]:
    """
    Filter cut sites based on the enzyme filter type.

    Returns:
        Tuple of (filtered cutting enzymes, non-cutting enzyme names)
    """
    cutting = []
    non_cutting = []

    for site in cut_sites:
        if site.total_cuts == 0:
            non_cutting.append(site.enzyme_name)
            continue

        # Apply filter
        include = False
        if enzyme_filter == EnzymeFilterType.ALL:
            include = True
        elif enzyme_filter == EnzymeFilterType.THREE_PRIME_OVERHANG:
            include = site.enzyme_type == EnzymeType.THREE_PRIME
        elif enzyme_filter == EnzymeFilterType.FIVE_PRIME_OVERHANG:
            include = site.enzyme_type == EnzymeType.FIVE_PRIME
        elif enzyme_filter == EnzymeFilterType.BLUNT:
            include = site.enzyme_type == EnzymeType.BLUNT
        elif enzyme_filter == EnzymeFilterType.CUT_ONCE:
            include = site.total_cuts == 1
        elif enzyme_filter == EnzymeFilterType.CUT_TWICE:
            include = site.total_cuts == 2
        elif enzyme_filter == EnzymeFilterType.SIX_BASE:
            include = len(site.recognition_seq.replace("N", "")) == 6
        elif enzyme_filter == EnzymeFilterType.NO_CUT:
            # This filter shows only non-cutting enzymes
            include = False

        if include:
            cutting.append(site)

    # Sort cutting enzymes by name
    cutting.sort(key=lambda x: x.enzyme_name)

    return cutting, sorted(non_cutting)


def _map_filter_type_to_config(filter_type: EnzymeFilterType) -> ConfigEnzymeFilterType:
    """Map schema EnzymeFilterType to config EnzymeFilterType."""
    mapping = {
        EnzymeFilterType.ALL: ConfigEnzymeFilterType.ALL,
        EnzymeFilterType.THREE_PRIME_OVERHANG: ConfigEnzymeFilterType.THREE_PRIME_OVERHANG,
        EnzymeFilterType.FIVE_PRIME_OVERHANG: ConfigEnzymeFilterType.FIVE_PRIME_OVERHANG,
        EnzymeFilterType.BLUNT: ConfigEnzymeFilterType.BLUNT,
        EnzymeFilterType.CUT_ONCE: ConfigEnzymeFilterType.CUT_ONCE,
        EnzymeFilterType.CUT_TWICE: ConfigEnzymeFilterType.CUT_TWICE,
        EnzymeFilterType.SIX_BASE: ConfigEnzymeFilterType.SIX_BASE,
    }
    return mapping.get(filter_type, ConfigEnzymeFilterType.ALL)


def get_restriction_mapper_config() -> RestrictionMapperConfigResponse:
    """Get restriction mapper configuration."""
    # Load enzymes to get actual count
    enzymes = load_enzymes(ConfigEnzymeFilterType.ALL)

    enzyme_filters = [
        EnzymeFilterInfo(
            value=EnzymeFilterType.ALL,
            display_name="All Enzymes",
            description="Show all cutting enzymes"
        ),
        EnzymeFilterInfo(
            value=EnzymeFilterType.FIVE_PRIME_OVERHANG,
            display_name="5' Overhang",
            description="Enzymes producing 5' overhangs (sticky ends)"
        ),
        EnzymeFilterInfo(
            value=EnzymeFilterType.THREE_PRIME_OVERHANG,
            display_name="3' Overhang",
            description="Enzymes producing 3' overhangs (sticky ends)"
        ),
        EnzymeFilterInfo(
            value=EnzymeFilterType.BLUNT,
            display_name="Blunt End",
            description="Enzymes producing blunt ends"
        ),
        EnzymeFilterInfo(
            value=EnzymeFilterType.CUT_ONCE,
            display_name="Cut Once",
            description="Enzymes that cut exactly once"
        ),
        EnzymeFilterInfo(
            value=EnzymeFilterType.CUT_TWICE,
            display_name="Cut Twice",
            description="Enzymes that cut exactly twice"
        ),
        EnzymeFilterInfo(
            value=EnzymeFilterType.SIX_BASE,
            display_name="Six-Base Cutters",
            description="Enzymes with 6-base recognition sequences"
        ),
        EnzymeFilterInfo(
            value=EnzymeFilterType.NO_CUT,
            display_name="Non-Cutting",
            description="Show enzymes that do not cut"
        ),
    ]

    return RestrictionMapperConfigResponse(
        enzyme_filters=enzyme_filters,
        total_enzymes=len(enzymes)
    )


def run_restriction_mapping(
    db: Session,
    locus: Optional[str] = None,
    sequence: Optional[str] = None,
    sequence_name: Optional[str] = None,
    enzyme_filter: EnzymeFilterType = EnzymeFilterType.ALL,
) -> RestrictionMapperResponse:
    """
    Run restriction enzyme mapping on a DNA sequence.

    Uses scan_for_matches binary when available, falls back to Python regex.

    Args:
        db: Database session
        locus: Gene name, ORF, or CGDID to map
        sequence: Raw DNA sequence (alternative to locus)
        sequence_name: Name for raw sequence
        enzyme_filter: Filter for enzyme selection

    Returns:
        RestrictionMapperResponse with mapping results
    """
    # Get sequence
    if locus:
        result = _get_sequence_for_locus(db, locus)
        if not result:
            return RestrictionMapperResponse(
                success=False,
                error=f"Locus '{locus}' not found or has no genomic sequence"
            )
        dna_sequence, feature_name, display_name, coordinates = result
        seq_name = display_name
    elif sequence:
        # Clean the sequence - remove non-DNA characters
        dna_sequence = "".join(c for c in sequence.upper() if c in "ACGT")
        if not dna_sequence:
            return RestrictionMapperResponse(
                success=False,
                error="Invalid sequence: no valid DNA bases found"
            )
        seq_name = sequence_name or "User Sequence"
        coordinates = None
    else:
        return RestrictionMapperResponse(
            success=False,
            error="Either locus or sequence must be provided"
        )

    # Load enzymes based on filter type
    config_filter = _map_filter_type_to_config(enzyme_filter)
    enzymes = load_enzymes(config_filter)

    # Check if binary is available
    use_binary = _check_binary_available()

    # Find cut sites for all enzymes
    all_cut_sites = []

    if use_binary:
        # Use scan_for_matches binary
        binary_results = _run_scan_for_matches(dna_sequence, seq_name, enzymes)

        for enzyme in enzymes:
            site = _find_cut_sites_binary(dna_sequence, enzyme, binary_results)
            all_cut_sites.append(site)
    else:
        # Fall back to Python regex
        for enzyme in enzymes:
            site = _find_cut_sites_python(dna_sequence, enzyme)
            all_cut_sites.append(site)

    # Apply filter
    if enzyme_filter == EnzymeFilterType.NO_CUT:
        # Special case: show only non-cutting enzymes
        cutting_enzymes = []
        non_cutting = [s.enzyme_name for s in all_cut_sites if s.total_cuts == 0]
    else:
        cutting_enzymes, non_cutting = _filter_enzymes(enzyme_filter, all_cut_sites)

    result = RestrictionMapResult(
        seq_name=seq_name,
        seq_length=len(dna_sequence),
        coordinates=coordinates,
        cutting_enzymes=cutting_enzymes,
        non_cutting_enzymes=non_cutting,
        total_enzymes_searched=len(enzymes),
    )

    return RestrictionMapperResponse(
        success=True,
        result=result
    )


def format_results_tsv(result: RestrictionMapResult) -> str:
    """Format restriction mapping results as TSV."""
    lines = []

    # Header
    lines.append(f"# Restriction Map for: {result.seq_name}")
    lines.append(f"# Sequence Length: {result.seq_length} bp")
    if result.coordinates:
        lines.append(f"# Coordinates: {result.coordinates}")
    lines.append("")

    # Cutting enzymes
    lines.append("Enzyme\tRecognition\tType\tCuts\tPositions\tFragment Sizes")

    for enzyme in result.cutting_enzymes:
        positions = ",".join(str(p) for p in sorted(
            set(enzyme.cut_positions_watson + enzyme.cut_positions_crick)
        ))
        fragments = ",".join(str(f) for f in enzyme.fragment_sizes)
        enzyme_type = enzyme.enzyme_type.value.replace("_", " ")

        lines.append(
            f"{enzyme.enzyme_name}\t{enzyme.recognition_seq}\t"
            f"{enzyme_type}\t{enzyme.total_cuts}\t{positions}\t{fragments}"
        )

    return "\n".join(lines)


def format_non_cutting_tsv(result: RestrictionMapResult) -> str:
    """Format non-cutting enzymes as TSV."""
    lines = []

    # Header
    lines.append(f"# Non-Cutting Enzymes for: {result.seq_name}")
    lines.append(f"# Sequence Length: {result.seq_length} bp")
    lines.append("")

    # Load enzymes to get pattern info
    enzymes = load_enzymes(ConfigEnzymeFilterType.ALL)
    enzyme_lookup = {e.name: e for e in enzymes}

    lines.append("Enzyme\tRecognition\tType")

    for enzyme_name in result.non_cutting_enzymes:
        if enzyme_name in enzyme_lookup:
            enzyme = enzyme_lookup[enzyme_name]
            enzyme_type = enzyme.enzyme_type.value.replace("_", " ")
            lines.append(f"{enzyme_name}\t{enzyme.pattern}\t{enzyme_type}")
        else:
            lines.append(f"{enzyme_name}\t-\t-")

    return "\n".join(lines)
