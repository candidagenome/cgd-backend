"""
Restriction Mapper Service - handles restriction enzyme mapping on DNA sequences.
"""
from __future__ import annotations

import re
from typing import Optional, List, Tuple
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


# IUPAC nucleotide codes to regex
IUPAC_TO_REGEX = {
    "A": "A",
    "C": "C",
    "G": "G",
    "T": "T",
    "U": "T",
    "R": "[AG]",      # Purine
    "Y": "[CT]",      # Pyrimidine
    "S": "[GC]",      # Strong
    "W": "[AT]",      # Weak
    "K": "[GT]",      # Keto
    "M": "[AC]",      # Amino
    "B": "[CGT]",     # Not A
    "D": "[AGT]",     # Not C
    "H": "[ACT]",     # Not G
    "V": "[ACG]",     # Not T
    "N": "[ACGT]",    # Any
}

# Complement mapping
COMPLEMENT = {"A": "T", "T": "A", "C": "G", "G": "C"}


# Restriction enzyme database
# Format: (name, offset, overhang, pattern, enzyme_type)
# offset: Position where the enzyme cuts on the Watson strand relative to pattern start
# overhang: Length of the overhang (positive = 5', negative = 3')
# pattern: Recognition sequence using IUPAC codes
# enzyme_type: Type of cut (5_prime, 3_prime, or blunt)
RESTRICTION_ENZYMES: List[Tuple[str, int, int, str, EnzymeType]] = [
    # 6-base cutters with 5' overhang
    ("EcoRI", 1, 4, "GAATTC", EnzymeType.FIVE_PRIME),
    ("BamHI", 1, 4, "GGATCC", EnzymeType.FIVE_PRIME),
    ("HindIII", 1, 4, "AAGCTT", EnzymeType.FIVE_PRIME),
    ("SalI", 1, 4, "GTCGAC", EnzymeType.FIVE_PRIME),
    ("XbaI", 1, 4, "TCTAGA", EnzymeType.FIVE_PRIME),
    ("XhoI", 1, 4, "CTCGAG", EnzymeType.FIVE_PRIME),
    ("NcoI", 1, 4, "CCATGG", EnzymeType.FIVE_PRIME),
    ("NdeI", 2, 2, "CATATG", EnzymeType.FIVE_PRIME),
    ("BglII", 1, 4, "AGATCT", EnzymeType.FIVE_PRIME),
    ("ClaI", 2, 2, "ATCGAT", EnzymeType.FIVE_PRIME),
    ("SacI", 5, -4, "GAGCTC", EnzymeType.THREE_PRIME),
    ("SphI", 5, -4, "GCATGC", EnzymeType.THREE_PRIME),
    ("KpnI", 5, -4, "GGTACC", EnzymeType.THREE_PRIME),
    ("PstI", 5, -4, "CTGCAG", EnzymeType.THREE_PRIME),
    ("ApaI", 5, -4, "GGGCCC", EnzymeType.THREE_PRIME),

    # 6-base cutters with blunt ends
    ("SmaI", 3, 0, "CCCGGG", EnzymeType.BLUNT),
    ("EcoRV", 3, 0, "GATATC", EnzymeType.BLUNT),
    ("StuI", 3, 0, "AGGCCT", EnzymeType.BLUNT),
    ("HpaI", 3, 0, "GTTAAC", EnzymeType.BLUNT),
    ("NruI", 3, 0, "TCGCGA", EnzymeType.BLUNT),
    ("PvuII", 3, 0, "CAGCTG", EnzymeType.BLUNT),
    ("ScaI", 3, 0, "AGTACT", EnzymeType.BLUNT),
    ("SnaBI", 3, 0, "TACGTA", EnzymeType.BLUNT),

    # 8-base cutters (rare cutters)
    ("NotI", 2, 4, "GCGGCCGC", EnzymeType.FIVE_PRIME),
    ("SfiI", 4, 3, "GGCCNNNNNGGCC", EnzymeType.THREE_PRIME),
    ("PacI", 5, -2, "TTAATTAA", EnzymeType.THREE_PRIME),
    ("AscI", 2, 4, "GGCGCGCC", EnzymeType.FIVE_PRIME),
    ("SbfI", 6, -4, "CCTGCAGG", EnzymeType.THREE_PRIME),
    ("FseI", 6, 2, "GGCCGGCC", EnzymeType.FIVE_PRIME),

    # 4-base cutters (frequent cutters)
    ("MboI", 0, 4, "GATC", EnzymeType.FIVE_PRIME),
    ("Sau3AI", 0, 4, "GATC", EnzymeType.FIVE_PRIME),
    ("HaeIII", 2, 0, "GGCC", EnzymeType.BLUNT),
    ("AluI", 2, 0, "AGCT", EnzymeType.BLUNT),
    ("RsaI", 2, 0, "GTAC", EnzymeType.BLUNT),
    ("TaqI", 1, 2, "TCGA", EnzymeType.FIVE_PRIME),
    ("MspI", 1, 2, "CCGG", EnzymeType.FIVE_PRIME),
    ("HpaII", 1, 2, "CCGG", EnzymeType.FIVE_PRIME),
    ("CfoI", 3, -2, "GCGC", EnzymeType.THREE_PRIME),

    # 5-base/ambiguous cutters
    ("HinfI", 1, 3, "GANTC", EnzymeType.FIVE_PRIME),
    ("DdeI", 1, 3, "CTNAG", EnzymeType.FIVE_PRIME),
    ("AvaI", 1, 4, "CYCGRG", EnzymeType.FIVE_PRIME),
    ("AvaII", 1, 4, "GGWCC", EnzymeType.FIVE_PRIME),
    ("BstNI", 2, 2, "CCWGG", EnzymeType.FIVE_PRIME),
    ("StyI", 1, 4, "CCWWGG", EnzymeType.FIVE_PRIME),
    ("AccI", 2, 2, "GTMKAC", EnzymeType.FIVE_PRIME),

    # Additional common enzymes
    ("NheI", 1, 4, "GCTAGC", EnzymeType.FIVE_PRIME),
    ("SpeI", 1, 4, "ACTAGT", EnzymeType.FIVE_PRIME),
    ("MluI", 1, 4, "ACGCGT", EnzymeType.FIVE_PRIME),
    ("BsiWI", 1, 4, "CGTACG", EnzymeType.FIVE_PRIME),
    ("AgeI", 1, 4, "ACCGGT", EnzymeType.FIVE_PRIME),
    ("BsrGI", 1, 4, "TGTACA", EnzymeType.FIVE_PRIME),
    ("PmeI", 4, 0, "GTTTAAAC", EnzymeType.BLUNT),
    ("SwaI", 4, 0, "ATTTAAAT", EnzymeType.BLUNT),
]


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


def _find_cut_sites(
    sequence: str,
    enzyme_name: str,
    pattern: str,
    offset: int,
    overhang: int,
    enzyme_type: EnzymeType,
) -> EnzymeCutSite:
    """
    Find all cut sites for an enzyme in the sequence.

    Args:
        sequence: DNA sequence (uppercase)
        enzyme_name: Name of the enzyme
        pattern: Recognition pattern (IUPAC)
        offset: Cut position offset from pattern start
        overhang: Overhang length (positive = 5', negative = 3', 0 = blunt)
        enzyme_type: Type of enzyme

    Returns:
        EnzymeCutSite with all cut positions and fragment sizes
    """
    regex_pattern = _iupac_to_regex(pattern)
    watson_cuts = []
    crick_cuts = []

    # Search Watson strand (+ strand)
    for match in re.finditer(regex_pattern, sequence, re.IGNORECASE):
        # Cut position is 1-based, after the offset position
        cut_pos = match.start() + offset + 1
        watson_cuts.append(cut_pos)

    # Search Crick strand (- strand)
    # Reverse complement the sequence and search
    rc_sequence = _reverse_complement(sequence)
    seq_len = len(sequence)

    for match in re.finditer(regex_pattern, rc_sequence, re.IGNORECASE):
        # Convert position back to Watson coordinates
        # The match is on the reverse complement, so we need to convert
        rc_cut_pos = match.start() + offset + 1
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

    return EnzymeCutSite(
        enzyme_name=enzyme_name,
        recognition_seq=pattern,
        enzyme_type=enzyme_type,
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


def get_restriction_mapper_config() -> RestrictionMapperConfigResponse:
    """Get restriction mapper configuration."""
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
        total_enzymes=len(RESTRICTION_ENZYMES)
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

    # Find cut sites for all enzymes
    all_cut_sites = []
    for name, offset, overhang, pattern, enz_type in RESTRICTION_ENZYMES:
        site = _find_cut_sites(
            dna_sequence, name, pattern, offset, overhang, enz_type
        )
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
        total_enzymes_searched=len(RESTRICTION_ENZYMES),
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

    # Create lookup for enzyme info
    enzyme_lookup = {e[0]: e for e in RESTRICTION_ENZYMES}

    lines.append("Enzyme\tRecognition\tType")

    for enzyme_name in result.non_cutting_enzymes:
        if enzyme_name in enzyme_lookup:
            _, _, _, pattern, enz_type = enzyme_lookup[enzyme_name]
            enzyme_type = enz_type.value.replace("_", " ")
            lines.append(f"{enzyme_name}\t{pattern}\t{enzyme_type}")
        else:
            lines.append(f"{enzyme_name}\t-\t-")

    return "\n".join(lines)
