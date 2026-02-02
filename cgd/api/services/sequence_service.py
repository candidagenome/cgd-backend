"""
Sequence Service - handles DNA/protein sequence retrieval.
"""
from __future__ import annotations

from typing import Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func

from cgd.models.models import Feature, Seq, FeatLocation, Organism
from cgd.schemas.sequence_schema import (
    SeqType,
    SeqFormat,
    SequenceInfo,
    SequenceResponse,
    CoordinateSequenceResponse,
)


# Complement mapping for reverse complement
COMPLEMENT_MAP = str.maketrans("ACGTacgt", "TGCAtgca")


def _reverse_complement(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    return seq.translate(COMPLEMENT_MAP)[::-1]


def _format_fasta_header(
    feature_name: Optional[str] = None,
    gene_name: Optional[str] = None,
    dbxref_id: Optional[str] = None,
    organism: Optional[str] = None,
    seq_type: str = "genomic",
    chromosome: Optional[str] = None,
    start: Optional[int] = None,
    end: Optional[int] = None,
    strand: Optional[str] = None,
) -> str:
    """Format a FASTA header line."""
    parts = []

    # Primary identifier
    if gene_name:
        parts.append(gene_name)
    elif feature_name:
        parts.append(feature_name)

    # CGDID
    if dbxref_id:
        parts.append(f"CGDID:{dbxref_id}")

    # Organism
    if organism:
        parts.append(organism)

    # Sequence type
    parts.append(seq_type)

    # Coordinates
    if chromosome and start and end:
        strand_char = "+" if strand == "W" else "-" if strand == "C" else ""
        parts.append(f"Chr{chromosome}:{start}-{end}({strand_char})")

    return ">" + " ".join(parts)


def _format_sequence(sequence: str, line_width: int = 60) -> str:
    """Format sequence with line breaks for FASTA output."""
    return "\n".join(
        sequence[i:i + line_width]
        for i in range(0, len(sequence), line_width)
    )


def get_sequence_by_feature(
    db: Session,
    query: str,
    seq_type: SeqType = SeqType.GENOMIC,
    flank_left: int = 0,
    flank_right: int = 0,
    reverse_complement: bool = False,
) -> Optional[SequenceResponse]:
    """
    Retrieve sequence for a feature by name or identifier.

    Args:
        db: Database session
        query: Gene name, feature name, or CGDID
        seq_type: Type of sequence (genomic, protein, coding)
        flank_left: Base pairs to include upstream
        flank_right: Base pairs to include downstream
        reverse_complement: Whether to return reverse complement

    Returns:
        SequenceResponse with sequence and metadata, or None if not found
    """
    query_upper = query.strip().upper()

    # Find feature by gene_name, feature_name, or dbxref_id
    feature = (
        db.query(Feature)
        .outerjoin(Organism, Feature.organism_no == Organism.organism_no)
        .filter(
            func.upper(Feature.gene_name) == query_upper
        )
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

    # Determine which sequence type to retrieve from DB
    # Note: Database stores lowercase values ("genomic", "protein")
    if seq_type == SeqType.PROTEIN:
        db_seq_type = "protein"
    else:
        db_seq_type = "genomic"

    # Get current sequence for this feature
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == feature.feature_no,
            Seq.seq_type == db_seq_type,
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    if not seq_record:
        return None

    sequence = seq_record.residues

    # Get location info for coordinates
    location = (
        db.query(FeatLocation)
        .filter(
            FeatLocation.feature_no == feature.feature_no,
            FeatLocation.is_loc_current == "Y"
        )
        .first()
    )

    chromosome = None
    start_coord = None
    end_coord = None
    strand = None

    if location:
        # Get chromosome name from root sequence
        root_seq = (
            db.query(Seq)
            .join(Feature, Seq.feature_no == Feature.feature_no)
            .filter(Seq.seq_no == location.root_seq_no)
            .first()
        )
        if root_seq and root_seq.feature:
            chromosome = root_seq.feature.feature_name

        start_coord = location.start_coord
        end_coord = location.stop_coord
        strand = location.strand

    # Handle flanking regions for genomic sequence
    if seq_type != SeqType.PROTEIN and (flank_left > 0 or flank_right > 0):
        sequence = _add_flanking_regions(
            db, feature, sequence, flank_left, flank_right, location
        )

    # Handle reverse complement
    if reverse_complement and seq_type != SeqType.PROTEIN:
        sequence = _reverse_complement(sequence)

    organism_name = feature.organism.organism_name if feature.organism else None

    info = SequenceInfo(
        feature_name=feature.feature_name,
        gene_name=feature.gene_name,
        dbxref_id=feature.dbxref_id,
        organism=organism_name,
        chromosome=chromosome,
        start=start_coord,
        end=end_coord,
        strand=strand,
        seq_type=seq_type.value,
        length=len(sequence),
    )

    fasta_header = _format_fasta_header(
        feature_name=feature.feature_name,
        gene_name=feature.gene_name,
        dbxref_id=feature.dbxref_id,
        organism=organism_name,
        seq_type=seq_type.value,
        chromosome=chromosome,
        start=start_coord,
        end=end_coord,
        strand=strand,
    )

    return SequenceResponse(
        sequence=sequence,
        info=info,
        fasta_header=fasta_header,
    )


def _add_flanking_regions(
    db: Session,
    feature: Feature,
    sequence: str,
    flank_left: int,
    flank_right: int,
    location: Optional[FeatLocation],
) -> str:
    """Add flanking regions to a sequence."""
    if not location:
        return sequence

    # Get the chromosome/root sequence
    root_seq = (
        db.query(Seq)
        .filter(
            Seq.seq_no == location.root_seq_no,
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    if not root_seq:
        return sequence

    chr_seq = root_seq.residues
    start = location.start_coord
    end = location.stop_coord
    strand = location.strand

    # Adjust for strand
    if strand == "W":
        # Watson strand: left flank is upstream, right flank is downstream
        left_start = max(0, start - 1 - flank_left)
        left_flank = chr_seq[left_start:start - 1] if flank_left > 0 else ""
        right_flank = chr_seq[end:end + flank_right] if flank_right > 0 else ""
        return left_flank + sequence + right_flank
    else:
        # Crick strand: need to reverse complement flanking regions
        right_start = max(0, start - 1 - flank_right)
        right_flank = chr_seq[right_start:start - 1] if flank_right > 0 else ""
        left_flank = chr_seq[end:end + flank_left] if flank_left > 0 else ""
        # Reverse complement the flanks
        left_flank = _reverse_complement(left_flank) if left_flank else ""
        right_flank = _reverse_complement(right_flank) if right_flank else ""
        return left_flank + sequence + right_flank

    return sequence


def get_sequence_by_coordinates(
    db: Session,
    chromosome: str,
    start: int,
    end: int,
    strand: str = "W",
    reverse_complement: bool = False,
) -> Optional[CoordinateSequenceResponse]:
    """
    Retrieve sequence for a chromosomal region.

    Args:
        db: Database session
        chromosome: Chromosome name (e.g., "Chr1", "1", "Ca21chr1_C_albicans_SC5314")
        start: Start coordinate (1-based)
        end: End coordinate (1-based)
        strand: Strand ('W' for Watson/+, 'C' for Crick/-)
        reverse_complement: Whether to return reverse complement

    Returns:
        CoordinateSequenceResponse or None if chromosome not found
    """
    # Normalize chromosome name
    chr_upper = chromosome.strip().upper()

    # Find chromosome sequence
    chr_seq = (
        db.query(Seq)
        .join(Feature, Seq.feature_no == Feature.feature_no)
        .filter(
            Seq.seq_type == "genomic",
            Seq.is_seq_current == "Y",
            func.upper(Feature.feature_name).like(f"%{chr_upper}%")
        )
        .first()
    )

    if not chr_seq:
        # Try without prefix
        chr_seq = (
            db.query(Seq)
            .join(Feature, Seq.feature_no == Feature.feature_no)
            .filter(
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y",
                Feature.feature_type == "chromosome",
                func.upper(Feature.feature_name).contains(chr_upper)
            )
            .first()
        )

    if not chr_seq:
        return None

    # Extract sequence (convert to 0-based indexing)
    full_sequence = chr_seq.residues
    seq_start = max(0, start - 1)
    seq_end = min(len(full_sequence), end)

    sequence = full_sequence[seq_start:seq_end]

    # Handle strand
    if strand == "C" or reverse_complement:
        sequence = _reverse_complement(sequence)

    chr_name = chr_seq.feature.feature_name if chr_seq.feature else chromosome

    fasta_header = f">{chr_name}:{start}-{end}({'+' if strand == 'W' else '-'})"

    return CoordinateSequenceResponse(
        chromosome=chr_name,
        start=start,
        end=end,
        strand=strand,
        sequence=sequence,
        length=len(sequence),
        fasta_header=fasta_header,
    )


def format_as_fasta(
    header: str,
    sequence: str,
    line_width: int = 60,
) -> str:
    """Format sequence as FASTA."""
    formatted_seq = _format_sequence(sequence, line_width)
    return f"{header}\n{formatted_seq}"
