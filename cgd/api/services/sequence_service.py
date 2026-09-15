"""
Sequence Service - handles DNA/protein sequence retrieval.
"""
from __future__ import annotations

from typing import Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, text

from cgd.models.models import Feature, Seq, FeatLocation, Organism, FeatRelationship
from cgd.schemas.sequence_schema import (
    SeqType,
    SeqFormat,
    SequenceInfo,
    SequenceResponse,
    CoordinateSequenceResponse,
)


# Complement mapping for reverse complement
COMPLEMENT_MAP = str.maketrans("ACGTacgt", "TGCAtgca")


def _fetch_residue_window(db: Session, seq_no: int, start: int, stop: int) -> Optional[str]:
    """Residues[start..stop] (1-based, inclusive) of a SEQ row via SQL SUBSTR.

    Never load a chromosome's full residues CLOB to slice it in Python: each
    load ships megabases and takes seconds, and under crawler load the
    accumulated holds exhausted the DB pool (prod incident 2026-09-15).
    SUBSTR on the CLOB ships only the requested window.
    """
    if stop < start:
        return None
    start = max(1, start)
    row = db.execute(
        text("SELECT SUBSTR(residues, :s, :l) FROM seq"
             " WHERE seq_no = :n AND is_seq_current = 'Y'"),
        {"s": start, "l": stop - start + 1, "n": seq_no},
    ).first()
    if not row or row[0] is None:
        return None
    val = row[0]
    return val.read() if hasattr(val, "read") else str(val)


def _reverse_complement(seq: str) -> str:
    """Return the reverse complement of a DNA sequence."""
    return seq.translate(COMPLEMENT_MAP)[::-1]


def _get_subfeatures(
    db: Session,
    feature: Feature,
    root_seq_no: Optional[int] = None,
) -> list:
    """
    Get subfeatures (CDS, UTR, exons) for a feature, ordered by start coordinate.

    Returns list of tuples: (feature_type, start_coord, stop_coord)

    A legacy ORF (e.g. orf19.5007) is mapped onto several assemblies, and its
    subfeatures carry a current FeatLocation on EACH assembly's root sequence.
    Pass ``root_seq_no`` (the parent location's root sequence) to keep only the
    subfeatures on that assembly; otherwise coordinates from different
    assemblies are pooled and min/max spans become meaningless (e.g. an ORF
    appearing to span millions of bp across two chromosomes).
    """
    query = (
        db.query(
            Feature.feature_type,
            FeatLocation.start_coord,
            FeatLocation.stop_coord,
        )
        .join(FeatRelationship, FeatRelationship.child_feature_no == Feature.feature_no)
        .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
        .filter(
            FeatRelationship.parent_feature_no == feature.feature_no,
            FeatRelationship.rank == 2,  # rank 2 = subfeature
            FeatLocation.is_loc_current == "Y",
        )
    )
    if root_seq_no is not None:
        query = query.filter(FeatLocation.root_seq_no == root_seq_no)

    return query.order_by(FeatLocation.start_coord).all()


def _get_genomic_utr_sequence(
    db: Session,
    feature: Feature,
    location: FeatLocation,
) -> Optional[str]:
    """
    Get genomic sequence including UTR regions.

    This returns the full genomic span from the first subfeature to the last,
    including any introns between them.
    """
    subfeatures = _get_subfeatures(db, feature, location.root_seq_no)

    if not subfeatures:
        return None

    # Find the extent from first to last subfeature
    all_coords = []
    for feat_type, start, stop in subfeatures:
        all_coords.extend([start, stop])

    if not all_coords:
        return None

    feat_start = min(all_coords)
    feat_stop = max(all_coords)

    # Fetch only the spanned window of the chromosome (never the full CLOB)
    sequence = _fetch_residue_window(db, location.root_seq_no, feat_start, feat_stop)
    if not sequence:
        return None

    # Reverse complement if on Crick strand
    if location.strand == "C":
        sequence = _reverse_complement(sequence)

    return sequence


def _get_coding_utr_sequence(
    db: Session,
    feature: Feature,
    location: FeatLocation,
) -> Optional[str]:
    """
    Get transcript/mRNA sequence - CDS with UTRs, introns spliced out.

    This concatenates all CDS, UTR, and Noncoding_exon subfeatures,
    excluding introns. This is the spliced transcript sequence.
    """
    subfeatures = _get_subfeatures(db, feature, location.root_seq_no)

    if not subfeatures:
        return None

    strand = location.strand

    # Filter to only CDS, UTR, and Noncoding_exon subfeatures (not introns)
    exon_types = ["cds", "utr", "noncoding_exon", "five_prime_utr", "three_prime_utr"]
    exon_subfeatures = []
    for feat_type, start, stop in subfeatures:
        feat_type_lower = (feat_type or "").lower()
        # Include if it matches any exon type and is NOT an intron
        if any(et in feat_type_lower for et in exon_types) and "intron" not in feat_type_lower:
            exon_subfeatures.append((feat_type, start, stop))

    if not exon_subfeatures:
        # Fall back to all non-intron subfeatures
        for feat_type, start, stop in subfeatures:
            feat_type_lower = (feat_type or "").lower()
            if "intron" not in feat_type_lower:
                exon_subfeatures.append((feat_type, start, stop))

    if not exon_subfeatures:
        return None

    # Fetch one bounding window covering all exon subfeatures (never the
    # full chromosome CLOB) and slice segments relative to it
    window_lo = min(min(s, e) for _, s, e in exon_subfeatures)
    window_hi = max(max(s, e) for _, s, e in exon_subfeatures)
    window = _fetch_residue_window(db, location.root_seq_no, window_lo, window_hi)
    if not window:
        return None

    # Extract each exon segment and concatenate
    segments = []
    for feat_type, start, stop in exon_subfeatures:
        # Ensure start < stop
        if start > stop:
            start, stop = stop, start

        # Extract segment (window is 1-based from window_lo)
        segment = window[start - window_lo:stop - window_lo + 1]

        # If Crick strand, reverse complement each segment
        if strand == "C":
            segment = _reverse_complement(segment)

        segments.append(segment)

    # For Crick strand, segments are ordered by genomic position but need to be
    # reversed for the transcript order
    if strand == "C":
        segments.reverse()

    return "".join(segments)


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
    headline: Optional[str] = None,
    use_systematic_name: bool = False,
) -> str:
    """Format a FASTA header line."""
    parts = []

    # Primary identifier - prefer systematic name if requested
    if use_systematic_name:
        if feature_name:
            parts.append(feature_name)
        elif gene_name:
            parts.append(gene_name)
    else:
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

    # Headline/description (functional annotation)
    if headline:
        parts.append(f"| {headline}")

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
    use_systematic_name: bool = False,
) -> Optional[SequenceResponse]:
    """
    Retrieve sequence for a feature by name or identifier.

    Args:
        db: Database session
        query: Gene name, feature name, or CGDID
        seq_type: Type of sequence:
            - genomic: Full genomic DNA sequence
            - protein: Translated protein sequence
            - coding: CDS/exons only (introns removed)
            - genomic_utr: Genomic sequence including UTR regions
            - coding_utr: Transcript/mRNA - CDS with UTRs, introns spliced out
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

    # Get location info for coordinates (needed for UTR sequence computation)
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
        # Get chromosome name from root sequence (name column only — loading
        # the Seq entity would drag the whole chromosome residues CLOB)
        root_name = (
            db.query(Feature.feature_name)
            .join(Seq, Seq.feature_no == Feature.feature_no)
            .filter(Seq.seq_no == location.root_seq_no)
            .first()
        )
        if root_name:
            chromosome = root_name[0]

        start_coord = location.start_coord
        end_coord = location.stop_coord
        strand = location.strand

    # Handle GENOMIC_UTR and CODING_UTR types (computed from subfeatures)
    if seq_type == SeqType.GENOMIC_UTR:
        if not location:
            return None
        sequence = _get_genomic_utr_sequence(db, feature, location)
        if not sequence:
            # Fall back to genomic sequence if UTR computation fails
            seq_record = (
                db.query(Seq)
                .filter(
                    Seq.feature_no == feature.feature_no,
                    Seq.seq_type == "genomic",
                    Seq.is_seq_current == "Y"
                )
                .first()
            )
            sequence = seq_record.residues if seq_record else None
    elif seq_type == SeqType.CODING_UTR:
        if not location:
            return None
        sequence = _get_coding_utr_sequence(db, feature, location)
        if not sequence:
            # Fall back to coding sequence if UTR computation fails
            seq_record = (
                db.query(Seq)
                .filter(
                    Seq.feature_no == feature.feature_no,
                    Seq.seq_type == "coding",
                    Seq.is_seq_current == "Y"
                )
                .first()
            )
            sequence = seq_record.residues if seq_record else None
    else:
        # Standard sequence types: retrieve from database
        # Note: Most organisms use lowercase ("genomic", "protein", "coding")
        # but some (e.g., C. tropicalis) may use mixed-case ("Genomic DNA", "Protein")
        if seq_type == SeqType.PROTEIN:
            db_seq_types = ["protein", "Protein"]
        elif seq_type == SeqType.CODING:
            db_seq_types = ["coding", "CDS"]
        else:
            db_seq_types = ["genomic", "Genomic DNA"]

        # Get current sequence for this feature (try multiple formats)
        seq_record = (
            db.query(Seq)
            .filter(
                Seq.feature_no == feature.feature_no,
                Seq.seq_type.in_(db_seq_types),
                Seq.is_seq_current == "Y"
            )
            .first()
        )

        if not seq_record:
            return None

        sequence = seq_record.residues

    if not sequence:
        return None

    # Handle flanking regions for genomic sequence types
    if seq_type not in (SeqType.PROTEIN, SeqType.CODING_UTR) and (flank_left > 0 or flank_right > 0):
        sequence = _add_flanking_regions(
            db, feature, sequence, flank_left, flank_right, location
        )

    # Handle reverse complement
    if reverse_complement and seq_type != SeqType.PROTEIN:
        sequence = _reverse_complement(sequence)

    # Convert to uppercase for display
    sequence = sequence.upper()

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
        headline=feature.headline,
        use_systematic_name=use_systematic_name,
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
    """Add flanking regions to a sequence.

    Flanking is anchored on the FeatLocation coordinates, which delimit the
    exact genomic span of the stored genomic ("with introns") sequence this
    function flanks. ``start_coord``/``stop_coord`` are NOT ordered low-to-high
    (on the Crick strand ``start_coord`` is the larger value), so we take the
    min/max to get the genomic low/high boundary.

    Note: do NOT anchor on the min/max of the subfeatures. Subfeatures include
    UTRs that extend beyond this span, which leaves UTR-sized gaps between the
    sequence and its flanks; subfeature coordinates can also be unreliable,
    spanning far beyond the gene and pushing a flank off the chromosome.
    """
    if not location:
        return sequence

    strand = location.strand

    # Genomic low/high boundary of the stored genomic sequence. start_coord and
    # stop_coord are swapped on the Crick strand, so normalize with min/max.
    feat_start = min(location.start_coord, location.stop_coord)
    feat_stop = max(location.start_coord, location.stop_coord)

    # Fetch only the flank windows (never the full chromosome CLOB); SUBSTR
    # clamps at the chromosome end, _fetch_residue_window clamps at 1.
    def upstream(n):
        return (_fetch_residue_window(
            db, location.root_seq_no, feat_start - n, feat_start - 1) or "") if n > 0 else ""

    def downstream(n):
        return (_fetch_residue_window(
            db, location.root_seq_no, feat_stop + 1, feat_stop + n) or "") if n > 0 else ""

    if strand == "W":
        # Watson strand: left flank is upstream, right flank is downstream
        return upstream(flank_left) + sequence + downstream(flank_right)
    else:
        # Crick strand: need to reverse complement flanking regions
        # For Crick strand genes, "left" flank (5') is downstream on chromosome
        right_flank = upstream(flank_right)
        left_flank = downstream(flank_left)
        # Reverse complement the flanks
        left_flank = _reverse_complement(left_flank) if left_flank else ""
        right_flank = _reverse_complement(right_flank) if right_flank else ""
        return left_flank + sequence + right_flank


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

    # Find the chromosome (columns only — never load the residues CLOB)
    chr_row = (
        db.query(Seq.seq_no, Seq.seq_length, Feature.feature_name)
        .join(Feature, Seq.feature_no == Feature.feature_no)
        .filter(
            Seq.seq_type == "genomic",
            Seq.is_seq_current == "Y",
            func.upper(Feature.feature_name).like(f"%{chr_upper}%")
        )
        .first()
    )

    if not chr_row:
        # Try without prefix
        chr_row = (
            db.query(Seq.seq_no, Seq.seq_length, Feature.feature_name)
            .join(Feature, Seq.feature_no == Feature.feature_no)
            .filter(
                Seq.seq_type == "genomic",
                Seq.is_seq_current == "Y",
                Feature.feature_type == "chromosome",
                func.upper(Feature.feature_name).contains(chr_upper)
            )
            .first()
        )

    if not chr_row:
        return None

    seq_no, seq_length, chr_feature_name = chr_row

    # Fetch only the requested window
    seq_end = min(seq_length, end) if seq_length else end
    sequence = _fetch_residue_window(db, seq_no, start, seq_end) or ""

    # Handle strand
    if strand == "C" or reverse_complement:
        sequence = _reverse_complement(sequence)

    # Convert to uppercase for display
    sequence = sequence.upper()

    chr_name = chr_feature_name or chromosome

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
