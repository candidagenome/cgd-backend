"""
Homology API Router.

Provides endpoints for homology data including sequence alignments.
"""
import re
import logging
from enum import Enum
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func

from cgd.core.settings import settings
from cgd.db.deps import get_db
from cgd.models.models import Feature, HomologyGroup, FeatHomology, Seq, FeatLocation

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["homology"])


class ClusterSeqType(str, Enum):
    """Sequence types for cluster download."""
    PROTEIN = "protein"
    CDS = "cds"
    GENOMIC = "genomic"
    GENOMIC_EXTENDED = "genomic_extended"


def _format_sequence(sequence: str, line_width: int = 60) -> str:
    """Format sequence with line breaks for FASTA output."""
    return "\n".join(
        sequence[i:i + line_width]
        for i in range(0, len(sequence), line_width)
    )


def _get_feature_sequence(
    db: Session,
    feature: Feature,
    seq_type: ClusterSeqType,
) -> Optional[tuple[str, str]]:
    """
    Get sequence for a feature.

    Returns tuple of (fasta_header, sequence) or None if not found.
    """
    # Determine database sequence type
    if seq_type == ClusterSeqType.PROTEIN:
        db_seq_type = "protein"
    else:
        db_seq_type = "genomic"

    # Get current sequence
    seq_record = (
        db.query(Seq)
        .filter(
            Seq.feature_no == feature.feature_no,
            Seq.seq_type == db_seq_type,
            Seq.is_seq_current == "Y"
        )
        .first()
    )

    if not seq_record or not seq_record.residues:
        return None

    sequence = seq_record.residues.upper()

    # For CDS, we need to extract coding sequence from genomic
    # The genomic sequence stored should already be the spliced CDS for ORFs
    # But let's handle extended genomic separately

    if seq_type == ClusterSeqType.GENOMIC_EXTENDED:
        # Add 1000bp flanking regions
        location = (
            db.query(FeatLocation)
            .filter(
                FeatLocation.feature_no == feature.feature_no,
                FeatLocation.is_loc_current == "Y"
            )
            .first()
        )

        if location:
            # Get the chromosome/root sequence
            root_seq = (
                db.query(Seq)
                .filter(
                    Seq.seq_no == location.root_seq_no,
                    Seq.is_seq_current == "Y"
                )
                .first()
            )

            if root_seq and root_seq.residues:
                chr_seq = root_seq.residues
                start = location.start_coord
                end = location.stop_coord
                strand = location.strand
                flank = 1000

                # Extract with flanking regions
                if strand == "W":
                    left_start = max(0, start - 1 - flank)
                    right_end = min(len(chr_seq), end + flank)
                    sequence = chr_seq[left_start:right_end].upper()
                else:
                    # Crick strand - need reverse complement
                    left_start = max(0, start - 1 - flank)
                    right_end = min(len(chr_seq), end + flank)
                    seq_region = chr_seq[left_start:right_end]
                    # Reverse complement
                    complement_map = str.maketrans("ACGTacgt", "TGCAtgca")
                    sequence = seq_region.translate(complement_map)[::-1].upper()

    # Build FASTA header
    gene_name = feature.gene_name or ""
    feature_name = feature.feature_name or ""
    organism_name = feature.organism.organism_name if feature.organism else ""

    # Format like "gene_name/feature_name" if both exist
    if gene_name and gene_name != feature_name:
        primary_id = f"{gene_name}/{feature_name}"
    else:
        primary_id = feature_name or gene_name

    header_parts = [primary_id]
    if feature.dbxref_id:
        header_parts.append(f"CGDID:{feature.dbxref_id}")
    if organism_name:
        header_parts.append(organism_name)
    header_parts.append(seq_type.value)

    fasta_header = ">" + " ".join(header_parts)

    return (fasta_header, sequence)


@router.get(
    "/homolog-sequences",
    response_class=PlainTextResponse,
    summary="Download cluster sequences in multi-FASTA format",
)
def get_cluster_sequences(
    cluster: str = Query(..., description="Feature name or gene name identifying the cluster"),
    type: ClusterSeqType = Query(..., description="Sequence type to download"),
    db: Session = Depends(get_db),
):
    """
    Download sequences for all members of an ortholog cluster in multi-FASTA format.

    Args:
        cluster: Feature name (e.g., C1_13700W_A) or gene name (e.g., ACT1)
        type: Sequence type - protein, cds, genomic, or genomic_extended (with 1000bp flanks)

    Returns:
        Multi-FASTA file with sequences for all CGD members of the cluster
    """
    cluster_name = cluster.strip()

    # Find the feature for the given cluster identifier
    query_feature = (
        db.query(Feature)
        .options(joinedload(Feature.organism))
        .filter(
            func.upper(Feature.feature_name) == func.upper(cluster_name)
        )
        .first()
    )

    if not query_feature:
        query_feature = (
            db.query(Feature)
            .options(joinedload(Feature.organism))
            .filter(
                func.upper(Feature.gene_name) == func.upper(cluster_name)
            )
            .first()
        )

    if not query_feature:
        raise HTTPException(
            status_code=404,
            detail=f"Feature not found: {cluster_name}"
        )

    # Find the CGOB homology group for this feature
    feat_homology = (
        db.query(FeatHomology)
        .join(HomologyGroup)
        .filter(
            FeatHomology.feature_no == query_feature.feature_no,
            HomologyGroup.homology_group_type == "ortholog",
            HomologyGroup.method == "CGOB",
        )
        .first()
    )

    if not feat_homology:
        raise HTTPException(
            status_code=404,
            detail=f"No CGOB ortholog cluster found for: {cluster_name}"
        )

    # Get all CGD features in this homology group
    cluster_features = (
        db.query(Feature)
        .options(joinedload(Feature.organism))
        .join(FeatHomology, Feature.feature_no == FeatHomology.feature_no)
        .filter(
            FeatHomology.homology_group_no == feat_homology.homology_group_no
        )
        .all()
    )

    if not cluster_features:
        raise HTTPException(
            status_code=404,
            detail=f"No features found in cluster for: {cluster_name}"
        )

    # Build multi-FASTA output
    fasta_entries = []

    for feature in cluster_features:
        result = _get_feature_sequence(db, feature, type)
        if result:
            header, sequence = result
            formatted_seq = _format_sequence(sequence)
            fasta_entries.append(f"{header}\n{formatted_seq}")

    if not fasta_entries:
        raise HTTPException(
            status_code=404,
            detail=f"No sequences found for cluster: {cluster_name}"
        )

    content = "\n".join(fasta_entries)

    # Determine filename
    filename = f"{cluster_name}_{type.value}.fasta"

    return PlainTextResponse(
        content=content,
        media_type="text/plain",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


def _parse_fasta(fasta_content: str) -> list[tuple[str, str]]:
    """Parse FASTA format. Returns list of (seq_id, sequence) tuples."""
    sequences = []
    current_id = None
    current_seq = []

    for line in fasta_content.split('\n'):
        line = line.strip()
        if not line:
            continue
        if line.startswith('>'):
            if current_id is not None:
                sequences.append((current_id, ''.join(current_seq)))
            current_id = line[1:].split()[0]
            current_seq = []
        else:
            current_seq.append(line)

    if current_id is not None:
        sequences.append((current_id, ''.join(current_seq)))

    return sequences


def _back_translate_alignment(
    protein_alignment: list[tuple[str, str]],
    coding_sequences: dict[str, str]
) -> list[tuple[str, str]]:
    """
    Generate coding sequence alignment by back-translating from protein alignment.

    For each position in the protein alignment:
    - If amino acid: copy the next 3 nucleotides (one codon)
    - If gap ('-'): insert '---' (three gaps)
    """
    coding_alignment = []

    for seq_id, protein_seq in protein_alignment:
        coding_seq = coding_sequences.get(seq_id, '')
        if not coding_seq:
            continue

        aligned_coding = []
        coding_idx = 0

        for aa in protein_seq:
            if aa == '-':
                aligned_coding.append('---')
            else:
                codon_end = coding_idx + 3
                if codon_end <= len(coding_seq):
                    aligned_coding.append(coding_seq[coding_idx:codon_end])
                else:
                    remaining = coding_seq[coding_idx:] if coding_idx < len(coding_seq) else ''
                    aligned_coding.append(remaining + 'N' * (3 - len(remaining)))
                coding_idx += 3

        coding_alignment.append((seq_id, ''.join(aligned_coding)))

    return coding_alignment


def _generate_coding_alignment_fasta(alignment_dir: Path, dbid: str) -> str:
    """Generate coding alignment FASTA by back-translating protein alignment."""
    protein_file = alignment_dir / f"{dbid}_protein_align.fasta"
    coding_file = alignment_dir / f"{dbid}_coding.fasta"

    if not protein_file.exists() or not coding_file.exists():
        return ""

    protein_content = protein_file.read_text()
    coding_content = coding_file.read_text()

    protein_alignment = _parse_fasta(protein_content)
    coding_seqs = {seq_id: seq for seq_id, seq in _parse_fasta(coding_content)}

    if not protein_alignment or not coding_seqs:
        return ""

    aligned_coding = _back_translate_alignment(protein_alignment, coding_seqs)

    # Format as FASTA
    lines = []
    for seq_id, sequence in aligned_coding:
        lines.append(f">{seq_id}")
        # Wrap sequence at 60 characters
        for i in range(0, len(sequence), 60):
            lines.append(sequence[i:i+60])

    return '\n'.join(lines)


def _get_alignment_dir(dbid: str) -> Path:
    """Get the alignment directory for a given dbid."""
    match = re.search(r'[^\d]*0*(\d+)', dbid)
    if not match:
        raise HTTPException(status_code=400, detail="Invalid dbid format")

    numeric_tag = int(match.group(1))
    bucket = numeric_tag // 100

    return Path(settings.cgd_data_dir) / "homology" / "alignments" / str(bucket)


@router.get(
    "/homology/alignment/{dbid}/{alignment_type}/fasta",
    response_class=PlainTextResponse,
    summary="Download alignment in FASTA format",
)
def get_alignment_fasta(dbid: str, alignment_type: str):
    """
    Download a sequence alignment in multi-FASTA format.

    Args:
        dbid: Database identifier (e.g., CAL0001234)
        alignment_type: "protein" or "coding"

    Returns:
        Alignment file content in FASTA format

    For coding alignments, the alignment is generated on-the-fly by back-translating
    the protein alignment (matching the Perl implementation in Tools/SeqAlign.pm).
    """
    if alignment_type not in ("protein", "coding"):
        raise HTTPException(
            status_code=400,
            detail="alignment_type must be 'protein' or 'coding'"
        )

    alignment_dir = _get_alignment_dir(dbid)

    try:
        if alignment_type == "protein":
            # Protein alignment - read directly from file
            align_file = alignment_dir / f"{dbid}_protein_align.fasta"
            if not align_file.exists():
                raise HTTPException(
                    status_code=404,
                    detail=f"Protein alignment file not found for {dbid}"
                )
            content = align_file.read_text()
        else:
            # Coding alignment - generate by back-translating protein alignment
            content = _generate_coding_alignment_fasta(alignment_dir, dbid)
            if not content:
                raise HTTPException(
                    status_code=404,
                    detail=f"Could not generate coding alignment for {dbid}"
                )

        return PlainTextResponse(
            content=content,
            media_type="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={dbid}_{alignment_type}_align.fasta"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating alignment: {e}")
        raise HTTPException(status_code=500, detail="Error generating alignment file")


@router.get(
    "/homology/alignment/{dbid}/{alignment_type}/clustalw",
    response_class=PlainTextResponse,
    summary="Download alignment in ClustalW format",
)
def get_alignment_clustalw(dbid: str, alignment_type: str):
    """
    Download a sequence alignment in ClustalW format.

    Args:
        dbid: Database identifier (e.g., CAL0001234)
        alignment_type: "protein" or "coding"

    Returns:
        Alignment file content in ClustalW format
    """
    if alignment_type not in ("protein", "coding"):
        raise HTTPException(
            status_code=400,
            detail="alignment_type must be 'protein' or 'coding'"
        )

    alignment_dir = _get_alignment_dir(dbid)
    clw_file = alignment_dir / f"{dbid}_{alignment_type}_align.clw"

    if not clw_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"ClustalW alignment file not found for {dbid}"
        )

    try:
        content = clw_file.read_text()
        return PlainTextResponse(
            content=content,
            media_type="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={dbid}_{alignment_type}_align.clw"
            }
        )
    except Exception as e:
        logger.error(f"Error reading ClustalW file: {e}")
        raise HTTPException(status_code=500, detail="Error reading alignment file")


@router.get(
    "/homology/tree/{dbid}/{tree_type}",
    response_class=PlainTextResponse,
    summary="Download phylogenetic tree file",
)
def get_tree_file(dbid: str, tree_type: str):
    """
    Download a phylogenetic tree file.

    Args:
        dbid: Database identifier (e.g., CAL0001234)
        tree_type: "unrooted", "rooted", "xml", or "annotated"

    Returns:
        Tree file content
    """
    valid_types = ("unrooted", "rooted", "xml", "annotated")
    if tree_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"tree_type must be one of: {', '.join(valid_types)}"
        )

    alignment_dir = _get_alignment_dir(dbid)

    # Map tree_type to filename
    filename_map = {
        "unrooted": f"{dbid}_tree_unrooted.par",
        "rooted": f"{dbid}_tree_rooted.par",
        "xml": f"{dbid}_tree_rooted.xml",
        "annotated": f"{dbid}_tree_annotated.xml",
    }

    tree_file = alignment_dir / filename_map[tree_type]

    if not tree_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Tree file not found for {dbid}"
        )

    try:
        content = tree_file.read_text()

        # Determine content type and extension
        if tree_type in ("xml", "annotated"):
            media_type = "application/xml"
            ext = "xml"
        else:
            media_type = "text/plain"
            ext = "nwk"

        return PlainTextResponse(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": f"attachment; filename={dbid}_tree_{tree_type}.{ext}"
            }
        )
    except Exception as e:
        logger.error(f"Error reading tree file: {e}")
        raise HTTPException(status_code=500, detail="Error reading tree file")
