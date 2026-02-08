"""
Homology API Router.

Provides endpoints for homology data including sequence alignments.
"""
import re
import logging
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

from cgd.core.settings import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/homology", tags=["homology"])


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
    "/alignment/{dbid}/{alignment_type}/fasta",
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
    "/alignment/{dbid}/{alignment_type}/clustalw",
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
    "/tree/{dbid}/{tree_type}",
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
