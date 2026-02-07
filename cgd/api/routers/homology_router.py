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
    """
    if alignment_type not in ("protein", "coding"):
        raise HTTPException(
            status_code=400,
            detail="alignment_type must be 'protein' or 'coding'"
        )

    alignment_dir = _get_alignment_dir(dbid)
    align_file = alignment_dir / f"{dbid}_{alignment_type}_align.fasta"

    # For coding, also try source file
    if not align_file.exists() and alignment_type == "coding":
        align_file = alignment_dir / f"{dbid}_coding.fasta"

    if not align_file.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Alignment file not found for {dbid}"
        )

    try:
        content = align_file.read_text()
        return PlainTextResponse(
            content=content,
            media_type="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={dbid}_{alignment_type}_align.fasta"
            }
        )
    except Exception as e:
        logger.error(f"Error reading alignment file: {e}")
        raise HTTPException(status_code=500, detail="Error reading alignment file")


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
