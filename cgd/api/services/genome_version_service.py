"""
Genome Version History Service.

Provides genome version history information for different strains/assemblies.
"""
from __future__ import annotations

import logging
from typing import List, Optional
from sqlalchemy.orm import Session

from cgd.models.models import Organism, GenomeVersion
from cgd.schemas.genome_version_schema import (
    GenomeVersionEntry,
    SeqSourceInfo,
    GenomeVersionConfigResponse,
    GenomeVersionHistoryResponse,
)

logger = logging.getLogger(__name__)

# Default strain abbreviation (C. albicans SC5314)
DEFAULT_STRAIN_ABBREV = "C_albicans_SC5314"

# Version format explanation text
VERSION_FORMAT_EXPLANATION = """
The version designation appears in the name of each of the relevant sequence files
that are available at CGD, so the exact source of the sequence data is always clear.

Version designations appear in the following format:
<strong>sXX-mYY-rZZ</strong>

where XX, YY, and ZZ are zero-padded integers.

<strong>XX</strong> is incremented when there is any change to the underlying genomic
(i.e., chromosome) sequence.

<strong>YY</strong> is incremented when there is any change to the coordinates of any
feature annotated in the genome (e.g., any change in location or boundary, or addition
or removal of a feature from the annotation). YY is reset to "01" when XX is incremented
(when a sequence change is made).

<strong>ZZ</strong> is incremented in response to curatorial changes that affect information
that appears in the GFF file, specifically gene names, gene aliases, gene IDs, gene
descriptions, feature types (e.g., gene or pseudogene), and ORF classifications or
qualifiers (e.g., Verified, Uncharacterized, Deleted, Merged). The file will be checked
on a weekly basis, as well as any time that the GFF file is regenerated manually, to see
if changes have occurred that warrant a change in the ZZ number. ZZ is reset to "01" when
XX or YY is incremented.
""".strip()


def _get_strains(db: Session) -> List[Organism]:
    """
    Get all strains (organisms with taxonomic_rank = 'Strain').

    Returns strains sorted by organism_order.
    """
    return (
        db.query(Organism)
        .filter(Organism.taxonomic_rank == "Strain")
        .order_by(Organism.organism_order)
        .all()
    )


def _get_strain_display_name(organism: Organism) -> str:
    """
    Get a display-friendly name for the strain.

    Uses organism_name but could be customized.
    """
    return organism.organism_name


def get_genome_version_config(db: Session) -> GenomeVersionConfigResponse:
    """
    Get configuration for the genome version history page.

    Returns available strains/assemblies for the dropdown.
    """
    strains = _get_strains(db)

    seq_sources = []
    for strain in strains:
        seq_sources.append(SeqSourceInfo(
            seq_source=strain.organism_abbrev,
            organism_abbrev=strain.organism_abbrev,
            organism_name=strain.organism_name,
            display_name=_get_strain_display_name(strain),
        ))

    # Determine default
    default_seq_source = DEFAULT_STRAIN_ABBREV
    if seq_sources and not any(s.seq_source == DEFAULT_STRAIN_ABBREV for s in seq_sources):
        default_seq_source = seq_sources[0].seq_source

    return GenomeVersionConfigResponse(
        seq_sources=seq_sources,
        default_seq_source=default_seq_source,
        version_format_explanation=VERSION_FORMAT_EXPLANATION,
    )


def get_genome_version_history(
    db: Session,
    seq_source: str,
) -> GenomeVersionHistoryResponse:
    """
    Get genome version history for a specific strain/assembly.

    Args:
        db: Database session
        seq_source: Organism abbreviation (strain identifier)

    Returns:
        Genome version history with all versions
    """
    # Get organism
    organism = (
        db.query(Organism)
        .filter(Organism.organism_abbrev == seq_source)
        .first()
    )

    if not organism:
        return GenomeVersionHistoryResponse(
            success=False,
            seq_source=seq_source,
            strain_display_name="",
            versions=[],
            error=f"Strain '{seq_source}' not found",
        )

    # Get genome versions for this organism
    genome_versions = (
        db.query(GenomeVersion)
        .filter(GenomeVersion.organism_no == organism.organism_no)
        .order_by(GenomeVersion.date_created.desc())
        .all()
    )

    versions = []
    for gv in genome_versions:
        # Determine if this is a major version (ends with r01)
        is_major = gv.genome_version.endswith("r01") if gv.genome_version else False

        versions.append(GenomeVersionEntry(
            genome_version=gv.genome_version,
            strain_name=_get_strain_display_name(organism),
            is_current=gv.is_ver_current == "Y",
            date_created=gv.date_created,
            description=gv.description,
            is_major_version=is_major,
        ))

    return GenomeVersionHistoryResponse(
        success=True,
        seq_source=seq_source,
        strain_display_name=_get_strain_display_name(organism),
        versions=versions,
    )
