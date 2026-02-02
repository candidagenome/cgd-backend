import os
import re
import json
import urllib.request
import urllib.error
from typing import Optional
from pathlib import Path
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_
from collections import defaultdict

from cgd.api.crud.locus_crud import get_features_for_locus_name
from cgd.schemas.locus_schema import (
    LocusByOrganismResponse,
    FeatureOut,
    AliasOut,
    AliasWithRefsOut,
    ExternalLinkOut,
    AlleleOut,
    AlleleLocationOut,
    AlleleSubfeatureOut,
    CandidaOrthologOut,
    ExternalOrthologOut,
    OtherStrainNameOut,
    SequenceDetailsResponse,
    SequenceDetailsForOrganism,
    SequenceLocationOut,
    SequenceOut,
    SubfeatureOut,
    SequenceResources,
    SequenceResourceItem,
    JBrowseInfo,
    LocusReferencesResponse,
    ReferencesForOrganism,
    ReferenceForLocus,
    CitationLinkForLocus,
    LiteratureTopicOut,
    LiteratureTopicGroupOut,
    LocusSummaryNotesResponse,
    SummaryNotesForOrganism,
    SummaryNoteOut,
    LocusHistoryResponse,
    LocusHistoryForOrganism,
    HistoryEventOut,
    ReferenceOutForHistory,
    CitationLinkForHistory,
    ContactOut,
    ReservedNameInfoOut,
    StandardNameInfoOut,
    AliasNameInfoOut,
    NomenclatureHistoryOut,
    NoteWithReferencesOut,
    NoteCategoryOut,
    NomenclatureNameWithRef,
    NomenclatureOut,
)
from cgd.schemas.go_schema import (
    GODetailsResponse,
    GODetailsForOrganism,
    GOAnnotationOut,
    GOTerm,
    GOEvidence,
    ReferenceForAnnotation as GORefForAnnotation,
    CitationLinkForGO,
)
from cgd.schemas.phenotype_schema import (
    PhenotypeDetailsResponse,
    PhenotypeDetailsForOrganism,
    PhenotypeAnnotationOut,
    PhenotypeTerm,
    ReferenceForAnnotation as PhenoRefForAnnotation,
    CitationLinkForPhenotype,
    ExperimentProperty,
)
from cgd.schemas.interaction_schema import (
    InteractionDetailsResponse,
    InteractionDetailsForOrganism,
    InteractionOut,
    InteractorOut,
)
from cgd.schemas.protein_schema import (
    ProteinDetailsResponse,
    ProteinDetailsForOrganism,
    ProteinInfoOut,
    ProteinAlleleNameOut,
    ProteinExternalLinkOut,
    ConservedDomainOut,
    StructuralInfoOut,
    ExperimentalObservationOut,
    ProteinHomologOut,
    SequenceDetailOut,
    ReferenceForProtein,
    CitationLinkForProtein,
    AlphaFoldInfo,
    ProteinPropertiesResponse,
    ProteinPropertiesForOrganism,
    AminoAcidComposition,
    BulkPropertyItem,
    ExtinctionCoefficient,
    AtomicCompositionItem,
    CodonUsageItem,
    ProteinDomainResponse,
    ProteinDomainForOrganism,
    InterProDomain,
    DomainEntry,
    DomainHit,
    TransmembraneDomain,
    SignalPeptide,
    DomainExternalLink,
)
from cgd.schemas.homology_schema import (
    HomologyDetailsResponse,
    HomologyDetailsForOrganism,
    HomologyGroupOut,
    HomologOut,
    OrthologClusterOut,
    OrthologOut,
    DownloadLinkOut,
    BestHitOut,
    BestHitsInCGDOut,
    ExternalHomologOut,
    ExternalHomologsSectionOut,
    PhylogeneticTreeOut,
)
from cgd.core.settings import settings
from cgd.models.locus_model import Feature
from cgd.models.go_model import GoAnnotation, GoRef
from cgd.models.phenotype_model import PhenoAnnotation
from cgd.models.interaction_model import FeatInteract
from cgd.models.homology_model import FeatHomology
from cgd.models.models import (
    RefLink,
    RefUrl,
    RefProperty,
    RefpropFeat,
    FeatAlias,
    Alias,
    FeatUrl,
    Url,
    FeatPara,
    Paragraph,
    FeatLocation,
    FeatRelationship,
    Seq,
    Note,
    NoteLink,
    Reference,
    FeatProperty,
    HomologyGroup,
    Dbxref,
    DbxrefFeat,
    DbxrefUrl,
    DbxrefHomology,
    ProteinInfo,
    ProteinDetail,
    WebDisplay,
    GeneReservation,
    CollGeneres,
    Colleague,
    Experiment,
    ExptExptprop,
    ExptProperty,
    CvtermGroup,
    CvtermRelationship,
    CvTerm,
    Cv,
)


# Organisms that use translation table 12 (CTG codes for Serine)
# CUG codons are only relevant for these organisms
TRANSLATION_TABLE_12_ORGANISMS = {
    'Candida albicans',
    'Candida albicans SC5314',
    'Candida dubliniensis',
    'Candida dubliniensis CD36',
    'Candida tropicalis',
    'Candida tropicalis MYA-3404',
    'Candida parapsilosis',
    'Candida parapsilosis CDC317',
    'Lodderomyces elongisporus',
    'Lodderomyces elongisporus NRRL YB-4239',
    'Candida auris',
    # Add other CTG clade species as needed
}


# Non-CGD ortholog sources to display with their species names
NON_CGD_ORTHOLOG_SOURCES = {
    'SGD': 'S. cerevisiae',
    'POMBASE': 'S. pombe',
    'AspGD': 'A. nidulans',
    'BROAD_NEUROSPORA': 'N. crassa',
}


def _build_citation_links_for_locus(ref, ref_urls=None) -> list[CitationLinkForLocus]:
    """
    Build citation links for a reference in locus context.

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: Optional list of RefUrl objects for additional links

    Returns:
        List of CitationLinkForLocus objects
    """
    links = []

    # CGD Paper link (always present) - always use dbxref_id (CGDID)
    links.append(CitationLinkForLocus(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLinkForLocus(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Process URLs from ref_url table (if provided)
    # Match Perl behavior: show all URLs except 'Reference supplement' and 'Reference Data'
    if ref_urls:
        for ref_url in ref_urls:
            url_obj = ref_url.url
            if url_obj and url_obj.url:
                url_type = (url_obj.url_type or "").lower()

                # Skip Reference supplement (displayed separately)
                if "supplement" in url_type:
                    links.append(CitationLinkForLocus(
                        name="Reference Supplement",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # Skip Reference Data (not shown as full text)
                elif "reference data" in url_type:
                    continue
                # Download Datasets
                elif any(kw in url_type for kw in ["download", "dataset"]):
                    links.append(CitationLinkForLocus(
                        name="Download Datasets",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # All other URL types are shown as Full Text (matching Perl default behavior)
                # This includes: Reference LINKOUT, Reference full text, and any others
                else:
                    links.append(CitationLinkForLocus(
                        name="Full Text",
                        url=url_obj.url,
                        link_type="external"
                    ))

    return links


def _build_citation_links_for_go(ref, ref_urls=None) -> list[CitationLinkForGO]:
    """
    Build citation links for a reference in GO annotation context.

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: Optional list of RefUrl objects for additional links

    Returns:
        List of CitationLinkForGO objects
    """
    links = []

    # CGD Paper link (always present) - always use dbxref_id (CGDID)
    links.append(CitationLinkForGO(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLinkForGO(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Process URLs from ref_url table (if provided)
    # Match Perl behavior: show all URLs except 'Reference supplement' and 'Reference Data'
    if ref_urls:
        for ref_url in ref_urls:
            url_obj = ref_url.url
            if url_obj and url_obj.url:
                url_type = (url_obj.url_type or "").lower()

                # Skip Reference supplement (displayed separately)
                if "supplement" in url_type:
                    links.append(CitationLinkForGO(
                        name="Reference Supplement",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # Skip Reference Data (not shown as full text)
                elif "reference data" in url_type:
                    continue
                # Download Datasets
                elif any(kw in url_type for kw in ["download", "dataset"]):
                    links.append(CitationLinkForGO(
                        name="Download Datasets",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # All other URL types are shown as Full Text (matching Perl default behavior)
                else:
                    links.append(CitationLinkForGO(
                        name="Full Text",
                        url=url_obj.url,
                        link_type="external"
                    ))

    return links


def _build_citation_links_for_protein(ref, ref_urls=None) -> list[CitationLinkForProtein]:
    """
    Build citation links for a reference in protein context.

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: Optional list of RefUrl objects for additional links

    Returns:
        List of CitationLinkForProtein objects
    """
    links = []

    # CGD Paper link (always present) - always use dbxref_id (CGDID)
    links.append(CitationLinkForProtein(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLinkForProtein(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Process URLs from ref_url table (if provided)
    # Match Perl behavior: show all URLs except 'Reference supplement' and 'Reference Data'
    if ref_urls:
        for ref_url in ref_urls:
            url_obj = ref_url.url
            if url_obj and url_obj.url:
                url_type = (url_obj.url_type or "").lower()

                # Skip Reference supplement (displayed separately)
                if "supplement" in url_type:
                    links.append(CitationLinkForProtein(
                        name="Reference Supplement",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # Skip Reference Data (not shown as full text)
                elif "reference data" in url_type:
                    continue
                # Download Datasets
                elif any(kw in url_type for kw in ["download", "dataset"]):
                    links.append(CitationLinkForProtein(
                        name="Download Datasets",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # All other URL types are shown as Full Text (matching Perl default behavior)
                else:
                    links.append(CitationLinkForProtein(
                        name="Full Text",
                        url=url_obj.url,
                        link_type="external"
                    ))

    return links


def _build_citation_links_for_phenotype(ref, ref_urls=None) -> list[CitationLinkForPhenotype]:
    """
    Build citation links for a reference in phenotype annotation context.

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: Optional list of RefUrl objects for additional links

    Returns:
        List of CitationLinkForPhenotype objects
    """
    links = []

    # CGD Paper link (always present) - always use dbxref_id (CGDID)
    links.append(CitationLinkForPhenotype(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLinkForPhenotype(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Process URLs from ref_url table (if provided)
    # Match Perl behavior: show all URLs except 'Reference supplement' and 'Reference Data'
    if ref_urls:
        for ref_url in ref_urls:
            url_obj = ref_url.url
            if url_obj and url_obj.url:
                url_type = (url_obj.url_type or "").lower()

                # Skip Reference supplement (displayed separately)
                if "supplement" in url_type:
                    links.append(CitationLinkForPhenotype(
                        name="Reference Supplement",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # Skip Reference Data (not shown as full text)
                elif "reference data" in url_type:
                    continue
                # Download Datasets
                elif any(kw in url_type for kw in ["download", "dataset"]):
                    links.append(CitationLinkForPhenotype(
                        name="Download Datasets",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # All other URL types are shown as Full Text (matching Perl default behavior)
                else:
                    links.append(CitationLinkForPhenotype(
                        name="Full Text",
                        url=url_obj.url,
                        link_type="external"
                    ))

    return links


def _build_citation_links_for_history(ref, ref_urls=None) -> list[CitationLinkForHistory]:
    """
    Build citation links for a reference in history/nomenclature context.

    Args:
        ref: Reference object with pubmed and dbxref_id
        ref_urls: Optional list of RefUrl objects for additional links

    Returns:
        List of CitationLinkForHistory objects
    """
    links = []

    # CGD Paper link (always present) - always use dbxref_id (CGDID)
    links.append(CitationLinkForHistory(
        name="CGD Paper",
        url=f"/reference/{ref.dbxref_id}",
        link_type="internal"
    ))

    # PubMed link (if pubmed ID exists)
    if ref.pubmed:
        links.append(CitationLinkForHistory(
            name="PubMed",
            url=f"https://pubmed.ncbi.nlm.nih.gov/{ref.pubmed}",
            link_type="external"
        ))

    # Process URLs from ref_url table (if provided)
    # Match Perl behavior: show all URLs except 'Reference supplement' and 'Reference Data'
    if ref_urls:
        for ref_url in ref_urls:
            url_obj = ref_url.url
            if url_obj and url_obj.url:
                url_type = (url_obj.url_type or "").lower()

                # Skip Reference supplement (displayed separately)
                if "supplement" in url_type:
                    links.append(CitationLinkForHistory(
                        name="Reference Supplement",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # Skip Reference Data (not shown as full text)
                elif "reference data" in url_type:
                    continue
                # Download Datasets
                elif any(kw in url_type for kw in ["download", "dataset"]):
                    links.append(CitationLinkForHistory(
                        name="Download Datasets",
                        url=url_obj.url,
                        link_type="external"
                    ))
                # All other URL types are shown as Full Text (matching Perl default behavior)
                else:
                    links.append(CitationLinkForHistory(
                        name="Full Text",
                        url=url_obj.url,
                        link_type="external"
                    ))

    return links


def _gene_name_to_protein_name(gene_name: str) -> str:
    """
    Convert gene name to protein name format.
    e.g., ACT1 -> Act1p, CDC42 -> Cdc42p, BRG1 -> Brg1p
    """
    if not gene_name:
        return ''
    # Capitalize first letter, lowercase rest, add 'p' suffix
    return gene_name.capitalize() + 'p'


def _systematic_name_to_protein_name(systematic_name: str) -> str:
    """
    Convert systematic ORF name to protein name format.
    e.g., C1_13700W_A -> C1_13700wp_a, C1_13700W_B -> C1_13700wp_b
    """
    if not systematic_name:
        return ''
    # Replace uppercase W with lowercase 'wp', and uppercase final letter with lowercase
    # Pattern: C1_13700W_A -> C1_13700wp_a
    # Match pattern like C1_13700W_A or orf19.1234
    match = re.match(r'^([A-Z]\d+_\d+)([A-Z])_([A-Z])$', systematic_name, re.IGNORECASE)
    if match:
        prefix = match.group(1)  # C1_13700
        letter = match.group(2)  # W
        allele = match.group(3)  # A
        return f"{prefix}{letter.lower()}p_{allele.lower()}"

    # Try pattern like orf19.1234
    if systematic_name.lower().startswith('orf'):
        return systematic_name.lower() + 'p'

    # Default: lowercase + p
    return systematic_name.lower() + 'p'


def _format_sequence_gcg(sequence: str, name: str, length: int, seq_type: str = 'Protein') -> str:
    """
    Format a sequence in GCG/Wisconsin format.

    GCG format:
    !!AA_SEQUENCE 1.0
    Name: ACT1p  Length: 375  Type: Protein  Check: 1234

    MDDDIAALV DSEVNHFNVE LDAIKG...
    """
    if not sequence:
        return ''

    # Calculate a simple checksum (sum of ASCII values mod 10000)
    checksum = sum(ord(c) for c in sequence) % 10000

    lines = [
        f"!!AA_SEQUENCE 1.0",
        f"{name}  Length: {length}  {seq_type}  Check: {checksum}",
        "",
    ]

    # Format sequence: 10 blocks of 6 chars per line (60 chars per line)
    # With position numbers at the start
    pos = 1
    for i in range(0, len(sequence), 50):
        chunk = sequence[i:i+50]
        # Split into groups of 10
        groups = [chunk[j:j+10] for j in range(0, len(chunk), 10)]
        line = f"{pos:8d}  " + " ".join(groups)
        lines.append(line)
        pos += 50

    return "\n".join(lines)


def _count_cug_codons(cds_sequence: str) -> int:
    """
    Count CUG codons (CTG in DNA) in a CDS sequence.

    In translation table 12 (used by most Candida species), CTG codes for
    Serine instead of Leucine. This function counts how many CTG codons
    are present in the coding sequence.

    Args:
        cds_sequence: The CDS DNA sequence (should be in-frame)

    Returns:
        Number of CTG codons found
    """
    if not cds_sequence:
        return 0

    cds_upper = cds_sequence.upper()
    count = 0

    # Count CTG codons at codon positions (every 3rd position)
    for i in range(0, len(cds_upper) - 2, 3):
        codon = cds_upper[i:i + 3]
        if codon == 'CTG':
            count += 1

    return count


def _get_cds_sequence(db: Session, feature_no: int) -> Optional[str]:
    """
    Get the current CDS sequence for a feature.

    Args:
        db: Database session
        feature_no: Feature number to get CDS for

    Returns:
        CDS sequence string or None if not found
    """
    seq_row = (
        db.query(Seq.residues)
        .filter(
            Seq.feature_no == feature_no,
            func.upper(Seq.seq_type) == 'CDS',
            Seq.is_seq_current == 'Y',
        )
        .first()
    )
    return seq_row[0] if seq_row else None


def _translate_codon(codon: str, use_table_12: bool = True) -> str:
    """
    Translate a single codon to amino acid.

    Args:
        codon: 3-letter DNA codon
        use_table_12: If True, use translation table 12 (CTG -> Ser)

    Returns:
        Single letter amino acid code, or 'X' for unknown
    """
    codon = codon.upper()

    # Standard genetic code with table 12 modification
    codon_table = {
        'TTT': 'F', 'TTC': 'F', 'TTA': 'L', 'TTG': 'L',
        'TCT': 'S', 'TCC': 'S', 'TCA': 'S', 'TCG': 'S',
        'TAT': 'Y', 'TAC': 'Y', 'TAA': '*', 'TAG': '*',
        'TGT': 'C', 'TGC': 'C', 'TGA': '*', 'TGG': 'W',
        'CTT': 'L', 'CTC': 'L', 'CTA': 'L',
        'CTG': 'S' if use_table_12 else 'L',  # Key difference for table 12
        'CCT': 'P', 'CCC': 'P', 'CCA': 'P', 'CCG': 'P',
        'CAT': 'H', 'CAC': 'H', 'CAA': 'Q', 'CAG': 'Q',
        'CGT': 'R', 'CGC': 'R', 'CGA': 'R', 'CGG': 'R',
        'ATT': 'I', 'ATC': 'I', 'ATA': 'I', 'ATG': 'M',
        'ACT': 'T', 'ACC': 'T', 'ACA': 'T', 'ACG': 'T',
        'AAT': 'N', 'AAC': 'N', 'AAA': 'K', 'AAG': 'K',
        'AGT': 'S', 'AGC': 'S', 'AGA': 'R', 'AGG': 'R',
        'GTT': 'V', 'GTC': 'V', 'GTA': 'V', 'GTG': 'V',
        'GCT': 'A', 'GCC': 'A', 'GCA': 'A', 'GCG': 'A',
        'GAT': 'D', 'GAC': 'D', 'GAA': 'E', 'GAG': 'E',
        'GGT': 'G', 'GGC': 'G', 'GGA': 'G', 'GGG': 'G',
    }

    return codon_table.get(codon, 'X')


def _translate_sequence(cds: str, use_table_12: bool = True) -> str:
    """
    Translate a CDS sequence to protein.

    Args:
        cds: CDS DNA sequence
        use_table_12: If True, use translation table 12

    Returns:
        Protein sequence string
    """
    if not cds:
        return ''

    protein = []
    cds_upper = cds.upper()

    for i in range(0, len(cds_upper) - 2, 3):
        codon = cds_upper[i:i + 3]
        # Skip codons with ambiguous bases
        if any(base not in 'ACGT' for base in codon):
            protein.append('X')
        else:
            protein.append(_translate_codon(codon, use_table_12))

    return ''.join(protein)


def _check_allelic_variation(
    primary_cds: Optional[str],
    allele_cds: Optional[str],
    primary_name: str,
    allele_name: str,
    use_table_12: bool = True
) -> Optional[str]:
    """
    Compare primary and allele CDS sequences to determine allelic variation.

    This implements the logic from the legacy Perl check_allele_variation()
    and get_variation_descriptions() functions.

    Args:
        primary_cds: CDS sequence of primary ORF
        allele_cds: CDS sequence of allele
        primary_name: Name of primary feature
        allele_name: Name of allele feature
        use_table_12: Whether to use translation table 12

    Returns:
        Description of allelic variation, or None if no comparison possible
    """
    if not primary_cds or not allele_cds:
        return None

    primary_upper = primary_cds.upper()
    allele_upper = allele_cds.upper()

    # Check for ambiguous sequences
    has_ambiguous_primary = any(base not in 'ACGT' for base in primary_upper)
    has_ambiguous_allele = any(base not in 'ACGT' for base in allele_upper)
    has_ambiguous = has_ambiguous_primary or has_ambiguous_allele

    descriptions = []

    # Compare CDS sequences
    if primary_upper == allele_upper:
        if has_ambiguous:
            descriptions.append(
                'Unphased variation between alleles (contains ambiguous sequences)'
            )
        else:
            descriptions.append('No allelic variation in feature')
    else:
        # CDS differs - check if synonymous or non-synonymous
        primary_protein = _translate_sequence(primary_upper, use_table_12)
        allele_protein = _translate_sequence(allele_upper, use_table_12)

        if primary_protein == allele_protein:
            descriptions.append('Synonymous variation between alleles')
        else:
            descriptions.append('Non-synonymous variation between alleles')

        if has_ambiguous:
            descriptions.append(
                'Unphased variation between alleles (contains ambiguous sequences)'
            )

    # Check for internal stop codons in primary
    primary_protein = _translate_sequence(primary_upper, use_table_12)
    if '*' in primary_protein[:-1]:  # Exclude terminal stop
        descriptions.append(f'{primary_name} contains internal stop codons')

    # Check for missing start codon in primary
    if len(primary_upper) >= 3:
        start_codon = primary_upper[:3]
        if start_codon != 'ATG':
            descriptions.append(f'{primary_name} lacks a start codon')

    # Check for missing terminal stop in primary
    if primary_protein and primary_protein[-1] != '*':
        descriptions.append(f'{primary_name} lacks a terminal stop codon')

    # Same checks for allele
    allele_protein = _translate_sequence(allele_upper, use_table_12)
    if '*' in allele_protein[:-1]:
        descriptions.append(f'{allele_name} contains internal stop codons')

    if len(allele_upper) >= 3:
        start_codon = allele_upper[:3]
        if start_codon != 'ATG':
            descriptions.append(f'{allele_name} lacks a start codon')

    if allele_protein and allele_protein[-1] != '*':
        descriptions.append(f'{allele_name} lacks a terminal stop codon')

    return '; '.join(descriptions) if descriptions else None


def _get_organism_info(f) -> tuple[str, int]:
    """Extract organism name and taxon_id from a feature, with fallback."""
    org = f.organism
    organism_name = None
    taxon_id = 0
    if org is not None:
        organism_name = (
            getattr(org, "organism_name", None)
            or getattr(org, "display_name", None)
            or getattr(org, "name", None)
        )
        taxon_id = getattr(org, "taxon_id", 0) or 0
    if not organism_name:
        organism_name = str(f.organism_no)
    return organism_name, taxon_id


# Default sequence sources per organism (matching Perl config)
DEFAULT_SEQ_SOURCES = {
    "Candida albicans SC5314": "C. albicans SC5314 Assembly 22",
    # Add other organisms/strains as needed
}


def _filter_features_by_preference(
    db: Session,
    features: list,
    prefer_seq_source: Optional[str] = None
) -> list:
    """
    Filter multiple features to return one per organism, similar to Perl
    check_multi_feature_list.

    Logic:
    1. Group features by organism
    2. For each organism with multiple features:
       a. Check for primary allele relationships - prefer parent over secondary
       b. Check for Assembly 22 relationships - prefer Assembly 22 version
       c. Prefer features from the default sequence source
       d. If still multiple, prefer feature_name starting with 'orf'
    3. Return one feature per organism

    Args:
        db: Database session
        features: List of Feature objects
        prefer_seq_source: Optional sequence source to prefer

    Returns:
        Filtered list of Feature objects (one per organism)
    """
    if not features:
        return features

    # Group features by organism_no
    features_by_org: dict[int, list] = defaultdict(list)
    for f in features:
        features_by_org[f.organism_no].append(f)

    result = []

    for org_no, org_features in features_by_org.items():
        if len(org_features) == 1:
            result.append(org_features[0])
            continue

        # Get organism name for default seq source lookup
        org_name, _ = _get_organism_info(org_features[0])
        default_source = prefer_seq_source or DEFAULT_SEQ_SOURCES.get(org_name)

        # Build a map of feature_no to feature for quick lookup
        feat_map = {f.feature_no: f for f in org_features}

        # Step 1: Check for Assembly 22 primary allele relationships
        # If a feature is an Assembly 21 version with an Assembly 22 equivalent,
        # prefer the Assembly 22 version
        a22_replacements = {}
        for f in org_features:
            # Query for Assembly 21 Primary Allele relationship where this
            # feature is the child (Assembly 21) and parent is Assembly 22
            a22_rel = (
                db.query(FeatRelationship)
                .filter(
                    FeatRelationship.child_feature_no == f.feature_no,
                    FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
                    FeatRelationship.rank == 3,
                )
                .first()
            )
            if a22_rel:
                # Check if the parent (A22) is in our feature list
                parent_no = a22_rel.parent_feature_no
                if parent_no in feat_map:
                    a22_replacements[f.feature_no] = parent_no

        # Remove Assembly 21 features that have Assembly 22 equivalents in the list
        if a22_replacements:
            org_features = [
                f for f in org_features
                if f.feature_no not in a22_replacements
            ]

        if len(org_features) == 1:
            result.append(org_features[0])
            continue

        # Step 2: Prefer features from the default sequence source
        if default_source:
            features_with_default_source = []
            for f in org_features:
                # Check if feature has sequences from default source
                seq_sources = (
                    db.query(Seq.source)
                    .filter(
                        Seq.feature_no == f.feature_no,
                        Seq.is_seq_current == 'Y',
                    )
                    .all()
                )
                sources = [s[0] for s in seq_sources]
                if default_source in sources:
                    features_with_default_source.append(f)

            if features_with_default_source:
                org_features = features_with_default_source

        if len(org_features) == 1:
            result.append(org_features[0])
            continue

        # Step 3: Check for deleted/unmapped features - deprioritize them
        non_deleted_features = []
        for f in org_features:
            props = (
                db.query(FeatProperty.property_value)
                .filter(FeatProperty.feature_no == f.feature_no)
                .all()
            )
            prop_values = [p[0].lower() if p[0] else '' for p in props]
            is_deleted = any('deleted' in pv for pv in prop_values)
            if not is_deleted:
                non_deleted_features.append(f)

        if non_deleted_features:
            org_features = non_deleted_features

        if len(org_features) == 1:
            result.append(org_features[0])
            continue

        # Step 4: Prefer feature_name starting with 'orf' (common convention)
        orf_features = [
            f for f in org_features
            if f.feature_name and f.feature_name.lower().startswith('orf')
        ]
        if orf_features:
            org_features = orf_features

        if len(org_features) == 1:
            result.append(org_features[0])
            continue

        # Step 5: If still multiple, sort by feature_name and take first
        # This ensures deterministic behavior
        org_features.sort(key=lambda f: f.feature_name or '')
        result.append(org_features[0])

    return result


def get_locus_by_organism(db: Session, name: str) -> LocusByOrganismResponse:
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_alias).joinedload(FeatAlias.alias),
            joinedload(Feature.feat_url).joinedload(FeatUrl.url),
            joinedload(Feature.feat_homology).joinedload(FeatHomology.homology_group),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        # Exclude alleles - match Perl behavior where feature_type 'allele'
        # is not in the web_metadata allowed list for Locus Page
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    out: dict[str, FeatureOut] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)

        # Get aliases and extract other strain names
        aliases = []
        other_strain_names = []
        for fa in f.feat_alias:
            alias = fa.alias
            if alias:
                aliases.append(AliasOut(
                    alias_name=alias.alias_name,
                    alias_type=alias.alias_type,
                ))
                # Collect "Other strain feature name" aliases with their strain info
                if alias.alias_type == 'Other strain feature name':
                    # Look up strain name from dbxref table (matches Perl behavior)
                    # The strain name is stored in dbxref.description where:
                    # source = 'Orthologous genes in Candida species', dbxref_type = 'Gene ID'
                    strain_dbxref = (
                        db.query(Dbxref)
                        .filter(
                            Dbxref.source == 'Orthologous genes in Candida species',
                            Dbxref.dbxref_type == 'Gene ID',
                            Dbxref.dbxref_id == alias.alias_name,
                        )
                        .first()
                    )
                    strain_name = None
                    if strain_dbxref and strain_dbxref.description:
                        strain_name = strain_dbxref.description
                    other_strain_names.append(OtherStrainNameOut(
                        alias_name=alias.alias_name,
                        strain_name=strain_name,
                    ))

        # Get external links from web_display table (like Perl code)
        # Use ORM relationships to iterate through URLs and find labels from web_display
        external_links = []

        # Links via feat_url relationship (substitution_value = 'FEATURE')
        for fu in f.feat_url:
            url = fu.url
            if not url or url.substitution_value != 'FEATURE':
                continue

            # Find web_display entry for this URL with 'Locus' page and 'External Links' location
            for wd in url.web_display:
                if wd.web_page_name == 'Locus' and wd.label_location == 'External Links':
                    url_str = url.url
                    # Substitute feature name in URL
                    if url_str:
                        url_str = url_str.replace('_SUBSTITUTE_THIS_', f.feature_name)
                    external_links.append(ExternalLinkOut(
                        label=wd.label_name,
                        url=url_str,
                        source=url.source,
                        url_type=url.url_type,
                    ))
                    break  # Only need one web_display entry per URL

        # Links via dbxref_url (for DBXREF substitution)
        dbxref_url_links = (
            db.query(
                WebDisplay.label_name,
                Url.url,
                Url.source,
                Url.url_type,
                Dbxref.dbxref_id,
            )
            .select_from(DbxrefUrl)
            .join(Dbxref, DbxrefUrl.dbxref_no == Dbxref.dbxref_no)
            .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
            .join(Url, DbxrefUrl.url_no == Url.url_no)
            .join(WebDisplay, Url.url_no == WebDisplay.url_no)
            .filter(
                DbxrefFeat.feature_no == f.feature_no,
                Url.substitution_value == 'DBXREF',
                WebDisplay.web_page_name == 'Locus',
                WebDisplay.label_location == 'External Links',
            )
            .all()
        )

        for label_name, url_str, source, url_type, dbxref_id in dbxref_url_links:
            # Substitute dbxref_id in URL
            if url_str and dbxref_id:
                url_str = url_str.replace('_SUBSTITUTE_THIS_', dbxref_id)
            external_links.append(ExternalLinkOut(
                label=label_name,
                url=url_str,
                source=source,
                url_type=url_type,
            ))

        # Sort by label name
        external_links.sort(key=lambda x: x.label or '')

        # Get Additional Info links (similar to external links but with label_location='Additional Info')
        additional_info_links = []
        for fu in f.feat_url:
            url = fu.url
            if not url or url.substitution_value != 'FEATURE':
                continue
            # Find web_display entry for this URL with 'Locus' page and 'Additional Info' location
            for wd in url.web_display:
                if wd.web_page_name == 'Locus' and wd.label_location == 'Additional Info':
                    url_str = url.url
                    if url_str:
                        url_str = url_str.replace('_SUBSTITUTE_THIS_', f.feature_name)
                    additional_info_links.append(ExternalLinkOut(
                        label=wd.label_name,
                        url=url_str,
                        source=url.source,
                        url_type=url.url_type,
                    ))
                    break
        additional_info_links.sort(key=lambda x: x.label or '')

        # Collect all references cited on this page from REF_LINK table and paragraph text
        # Reference index maps dbxref_id -> index number (1, 2, 3, ...)
        ref_index = {}
        next_ref_index = 1

        def add_refs_from_ref_link(tab_name: str, col_name: str, primary_key: int):
            """Query REF_LINK table and add references to ref_index, return list of dbxref_ids"""
            nonlocal next_ref_index
            ref_links = (
                db.query(RefLink, Reference)
                .join(Reference, RefLink.reference_no == Reference.reference_no)
                .filter(
                    func.upper(RefLink.tab_name) == tab_name.upper(),
                    func.upper(RefLink.col_name) == col_name.upper(),
                    RefLink.primary_key == primary_key,
                )
                .all()
            )
            dbxref_ids = []
            for rl, ref in ref_links:
                # Skip "information without a citation" references
                if ref.citation and 'information without a citation' in ref.citation.lower():
                    continue
                if ref.dbxref_id not in ref_index:
                    ref_index[ref.dbxref_id] = next_ref_index
                    next_ref_index += 1
                dbxref_ids.append(ref.dbxref_id)
            return dbxref_ids

        def format_ref_superscript(dbxref_ids: list, use_parentheses: bool = False) -> str:
            """Format reference indices as superscript HTML with commas between indices"""
            if not dbxref_ids:
                return ""
            indices = sorted(set(ref_index.get(d) for d in dbxref_ids if d in ref_index))
            if not indices:
                return ""
            # Create comma-separated list of linked indices
            links = [f'<a href="#ref{idx}" class="ref-link">{idx}</a>' for idx in indices]
            indices_str = ', '.join(links)
            if use_parentheses:
                return f'({indices_str})'
            return f'<sup>{indices_str}</sup>'

        # Get references for gene_name (Standard Name)
        gene_name_refs = []
        gene_name_with_refs = f.gene_name or ""
        if f.gene_name:
            gene_name_refs = add_refs_from_ref_link('FEATURE', 'GENE_NAME', f.feature_no)
            if gene_name_refs:
                ref_sup = format_ref_superscript(gene_name_refs)
                gene_name_with_refs = f'<i>{f.gene_name}</i>{ref_sup}'

        # Get references for aliases BEFORE headline/description
        # so that alias references get smaller index numbers
        aliases_with_refs = []
        for alias in aliases:
            # Find feat_alias_no for this alias
            feat_alias = (
                db.query(FeatAlias)
                .join(Alias, FeatAlias.alias_no == Alias.alias_no)
                .filter(
                    FeatAlias.feature_no == f.feature_no,
                    Alias.alias_name == alias.alias_name,
                )
                .first()
            )
            if feat_alias:
                alias_refs = add_refs_from_ref_link('FEAT_ALIAS', 'FEAT_ALIAS_NO', feat_alias.feat_alias_no)
                if alias_refs:
                    ref_sup = format_ref_superscript(alias_refs)
                    aliases_with_refs.append({
                        'alias_name': alias.alias_name,
                        'alias_type': alias.alias_type,
                        'alias_name_with_refs': f'{alias.alias_name}{ref_sup}',
                    })
                else:
                    aliases_with_refs.append({
                        'alias_name': alias.alias_name,
                        'alias_type': alias.alias_type,
                        'alias_name_with_refs': alias.alias_name,
                    })
            else:
                aliases_with_refs.append({
                    'alias_name': alias.alias_name,
                    'alias_type': alias.alias_type,
                    'alias_name_with_refs': alias.alias_name,
                })

        # Get references for headline (Description) - AFTER aliases
        headline_refs = []
        headline_with_refs = f.headline or ""
        if f.headline:
            headline_refs = add_refs_from_ref_link('FEATURE', 'HEADLINE', f.feature_no)
            if headline_refs:
                # Use parentheses format for description references
                ref_str = format_ref_superscript(headline_refs, use_parentheses=True)
                headline_with_refs = f'{f.headline} {ref_str}'

        # Get references for name_description
        name_desc_refs = []
        name_description_with_refs = f.name_description or ""
        if f.name_description:
            name_desc_refs = add_refs_from_ref_link('FEATURE', 'NAME_DESCRIPTION', f.feature_no)
            if name_desc_refs:
                ref_sup = format_ref_superscript(name_desc_refs)
                name_description_with_refs = f'{f.name_description}{ref_sup}'

        # Get summary notes (paragraphs) and extract references from paragraph text
        summary_notes = []
        summary_notes_last_updated = None
        all_paragraph_text = ""
        for fp in sorted(f.feat_para, key=lambda x: x.paragraph_order):
            para = fp.paragraph
            if para:
                all_paragraph_text += para.paragraph_text + " "
                # Track the most recent update date
                if summary_notes_last_updated is None or para.date_edited > summary_notes_last_updated:
                    summary_notes_last_updated = para.date_edited

        # Extract reference IDs from summary notes
        # Reference tags look like: <reference:CAL0000001>
        ref_pattern = re.compile(r'<reference:(CA[A-Z][0-9]+)>')
        ref_matches = ref_pattern.findall(all_paragraph_text)
        for ref_id in ref_matches:
            if ref_id not in ref_index:
                ref_index[ref_id] = next_ref_index
                next_ref_index += 1

        # Now process paragraph text to replace reference tags with numbered links
        for fp in sorted(f.feat_para, key=lambda x: x.paragraph_order):
            para = fp.paragraph
            if para:
                processed_text = para.paragraph_text
                # Replace <reference:CGDID> with numbered link
                def replace_ref(match):
                    ref_id = match.group(1)
                    idx = ref_index.get(ref_id, 0)
                    if idx:
                        return f'<a href="#ref{idx}" class="ref-link">{idx}</a>'
                    return match.group(0)
                processed_text = ref_pattern.sub(replace_ref, processed_text)
                summary_notes.append(SummaryNoteOut(
                    paragraph_no=para.paragraph_no,
                    paragraph_text=processed_text,
                    paragraph_order=fp.paragraph_order,
                    date_edited=para.date_edited,
                ))

        # Fetch all reference details and build cited_references list
        cited_references = []
        if ref_index:
            all_ref_ids = list(ref_index.keys())
            refs = (
                db.query(Reference)
                .options(joinedload(Reference.ref_url).joinedload(RefUrl.url))
                .filter(Reference.dbxref_id.in_(all_ref_ids))
                .all()
            )
            ref_map = {r.dbxref_id: r for r in refs}
            # Sort by index number
            for ref_id in sorted(ref_index.keys(), key=lambda x: ref_index[x]):
                ref = ref_map.get(ref_id)
                if ref:
                    cited_references.append(ReferenceForLocus(
                        reference_no=ref.reference_no,
                        pubmed=ref.pubmed,
                        dbxref_id=ref.dbxref_id,
                        citation=ref.citation,
                        title=ref.title,
                        year=ref.year,
                        links=_build_citation_links_for_locus(ref, ref.ref_url),
                    ))

        # Build literature guide URL
        literature_guide_url = f"http://www.candidagenome.org/cgi-bin/litGuide.pl?dbid={f.dbxref_id}"

        # Get Assembly 21 identifier (if this is Assembly 22, find the Assembly 21 child)
        assembly_21_identifier = None
        a21_rel = (
            db.query(FeatRelationship)
            .filter(
                FeatRelationship.parent_feature_no == f.feature_no,
                FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
            )
            .first()
        )
        if a21_rel:
            a21_feature = (
                db.query(Feature)
                .filter(Feature.feature_no == a21_rel.child_feature_no)
                .first()
            )
            if a21_feature and a21_feature.feature_name != f.feature_name:
                assembly_21_identifier = a21_feature.feature_name

        # Get feature qualifier from FEAT_PROPERTY
        feature_qualifier = None
        qualifier_row = (
            db.query(FeatProperty.property_value)
            .filter(
                FeatProperty.feature_no == f.feature_no,
                FeatProperty.property_type == 'feature_qualifier',
            )
            .first()
        )
        if qualifier_row:
            feature_qualifier = qualifier_row[0]

        # Get alleles for this locus
        alleles = []
        allele_relationships = (
            db.query(FeatRelationship)
            .filter(
                FeatRelationship.parent_feature_no == f.feature_no,
                FeatRelationship.relationship_type == 'allele',
            )
            .all()
        )
        for ar in allele_relationships:
            allele_feature = (
                db.query(Feature)
                .filter(
                    Feature.feature_no == ar.child_feature_no,
                    func.lower(Feature.feature_type) == 'allele',
                )
                .first()
            )
            if allele_feature:
                alleles.append(AlleleOut(
                    feature_no=allele_feature.feature_no,
                    feature_name=allele_feature.feature_name,
                    gene_name=allele_feature.gene_name,
                    dbxref_id=allele_feature.dbxref_id,
                ))

        # Get Candida orthologs (internal CGD species via CGOB method)
        candida_orthologs = []
        for fh in f.feat_homology:
            hg = fh.homology_group
            if hg and hg.homology_group_type == 'ortholog' and hg.method == 'CGOB':
                # Get other features in same homology group
                other_members = (
                    db.query(FeatHomology)
                    .filter(
                        FeatHomology.homology_group_no == hg.homology_group_no,
                        FeatHomology.feature_no != f.feature_no,
                    )
                    .all()
                )
                for om in other_members:
                    other_feat = (
                        db.query(Feature)
                        .options(joinedload(Feature.organism))
                        .filter(Feature.feature_no == om.feature_no)
                        .first()
                    )
                    if other_feat:
                        other_org_name, _ = _get_organism_info(other_feat)
                        candida_orthologs.append(CandidaOrthologOut(
                            feature_name=other_feat.feature_name,
                            gene_name=other_feat.gene_name,
                            organism_name=other_org_name,
                            dbxref_id=other_feat.dbxref_id,
                        ))

        # Get external orthologs (non-CGD species)
        external_orthologs = []
        dbxref_feats = (
            db.query(DbxrefFeat)
            .filter(DbxrefFeat.feature_no == f.feature_no)
            .all()
        )
        for df in dbxref_feats:
            dbxref = (
                db.query(Dbxref)
                .filter(Dbxref.dbxref_no == df.dbxref_no)
                .first()
            )
            # Only include the 4 allowed non-CGD ortholog sources
            if dbxref and dbxref.source in NON_CGD_ORTHOLOG_SOURCES:
                # Get URL for this dbxref if available
                dbxref_url_row = (
                    db.query(DbxrefUrl)
                    .join(Url)
                    .filter(DbxrefUrl.dbxref_no == dbxref.dbxref_no)
                    .first()
                )
                url_str = None
                if dbxref_url_row:
                    url_obj = (
                        db.query(Url)
                        .filter(Url.url_no == dbxref_url_row.url_no)
                        .first()
                    )
                    if url_obj and url_obj.url:
                        url_str = url_obj.url.replace('_SUBSTITUTE_THIS_', dbxref.dbxref_id or '')

                external_orthologs.append(ExternalOrthologOut(
                    dbxref_id=dbxref.dbxref_id,
                    description=dbxref.description,
                    source=dbxref.source,
                    url=url_str,
                    species_name=NON_CGD_ORTHOLOG_SOURCES.get(dbxref.source),
                ))

        # Build ortholog cluster URL from SGD ortholog (for CGOB viewer)
        # Use gene name (description) instead of SGD ID (dbxref_id)
        ortholog_cluster_url = None
        sgd_ortholog = next(
            (eo for eo in external_orthologs if eo.source == 'SGD'),
            None
        )
        if sgd_ortholog:
            # Prefer description (gene name like "ACT1") over dbxref_id (SGD ID like "S000001855")
            gene_name = sgd_ortholog.description or sgd_ortholog.dbxref_id
            if gene_name:
                ortholog_cluster_url = f"http://cgob3.ucd.ie/cgob.pl?gene={gene_name}"

        # Get CUG codons by counting CTG in CDS sequence
        # Only for organisms using translation table 12 (CTG clade)
        cug_codons = None
        use_table_12 = any(
            org_substr in organism_name
            for org_substr in TRANSLATION_TABLE_12_ORGANISMS
        )

        # Only compute CUG for ORFs that are not deleted/unmapped
        if (
            use_table_12
            and f.feature_type
            and f.feature_type.upper() == 'ORF'
            and feature_qualifier
            and 'deleted' not in feature_qualifier.lower()
            and 'not physically mapped' not in feature_qualifier.lower()
        ):
            primary_cds = _get_cds_sequence(db, f.feature_no)
            if primary_cds:
                cug_codons = _count_cug_codons(primary_cds)

        # Compute allelic variation if alleles exist
        allelic_variation = None
        if alleles and use_table_12 and f.feature_type and f.feature_type.upper() == 'ORF':
            primary_cds = _get_cds_sequence(db, f.feature_no)
            if primary_cds and len(alleles) > 0:
                # Get the first allele's CDS for comparison
                first_allele = alleles[0]
                allele_cds = _get_cds_sequence(db, first_allele.feature_no)
                if allele_cds:
                    allelic_variation = _check_allelic_variation(
                        primary_cds,
                        allele_cds,
                        f.feature_name,
                        first_allele.feature_name,
                        use_table_12=use_table_12
                    )

        # Convert aliases_with_refs to AliasWithRefsOut objects
        aliases_with_refs_out = [
            AliasWithRefsOut(
                alias_name=a['alias_name'],
                alias_type=a['alias_type'],
                alias_name_with_refs=a['alias_name_with_refs'],
            )
            for a in aliases_with_refs
        ]

        feature_out = FeatureOut(
            feature_no=f.feature_no,
            organism_no=f.organism_no,
            taxon_id=taxon_id,
            feature_name=f.feature_name,
            dbxref_id=f.dbxref_id,
            feature_type=f.feature_type,
            source=f.source,
            date_created=f.date_created,
            created_by=f.created_by,
            gene_name=f.gene_name,
            name_description=f.name_description,
            headline=f.headline,
            aliases=aliases,
            external_links=external_links,
            additional_info_links=additional_info_links,
            summary_notes=summary_notes,
            summary_notes_last_updated=summary_notes_last_updated,
            cited_references=cited_references,
            literature_guide_url=literature_guide_url,
            gene_name_with_refs=gene_name_with_refs if gene_name_with_refs else None,
            headline_with_refs=headline_with_refs if headline_with_refs else None,
            name_description_with_refs=name_description_with_refs if name_description_with_refs else None,
            aliases_with_refs=aliases_with_refs_out,
            assembly_21_identifier=assembly_21_identifier,
            feature_qualifier=feature_qualifier,
            alleles=alleles,
            other_strain_names=other_strain_names,
            candida_orthologs=candida_orthologs,
            external_orthologs=external_orthologs,
            ortholog_cluster_url=ortholog_cluster_url,
            cug_codons=cug_codons,
            allelic_variation=allelic_variation,
        )
        out[organism_name] = feature_out

    return LocusByOrganismResponse(results=out)


def get_locus_go_details(db: Session, name: str) -> GODetailsResponse:
    """
    Query GO annotations for each feature matching the locus name,
    grouped by organism.
    """
    from cgd.models.models import GoQualifier, GorefDbxref

    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.go_annotation).joinedload(GoAnnotation.go),
            joinedload(Feature.go_annotation)
                .joinedload(GoAnnotation.go_ref)
                .joinedload(GoRef.reference),
            joinedload(Feature.go_annotation)
                .joinedload(GoAnnotation.go_ref)
                .joinedload(GoRef.go_qualifier),
            joinedload(Feature.go_annotation)
                .joinedload(GoAnnotation.go_ref)
                .joinedload(GoRef.goref_dbxref)
                .joinedload(GorefDbxref.dbxref),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    # Source to species mapping for "with" display
    source_to_species = {
        'SGD': 'S. cerevisiae',
        'POMBASE': 'S. pombe',
        'AspGD': 'A. nidulans',
        'CGD': 'C. albicans',
        'BROAD_NEUROSPORA': 'N. crassa',
    }

    out: dict[str, GODetailsForOrganism] = {}

    # First pass: collect all reference_nos from all GO annotations
    all_ref_nos = set()
    for f in features:
        for ga in f.go_annotation:
            for gr in ga.go_ref:
                if gr.reference:
                    all_ref_nos.add(gr.reference.reference_no)

    # Load ref_urls for all references in one query
    ref_url_map: dict[int, list] = {}
    if all_ref_nos:
        ref_url_query = (
            db.query(RefUrl)
            .options(joinedload(RefUrl.url))
            .filter(RefUrl.reference_no.in_(list(all_ref_nos)))
            .all()
        )
        for ref_url in ref_url_query:
            if ref_url.reference_no not in ref_url_map:
                ref_url_map[ref_url.reference_no] = []
            ref_url_map[ref_url.reference_no].append(ref_url)

    # Second pass: build annotations with links
    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        annotations = []
        max_last_reviewed = None  # Track max date_last_reviewed for manually curated annotations

        for ga in f.go_annotation:
            go = ga.go
            if go is None:
                continue

            # Format GOID as GO:XXXXXXX
            goid_str = f"GO:{go.goid:07d}" if isinstance(go.goid, int) else str(go.goid)

            term = GOTerm(
                goid=goid_str,
                display_name=go.go_term,
                aspect=go.go_aspect,
                link=f"/go/{goid_str}",
            )

            # Collect qualifiers and with_from from all go_refs
            qualifiers = set()
            with_from_parts = []

            for gr in ga.go_ref:
                # Get qualifiers
                for gq in gr.go_qualifier:
                    if gq.qualifier:
                        qualifiers.add(gq.qualifier)

                # Get "with" supporting evidence
                for grd in gr.goref_dbxref:
                    if grd.support_type and grd.support_type.upper() == 'WITH':
                        dbx = grd.dbxref
                        if dbx:
                            source = dbx.source
                            display_name = dbx.description or dbx.dbxref_id

                            # For CGD internal references, look up the feature to
                            # get gene_name/feature_name instead of dbxref_id
                            if source == 'CGD' and dbx.dbxref_id:
                                feat = (
                                    db.query(Feature)
                                    .filter(Feature.dbxref_id == dbx.dbxref_id)
                                    .first()
                                )
                                if feat:
                                    display_name = feat.gene_name or feat.feature_name

                            species = source_to_species.get(source, source)
                            with_from_parts.append(f"{species}: {display_name}")

            # Build with_from string
            with_from_str = None
            if with_from_parts:
                with_from_str = ", ".join(sorted(set(with_from_parts)))

            evidence = GOEvidence(
                code=ga.go_evidence,
                with_from=with_from_str,
            )

            # Build qualifier string (NOT takes precedence)
            qualifier_str = None
            if qualifiers:
                if 'NOT' in qualifiers:
                    qualifier_str = 'NOT'
                else:
                    qualifier_str = ', '.join(sorted(qualifiers))

            # Get references from go_ref relationship with full citation data and links
            references = []
            for gr in ga.go_ref:
                ref = gr.reference
                if ref:
                    ref_urls = ref_url_map.get(ref.reference_no, [])
                    references.append(GORefForAnnotation(
                        reference_no=ref.reference_no,
                        pubmed=ref.pubmed,
                        dbxref_id=ref.dbxref_id,
                        citation=ref.citation,
                        journal_name=ref.journal.full_name if ref.journal else None,
                        year=ref.year,
                        links=_build_citation_links_for_go(ref, ref_urls),
                    ))

            # Format date_created
            date_created_str = None
            if ga.date_created:
                date_created_str = ga.date_created.strftime('%Y-%m-%d')

            # Track max date_last_reviewed for manually curated annotations
            if ga.annotation_type and ga.annotation_type.lower() == 'manually curated':
                if ga.date_last_reviewed:
                    if max_last_reviewed is None or ga.date_last_reviewed > max_last_reviewed:
                        max_last_reviewed = ga.date_last_reviewed

            annotations.append(GOAnnotationOut(
                term=term,
                evidence=evidence,
                references=references,
                qualifier=qualifier_str,
                annotation_type=ga.annotation_type,
                source=ga.source,
                date_created=date_created_str,
            ))

        # Format last_reviewed_date
        last_reviewed_str = None
        if max_last_reviewed:
            last_reviewed_str = max_last_reviewed.strftime('%Y-%m-%d')

        out[organism_name] = GODetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            annotations=annotations,
            last_reviewed_date=last_reviewed_str,
        )

    return GODetailsResponse(results=out)


def get_locus_phenotype_details(db: Session, name: str) -> PhenotypeDetailsResponse:
    """
    Query phenotype annotations for each feature matching the locus name,
    grouped by organism.
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.pheno_annotation).joinedload(PhenoAnnotation.phenotype),
            joinedload(Feature.pheno_annotation).joinedload(PhenoAnnotation.experiment),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    out: dict[str, PhenotypeDetailsForOrganism] = {}

    # First pass: collect all pheno_annotation_nos to fetch references
    all_pheno_annotation_nos = []
    for f in features:
        for pa in f.pheno_annotation:
            if pa.phenotype:
                all_pheno_annotation_nos.append(pa.pheno_annotation_no)

    # Load all ref_links for phenotype annotations in one query
    all_ref_links = []
    if all_pheno_annotation_nos:
        all_ref_links = (
            db.query(RefLink)
            .options(joinedload(RefLink.reference).joinedload(Reference.journal))
            .filter(
                RefLink.tab_name == "PHENO_ANNOTATION",
                RefLink.primary_key.in_(all_pheno_annotation_nos),
            )
            .all()
        )

    # Collect all reference_nos and build ref_link map
    ref_link_map: dict[int, list] = {}  # pheno_annotation_no -> list of RefLink
    all_ref_nos = set()
    for rl in all_ref_links:
        if rl.reference:
            all_ref_nos.add(rl.reference.reference_no)
            if rl.primary_key not in ref_link_map:
                ref_link_map[rl.primary_key] = []
            ref_link_map[rl.primary_key].append(rl)

    # Load ref_urls for all references in one query
    ref_url_map: dict[int, list] = {}
    if all_ref_nos:
        ref_url_query = (
            db.query(RefUrl)
            .options(joinedload(RefUrl.url))
            .filter(RefUrl.reference_no.in_(list(all_ref_nos)))
            .all()
        )
        for ref_url in ref_url_query:
            if ref_url.reference_no not in ref_url_map:
                ref_url_map[ref_url.reference_no] = []
            ref_url_map[ref_url.reference_no].append(ref_url)

    # Second pass: build annotations with links
    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        annotations = []
        for pa in f.pheno_annotation:
            phenotype = pa.phenotype
            if phenotype is None:
                continue

            pheno_term = PhenotypeTerm(
                display_name=phenotype.observable,
                link=f"/phenotype/{phenotype.phenotype_no}",
            )

            experiment = pa.experiment
            experiment_comment = None
            strain = None
            alleles_list = []
            chemicals_list = []
            details_list = []

            if experiment:
                experiment_comment = experiment.experiment_comment
                # Get experiment properties via expt_exptprop junction table
                expt_props = (
                    db.query(ExptProperty)
                    .join(ExptExptprop, ExptExptprop.expt_property_no == ExptProperty.expt_property_no)
                    .filter(ExptExptprop.experiment_no == experiment.experiment_no)
                    .all()
                )
                for prop in expt_props:
                    prop_type = prop.property_type
                    if prop_type == 'strain_background':
                        strain = prop.property_value
                    elif prop_type == 'Allele':
                        alleles_list.append(ExperimentProperty(
                            property_type=prop_type,
                            property_value=prop.property_value,
                            property_description=prop.property_description,
                        ))
                    elif prop_type in ('Chemical_pending', 'chebi_ontology'):
                        chemicals_list.append(ExperimentProperty(
                            property_type=prop_type,
                            property_value=prop.property_value,
                            property_description=prop.property_description,
                        ))
                    elif prop_type in ('Condition', 'Details', 'Reporter', 'Numerical_value'):
                        details_list.append(ExperimentProperty(
                            property_type=prop_type,
                            property_value=prop.property_value,
                            property_description=prop.property_description,
                        ))

            # Use raw experiment_type and mutant_type values (matching Perl behavior)
            # Frontend handles grouping into "Classical Genetics" / "Large-scale Survey" categories
            raw_experiment_type = phenotype.experiment_type
            mutant_type = phenotype.mutant_type

            # Get references from pre-loaded ref_link_map with links
            pheno_references = []
            annotation_ref_links = ref_link_map.get(pa.pheno_annotation_no, [])
            for rl in annotation_ref_links:
                ref = rl.reference
                if ref:
                    ref_urls = ref_url_map.get(ref.reference_no, [])
                    pheno_references.append(PhenoRefForAnnotation(
                        reference_no=ref.reference_no,
                        pubmed=ref.pubmed,
                        dbxref_id=ref.dbxref_id,
                        citation=ref.citation,
                        journal_name=ref.journal.full_name if ref.journal else None,
                        year=ref.year,
                        links=_build_citation_links_for_phenotype(ref, ref_urls),
                    ))

            annotations.append(PhenotypeAnnotationOut(
                phenotype=pheno_term,
                qualifier=phenotype.qualifier,
                experiment_type=raw_experiment_type,
                experiment_comment=experiment_comment,
                mutant_type=mutant_type,
                strain=strain,
                alleles=alleles_list,
                chemicals=chemicals_list,
                details=details_list,
                references=pheno_references,
            ))

        out[organism_name] = PhenotypeDetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            annotations=annotations,
        )

    return PhenotypeDetailsResponse(results=out)


def get_locus_interaction_details(db: Session, name: str) -> InteractionDetailsResponse:
    """
    Query interaction data for each feature matching the locus name,
    grouped by organism. Excludes genetic interactions (those in interact_pheno).
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_interact).joinedload(FeatInteract.interaction),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    out: dict[str, InteractionDetailsForOrganism] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        interactions = []
        seen_interactions = set()

        for fi in f.feat_interact:
            interaction = fi.interaction
            if interaction is None:
                continue

            # Skip if already processed (dedup)
            if interaction.interaction_no in seen_interactions:
                continue
            seen_interactions.add(interaction.interaction_no)

            # Skip genetic interactions (those linked to phenotypes)
            if interaction.interact_pheno:
                continue

            # Get all interactors for this interaction
            interactors = []
            for other_fi in interaction.feat_interact:
                other_feat = other_fi.feature
                if other_feat and other_feat.feature_no != f.feature_no:
                    interactors.append(InteractorOut(
                        feature_name=other_feat.feature_name,
                        gene_name=other_feat.gene_name,
                        action=other_fi.action,
                    ))

            # Get references via ref_link table
            references = []
            ref_links = (
                db.query(RefLink)
                .filter(
                    RefLink.tab_name == "INTERACTION",
                    RefLink.primary_key == interaction.interaction_no,
                )
                .all()
            )
            for rl in ref_links:
                ref = rl.reference
                if ref and ref.pubmed:
                    references.append(f"PMID:{ref.pubmed}")
                elif ref:
                    references.append(ref.dbxref_id)

            interactions.append(InteractionOut(
                interaction_no=interaction.interaction_no,
                experiment_type=interaction.experiment_type,
                description=interaction.description,
                source=interaction.source,
                interactors=interactors,
                references=references,
            ))

        out[organism_name] = InteractionDetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            interactions=interactions,
        )

    return InteractionDetailsResponse(results=out)


def get_locus_protein_details(db: Session, name: str) -> ProteinDetailsResponse:
    """
    Query protein information for each feature matching the locus name,
    grouped by organism.

    Returns data matching the Perl protein page format:
    - Stanford Name (gene_name)
    - Systematic Name (feature_name)
    - Alias Names
    - Description (headline)
    - Experimental Observations
    - Structural Information
    - Conserved Domains
    - Sequence Detail
    - Homologs
    - External Sequence Database
    - References Cited on This Page
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.protein_info).joinedload(ProteinInfo.protein_detail),
            joinedload(Feature.feat_alias).joinedload(FeatAlias.alias),
            joinedload(Feature.feat_url).joinedload(FeatUrl.url).joinedload(Url.web_display),
            joinedload(Feature.feat_homology).joinedload(FeatHomology.homology_group),
            joinedload(Feature.seq),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    out: dict[str, ProteinDetailsForOrganism] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        # --- Reference tracking for this feature ---
        ref_index: dict[str, int] = {}  # dbxref_id -> reference number (1-indexed)
        next_ref_index = 1

        def add_refs_from_ref_link(tab_name: str, col_name: str, primary_key: int) -> list[str]:
            """Query RefLink table and add references to ref_index. Returns list of dbxref_ids."""
            nonlocal next_ref_index
            refs = (
                db.query(Reference)
                .join(RefLink, RefLink.reference_no == Reference.reference_no)
                .filter(
                    RefLink.tab_name == tab_name,
                    RefLink.col_name == col_name,
                    RefLink.primary_key == primary_key,
                )
                .all()
            )
            dbxref_ids = []
            for ref in refs:
                if ref.dbxref_id not in ref_index:
                    ref_index[ref.dbxref_id] = next_ref_index
                    next_ref_index += 1
                dbxref_ids.append(ref.dbxref_id)
            return dbxref_ids

        def format_ref_superscript(dbxref_ids: list[str], use_parentheses: bool = False) -> str:
            """Format reference indices as HTML superscript links."""
            if not dbxref_ids:
                return ""
            indices = sorted(set(ref_index.get(d) for d in dbxref_ids if d in ref_index))
            if not indices:
                return ""
            links = [f'<a href="#ref{idx}" class="ref-link">{idx}</a>' for idx in indices]
            indices_str = ', '.join(links)
            if use_parentheses:
                return f'({indices_str})'
            return f'<sup>{indices_str}</sup>'

        # Section 1 & 2: Stanford Name and Systematic Name (with protein format)
        stanford_name = f.gene_name
        systematic_name = f.feature_name
        protein_standard_name = _gene_name_to_protein_name(stanford_name) if stanford_name else None
        protein_systematic_name = _systematic_name_to_protein_name(systematic_name) if systematic_name else None

        # Get references for protein standard name (gene_name)
        protein_standard_name_with_refs = protein_standard_name or ""
        if stanford_name:
            gene_name_refs = add_refs_from_ref_link('FEATURE', 'GENE_NAME', f.feature_no)
            if gene_name_refs and protein_standard_name:
                ref_sup = format_ref_superscript(gene_name_refs)
                protein_standard_name_with_refs = f'{protein_standard_name}{ref_sup}'

        # Section 3: Allele Names - get from FeatRelationship (like Summary tab)
        allele_names = []
        allele_relationships = (
            db.query(FeatRelationship)
            .filter(
                FeatRelationship.parent_feature_no == f.feature_no,
                FeatRelationship.relationship_type == 'allele',
            )
            .all()
        )
        for ar in allele_relationships:
            allele_feature = (
                db.query(Feature)
                .filter(
                    Feature.feature_no == ar.child_feature_no,
                    func.lower(Feature.feature_type) == 'allele',
                )
                .first()
            )
            if allele_feature:
                # Convert to protein format (e.g., C1_13700W_B -> C1_13700wp_b)
                allele_name = allele_feature.feature_name
                protein_allele = _systematic_name_to_protein_name(allele_name)

                # Get references for this allele (from FEATURE table, GENE_NAME column)
                allele_refs = add_refs_from_ref_link('FEATURE', 'GENE_NAME', allele_feature.feature_no)
                if allele_refs:
                    ref_sup = format_ref_superscript(allele_refs)
                    allele_with_refs = f'{protein_allele}{ref_sup}'
                else:
                    allele_with_refs = protein_allele

                allele_names.append(ProteinAlleleNameOut(
                    allele_name=allele_name,
                    protein_allele_name=protein_allele,
                    allele_name_with_refs=allele_with_refs,
                ))

        # Section 4: Description
        description = f.headline
        description_with_refs = description or ""
        if description:
            headline_refs = add_refs_from_ref_link('FEATURE', 'HEADLINE', f.feature_no)
            if headline_refs:
                ref_str = format_ref_superscript(headline_refs, use_parentheses=True)
                description_with_refs = f'{description} {ref_str}'

        # Section 4b: Name Description
        name_description = f.name_description
        name_description_with_refs = name_description or ""
        if name_description:
            name_desc_refs = add_refs_from_ref_link('FEATURE', 'NAME_DESCRIPTION', f.feature_no)
            if name_desc_refs:
                ref_str = format_ref_superscript(name_desc_refs)
                name_description_with_refs = f'{name_description}{ref_str}'

        # Section 5: Experimental Observations (from protein_detail with specific groups)
        experimental_observations = []

        # Section 6 & 7: Structural Information and Conserved Domains
        structural_info = []
        conserved_domains = []
        protein_info = None

        if f.protein_info:
            pi = f.protein_info[0]

            # Build amino acid composition dictionary
            amino_acids = {
                "ala": pi.ala, "arg": pi.arg, "asn": pi.asn, "asp": pi.asp,
                "cys": pi.cys, "gln": pi.gln, "glu": pi.glu, "gly": pi.gly,
                "his": pi.his, "ile": pi.ile, "leu": pi.leu, "lys": pi.lys,
                "met": pi.met, "phe": pi.phe, "pro": pi.pro, "ser": pi.ser,
                "thr": pi.thr, "trp": pi.trp, "tyr": pi.tyr, "val": pi.val,
            }
            amino_acids = {k: v for k, v in amino_acids.items() if v is not None}

            protein_info = ProteinInfoOut(
                protein_length=pi.protein_length,
                molecular_weight=pi.molecular_weight,
                pi=float(pi.pi) if pi.pi is not None else None,
                cai=float(pi.cai) if pi.cai is not None else None,
                codon_bias=float(pi.codon_bias) if pi.codon_bias is not None else None,
                fop_score=float(pi.fop_score) if pi.fop_score is not None else None,
                n_term_seq=pi.n_term_seq,
                c_term_seq=pi.c_term_seq,
                gravy_score=float(pi.gravy_score) if pi.gravy_score is not None else None,
                aromaticity_score=float(pi.aromaticity_score) if pi.aromaticity_score is not None else None,
                amino_acids=amino_acids if amino_acids else None,
            )

            # Process protein_detail for domains and structural info
            for pd in pi.protein_detail:
                group = pd.protein_detail_group or ''
                group_lower = group.lower()

                # Conserved domains typically have group containing 'domain' or specific types
                if 'domain' in group_lower or group_lower in ('pfam', 'smart', 'interpro', 'prosite'):
                    conserved_domains.append(ConservedDomainOut(
                        domain_name=pd.protein_detail_value,
                        domain_type=pd.protein_detail_type,
                        domain_group=pd.protein_detail_group,
                        start_coord=pd.start_coord,
                        stop_coord=pd.stop_coord,
                        interpro_id=pd.interpro_dbxref_id,
                        member_db_id=pd.member_dbxref_id,
                    ))
                else:
                    # Other protein details go to structural info
                    structural_info.append(StructuralInfoOut(
                        info_type=pd.protein_detail_type,
                        info_value=pd.protein_detail_value,
                        info_unit=pd.protein_detail_unit,
                        start_coord=pd.start_coord,
                        stop_coord=pd.stop_coord,
                    ))

        # Section 8: Sequence Detail
        sequence_detail = None
        protein_sequence = None
        cds_length = None

        for seq in f.seq:
            if seq.is_seq_current == 'Y':
                seq_type_upper = (seq.seq_type or '').upper()
                if seq_type_upper == 'PROTEIN':
                    protein_sequence = seq.residues
                elif seq_type_upper == 'CDS':
                    cds_length = seq.seq_length

        if protein_info or protein_sequence:
            # Generate GCG format sequence
            protein_seq_gcg = None
            if protein_sequence:
                seq_name = protein_standard_name or protein_systematic_name or systematic_name
                seq_length = len(protein_sequence)
                protein_seq_gcg = _format_sequence_gcg(protein_sequence, seq_name, seq_length)

            sequence_detail = SequenceDetailOut(
                protein_length=protein_info.protein_length if protein_info else None,
                protein_sequence=protein_sequence,
                protein_sequence_gcg=protein_seq_gcg,
                n_term_seq=protein_info.n_term_seq if protein_info else None,
                c_term_seq=protein_info.c_term_seq if protein_info else None,
                cds_length=cds_length,
            )

        # Section 9: Homologs
        homologs = []
        seen_homologs = set()

        for fh in f.feat_homology:
            hg = fh.homology_group
            if hg is None:
                continue

            # Get internal (CGD) members via feat_homology
            for other_fh in hg.feat_homology:
                other_feat = other_fh.feature
                if other_feat and other_feat.feature_no != f.feature_no:
                    key = (other_feat.feature_no, 'internal')
                    if key in seen_homologs:
                        continue
                    seen_homologs.add(key)

                    other_org_name, _ = _get_organism_info(other_feat)
                    # Convert to protein name format
                    other_protein_name = _gene_name_to_protein_name(other_feat.gene_name) if other_feat.gene_name else None
                    homologs.append(ProteinHomologOut(
                        feature_name=other_feat.feature_name,
                        gene_name=other_feat.gene_name,
                        protein_name=other_protein_name,
                        organism_name=other_org_name,
                        dbxref_id=other_feat.dbxref_id,
                        source=hg.homology_group_type,
                    ))

        # Get external homologs (SGD, POMBASE, etc.)
        ext_homologs = (
            db.query(
                Dbxref.dbxref_id,
                Dbxref.description,
                Dbxref.source,
            )
            .select_from(DbxrefHomology)
            .join(HomologyGroup, DbxrefHomology.homology_group_no == HomologyGroup.homology_group_no)
            .join(FeatHomology, HomologyGroup.homology_group_no == FeatHomology.homology_group_no)
            .join(Dbxref, DbxrefHomology.dbxref_no == Dbxref.dbxref_no)
            .filter(FeatHomology.feature_no == f.feature_no)
            .all()
        )

        for dbxref_id, desc, source in ext_homologs:
            key = (dbxref_id, source)
            if key in seen_homologs:
                continue
            seen_homologs.add(key)

            species = NON_CGD_ORTHOLOG_SOURCES.get(source, source)
            homologs.append(ProteinHomologOut(
                feature_name=dbxref_id,
                gene_name=None,
                protein_name=None,
                organism_name=species,
                dbxref_id=dbxref_id,
                source=source,
            ))

        # BLAST URL for homologs
        blast_url = f"/cgi-bin/compute/blast-sgd.pl?protein={protein_standard_name or systematic_name}"

        # Section 10: External Sequence Database links
        external_links = []

        # Links via feat_url relationship for protein page
        for fu in f.feat_url:
            url = fu.url
            if not url or url.substitution_value != 'FEATURE':
                continue

            for wd in url.web_display:
                if wd.web_page_name == 'protein' and wd.label_location == 'External Links':
                    url_str = url.url
                    if url_str:
                        url_str = url_str.replace('_SUBSTITUTE_THIS_', f.feature_name)
                    external_links.append(ProteinExternalLinkOut(
                        label=wd.label_name,
                        url=url_str,
                        source=url.source,
                        url_type=url.url_type,
                    ))
                    break

        # Links via dbxref_url (for DBXREF substitution) - protein page
        dbxref_url_links = (
            db.query(
                WebDisplay.label_name,
                Url.url,
                Url.source,
                Url.url_type,
                Dbxref.dbxref_id,
            )
            .select_from(DbxrefUrl)
            .join(Dbxref, DbxrefUrl.dbxref_no == Dbxref.dbxref_no)
            .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
            .join(Url, DbxrefUrl.url_no == Url.url_no)
            .join(WebDisplay, Url.url_no == WebDisplay.url_no)
            .filter(
                DbxrefFeat.feature_no == f.feature_no,
                Url.substitution_value == 'DBXREF',
                WebDisplay.web_page_name == 'protein',
                WebDisplay.label_location == 'External Links',
            )
            .all()
        )

        for label_name, url_str, source, url_type, dbxref_id in dbxref_url_links:
            if url_str:
                url_str = url_str.replace('_SUBSTITUTE_THIS_', dbxref_id)
            external_links.append(ProteinExternalLinkOut(
                label=label_name,
                url=url_str or '',
                source=source,
                url_type=url_type,
            ))

        external_links.sort(key=lambda x: x.label or '')

        # AlphaFold structure lookup
        # Look for UniProt ID in dbxref_feat and construct AlphaFold URL
        # Perl query: source = 'EBI' and dbxref_type in ('SwissProt', 'TrEMBL')
        alphafold_info = None
        uniprot_id = None

        # Query for UniProt/SwissProt dbxref (matching Perl get_uniprot_dbxref)
        uniprot_dbxref = (
            db.query(Dbxref.dbxref_id, Dbxref.dbxref_type)
            .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
            .filter(
                DbxrefFeat.feature_no == f.feature_no,
                Dbxref.source == 'EBI',
                Dbxref.dbxref_type.in_(['SwissProt', 'TrEMBL']),
            )
            .all()
        )

        # Prefer SwissProt over TrEMBL (like Perl does)
        for dbxref_id, dbxref_type in uniprot_dbxref:
            uniprot_id = dbxref_id
            if dbxref_type == 'SwissProt':
                break  # SwissProt is preferred

        if uniprot_id:
            alphafold_url = f"https://alphafold.ebi.ac.uk/entry/{uniprot_id}"
            alphafold_info = AlphaFoldInfo(
                uniprot_id=uniprot_id,
                alphafold_url=alphafold_url,
                structure_available=True,
            )

        # Section 11: References Cited on This Page
        # Build cited_references list ordered by ref_index (the order they were cited)
        cited_references = []
        if ref_index:
            # Sort by index number to maintain citation order
            sorted_refs = sorted(ref_index.items(), key=lambda x: x[1])
            for dbxref_id, idx in sorted_refs:
                ref = (
                    db.query(Reference)
                    .options(joinedload(Reference.ref_url).joinedload(RefUrl.url))
                    .filter(Reference.dbxref_id == dbxref_id)
                    .first()
                )
                if ref:
                    cited_references.append(ReferenceForProtein(
                        reference_no=ref.reference_no,
                        pubmed=ref.pubmed,
                        dbxref_id=ref.dbxref_id,
                        citation=ref.citation or '',
                        title=ref.title,
                        year=ref.year,
                        links=_build_citation_links_for_protein(ref, ref.ref_url),
                    ))

        # Literature guide URL
        literature_guide_url = f"/cgi-bin/reference/referenceTab.pl?locus={f.feature_name}"

        # PBrowse URL for domain visualization
        # URL structure: /jbrowse/index.html?data=cgd_data/{strain}_prot&loc={feature}:1..{len}&tracklist=0&nav=0&overview=0&tracks=...
        pbrowse_url = None
        protein_length = protein_info.protein_length if protein_info else None
        if protein_length and f.organism:
            strain_abbrev = f.organism.organism_abbrev
            # Build tracks list based on conserved domains present
            domain_order = ['Pfam', 'PANTHER', 'SUPERFAMILY', 'CATH', 'SMART',
                            'ProSiteProfiles', 'CDD', 'NCBIfam', 'PIRSF', 'Hamap', 'SFLD']
            motif_order = ['PRINTS', 'ProSitePatterns', 'SignalP']
            strux_order = ['TMHMM', 'Coils', 'MobiDBLite']

            # Collect domain types present in this protein
            domain_types_present = set()
            for cd in conserved_domains:
                if cd.domain_type:
                    domain_types_present.add(cd.domain_type)

            # Also check structural info for motifs and structural regions
            for si in structural_info:
                if si.info_type:
                    domain_types_present.add(si.info_type)

            # Build tracks string: Sequence, Protein, then add domain types present
            # Match Perl: tracks=Sequence%2CProtein%2C{domain_types}
            tracks = ['Sequence', 'Protein']
            for trk in domain_order + motif_order + strux_order:
                if trk in domain_types_present:
                    tracks.append(trk)

            tracks_str = '%2C'.join(tracks)  # URL-encoded comma
            # Use full CGD URL for JBrowse iframe - match Perl URL format exactly
            # Perl: pbrowseHome + '&loc=' + feature + ':1..' + len + '&tracklist=0&nav=0&overview=0&tracks=...'
            pbrowse_url = (
                f"http://www.candidagenome.org/jbrowse/index.html"
                f"?data=cgd_data/{strain_abbrev}_prot"
                f"&loc={f.feature_name}:1..{protein_length}"
                f"&tracklist=0&nav=0&overview=0&tracks={tracks_str}"
            )

        out[organism_name] = ProteinDetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            stanford_name=stanford_name,
            protein_standard_name=protein_standard_name,
            protein_standard_name_with_refs=protein_standard_name_with_refs if protein_standard_name_with_refs else None,
            systematic_name=systematic_name,
            protein_systematic_name=protein_systematic_name,
            allele_names=allele_names,
            description=description,
            description_with_refs=description_with_refs if description_with_refs else None,
            name_description=name_description,
            name_description_with_refs=name_description_with_refs if name_description_with_refs else None,
            experimental_observations=experimental_observations,
            structural_info=structural_info,
            protein_info=protein_info,
            alphafold_info=alphafold_info,
            conserved_domains=conserved_domains,
            sequence_detail=sequence_detail,
            homologs=homologs,
            blast_url=blast_url,
            external_links=external_links,
            cited_references=cited_references,
            literature_guide_url=literature_guide_url,
            pbrowse_url=pbrowse_url,
        )

    return ProteinDetailsResponse(results=out)


def _fetch_sgd_gene_info(systematic_name: str) -> tuple[Optional[str], Optional[str]]:
    """
    Fetch gene name and status from SGD API for a given systematic name.
    Returns (gene_name, status) tuple, or (None, None) on error.
    Uses urllib to avoid adding requests as a dependency.
    """
    try:
        url = f"https://www.yeastgenome.org/backend/locus/{systematic_name}"
        req = urllib.request.Request(url, headers={'User-Agent': 'CGD-Backend/1.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode('utf-8'))
                gene_name = data.get('gene_name') or data.get('display_name')
                status = data.get('qualifier')  # "Verified", "Uncharacterized", etc.
                return gene_name, status
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, Exception):
        pass
    return None, None


def _load_phylogenetic_tree(dbid: str) -> Optional[PhylogeneticTreeOut]:
    """
    Load phylogenetic tree data for a given locus.
    Tree files are stored in {CGD_DATA_DIR}/homology/alignments/{bucket}/{dbid}_tree_*.par
    where bucket = int(numeric_part_of_dbid / 100)
    Returns PhylogeneticTreeOut or None if no tree files exist.
    """
    # Extract numeric part from dbid (e.g., "13700" from "C1_13700W_A")
    # Pattern matches digits, ignoring leading zeros
    match = re.search(r'[^\d]*0*(\d+)', dbid)
    if not match:
        return None

    numeric_tag = int(match.group(1))
    bucket = numeric_tag // 100

    alignment_dir = Path(settings.cgd_data_dir) / "homology" / "alignments" / str(bucket)
    unrooted_tree_file = alignment_dir / f"{dbid}_tree_unrooted.par"
    rooted_tree_file = alignment_dir / f"{dbid}_tree_rooted.par"

    # Check if tree files exist
    if not unrooted_tree_file.exists():
        return None

    try:
        # Read the rooted tree (preferred) or unrooted tree
        tree_file = rooted_tree_file if rooted_tree_file.exists() else unrooted_tree_file
        newick_tree = tree_file.read_text().strip()

        # Parse basic tree statistics from Newick format
        # Count leaves (number of names before colons or commas)
        # This is a simple heuristic - leaves are text between ( or , and : or )
        leaf_count = newick_tree.count(',') + 1 if ',' in newick_tree else 1

        # Calculate approximate tree length by summing branch lengths
        # Branch lengths appear after : in Newick format
        branch_lengths = re.findall(r':([0-9.]+)', newick_tree)
        tree_length = sum(float(bl) for bl in branch_lengths) if branch_lengths else None

        # Build download links
        download_links = []
        if unrooted_tree_file.exists():
            download_links.append(DownloadLinkOut(
                label="Unrooted Tree (Newick format)",
                url=f"/cgi-bin/compute/get_tree_file.pl?dbid={dbid}&type=unrooted"
            ))
        if rooted_tree_file.exists():
            download_links.append(DownloadLinkOut(
                label="Rooted Tree (Newick format)",
                url=f"/cgi-bin/compute/get_tree_file.pl?dbid={dbid}&type=rooted"
            ))

        return PhylogeneticTreeOut(
            newick_tree=newick_tree,
            tree_length=round(tree_length, 4) if tree_length else None,
            leaf_count=leaf_count,
            method="SEMPHY",
            download_links=download_links,
        )

    except Exception:
        return None


def get_locus_homology_details(db: Session, name: str) -> HomologyDetailsResponse:
    """
    Query homology group data for each feature matching the locus name,
    grouped by organism.
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_homology)
                .joinedload(FeatHomology.homology_group)
                .joinedload(HomologyGroup.dbxref_homology)
                .joinedload(DbxrefHomology.dbxref),
            joinedload(Feature.feat_homology)
                .joinedload(FeatHomology.homology_group)
                .joinedload(HomologyGroup.feat_homology)
                .joinedload(FeatHomology.feature)
                .joinedload(Feature.organism),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    out: dict[str, HomologyDetailsForOrganism] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        homology_groups = []
        seen_groups = set()

        for fh in f.feat_homology:
            hg = fh.homology_group
            if hg is None:
                continue

            # Skip if already processed
            if hg.homology_group_no in seen_groups:
                continue
            seen_groups.add(hg.homology_group_no)

            # Get all members in this homology group
            members = []

            # Get internal (CGD) members via feat_homology
            for other_fh in hg.feat_homology:
                other_feat = other_fh.feature
                if other_feat and other_feat.feature_no != f.feature_no:
                    other_org_name, _ = _get_organism_info(other_feat)
                    members.append(HomologOut(
                        feature_name=other_feat.feature_name,
                        gene_name=other_feat.gene_name,
                        organism_name=other_org_name,
                        dbxref_id=other_feat.dbxref_id,
                    ))

            # Get external members via dbxref_homology
            for dh in hg.dbxref_homology:
                dbxref = dh.dbxref
                ext_org = "External"
                if dbxref:
                    ext_org = dbxref.source if hasattr(dbxref, "source") else "External"
                members.append(HomologOut(
                    feature_name=dh.name,
                    gene_name=None,
                    organism_name=ext_org,
                    dbxref_id=dbxref.dbxref_id if dbxref else dh.name,
                ))

            homology_groups.append(HomologyGroupOut(
                homology_group_type=hg.homology_group_type,
                method=hg.method,
                members=members,
            ))

        # Build Ortholog Cluster section (CGOB orthologs)
        ortholog_cluster = None
        for fh in f.feat_homology:
            hg = fh.homology_group
            if hg and hg.homology_group_type == 'ortholog' and hg.method == 'CGOB':
                orthologs = []

                # Helper to format sequence_id as "gene_name/feature_name"
                def format_seq_id(gene_name, feature_name):
                    if gene_name and gene_name != feature_name:
                        return f"{gene_name}/{feature_name}"
                    return feature_name

                # Get orf19 identifier for CGOB link (Assembly 19/21 identifier)
                # For Assembly 22 features, look up via feat_relationship
                orf19_id = f.feature_name  # Default to current feature name
                orf19_row = (
                    db.query(Feature.feature_name)
                    .join(
                        FeatRelationship,
                        Feature.feature_no == FeatRelationship.child_feature_no
                    )
                    .filter(
                        FeatRelationship.parent_feature_no == f.feature_no,
                        FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
                        FeatRelationship.rank == 3,
                    )
                    .first()
                )
                if orf19_row:
                    orf19_id = orf19_row[0]

                # Add query gene first
                query_status = None
                qualifier_row = (
                    db.query(FeatProperty.property_value)
                    .filter(
                        FeatProperty.feature_no == f.feature_no,
                        FeatProperty.property_type == 'feature_qualifier',
                    )
                    .first()
                )
                if qualifier_row:
                    query_status = qualifier_row[0].upper() if qualifier_row[0] else None

                orthologs.append(OrthologOut(
                    sequence_id=format_seq_id(f.gene_name, f.feature_name),
                    feature_name=f.feature_name,
                    organism_name=organism_name,
                    source='CGD',
                    status=query_status,
                    is_query=True,
                ))

                # Add other CGD orthologs (only from database strains, not "aliens")
                for other_fh in hg.feat_homology:
                    other_feat = other_fh.feature
                    if other_feat and other_feat.feature_no != f.feature_no:
                        other_org_name, _ = _get_organism_info(other_feat)
                        # Get status for this ortholog
                        other_status = None
                        other_qualifier = (
                            db.query(FeatProperty.property_value)
                            .filter(
                                FeatProperty.feature_no == other_feat.feature_no,
                                FeatProperty.property_type == 'feature_qualifier',
                            )
                            .first()
                        )
                        if other_qualifier:
                            other_status = other_qualifier[0].upper() if other_qualifier[0] else None

                        orthologs.append(OrthologOut(
                            sequence_id=format_seq_id(other_feat.gene_name, other_feat.feature_name),
                            feature_name=other_feat.feature_name,
                            organism_name=other_org_name,
                            source='CGD',
                            status=other_status,
                            is_query=False,
                        ))

                # Add external orthologs (SGD, EnsemblFungi) to the cluster table
                # Use the eager-loaded relationship from homology_group
                # dh.name stores the organism name (matches Perl behavior in CGOB.pm)
                # Order: CGD first (already added), then SGD, then EnsemblFungi

                # "Aliens" are excluded from the cluster table (from Perl CGOB.pm)
                # These are non-standard strains not in the main CGOB analysis
                alien_organisms = {
                    'Candida tenuis NRRL Y-1498',
                    'Pichia stipitis Pignal',
                    'Spathaspora passalidarum NRRL Y-27907',
                    'Candida metapsilosis',
                    'Candida orthopsilosis NEW ASSEMBLY',
                    'Candida tropicalis NEW ASSEMBLY',
                }

                # Collect SGD and EnsemblFungi orthologs separately for ordering
                sgd_orthologs = []
                ensembl_orthologs = []

                for dh in hg.dbxref_homology:
                    dbxref = dh.dbxref
                    if not dbxref:
                        continue

                    # dh.name stores the organism name (from Perl CGOB.pm SQL:
                    # SELECT d.dbxref_id, dh.name where dh.name is used as organism)
                    ext_org = dh.name or ''
                    ext_org = ext_org.strip()

                    # Skip "alien" organisms (matches Perl FormatHomolog.pm behavior)
                    if ext_org in alien_organisms:
                        continue

                    # Determine source based on organism name (matching Perl CGOB.pm)
                    # SGD = Saccharomyces cerevisiae, EnsemblFungi = non-CGD strains
                    ext_seq_id = dbxref.dbxref_id
                    ext_status = None

                    if 'Saccharomyces cerevisiae' in ext_org:
                        ext_source = 'SGD'
                        ext_url = f"https://www.yeastgenome.org/locus/{dbxref.dbxref_id}"
                        # Fetch gene name and status from SGD API
                        sgd_gene_name, sgd_status = _fetch_sgd_gene_info(dbxref.dbxref_id)
                        if sgd_gene_name:
                            ext_seq_id = f"{sgd_gene_name}/{dbxref.dbxref_id}"
                        if sgd_status:
                            ext_status = sgd_status.upper()
                        sgd_orthologs.append(OrthologOut(
                            sequence_id=ext_seq_id,
                            feature_name=dbxref.dbxref_id,
                            organism_name=ext_org,
                            source=ext_source,
                            status=ext_status,
                            is_query=False,
                            url=ext_url,
                        ))
                    else:
                        ext_source = 'EnsemblFungi'
                        ext_url = f"https://fungi.ensembl.org/id/{dbxref.dbxref_id}"
                        ensembl_orthologs.append(OrthologOut(
                            sequence_id=ext_seq_id,
                            feature_name=dbxref.dbxref_id,
                            organism_name=ext_org,
                            source=ext_source,
                            status=ext_status,
                            is_query=False,
                            url=ext_url,
                        ))

                # Add SGD orthologs first (after CGD), then EnsemblFungi
                orthologs.extend(sgd_orthologs)
                orthologs.extend(ensembl_orthologs)

                # Build CGOB cluster URL using orf19 identifier
                cluster_url = f"http://cgob3.ucd.ie/cgob.pl?gene={orf19_id}"
                cluster_id = hg.homology_group_id or f.feature_name

                download_links = [
                    DownloadLinkOut(
                        label="Proteins (multi-FASTA format)",
                        url=f"/cgi-bin/compute/get_homolog_seqs.pl?cluster={cluster_id}&type=protein"
                    ),
                    DownloadLinkOut(
                        label="Coding (multi-FASTA format)",
                        url=f"/cgi-bin/compute/get_homolog_seqs.pl?cluster={cluster_id}&type=cds"
                    ),
                    DownloadLinkOut(
                        label="Genomic (multi-FASTA format)",
                        url=f"/cgi-bin/compute/get_homolog_seqs.pl?cluster={cluster_id}&type=genomic"
                    ),
                    DownloadLinkOut(
                        label="Genomic +/- 1000 BP (multi-FASTA format)",
                        url=f"/cgi-bin/compute/get_homolog_seqs.pl?cluster={cluster_id}&type=genomic_extended"
                    ),
                ]

                ortholog_cluster = OrthologClusterOut(
                    cluster_name=hg.homology_group_id,
                    method=hg.method,
                    cluster_url=cluster_url,
                    download_links=download_links,
                    orthologs=orthologs,
                )
                break  # Only use first CGOB cluster

        # --- Best hits in CGD species (BLAST) ---
        best_hits_cgd = None
        cgd_organism_name = f.organism.common_name if f.organism else organism_name
        best_hit_type = f"best hit for {cgd_organism_name}"

        # Find best hit homology groups for this feature
        best_hit_by_species: dict[str, list[BestHitOut]] = {}
        for fh in f.feat_homology:
            hg = fh.homology_group
            if hg and hg.homology_group_type == best_hit_type and hg.method == 'BLAST':
                # Get all other features in this homology group
                for other_fh in hg.feat_homology:
                    other_feat = other_fh.feature
                    if other_feat and other_feat.feature_no != f.feature_no:
                        other_org_name, _ = _get_organism_info(other_feat)
                        display_name = other_feat.feature_name
                        if other_feat.gene_name and other_feat.gene_name != other_feat.feature_name:
                            display_name = f"{other_feat.gene_name}/{other_feat.feature_name}"

                        if other_org_name not in best_hit_by_species:
                            best_hit_by_species[other_org_name] = []

                        best_hit_by_species[other_org_name].append(BestHitOut(
                            feature_name=other_feat.feature_name,
                            gene_name=other_feat.gene_name,
                            display_name=display_name,
                            organism_name=other_org_name,
                            url=f"/locus/{other_feat.feature_name}",
                        ))

        if best_hit_by_species:
            best_hits_cgd = BestHitsInCGDOut(by_species=best_hit_by_species)

        # --- External orthologs and best hits (via dbxref_feat) ---
        # Source to species mapping
        source_to_species = {
            'SGD': 'S. cerevisiae',
            'SGD_BEST_HIT': 'S. cerevisiae',
            'POMBASE': 'S. pombe',
            'POMBASE_BEST_HIT': 'S. pombe',
            'AspGD': 'A. nidulans',
            'AspGD_BEST_HIT': 'A. nidulans',
            'BROAD_NEUROSPORA': 'N. crassa',
            'BROAD_NEUROSPORA_BEST_HIT': 'N. crassa',
            'dictyBase': 'D. discoideum',
            'MGD': 'M. musculus',
            'RGD': 'R. norvegicus',
        }

        # Source to URL template mapping
        source_to_url = {
            'SGD': 'https://www.yeastgenome.org/locus/{id}',
            'SGD_BEST_HIT': 'https://www.yeastgenome.org/locus/{id}',
            'POMBASE': 'https://www.pombase.org/gene/{id}',
            'POMBASE_BEST_HIT': 'https://www.pombase.org/gene/{id}',
            'AspGD': 'http://www.aspergillusgenome.org/cgi-bin/locus.pl?locus={id}',
            'AspGD_BEST_HIT': 'http://www.aspergillusgenome.org/cgi-bin/locus.pl?locus={id}',
            'BROAD_NEUROSPORA': 'https://fungidb.org/fungidb/app/record/gene/{id}',
            'BROAD_NEUROSPORA_BEST_HIT': 'https://fungidb.org/fungidb/app/record/gene/{id}',
            'dictyBase': 'http://dictybase.org/gene/{id}',
            'MGD': 'http://www.informatics.jax.org/marker/{id}',
            'RGD': 'https://rgd.mcw.edu/rgdweb/report/gene/main.html?id={id}',
        }

        # Fungal sources (for Orthologs/Best hits in fungal species)
        fungal_sources = {'SGD', 'SGD_BEST_HIT', 'POMBASE', 'POMBASE_BEST_HIT',
                         'AspGD', 'AspGD_BEST_HIT', 'BROAD_NEUROSPORA', 'BROAD_NEUROSPORA_BEST_HIT'}
        # Other species sources (for Reciprocal best hits)
        other_sources = {'dictyBase', 'MGD', 'RGD'}

        # Query external homologs via dbxref_feat
        external_dbxrefs = (
            db.query(Dbxref)
            .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
            .filter(DbxrefFeat.feature_no == f.feature_no)
            .filter(Dbxref.source.in_(list(source_to_species.keys())))
            .all()
        )

        orthologs_fungal_by_source: dict[str, list[ExternalHomologOut]] = {}
        best_hits_fungal_by_source: dict[str, list[ExternalHomologOut]] = {}
        reciprocal_by_source: dict[str, list[ExternalHomologOut]] = {}

        for dbx in external_dbxrefs:
            source = dbx.source
            if not source:
                continue

            species_name = source_to_species.get(source, source)
            url_template = source_to_url.get(source)
            url = url_template.format(id=dbx.dbxref_id) if url_template else None

            # Use description as display name if available, otherwise dbxref_id
            display_name = dbx.description or dbx.dbxref_id

            homolog = ExternalHomologOut(
                dbxref_id=dbx.dbxref_id,
                display_name=display_name,
                organism_name=species_name,
                source=source.replace('_BEST_HIT', ''),  # Normalize source name
                url=url,
            )

            # Categorize based on source
            if source in other_sources:
                # Reciprocal best hits in other species
                norm_source = source
                if norm_source not in reciprocal_by_source:
                    reciprocal_by_source[norm_source] = []
                reciprocal_by_source[norm_source].append(homolog)
            elif source in fungal_sources:
                if 'BEST_HIT' in source:
                    # Best hits in fungal species
                    norm_source = source.replace('_BEST_HIT', '')
                    if norm_source not in best_hits_fungal_by_source:
                        best_hits_fungal_by_source[norm_source] = []
                    best_hits_fungal_by_source[norm_source].append(homolog)
                else:
                    # Orthologs in fungal species
                    if source not in orthologs_fungal_by_source:
                        orthologs_fungal_by_source[source] = []
                    orthologs_fungal_by_source[source].append(homolog)

        orthologs_fungal = ExternalHomologsSectionOut(by_source=orthologs_fungal_by_source) if orthologs_fungal_by_source else None
        best_hits_fungal = ExternalHomologsSectionOut(by_source=best_hits_fungal_by_source) if best_hits_fungal_by_source else None
        reciprocal_best_hits = ExternalHomologsSectionOut(by_source=reciprocal_by_source) if reciprocal_by_source else None

        # Load phylogenetic tree if available
        # Use dbxref_id (Stanford ID like CAL0126527) as the dbid for tree files
        phylogenetic_tree = _load_phylogenetic_tree(f.dbxref_id) if f.dbxref_id else None

        out[organism_name] = HomologyDetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            homology_groups=homology_groups,
            ortholog_cluster=ortholog_cluster,
            phylogenetic_tree=phylogenetic_tree,
            best_hits_cgd=best_hits_cgd,
            orthologs_fungal=orthologs_fungal,
            best_hits_fungal=best_hits_fungal,
            reciprocal_best_hits=reciprocal_best_hits,
        )

    return HomologyDetailsResponse(results=out)


def _get_sequence_resources(
    db: Session,
    feature_name: str,
    seq_source: Optional[str] = None,
) -> Optional[SequenceResources]:
    """
    Get sequence resource pulldown menu data for a feature.

    Queries the web_display table to get URLs for:
    - Retrieve Sequences
    - Sequence Analysis Tools
    - Maps & Displays

    Args:
        db: Database session
        feature_name: Name of the feature (used to substitute in URLs)
        seq_source: Optional sequence source for filtering

    Returns:
        SequenceResources object with pulldown menu items, or None if no data
    """
    retrieve_sequences = []
    sequence_analysis_tools = []
    maps_displays = []

    # Query web_display for locus page resources
    web_displays = (
        db.query(WebDisplay)
        .filter(
            WebDisplay.web_page_name == 'locus',
            WebDisplay.label_type == 'Pull-down',
            WebDisplay.label_location.in_([
                'Retrieve Sequences',
                'Sequence Analysis Tools',
                'Maps & Displays',
            ]),
        )
        .all()
    )

    for wd in web_displays:
        url = wd.url
        if not url:
            continue

        # Substitute feature name in URL
        url_str = url.url
        if url_str:
            url_str = url_str.replace('_SUBSTITUTE_THIS_', feature_name)

        item = SequenceResourceItem(
            label=wd.label_name,
            url=url_str or '',
        )

        if wd.label_location == 'Retrieve Sequences':
            retrieve_sequences.append(item)
        elif wd.label_location == 'Sequence Analysis Tools':
            sequence_analysis_tools.append(item)
        elif wd.label_location == 'Maps & Displays':
            maps_displays.append(item)

    # Only return resources if we have at least one item
    if not retrieve_sequences and not sequence_analysis_tools and not maps_displays:
        return None

    return SequenceResources(
        retrieve_sequences=retrieve_sequences,
        sequence_analysis_tools=sequence_analysis_tools,
        maps_displays=maps_displays,
    )


# JBrowse configuration per organism/strain
# Format: { organism_name: { 'data_path': str, 'tracks': str, 'mini_tracks': str } }
JBROWSE_CONFIG = {
    "Candida albicans SC5314": {
        "data_path": "cgd_data/C_albicans_SC5314",
        "tracks": "DNA,Transcribed Features",
        "mini_tracks": "DNA,Transcribed Features",
    },
    "Candida glabrata CBS138": {
        "data_path": "cgd_data/C_glabrata_CBS138",
        "tracks": "DNA,Transcribed Features",
        "mini_tracks": "DNA,Transcribed Features",
    },
    # Add other organisms as needed
}

# JBrowse base URL
JBROWSE_BASE_URL = "http://www.candidagenome.org/jbrowse/index.html"

# Flanking basepairs to add to JBrowse coordinates (matching Perl JBROWSE_EXT)
JBROWSE_FLANK = 1000


def _get_jbrowse_info(
    organism_name: str,
    feature_name: str,
    chromosome: Optional[str],
    start_coord: Optional[int],
    stop_coord: Optional[int],
    feature_qualifier: Optional[str] = None,
) -> Optional[JBrowseInfo]:
    """
    Generate JBrowse URLs for embedding and linking.

    This implements the JBrowse section from the Perl code.
    Returns None if the feature is deleted/unmapped or has no location.

    Args:
        organism_name: Name of the organism (for config lookup)
        feature_name: Name of the feature
        chromosome: Chromosome/contig name
        start_coord: Start coordinate
        stop_coord: Stop coordinate
        feature_qualifier: Feature qualifier (to check for deleted/unmapped)

    Returns:
        JBrowseInfo object or None if not applicable
    """
    from urllib.parse import quote

    # Skip deleted or unmapped features (matching Perl behavior)
    if feature_qualifier:
        qual_lower = feature_qualifier.lower()
        if 'deleted' in qual_lower or 'not physically mapped' in qual_lower:
            return None

    # Skip if no location data
    if not chromosome or start_coord is None or stop_coord is None:
        return None

    # Get JBrowse config for this organism
    config = JBROWSE_CONFIG.get(organism_name)
    if not config:
        # Try partial match (e.g., "Candida albicans" matches "Candida albicans SC5314")
        for org_key, org_config in JBROWSE_CONFIG.items():
            if organism_name in org_key or org_key in organism_name:
                config = org_config
                break

    if not config:
        return None

    # Calculate coordinates with flanking region
    low = min(start_coord, stop_coord)
    high = max(start_coord, stop_coord)
    low_flanked = max(1, low - JBROWSE_FLANK)
    high_flanked = high + JBROWSE_FLANK

    # URL encode the parameters
    data_encoded = quote(config['data_path'], safe='')
    tracks_encoded = quote(config['tracks'], safe='')
    mini_tracks_encoded = quote(config['mini_tracks'], safe='')
    loc_encoded = quote(f"{chromosome}:{low_flanked}..{high_flanked}", safe='')

    # Build URLs with proper JBrowse parameters
    # Format: ?data=...&tracklist=1&nav=1&overview=1&tracks=...&loc=...&highlight=
    base_params = f"?data={data_encoded}&tracklist=1&nav=1&overview=1"

    embed_url = f"{JBROWSE_BASE_URL}{base_params}&tracks={mini_tracks_encoded}&loc={loc_encoded}&highlight="
    full_url = f"{JBROWSE_BASE_URL}{base_params}&tracks={tracks_encoded}&loc={loc_encoded}&highlight="

    return JBrowseInfo(
        embed_url=embed_url,
        full_url=full_url,
        feature_name=feature_name,
        chromosome=chromosome,
        start_coord=start_coord,
        stop_coord=stop_coord,
    )


def _get_allele_locations(
    db: Session,
    feature_no: int,
    feature_name: str,
) -> list[AlleleLocationOut]:
    """
    Get location information for alleles associated with a feature.

    This implements the "Allele Location" section from the Perl code.
    Only returns secondary alleles (not the primary allele which has the
    same feature_name as the main feature).

    Args:
        db: Database session
        feature_no: Feature number of the main feature
        feature_name: Feature name of the main feature (to exclude primary allele)

    Returns:
        List of AlleleLocationOut objects for each secondary allele
    """
    allele_locations = []

    # Get allele relationships for this feature
    allele_relationships = (
        db.query(FeatRelationship)
        .filter(
            FeatRelationship.parent_feature_no == feature_no,
            FeatRelationship.relationship_type == 'allele',
        )
        .all()
    )

    for ar in allele_relationships:
        # Get the allele feature with its location
        allele = (
            db.query(Feature)
            .options(
                joinedload(Feature.feat_location),
                joinedload(Feature.seq),
            )
            .filter(
                Feature.feature_no == ar.child_feature_no,
                func.lower(Feature.feature_type) == 'allele',
            )
            .first()
        )

        if not allele:
            continue

        # Skip the primary allele (same name as main feature)
        if allele.feature_name == feature_name:
            continue

        # Get current location for this allele
        chromosome = None
        start_coord = None
        stop_coord = None
        strand = None
        coord_version = None
        seq_version = None
        allele_start = None  # For relative coordinate calculation

        for fl in allele.feat_location:
            if fl.is_loc_current == 'Y':
                # Get chromosome name from root_seq_no
                if fl.root_seq_no:
                    root_seq_result = (
                        db.query(Feature.feature_name)
                        .join(Seq, Seq.feature_no == Feature.feature_no)
                        .filter(Seq.seq_no == fl.root_seq_no)
                        .first()
                    )
                    if root_seq_result:
                        chromosome = root_seq_result[0]

                start_coord = fl.start_coord
                stop_coord = fl.stop_coord
                strand = fl.strand
                coord_version = fl.coord_version if hasattr(fl, 'coord_version') else None
                allele_start = start_coord
                break

        # Get seq_version from current sequence
        for seq in allele.seq:
            if seq.is_seq_current == 'Y':
                seq_version = seq.seq_version
                break

        # Skip alleles without location
        if start_coord is None:
            continue

        # Get subfeatures for this allele
        subfeatures = []
        subfeature_rows = (
            db.query(
                Feature.feature_type,
                FeatLocation.start_coord,
                FeatLocation.stop_coord,
                FeatLocation.coord_version,
                Seq.seq_version,
            )
            .join(FeatRelationship, FeatRelationship.child_feature_no == Feature.feature_no)
            .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
            .outerjoin(Seq, (Seq.feature_no == Feature.feature_no) & (Seq.is_seq_current == 'Y'))
            .filter(
                FeatRelationship.parent_feature_no == allele.feature_no,
                FeatRelationship.rank == 2,  # rank 2 = subfeature
                FeatLocation.is_loc_current == 'Y',
            )
            .order_by(FeatLocation.start_coord)
            .all()
        )

        for row in subfeature_rows:
            feat_type, sf_start, sf_stop, sf_coord_ver, sf_seq_ver = row

            # Calculate relative coordinates
            relative_start = None
            relative_stop = None
            if allele_start is not None:
                if strand and strand.upper().startswith('C'):
                    # Crick strand - reverse relative coords
                    relative_start = allele_start - sf_start + 1
                    relative_stop = allele_start - sf_stop + 1
                else:
                    # Watson strand
                    relative_start = sf_start - allele_start + 1
                    relative_stop = sf_stop - allele_start + 1

                # Genomic relative location doesn't have 0
                if relative_start is not None and relative_start <= 0:
                    relative_start -= 1
                if relative_stop is not None and relative_stop <= 0:
                    relative_stop -= 1

            # Format feature type for display
            display_type = feat_type
            if display_type:
                display_type = display_type.replace('five_prime_', "5' ")
                display_type = display_type.replace('three_prime_', "3' ")
                display_type = display_type.replace('_', ' ')
                display_type = display_type.capitalize()

            subfeatures.append(AlleleSubfeatureOut(
                feature_type=display_type or feat_type,
                start_coord=sf_start,
                stop_coord=sf_stop,
                relative_start=relative_start,
                relative_stop=relative_stop,
                coord_version=sf_coord_ver,
                seq_version=sf_seq_ver,
            ))

        allele_locations.append(AlleleLocationOut(
            feature_no=allele.feature_no,
            feature_name=allele.feature_name,
            gene_name=allele.gene_name,
            chromosome=chromosome,
            start_coord=start_coord,
            stop_coord=stop_coord,
            strand=strand,
            coord_version=coord_version,
            seq_version=seq_version,
            subfeatures=subfeatures,
        ))

    return allele_locations


def get_locus_sequence_details(db: Session, name: str) -> SequenceDetailsResponse:
    """
    Query sequence and location information for each feature matching the locus name,
    grouped by organism. Includes subfeatures (introns, exons, CDS, UTRs).
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_location),
            joinedload(Feature.seq),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    out: dict[str, SequenceDetailsForOrganism] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        # Get the feature's start coord for relative coordinate calculation
        feature_start = None
        feature_strand = None

        # Get locations
        locations = []
        for fl in f.feat_location:
            # Get chromosome name from root_seq_no
            # root_seq_no points to the Seq record for the chromosome,
            # which is linked to a Feature with the chromosome name
            chromosome = None
            if fl.root_seq_no:
                root_seq_result = (
                    db.query(Feature.feature_name)
                    .join(Seq, Seq.feature_no == Feature.feature_no)
                    .filter(Seq.seq_no == fl.root_seq_no)
                    .first()
                )
                if root_seq_result:
                    chromosome = root_seq_result[0]

            # Store first current location's start for relative coords
            if fl.is_loc_current == 'Y' and feature_start is None:
                feature_start = fl.start_coord
                feature_strand = fl.strand

            locations.append(SequenceLocationOut(
                chromosome=chromosome,
                start_coord=fl.start_coord,
                stop_coord=fl.stop_coord,
                strand=fl.strand,
                is_current=(fl.is_loc_current == 'Y'),
                coord_version=fl.coord_version if hasattr(fl, 'coord_version') else None,
                source=fl.source if hasattr(fl, 'source') else None,
            ))

        # Get sequences - also capture seq_version for the location
        # Deduplicate: only keep the most recent sequence per type (for both current and archived)
        sequences = []
        seq_version_for_location = None
        seen_current_types = {}  # Track best current sequence per type
        seen_archived_types = {}  # Track best archived sequence per type

        for seq in f.seq:
            is_current = (seq.is_seq_current == 'Y')
            seq_type_lower = (seq.seq_type or '').lower()

            if is_current:
                # For current sequences, only keep the one with highest seq_no per type
                existing = seen_current_types.get(seq_type_lower)
                if existing is None or seq.seq_no > existing.seq_no:
                    seen_current_types[seq_type_lower] = seq
                    if seq_version_for_location is None:
                        seq_version_for_location = seq.seq_version
            else:
                # For archived sequences, only keep the one with highest seq_no per type
                existing = seen_archived_types.get(seq_type_lower)
                if existing is None or seq.seq_no > existing.seq_no:
                    seen_archived_types[seq_type_lower] = seq

        # Add the best current sequence for each type
        for seq in seen_current_types.values():
            residues = seq.residues
            if residues and len(residues) > 1000:
                residues = residues[:1000] + "..."

            sequences.append(SequenceOut(
                seq_type=seq.seq_type,
                seq_length=seq.seq_length,
                source=seq.source,
                seq_version=seq.seq_version,
                is_current=True,
                residues=residues,
            ))

        # Add archived sequences only if they differ from current (different source/length)
        for seq_type_lower, seq in seen_archived_types.items():
            current_seq = seen_current_types.get(seq_type_lower)
            # Skip archived if current exists with same length (likely same content)
            if current_seq and current_seq.seq_length == seq.seq_length:
                continue

            residues = seq.residues
            if residues and len(residues) > 1000:
                residues = residues[:1000] + "..."

            sequences.append(SequenceOut(
                seq_type=seq.seq_type,
                seq_length=seq.seq_length,
                source=seq.source,
                seq_version=seq.seq_version,
                is_current=False,
                residues=residues,
            ))

        # Update locations with seq_version
        for loc in locations:
            if loc.is_current and seq_version_for_location:
                loc.seq_version = seq_version_for_location

        # Get subfeatures via feat_relationship (rank=2 means subfeature)
        subfeatures = []
        subfeature_rows = (
            db.query(
                Feature.feature_type,
                FeatLocation.start_coord,
                FeatLocation.stop_coord,
                FeatLocation.coord_version,
                Seq.seq_version,
            )
            .join(FeatRelationship, FeatRelationship.child_feature_no == Feature.feature_no)
            .join(FeatLocation, FeatLocation.feature_no == Feature.feature_no)
            .outerjoin(Seq, (Seq.feature_no == Feature.feature_no) & (Seq.is_seq_current == 'Y'))
            .filter(
                FeatRelationship.parent_feature_no == f.feature_no,
                FeatRelationship.rank == 2,  # rank 2 = subfeature
                FeatLocation.is_loc_current == 'Y',
            )
            .order_by(FeatLocation.start_coord)
            .all()
        )

        for row in subfeature_rows:
            feat_type, start, stop, coord_ver, seq_ver = row

            # Calculate relative coordinates
            relative_start = None
            relative_stop = None
            if feature_start is not None:
                if feature_strand and feature_strand.upper().startswith('C'):
                    # Crick strand - reverse relative coords
                    relative_start = feature_start - start + 1
                    relative_stop = feature_start - stop + 1
                else:
                    # Watson strand
                    relative_start = start - feature_start + 1
                    relative_stop = stop - feature_start + 1

                # Genomic relative location doesn't have 0
                if relative_start is not None and relative_start <= 0:
                    relative_start -= 1
                if relative_stop is not None and relative_stop <= 0:
                    relative_stop -= 1

            # Format feature type for display (five_prime_UTR -> 5' UTR)
            display_type = feat_type
            if display_type:
                display_type = display_type.replace('five_prime_', "5' ")
                display_type = display_type.replace('three_prime_', "3' ")
                display_type = display_type.replace('_', ' ')
                display_type = display_type.capitalize()

            subfeatures.append(SubfeatureOut(
                feature_type=display_type or feat_type,
                start_coord=start,
                stop_coord=stop,
                relative_start=relative_start,
                relative_stop=relative_stop,
                coord_version=coord_ver,
                seq_version=seq_ver,
            ))

        # Get sequence resources (pulldown menus)
        seq_source = None
        for loc in locations:
            if loc.is_current and loc.source:
                seq_source = loc.source
                break
        sequence_resources = _get_sequence_resources(db, f.feature_name, seq_source)

        # Get allele locations (secondary alleles only)
        allele_locations = _get_allele_locations(db, f.feature_no, f.feature_name)

        # Get feature qualifier for JBrowse check
        feature_qualifier = None
        qualifier_row = (
            db.query(FeatProperty.property_value)
            .filter(
                FeatProperty.feature_no == f.feature_no,
                FeatProperty.property_type == 'feature_qualifier',
            )
            .first()
        )
        if qualifier_row:
            feature_qualifier = qualifier_row[0]

        # Get JBrowse info (only for features with location that are not deleted/unmapped)
        jbrowse_info = None
        if locations:
            # Use first current location for JBrowse
            current_loc = next((loc for loc in locations if loc.is_current), None)
            if current_loc:
                jbrowse_info = _get_jbrowse_info(
                    organism_name=organism_name,
                    feature_name=f.feature_name,
                    chromosome=current_loc.chromosome,
                    start_coord=current_loc.start_coord,
                    stop_coord=current_loc.stop_coord,
                    feature_qualifier=feature_qualifier,
                )

        out[organism_name] = SequenceDetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            locations=locations,
            sequences=sequences,
            subfeatures=subfeatures,
            sequence_resources=sequence_resources,
            allele_locations=allele_locations,
            jbrowse_info=jbrowse_info,
        )

    return SequenceDetailsResponse(results=out)


def get_locus_references(db: Session, name: str) -> LocusReferencesResponse:
    """
    Query references citing this locus, grouped by organism.
    Uses RefProperty and RefpropFeat tables (like Perl LiteratureGuide.pm)
    to get all references associated with literature topics for this feature.
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    # Get all literature topic parent-child relationships from cvterm_relationship table
    # Query (like Perl): SELECT CT1.term_name (parent), CT2.term_name (child)
    #        FROM cv_term CT1, cv_term CT2, cvterm_relationship CR, cv CV
    #        WHERE CR.child_cv_term_no = CT2.cv_term_no
    #        AND CR.parent_cv_term_no = CT1.cv_term_no
    #        AND CT1.cv_no = CV.cv_no AND CV.cv_name = 'literature_topic'
    from sqlalchemy.orm import aliased
    ParentTerm = aliased(CvTerm)
    ChildTerm = aliased(CvTerm)

    topic_relations_query = (
        db.query(ParentTerm.term_name.label('parent'), ChildTerm.term_name.label('child'))
        .join(CvtermRelationship, CvtermRelationship.parent_cv_term_no == ParentTerm.cv_term_no)
        .join(ChildTerm, CvtermRelationship.child_cv_term_no == ChildTerm.cv_term_no)
        .join(Cv, ParentTerm.cv_no == Cv.cv_no)
        .filter(Cv.cv_name == 'literature_topic')
        .all()
    )

    # Build a mapping of parent_topic -> [child_topics]
    all_topic_groups: dict[str, list[str]] = {}
    for parent, child in topic_relations_query:
        if parent not in all_topic_groups:
            all_topic_groups[parent] = []
        all_topic_groups[parent].append(child)

    # Get max PubMed search date (global, not per-locus)
    # SELECT max(date_created) FROM reference WHERE source = 'PubMed script'
    max_pubmed_date_result = (
        db.query(func.max(Reference.date_created))
        .filter(Reference.source == 'PubMed script')
        .scalar()
    )

    out: dict[str, ReferencesForOrganism] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        # Get references via RefProperty and RefpropFeat tables
        # This matches the Perl LiteratureGuide query:
        # SELECT distinct REF_PROPERTY.reference_no, REF_PROPERTY.property_value, REFPROP_FEAT.feature_no
        # FROM REF_PROPERTY, REFPROP_FEAT
        # WHERE REF_PROPERTY.ref_property_no = REFPROP_FEAT.ref_property_no(+)
        # AND REFPROP_FEAT.feature_no = ?
        ref_props = (
            db.query(RefProperty, Reference, RefpropFeat.date_created)
            .join(RefpropFeat, RefProperty.ref_property_no == RefpropFeat.ref_property_no)
            .join(Reference, RefProperty.reference_no == Reference.reference_no)
            .filter(RefpropFeat.feature_no == f.feature_no)
            .all()
        )

        # Collect unique references with their topics
        references = []
        seen_refs = set()
        ref_topics: dict[int, list[str]] = {}  # reference_no -> list of topics
        topic_counts: dict[str, int] = {}  # topic_name -> count of refs with this topic
        max_curated_date = None

        for rp, ref, refprop_feat_date in ref_props:
            if ref:
                # Track topics for each reference
                if ref.reference_no not in ref_topics:
                    ref_topics[ref.reference_no] = []
                if rp.property_value:
                    ref_topics[ref.reference_no].append(rp.property_value)
                    # Count topics
                    if rp.property_value not in topic_counts:
                        topic_counts[rp.property_value] = 0
                    topic_counts[rp.property_value] += 1

                    # Track max curated date (for curated refs, not 'Not yet curated')
                    if rp.property_value != 'Not yet curated' and refprop_feat_date:
                        if max_curated_date is None or refprop_feat_date > max_curated_date:
                            max_curated_date = refprop_feat_date

                # Add reference if not seen
                if ref.reference_no not in seen_refs:
                    seen_refs.add(ref.reference_no)
                    references.append({
                        'ref': ref,
                        'reference_no': ref.reference_no,
                    })

        # Get curation status properties (High Priority, Not yet curated) that might not be in refprop_feat
        # These are stored in ref_property without a feature link
        ref_nos = list(seen_refs)
        if ref_nos:
            curation_status_query = (
                db.query(RefProperty.reference_no, RefProperty.property_value)
                .filter(RefProperty.reference_no.in_(ref_nos))
                .filter(RefProperty.property_value.in_(['High Priority', 'Not yet curated']))
                .all()
            )

            for ref_no, prop_value in curation_status_query:
                if ref_no in ref_topics:
                    if prop_value not in ref_topics[ref_no]:
                        ref_topics[ref_no].append(prop_value)
                        # Update topic counts
                        if prop_value not in topic_counts:
                            topic_counts[prop_value] = 0
                        topic_counts[prop_value] += 1

        # Load ref_urls for all references (Full Text links, supplements, etc.)
        ref_url_map: dict[int, list] = {}  # reference_no -> list of RefUrl objects
        if ref_nos:
            ref_url_query = (
                db.query(RefUrl)
                .options(joinedload(RefUrl.url))
                .filter(RefUrl.reference_no.in_(ref_nos))
                .all()
            )
            for ref_url in ref_url_query:
                if ref_url.reference_no not in ref_url_map:
                    ref_url_map[ref_url.reference_no] = []
                ref_url_map[ref_url.reference_no].append(ref_url)

        # Get other genes for all references in one query
        # Query: Find all features associated with these references via refprop_feat
        # Filter to match Perl _get_gene_list: same organism, current location, current seq,
        # same seq source, and specific feature types (ORF, etc.)
        other_genes_map: dict[int, list[str]] = {}  # reference_no -> list of gene names

        # Get the seq_source for current feature
        current_seq_source = (
            db.query(Seq.source)
            .join(FeatLocation, Seq.seq_no == FeatLocation.root_seq_no)
            .filter(FeatLocation.feature_no == f.feature_no)
            .filter(FeatLocation.is_loc_current == 'Y')
            .filter(Seq.is_seq_current == 'Y')
            .first()
        )
        seq_source = current_seq_source[0] if current_seq_source else None

        if ref_nos and seq_source:
            # Valid feature types for locus page (matching Perl web_metadata query)
            valid_feature_types = ['ORF', 'blocked_reading_frame', 'pseudogene',
                                   'transposable_element_gene', 'gene_group', 'ncRNA_gene',
                                   'rRNA_gene', 'snoRNA_gene', 'snRNA_gene', 'tRNA_gene']

            other_genes_query = (
                db.query(RefProperty.reference_no, Feature.gene_name, Feature.feature_name)
                .join(RefpropFeat, RefProperty.ref_property_no == RefpropFeat.ref_property_no)
                .join(Feature, RefpropFeat.feature_no == Feature.feature_no)
                .join(FeatLocation, Feature.feature_no == FeatLocation.feature_no)
                .join(Seq, FeatLocation.root_seq_no == Seq.seq_no)
                .filter(RefProperty.reference_no.in_(ref_nos))
                .filter(Feature.feature_no != f.feature_no)  # Exclude current feature
                .filter(Feature.organism_no == f.organism_no)  # Same organism only
                .filter(Feature.feature_type.in_(valid_feature_types))  # Only valid feature types
                .filter(FeatLocation.is_loc_current == 'Y')  # Only features with current location
                .filter(Seq.is_seq_current == 'Y')  # Only current sequences
                .filter(Seq.source == seq_source)  # Same seq source as current feature
                .distinct()
                .all()
            )

            for ref_no, gene_name, feature_name in other_genes_query:
                if ref_no not in other_genes_map:
                    other_genes_map[ref_no] = []
                display_name = gene_name or feature_name
                if display_name and display_name not in other_genes_map[ref_no]:
                    other_genes_map[ref_no].append(display_name)

        # Build final reference list with topics and other genes
        final_references = []
        for ref_data in references:
            ref = ref_data['ref']
            topics = ref_topics.get(ref.reference_no, [])
            other_genes = other_genes_map.get(ref.reference_no, [])
            ref_urls = ref_url_map.get(ref.reference_no, [])
            final_references.append(ReferenceForLocus(
                reference_no=ref.reference_no,
                pubmed=ref.pubmed,
                dbxref_id=ref.dbxref_id,
                citation=ref.citation,
                title=ref.title,
                year=ref.year,
                links=_build_citation_links_for_locus(ref, ref_urls),
                topics=list(set(topics)),  # Deduplicate topics
                other_genes=sorted(other_genes),  # Sort for consistent display
            ))

        # Build topic groups with counts (only include topics that have refs)
        topic_groups_out = []
        for group_name, topics in sorted(all_topic_groups.items()):
            group_topics = []
            for topic_name in sorted(topics):
                count = topic_counts.get(topic_name, 0)
                if count > 0:
                    group_topics.append(LiteratureTopicOut(
                        topic_name=topic_name,
                        count=count,
                    ))
            if group_topics:
                topic_groups_out.append(LiteratureTopicGroupOut(
                    group_name=group_name,
                    topics=group_topics,
                ))

        out[organism_name] = ReferencesForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            references=final_references,
            topic_groups=topic_groups_out,
            last_curated_date=max_curated_date,
            last_pubmed_search_date=max_pubmed_date_result,
        )

    return LocusReferencesResponse(results=out)


def get_locus_summary_notes(db: Session, name: str) -> LocusSummaryNotesResponse:
    """
    Query summary paragraphs for this locus, grouped by organism.
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.feat_para).joinedload(FeatPara.paragraph),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    out: dict[str, SummaryNotesForOrganism] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        # Get paragraphs ordered by paragraph_order
        summary_notes = []
        for fp in sorted(f.feat_para, key=lambda x: x.paragraph_order):
            para = fp.paragraph
            if para:
                summary_notes.append(SummaryNoteOut(
                    paragraph_no=para.paragraph_no,
                    paragraph_text=para.paragraph_text,
                    paragraph_order=fp.paragraph_order,
                    date_edited=para.date_edited,
                ))

        out[organism_name] = SummaryNotesForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            summary_notes=summary_notes,
        )

    return LocusSummaryNotesResponse(results=out)


def _format_citation(citation: str) -> str:
    """Format citation to 'FirstAuthor et al' format"""
    if not citation:
        return ""
    # Extract first author and add "et al"
    # Citation format is typically "Author1, Author2... (Year) Title..."
    match = re.match(r'^([^\s,]+)', citation)
    if match:
        return f"{match.group(1)} et al"
    return citation[:30] + "..." if len(citation) > 30 else citation


def _get_references_for_entity(
    db: Session, tab_name: str, col_name: str, primary_key: int
) -> list[ReferenceOutForHistory]:
    """Get references linked to an entity via ref_link table"""
    ref_links = (
        db.query(RefLink)
        .options(
            joinedload(RefLink.reference).joinedload(Reference.journal)
        )
        .filter(
            RefLink.tab_name == tab_name,
            RefLink.col_name == col_name,
            RefLink.primary_key == primary_key,
        )
        .all()
    )

    # Collect all reference_nos
    all_ref_nos = set()
    for rl in ref_links:
        if rl.reference:
            all_ref_nos.add(rl.reference.reference_no)

    # Load ref_urls for all references in one query
    ref_url_map: dict[int, list] = {}
    if all_ref_nos:
        ref_url_query = (
            db.query(RefUrl)
            .options(joinedload(RefUrl.url))
            .filter(RefUrl.reference_no.in_(list(all_ref_nos)))
            .all()
        )
        for ref_url in ref_url_query:
            if ref_url.reference_no not in ref_url_map:
                ref_url_map[ref_url.reference_no] = []
            ref_url_map[ref_url.reference_no].append(ref_url)

    refs = []
    for rl in ref_links:
        ref = rl.reference
        if ref:
            formatted = _format_citation(ref.citation)
            # Get journal name if available
            journal_name = None
            if ref.journal:
                journal_name = ref.journal.abbreviation or ref.journal.full_name
            ref_urls = ref_url_map.get(ref.reference_no, [])
            refs.append(ReferenceOutForHistory(
                reference_no=ref.reference_no,
                dbxref_id=ref.dbxref_id,
                citation=ref.citation,
                formatted_citation=formatted,
                display_name=ref.citation,  # Use full citation for display (like GO tab)
                link=f"/reference/{ref.dbxref_id}",
                pubmed=ref.pubmed,  # Include PubMed ID for linking
                journal_name=journal_name,  # Include journal name for formatting
                links=_build_citation_links_for_history(ref, ref_urls),
            ))
    return refs


def _get_nomenclature_history(
    db: Session, feature: Feature
) -> Optional[NomenclatureHistoryOut]:
    """
    Get nomenclature history for a feature including:
    - Reserved name info (if name is still reserved)
    - Standard name info (with date standardized and references)
    - Alias names with references
    """
    # Get gene reservation info
    gene_reservations = (
        db.query(GeneReservation)
        .options(joinedload(GeneReservation.coll_generes).joinedload(CollGeneres.colleague))
        .filter(GeneReservation.feature_no == feature.feature_no)
        .all()
    )

    reserved_name_info = None
    standard_name_info = None
    date_standardized = None

    if gene_reservations:
        gr = gene_reservations[0]
        date_standardized = gr.date_standardized

        # Get contacts for this reservation
        contacts = []
        for cg in gr.coll_generes:
            if cg.colleague:
                contacts.append(ContactOut(
                    colleague_no=cg.colleague.colleague_no,
                    first_name=cg.colleague.first_name,
                    last_name=cg.colleague.last_name,
                ))

        # Get references for gene name
        gene_name_refs = _get_references_for_entity(
            db, "FEATURE", "GENE_NAME", feature.feature_no
        )

        # If no standardization date, show as reserved name
        if not date_standardized:
            reserved_name_info = ReservedNameInfoOut(
                reserved_name=feature.gene_name or feature.feature_name,
                contacts=contacts,
                reservation_date=gr.reservation_date,
                expiration_date=gr.expiration_date,
                references=gene_name_refs,
            )
        else:
            # Has been standardized - show standard name info
            standard_name_info = StandardNameInfoOut(
                standard_name=feature.gene_name or feature.feature_name,
                date_standardized=date_standardized,
                references=gene_name_refs,
            )
    else:
        # No reservation record - only show standard name if there are references
        # (per Perl logic: show only if references OR date_standardized, and
        # without a reservation there's no date_standardized)
        gene_name_refs = _get_references_for_entity(
            db, "FEATURE", "GENE_NAME", feature.feature_no
        )
        if gene_name_refs:
            standard_name_info = StandardNameInfoOut(
                standard_name=feature.gene_name or feature.feature_name,
                date_standardized=None,
                references=gene_name_refs,
            )

    # Get alias names with references
    feat_aliases = (
        db.query(FeatAlias)
        .options(joinedload(FeatAlias.alias))
        .filter(FeatAlias.feature_no == feature.feature_no)
        .all()
    )

    alias_names = []
    for fa in feat_aliases:
        if fa.alias:
            # Get references for this feat_alias
            alias_refs = _get_references_for_entity(
                db, "FEAT_ALIAS", "FEAT_ALIAS_NO", fa.feat_alias_no
            )
            # Only include aliases that have references (per Perl logic)
            if alias_refs:
                alias_names.append(AliasNameInfoOut(
                    alias_name=fa.alias.alias_name,
                    references=alias_refs,
                ))

    # Only return if there's something to show
    if reserved_name_info or standard_name_info or alias_names:
        return NomenclatureHistoryOut(
            reserved_name_info=reserved_name_info,
            standard_name_info=standard_name_info,
            alias_names=alias_names,
        )
    return None


# Note categories configuration (from LocusHistory.conf)
NOTE_CATEGORIES = [
    ("Nomenclature History Notes", ["Nomenclature history", "Nomenclature conflict"]),
    ("Sequence Annotation Notes", [
        "Proposed annotation change", "Proposed sequence change",
        "Annotation change", "Sequence change"
    ]),
    ("Curation Notes", ["Curation note"]),
    ("Mapping Notes", ["Mapping"]),
    ("Other Notes", ["Other"]),
    ("Alternative processing Notes", ["Alternative processing"]),
    ("Repeated Notes", ["Repeated"]),
]


def _get_categorized_notes(
    db: Session, feature: Feature
) -> list[NoteCategoryOut]:
    """
    Get notes for a feature organized by category.
    Each note includes its references.
    """
    # Get all notes for this feature
    note_links = (
        db.query(NoteLink)
        .options(joinedload(NoteLink.note))
        .filter(
            NoteLink.tab_name == "FEATURE",
            NoteLink.primary_key == feature.feature_no,
        )
        .all()
    )

    # Build a dict of note_type -> list of notes
    notes_by_type: dict[str, list] = defaultdict(list)
    seen_notes = set()
    for nl in note_links:
        note = nl.note
        if note and note.note_no not in seen_notes:
            seen_notes.add(note.note_no)
            # Get references for this note
            note_refs = _get_references_for_entity(
                db, "NOTE", "NOTE_NO", note.note_no
            )
            notes_by_type[note.note_type.upper() if note.note_type else ""].append({
                "note_no": note.note_no,
                "note": note.note,
                "note_type": note.note_type,
                "date": nl.date_created or note.date_created,
                "references": note_refs,
            })

    # Build categorized output
    categories = []
    for category_name, note_types in NOTE_CATEGORIES:
        category_notes = []
        for nt in note_types:
            for note_data in notes_by_type.get(nt.upper(), []):
                category_notes.append(NoteWithReferencesOut(
                    date=note_data["date"],
                    note=note_data["note"],
                    references=note_data["references"],
                ))

        if category_notes:
            # Sort by date within category
            category_notes.sort(key=lambda x: x.date, reverse=True)
            categories.append(NoteCategoryOut(
                category=category_name,
                notes=category_notes,
            ))

    return categories


def _build_nomenclature_for_frontend(
    nomenclature_history: Optional[NomenclatureHistoryOut]
) -> Optional[NomenclatureOut]:
    """
    Convert nomenclature_history to the format expected by the frontend.
    Frontend expects:
    - nomenclature.standard: array of {name, reference}
    - nomenclature.aliases: array of {name, reference}
    """
    if not nomenclature_history:
        return None

    standard_names = []
    aliases = []

    # Convert standard name info
    if nomenclature_history.standard_name_info:
        std = nomenclature_history.standard_name_info
        # If there are references, create one entry per reference
        if std.references:
            for ref in std.references:
                # Add link and display_name to reference
                ref_with_link = ReferenceOutForHistory(
                    reference_no=ref.reference_no,
                    dbxref_id=ref.dbxref_id,
                    citation=ref.citation,
                    formatted_citation=ref.formatted_citation,
                    display_name=ref.formatted_citation,
                    link=f"/reference/{ref.dbxref_id}",
                )
                standard_names.append(NomenclatureNameWithRef(
                    name=std.standard_name,
                    reference=ref_with_link,
                ))
        else:
            # No references, just show the name
            standard_names.append(NomenclatureNameWithRef(
                name=std.standard_name,
                reference=None,
            ))

    # Convert reserved name info (show as standard if reserved)
    if nomenclature_history.reserved_name_info:
        res = nomenclature_history.reserved_name_info
        if res.references:
            for ref in res.references:
                ref_with_link = ReferenceOutForHistory(
                    reference_no=ref.reference_no,
                    dbxref_id=ref.dbxref_id,
                    citation=ref.citation,
                    formatted_citation=ref.formatted_citation,
                    display_name=ref.formatted_citation,
                    link=f"/reference/{ref.dbxref_id}",
                )
                standard_names.append(NomenclatureNameWithRef(
                    name=res.reserved_name,
                    reference=ref_with_link,
                ))
        else:
            standard_names.append(NomenclatureNameWithRef(
                name=res.reserved_name,
                reference=None,
            ))

    # Convert alias names
    for alias in nomenclature_history.alias_names:
        if alias.references:
            for ref in alias.references:
                ref_with_link = ReferenceOutForHistory(
                    reference_no=ref.reference_no,
                    dbxref_id=ref.dbxref_id,
                    citation=ref.citation,
                    formatted_citation=ref.formatted_citation,
                    display_name=ref.formatted_citation,
                    link=f"/reference/{ref.dbxref_id}",
                )
                aliases.append(NomenclatureNameWithRef(
                    name=alias.alias_name,
                    reference=ref_with_link,
                ))
        else:
            aliases.append(NomenclatureNameWithRef(
                name=alias.alias_name,
                reference=None,
            ))

    if standard_names or aliases:
        return NomenclatureOut(
            standard=standard_names,
            aliases=aliases,
        )
    return None


# Note types for sequence annotation and curation notes
SEQUENCE_ANNOTATION_NOTE_TYPES = [
    "Proposed annotation change", "Proposed sequence change",
    "Annotation change", "Sequence change"
]
CURATION_NOTE_TYPES = ["Curation note"]


def get_locus_history(db: Session, name: str) -> LocusHistoryResponse:
    """
    Query history/notes for this locus, grouped by organism.
    Includes nomenclature history (reserved name, standard name, aliases)
    and notes organized by category.
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
        )
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    # Filter to one feature per organism (like Perl check_multi_feature_list)
    features = _filter_features_by_preference(db, features)

    out: dict[str, LocusHistoryForOrganism] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        # Get nomenclature history (internal format)
        nomenclature_history = _get_nomenclature_history(db, f)

        # Convert to frontend format
        nomenclature = _build_nomenclature_for_frontend(nomenclature_history)

        # Get categorized notes
        note_categories = _get_categorized_notes(db, f)

        # Get all notes for this feature
        note_links = (
            db.query(NoteLink)
            .options(joinedload(NoteLink.note))
            .filter(
                NoteLink.tab_name == "FEATURE",
                NoteLink.primary_key == f.feature_no,
            )
            .all()
        )

        history = []
        sequence_annotation_notes = []
        curation_notes = []
        seen_notes = set()

        for nl in note_links:
            note = nl.note
            if note and note.note_no not in seen_notes:
                seen_notes.add(note.note_no)
                event = HistoryEventOut(
                    event_type=note.note_type,
                    date=note.date_created,
                    note=note.note,
                )
                history.append(event)

                # Categorize into sequence annotation or curation notes
                note_type_upper = (note.note_type or "").upper()
                if any(nt.upper() == note_type_upper for nt in SEQUENCE_ANNOTATION_NOTE_TYPES):
                    sequence_annotation_notes.append(event)
                elif any(nt.upper() == note_type_upper for nt in CURATION_NOTE_TYPES):
                    curation_notes.append(event)

        # Sort by date descending (most recent first)
        history.sort(key=lambda x: x.date, reverse=True)
        sequence_annotation_notes.sort(key=lambda x: x.date, reverse=True)
        curation_notes.sort(key=lambda x: x.date, reverse=True)

        out[organism_name] = LocusHistoryForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            nomenclature=nomenclature,
            nomenclature_history=nomenclature_history,
            note_categories=note_categories,
            sequence_annotation_notes=sequence_annotation_notes,
            curation_notes=curation_notes,
            history=history,
        )

    return LocusHistoryResponse(results=out)


# Amino acid abbreviations mapping (3-letter to 1-letter)
AA_ABBREV = {
    'ala': ('A', 'Ala'), 'arg': ('R', 'Arg'), 'asn': ('N', 'Asn'), 'asp': ('D', 'Asp'),
    'cys': ('C', 'Cys'), 'gln': ('Q', 'Gln'), 'glu': ('E', 'Glu'), 'gly': ('G', 'Gly'),
    'his': ('H', 'His'), 'ile': ('I', 'Ile'), 'leu': ('L', 'Leu'), 'lys': ('K', 'Lys'),
    'met': ('M', 'Met'), 'phe': ('F', 'Phe'), 'pro': ('P', 'Pro'), 'ser': ('S', 'Ser'),
    'thr': ('T', 'Thr'), 'trp': ('W', 'Trp'), 'tyr': ('Y', 'Tyr'), 'val': ('V', 'Val'),
}

# Instability index cutoff
STABLE_INDEX_CUTOFF = 40


def get_locus_protein_properties(db: Session, name: str) -> ProteinPropertiesResponse:
    """
    Get physico-chemical properties for a protein.

    This includes:
    - Amino acid composition
    - Bulk protein properties (pI, GRAVY, aromaticity, aliphatic index, instability index)
    - Extinction coefficients
    - Codon usage statistics
    - Atomic composition
    """
    features = get_features_for_locus_name(db, name)
    if not features:
        return ProteinPropertiesResponse(results={})

    # Eager load protein_info and protein_detail
    feature_nos = [f.feature_no for f in features]
    features_with_protein = (
        db.query(Feature)
        .options(
            joinedload(Feature.protein_info).joinedload(ProteinInfo.protein_detail),
            joinedload(Feature.organism),
        )
        .filter(Feature.feature_no.in_(feature_nos))
        .all()
    )

    # Build one entry per organism
    seen_organisms = set()
    out: dict[str, ProteinPropertiesForOrganism] = {}

    for f in features_with_protein:
        organism_name, taxon_id = _get_organism_info(f)
        if organism_name in seen_organisms:
            continue
        seen_organisms.add(organism_name)
        locus_display_name = f.gene_name or f.feature_name

        # Build protein name (e.g., "Act1p/C1_13700wp_a")
        protein_name = ""
        if f.gene_name:
            protein_name = _gene_name_to_protein_name(f.gene_name) + "/"
        protein_name += _systematic_name_to_protein_name(f.feature_name)

        pi = f.protein_info[0] if f.protein_info else None
        if not pi:
            out[organism_name] = ProteinPropertiesForOrganism(
                locus_display_name=locus_display_name,
                protein_name=protein_name,
                taxon_id=taxon_id,
                organism_name=organism_name,
                has_ambiguous_residues=True,
            )
            continue

        protein_length = pi.protein_length or 0

        # Section 1: Amino Acid Composition
        aa_composition = []
        for aa_col, (one_letter, three_letter) in sorted(AA_ABBREV.items(), key=lambda x: x[1][0]):
            count = getattr(pi, aa_col, 0) or 0
            percentage = round((count * 100 / protein_length), 1) if protein_length > 0 else 0.0
            aa_composition.append(AminoAcidComposition(
                amino_acid=f"{one_letter} ({three_letter})",
                count=count,
                percentage=percentage,
            ))

        # Section 2: Bulk Protein Properties
        bulk_properties = []
        if pi.pi is not None:
            bulk_properties.append(BulkPropertyItem(
                label="Isoelectric Point (pI)",
                value=f"{float(pi.pi):.2f}",
            ))
        if pi.gravy_score is not None:
            bulk_properties.append(BulkPropertyItem(
                label="Average Hydropathy (GRAVY)",
                value=f"{float(pi.gravy_score):.2f}",
            ))
        if pi.aromaticity_score is not None:
            bulk_properties.append(BulkPropertyItem(
                label="Aromaticity Score",
                value=f"{float(pi.aromaticity_score):.2f}",
            ))

        # Get aliphatic index and instability index from protein_detail
        for pd in pi.protein_detail:
            if pd.protein_detail_group and pd.protein_detail_group.upper() == 'ALIPHATIC INDEX':
                bulk_properties.append(BulkPropertyItem(
                    label="Aliphatic Index",
                    value=pd.protein_detail_value,
                ))
            elif pd.protein_detail_group and pd.protein_detail_group.upper() == 'INSTABILITY INDEX':
                try:
                    ii_value = float(pd.protein_detail_value)
                    stability = "(stable)" if ii_value < STABLE_INDEX_CUTOFF else "(unstable)"
                    bulk_properties.append(BulkPropertyItem(
                        label="Instability Index",
                        value=f"{ii_value:.2f}",
                        note=stability,
                    ))
                except (ValueError, TypeError):
                    bulk_properties.append(BulkPropertyItem(
                        label="Instability Index",
                        value=pd.protein_detail_value,
                    ))

        # Section 3: Extinction Coefficients
        extinction_coefficients = []
        for pd in pi.protein_detail:
            if pd.protein_detail_group and 'EXTINCTION' in pd.protein_detail_group.upper():
                pd_type = pd.protein_detail_type or ""
                if 'all' in pd_type.lower():
                    condition = "Assuming all Cys residues exist as cysteine (-C-SH)"
                else:
                    condition = "Assuming all Cys residues exist as half-cystines (-C-S-S-C-)"
                try:
                    value = float(pd.protein_detail_value)
                    extinction_coefficients.append(ExtinctionCoefficient(
                        condition=condition,
                        value=value,
                        unit="M⁻¹ cm⁻¹",
                    ))
                except (ValueError, TypeError):
                    pass

        # Section 4: Codon Usage Statistics
        codon_usage = []
        if pi.codon_bias is not None:
            codon_usage.append(CodonUsageItem(
                label="Codon Bias Index",
                value=round(float(pi.codon_bias), 3),
            ))
        if pi.cai is not None:
            codon_usage.append(CodonUsageItem(
                label="Codon Adaptation Index (CAI)",
                value=round(float(pi.cai), 3),
            ))
        if pi.fop_score is not None:
            codon_usage.append(CodonUsageItem(
                label="Frequency of Optimal Codons (FOP)",
                value=round(float(pi.fop_score), 3),
            ))

        # Section 5: Atomic Composition
        atomic_composition = []
        for pd in pi.protein_detail:
            if pd.protein_detail_group and 'ATOMIC' in pd.protein_detail_group.upper():
                try:
                    count = int(float(pd.protein_detail_value))
                    atom_name = pd.protein_detail_type or "Unknown"
                    atom_name = atom_name.title()
                    atomic_composition.append(AtomicCompositionItem(
                        atom=atom_name,
                        count=count,
                    ))
                except (ValueError, TypeError):
                    pass

        # Sort atomic composition alphabetically
        atomic_composition.sort(key=lambda x: x.atom)

        out[organism_name] = ProteinPropertiesForOrganism(
            locus_display_name=locus_display_name,
            protein_name=protein_name,
            taxon_id=taxon_id,
            organism_name=organism_name,
            amino_acid_composition=aa_composition,
            protein_length=protein_length,
            bulk_properties=bulk_properties,
            extinction_coefficients=extinction_coefficients,
            codon_usage=codon_usage,
            atomic_composition=atomic_composition,
            has_ambiguous_residues=False,
            protein_page_url=f"/locus/{locus_display_name}#protein",
        )

    return ProteinPropertiesResponse(results=out)


# External domain database URLs
DOMAIN_DB_URLS = {
    'InterPro': 'https://www.ebi.ac.uk/interpro/entry/InterPro/',
    'Pfam': 'https://www.ebi.ac.uk/interpro/entry/pfam/',
    'SMART': 'http://smart.embl-heidelberg.de/smart/do_annotation.pl?ACC=',
    'ProSiteProfiles': 'https://prosite.expasy.org/',
    'ProSitePatterns': 'https://prosite.expasy.org/',
    'PRINTS': 'http://umber.sbs.man.ac.uk/cgi-bin/dbbrowser/sprint/searchprintss.cgi?display_opts=Prints&queryform=false&prints_accn=',
    'TIGRFAMs': 'https://www.ncbi.nlm.nih.gov/genome/annotation_prok/tigrfams/',
    'SUPERFAMILY': 'https://supfam.org/SUPERFAMILY/cgi-bin/scop.cgi?sunid=',
    'Gene3D': 'http://www.cathdb.info/version/latest/superfamily/',
    'PANTHER': 'http://www.pantherdb.org/panther/family.do?clsAccession=',
    'CDD': 'https://www.ncbi.nlm.nih.gov/Structure/cdd/cddsrv.cgi?uid=',
    'PIRSF': 'https://proteininformationresource.org/cgi-bin/ipcSF?id=',
    'Coils': None,
    'MobiDBLite': None,
    'SignalP': None,
    'TMHMM': None,
}


def _infer_domain_db_from_accession(accession: str) -> tuple[str, str]:
    """
    Infer the member database name from the accession prefix.
    Returns (database_name, url) tuple.
    """
    acc_upper = accession.upper()

    if acc_upper.startswith('PF'):
        return 'Pfam', f'https://www.ebi.ac.uk/interpro/entry/pfam/{accession}'
    elif acc_upper.startswith('PTHR'):
        return 'PANTHER', f'http://www.pantherdb.org/panther/family.do?clsAccession={accession}'
    elif acc_upper.startswith('SM'):
        return 'SMART', f'http://smart.embl-heidelberg.de/smart/do_annotation.pl?ACC={accession}'
    elif acc_upper.startswith('SSF'):
        return 'SUPERFAMILY', f'https://supfam.org/SUPERFAMILY/cgi-bin/scop.cgi?sunid={accession.replace("SSF", "")}'
    elif acc_upper.startswith('G3DSA:'):
        return 'Gene3D', f'http://www.cathdb.info/version/latest/superfamily/{accession.replace("G3DSA:", "")}'
    elif acc_upper.startswith('CD'):
        return 'CDD', f'https://www.ncbi.nlm.nih.gov/Structure/cdd/cddsrv.cgi?uid={accession}'
    elif acc_upper.startswith('PS'):
        return 'ProSite', f'https://prosite.expasy.org/{accession}'
    elif acc_upper.startswith('PR'):
        return 'PRINTS', f'http://umber.sbs.man.ac.uk/cgi-bin/dbbrowser/sprint/searchprintss.cgi?display_opts=Prints&queryform=false&prints_accn={accession}'
    elif acc_upper.startswith('TIGR'):
        return 'TIGRFAMs', f'https://www.ncbi.nlm.nih.gov/genome/annotation_prok/tigrfams/{accession}'
    elif acc_upper.startswith('PIRSF'):
        return 'PIRSF', f'https://proteininformationresource.org/cgi-bin/ipcSF?id={accession}'
    elif acc_upper.startswith('IPR'):
        return 'InterPro', f'https://www.ebi.ac.uk/interpro/entry/InterPro/{accession}'
    else:
        return 'Unknown', None

EXTERNAL_DOMAIN_LINKS = [
    {
        'name': 'NCBI DART',
        'url_template': 'https://www.ncbi.nlm.nih.gov/Structure/lexington/lexington.cgi?cmd=rps&query={sequence}',
        'description': 'NCBI Domain Architecture Retrieval Tool',
    },
    {
        'name': 'SMART',
        'url_template': 'http://smart.embl.de/smart/show_motifs.pl?SEQUENCE={sequence}',
        'description': 'Simple Modular Architecture Research Tool',
    },
    {
        'name': 'Pfam',
        'url_template': 'https://www.ebi.ac.uk/Tools/hmmer/search/hmmscan',
        'description': 'Protein families database',
    },
    {
        'name': 'Prosite',
        'url_template': 'https://prosite.expasy.org/scanprosite/',
        'description': 'Protein domain and family database',
    },
]


def get_locus_domain_details(db: Session, name: str) -> ProteinDomainResponse:
    """
    Get domain/motif information for a protein, grouped by organism.

    Returns conserved domains (grouped by InterPro), transmembrane domains,
    signal peptides, and external links.
    """
    from cgd.models.models import Feature, ProteinInfo, ProteinDetail, Seq

    # Get features matching the name
    features = get_features_for_locus_name(db, name)
    if not features:
        return ProteinDomainResponse(results={})

    feature_nos = [f.feature_no for f in features]

    # Fetch features with protein_info and protein_detail
    features_with_protein = (
        db.query(Feature)
        .options(
            joinedload(Feature.protein_info).joinedload(ProteinInfo.protein_detail),
            joinedload(Feature.organism),
            joinedload(Feature.seq),
        )
        .filter(Feature.feature_no.in_(feature_nos))
        .all()
    )

    # Build one entry per organism
    seen_organisms = set()
    out: dict[str, ProteinDomainForOrganism] = {}

    for f in features_with_protein:
        organism_name, taxon_id = _get_organism_info(f)
        if organism_name in seen_organisms:
            continue
        seen_organisms.add(organism_name)

        locus_display_name = f.gene_name or f.feature_name

        # Build protein name (e.g., "Act1p/C1_13700wp_a")
        protein_name = ""
        if f.gene_name:
            protein_name = _gene_name_to_protein_name(f.gene_name) + "/"
        protein_name += _systematic_name_to_protein_name(f.feature_name)

        pi = f.protein_info[0] if f.protein_info else None
        protein_length = pi.protein_length if pi else None

        # Get protein sequence for external links
        protein_sequence = None
        for seq in f.seq:
            if seq.is_seq_current == 'Y' and (seq.seq_type or '').upper() == 'PROTEIN':
                protein_sequence = seq.residues
                break

        # Organize domains by InterPro ID
        interpro_groups: dict[str, dict] = {}  # interpro_id -> {desc, members}
        transmembrane_domains = []
        signal_peptides = []

        if pi:
            for pd in pi.protein_detail:
                group = (pd.protein_detail_group or '').lower()
                detail_type = pd.protein_detail_type or ''

                # Check for TMHMM (transmembrane)
                if 'tmhmm' in group.lower() or detail_type.upper() == 'TMHMM':
                    if pd.start_coord is not None:
                        transmembrane_domains.append(TransmembraneDomain(
                            type=pd.protein_detail_value or 'transmembrane helix',
                            start_coord=pd.start_coord,
                            stop_coord=pd.stop_coord or pd.start_coord,
                        ))
                    continue

                # Check for SignalP (signal peptide)
                if 'signalp' in group.lower() or detail_type.upper() == 'SIGNALP':
                    if pd.start_coord is not None:
                        signal_peptides.append(SignalPeptide(
                            type=pd.protein_detail_value or 'signal peptide',
                            start_coord=pd.start_coord,
                            stop_coord=pd.stop_coord,
                        ))
                    continue

                # Skip non-domain entries
                if 'domain' not in group and group not in ('pfam', 'smart', 'interpro', 'prosite', 'prints', 'tigrfams', 'superfamily', 'gene3d', 'panther', 'cdd', 'pirsf', 'prositeprofiles', 'prositepatterns'):
                    continue

                # The actual accession (like PF00022, PTHR11937) is in protein_detail_value
                member_accession = pd.protein_detail_value or ''
                if not member_accession:
                    continue

                # Infer database name and URL from accession prefix
                member_db, member_url = _infer_domain_db_from_accession(member_accession)

                # Get InterPro ID (or use 'unintegrated' if none)
                # interpro_dbxref_id might be internal ID - group domains by it
                interpro_id = pd.interpro_dbxref_id or 'unintegrated'

                # Initialize InterPro group if needed
                if interpro_id not in interpro_groups:
                    interpro_groups[interpro_id] = {
                        'desc': None,  # We don't have the InterPro description
                        'members': {},
                    }

                # Key for this member domain
                member_key = f"{member_db}:{member_accession}"

                if member_key not in interpro_groups[interpro_id]['members']:
                    interpro_groups[interpro_id]['members'][member_key] = {
                        'db': member_db,
                        'id': member_accession,
                        'desc': pd.protein_detail_type or '',  # Type might have description
                        'url': member_url,
                        'hits': [],
                    }

                # Add hit coordinates
                if pd.start_coord is not None:
                    interpro_groups[interpro_id]['members'][member_key]['hits'].append({
                        'start': pd.start_coord,
                        'stop': pd.stop_coord,
                        'evalue': None,  # Not stored in protein_detail
                    })

        # Build InterPro domain list
        interpro_domains = []
        for ipr_id, ipr_data in sorted(interpro_groups.items()):
            member_domains = []
            for member_key, member_data in sorted(ipr_data['members'].items()):
                hits = [
                    DomainHit(
                        start_coord=h['start'],
                        stop_coord=h['stop'],
                        evalue=h['evalue'],
                    )
                    for h in member_data['hits']
                ]
                member_domains.append(DomainEntry(
                    member_db=member_data['db'],
                    member_id=member_data['id'],
                    description=member_data['desc'],
                    hits=hits,
                    member_url=member_data['url'],
                ))

            # InterPro URL
            interpro_url = None
            if ipr_id != 'unintegrated' and ipr_id:
                interpro_url = DOMAIN_DB_URLS.get('InterPro', '') + ipr_id

            interpro_domains.append(InterProDomain(
                interpro_id=ipr_id if ipr_id != 'unintegrated' else None,
                interpro_description=ipr_data['desc'],
                interpro_url=interpro_url,
                member_domains=member_domains,
            ))

        # Build external links
        external_links = []
        for link_info in EXTERNAL_DOMAIN_LINKS:
            url = link_info['url_template']
            if '{sequence}' in url and protein_sequence:
                # Remove stop codon if present
                seq = protein_sequence.rstrip('*')
                url = url.format(sequence=seq)
            external_links.append(DomainExternalLink(
                name=link_info['name'],
                url=url,
                description=link_info.get('description'),
            ))

        out[organism_name] = ProteinDomainForOrganism(
            locus_display_name=locus_display_name,
            protein_name=protein_name,
            taxon_id=taxon_id,
            organism_name=organism_name,
            protein_length=protein_length,
            interpro_domains=interpro_domains,
            transmembrane_domains=transmembrane_domains,
            signal_peptides=signal_peptides,
            external_links=external_links,
            protein_page_url=f"/locus/{locus_display_name}#protein",
        )

    return ProteinDomainResponse(results=out)
