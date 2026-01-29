import re
from typing import Optional
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
    LocusSummaryNotesResponse,
    SummaryNotesForOrganism,
    SummaryNoteOut,
    LocusHistoryResponse,
    LocusHistoryForOrganism,
    HistoryEventOut,
    ReferenceOutForHistory,
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
)
from cgd.schemas.phenotype_schema import (
    PhenotypeDetailsResponse,
    PhenotypeDetailsForOrganism,
    PhenotypeAnnotationOut,
    PhenotypeTerm,
    ReferenceStub,
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
)
from cgd.schemas.homology_schema import (
    HomologyDetailsResponse,
    HomologyDetailsForOrganism,
    HomologyGroupOut,
    HomologOut,
)
from cgd.models.locus_model import Feature
from cgd.models.go_model import GoAnnotation, GoRef
from cgd.models.phenotype_model import PhenoAnnotation
from cgd.models.interaction_model import FeatInteract
from cgd.models.homology_model import FeatHomology
from cgd.models.models import (
    RefLink,
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
    WebDisplay,
    GeneReservation,
    CollGeneres,
    Colleague,
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
                        citation=ref.citation,
                        title=ref.title,
                        year=ref.year,
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
            joinedload(Feature.go_annotation).joinedload(GoAnnotation.go_ref).joinedload(GoRef.reference),
            joinedload(Feature.go_annotation).joinedload(GoAnnotation.go_ref).joinedload(GoRef.go_qualifier),
            joinedload(Feature.go_annotation).joinedload(GoAnnotation.go_ref).joinedload(GoRef.goref_dbxref).joinedload(GorefDbxref.dbxref),
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

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        annotations = []
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

            # Get references from go_ref relationship
            references = []
            for gr in ga.go_ref:
                ref = gr.reference
                if ref and ref.pubmed:
                    references.append(f"PMID:{ref.pubmed}")
                elif ref:
                    references.append(ref.dbxref_id)

            # Format date_created
            date_created_str = None
            if ga.date_created:
                date_created_str = ga.date_created.strftime('%Y-%m-%d')

            annotations.append(GOAnnotationOut(
                term=term,
                evidence=evidence,
                references=references,
                qualifier=qualifier_str,
                annotation_type=ga.annotation_type,
                source=ga.source,
                date_created=date_created_str,
            ))

        out[organism_name] = GODetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            annotations=annotations,
        )

    return GODetailsResponse(results=out)


def _map_experiment_type_to_root(experiment_type: str) -> str:
    """
    Map experiment types to root categories: 'Classical genetics' or 'Large-scale survey'.
    This mimics the Perl _mapAllExperimentTypesToRootNode function.
    """
    if not experiment_type:
        return 'Classical genetics'

    exp_lower = experiment_type.lower()

    # Large-scale survey experiment types
    large_scale_types = [
        'large-scale survey',
        'systematic mutation set',
        'systematic deletion',
        'systematic overexpression',
        'tn insertion mutagenesis',
        'signature-tagged mutagenesis',
        'uv mutagenesis',
    ]

    for ls_type in large_scale_types:
        if ls_type in exp_lower:
            return 'Large-scale survey'

    return 'Classical genetics'


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
            strain = None
            if experiment:
                strain = getattr(experiment, "strain_background", None)

            # Map experiment_type to root category
            mapped_experiment_type = _map_experiment_type_to_root(phenotype.experiment_type)

            # CGD-specific handling: combine diploid info with null mutant type
            # For "homozygous diploid" or "heterozygous diploid" with mutant_type="null",
            # display as "homozygous null" or "heterozygous null"
            mutant_type = phenotype.mutant_type
            raw_experiment_type = phenotype.experiment_type or ''
            if 'homozygous diploid' in raw_experiment_type.lower() and mutant_type == 'null':
                mutant_type = 'homozygous null'
            elif 'heterozygous diploid' in raw_experiment_type.lower() and mutant_type == 'null':
                mutant_type = 'heterozygous null'

            annotations.append(PhenotypeAnnotationOut(
                phenotype=pheno_term,
                qualifier=phenotype.qualifier,
                experiment_type=mapped_experiment_type,
                mutant_type=mutant_type,
                strain=strain,
                references=[],  # References could be added via ref_link if needed
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
    """
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.protein_info),
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

        protein_info = None
        # Typically one protein_info per feature, but handle list
        if f.protein_info:
            pi = f.protein_info[0]

            # Build amino acid composition dictionary
            amino_acids = {
                "ala": pi.ala,
                "arg": pi.arg,
                "asn": pi.asn,
                "asp": pi.asp,
                "cys": pi.cys,
                "gln": pi.gln,
                "glu": pi.glu,
                "gly": pi.gly,
                "his": pi.his,
                "ile": pi.ile,
                "leu": pi.leu,
                "lys": pi.lys,
                "met": pi.met,
                "phe": pi.phe,
                "pro": pi.pro,
                "ser": pi.ser,
                "thr": pi.thr,
                "trp": pi.trp,
                "tyr": pi.tyr,
                "val": pi.val,
            }
            # Filter out None values
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

        out[organism_name] = ProteinDetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            protein_info=protein_info,
        )

    return ProteinDetailsResponse(results=out)


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
            joinedload(Feature.feat_homology).joinedload(FeatHomology.homology_group),
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

        out[organism_name] = HomologyDetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            homology_groups=homology_groups,
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
        sequences = []
        seq_version_for_location = None
        for seq in f.seq:
            # Truncate residues for API response (full sequence available via separate endpoint)
            residues = seq.residues
            if residues and len(residues) > 1000:
                residues = residues[:1000] + "..."

            # Store current sequence version for display
            if seq.is_seq_current == 'Y' and seq_version_for_location is None:
                seq_version_for_location = seq.seq_version

            sequences.append(SequenceOut(
                seq_type=seq.seq_type,
                seq_length=seq.seq_length,
                source=seq.source,
                seq_version=seq.seq_version,
                is_current=(seq.is_seq_current == 'Y'),
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

    out: dict[str, ReferencesForOrganism] = {}

    for f in features:
        organism_name, taxon_id = _get_organism_info(f)
        locus_display_name = f.gene_name or f.feature_name

        # Get references via ref_link
        ref_links = (
            db.query(RefLink)
            .options(joinedload(RefLink.reference))
            .filter(
                RefLink.tab_name == "FEATURE",
                RefLink.primary_key == f.feature_no,
            )
            .all()
        )

        references = []
        seen_refs = set()
        for rl in ref_links:
            ref = rl.reference
            if ref and ref.reference_no not in seen_refs:
                seen_refs.add(ref.reference_no)
                references.append(ReferenceForLocus(
                    reference_no=ref.reference_no,
                    pubmed=ref.pubmed,
                    citation=ref.citation,
                    title=ref.title,
                    year=ref.year,
                ))

        out[organism_name] = ReferencesForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            references=references,
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
        .options(joinedload(RefLink.reference))
        .filter(
            RefLink.tab_name == tab_name,
            RefLink.col_name == col_name,
            RefLink.primary_key == primary_key,
        )
        .all()
    )

    refs = []
    for rl in ref_links:
        ref = rl.reference
        if ref:
            formatted = _format_citation(ref.citation)
            refs.append(ReferenceOutForHistory(
                reference_no=ref.reference_no,
                dbxref_id=ref.dbxref_id,
                citation=ref.citation,
                formatted_citation=formatted,
                display_name=formatted,  # Use formatted citation for display
                link=f"/reference/{ref.dbxref_id}",
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
