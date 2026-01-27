from typing import Optional
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_
from collections import defaultdict

from cgd.api.crud.locus_crud import get_features_for_locus_name
from cgd.schemas.locus_schema import (
    LocusByOrganismResponse,
    FeatureOut,
    AliasOut,
    ExternalLinkOut,
    AlleleOut,
    CandidaOrthologOut,
    ExternalOrthologOut,
    OtherStrainNameOut,
    SequenceDetailsResponse,
    SequenceDetailsForOrganism,
    SequenceLocationOut,
    SequenceOut,
    LocusReferencesResponse,
    ReferencesForOrganism,
    ReferenceForLocus,
    LocusSummaryNotesResponse,
    SummaryNotesForOrganism,
    SummaryNoteOut,
    LocusHistoryResponse,
    LocusHistoryForOrganism,
    HistoryEventOut,
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

        # Get external links
        external_links = []
        for fu in f.feat_url:
            url = fu.url
            if url:
                external_links.append(ExternalLinkOut(
                    source=url.source,
                    url_type=url.url_type,
                    url=url.url,
                ))

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
        ortholog_cluster_url = None
        sgd_ortholog = next(
            (eo for eo in external_orthologs if eo.source == 'SGD'),
            None
        )
        if sgd_ortholog and sgd_ortholog.dbxref_id:
            ortholog_cluster_url = f"http://cgob3.ucd.ie/cgob.pl?gene={sgd_ortholog.dbxref_id}"

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
    n = name.strip()
    features = (
        db.query(Feature)
        .options(
            joinedload(Feature.organism),
            joinedload(Feature.go_annotation).joinedload(GoAnnotation.go),
            joinedload(Feature.go_annotation).joinedload(GoAnnotation.go_ref).joinedload(GoRef.reference),
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

            evidence = GOEvidence(
                code=ga.go_evidence,
                with_from=None,  # Could be extended if with_from data exists
            )

            # Get references from go_ref relationship
            references = []
            for gr in ga.go_ref:
                ref = gr.reference
                if ref and ref.pubmed:
                    references.append(f"PMID:{ref.pubmed}")
                elif ref:
                    references.append(ref.dbxref_id)

            annotations.append(GOAnnotationOut(
                term=term,
                evidence=evidence,
                references=references,
            ))

        out[organism_name] = GODetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            annotations=annotations,
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
            if experiment:
                experiment_comment = getattr(experiment, "experiment_comment", None)
                strain = getattr(experiment, "strain_background", None)

            annotations.append(PhenotypeAnnotationOut(
                phenotype=pheno_term,
                qualifier=phenotype.qualifier,
                experiment=phenotype.experiment_type,
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


def get_locus_sequence_details(db: Session, name: str) -> SequenceDetailsResponse:
    """
    Query sequence and location information for each feature matching the locus name,
    grouped by organism.
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

        # Get locations
        locations = []
        for fl in f.feat_location:
            # Get chromosome name from root_seq if available
            chromosome = None
            if hasattr(fl, 'seq') and fl.seq:
                root_feat = fl.seq.feature if hasattr(fl.seq, 'feature') else None
                if root_feat:
                    chromosome = root_feat.feature_name

            locations.append(SequenceLocationOut(
                chromosome=chromosome,
                start_coord=fl.start_coord,
                stop_coord=fl.stop_coord,
                strand=fl.strand,
                is_current=(fl.is_loc_current == 'Y'),
            ))

        # Get sequences
        sequences = []
        for seq in f.seq:
            # Truncate residues for API response (full sequence available via separate endpoint)
            residues = seq.residues
            if residues and len(residues) > 1000:
                residues = residues[:1000] + "..."

            sequences.append(SequenceOut(
                seq_type=seq.seq_type,
                seq_length=seq.seq_length,
                source=seq.source,
                seq_version=seq.seq_version,
                is_current=(seq.is_seq_current == 'Y'),
                residues=residues,
            ))

        out[organism_name] = SequenceDetailsForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            locations=locations,
            sequences=sequences,
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


def get_locus_history(db: Session, name: str) -> LocusHistoryResponse:
    """
    Query history/notes for this locus, grouped by organism.
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

        # Get history notes via note_link
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
        seen_notes = set()
        for nl in note_links:
            note = nl.note
            if note and note.note_no not in seen_notes:
                seen_notes.add(note.note_no)
                history.append(HistoryEventOut(
                    event_type=note.note_type,
                    date=note.date_created,
                    note=note.note,
                ))

        # Sort by date descending (most recent first)
        history.sort(key=lambda x: x.date, reverse=True)

        out[organism_name] = LocusHistoryForOrganism(
            locus_display_name=locus_display_name,
            taxon_id=taxon_id,
            history=history,
        )

    return LocusHistoryResponse(results=out)
