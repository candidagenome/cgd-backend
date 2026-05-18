"""Service for ortholog converter endpoint."""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from cgd.models.models import (
    Feature,
    FeatHomology,
    FeatRelationship,
    HomologyGroup,
    DbxrefHomology,
    Dbxref,
)
from cgd.schemas.ortholog_converter_schema import (
    TargetOrganism,
    SourceOrganism,
    TARGET_ORGANISM_DISPLAY_NAMES,
    SOURCE_ORGANISM_DISPLAY_NAMES,
    EXTERNAL_ORGANISM_SOURCES,
    OrthologResult,
    OrthologConvertResponse,
    TargetOrganismInfo,
    SourceOrganismInfo,
    AvailableTargetsResponse,
)

logger = logging.getLogger(__name__)


def _get_organism_name(feature: Feature) -> Optional[str]:
    """Extract organism name from a feature."""
    org = feature.organism
    if org is None:
        return None
    return (
        getattr(org, "organism_name", None)
        or getattr(org, "display_name", None)
        or getattr(org, "name", None)
    )


def _get_a21_exclusion_set(db: Session) -> set[int]:
    """
    Get set of feature_nos to exclude (Assembly 21 features with A22 equivalents).
    Based on es_indexer.py pattern.
    """
    # Direct Assembly 21 features
    direct_a21 = (
        db.query(FeatRelationship.child_feature_no)
        .filter(
            FeatRelationship.relationship_type == 'Assembly 21 Primary Allele',
            FeatRelationship.rank == 3,
        )
        .all()
    )
    exclude_set = {r[0] for r in direct_a21}

    # Alleles of Assembly 21 features - batch to avoid Oracle 1000-item IN clause limit
    exclude_list = list(exclude_set)
    batch_size = 900
    for i in range(0, len(exclude_list), batch_size):
        batch = exclude_list[i:i + batch_size]
        alleles_of_a21 = (
            db.query(FeatRelationship.child_feature_no)
            .filter(
                FeatRelationship.relationship_type == 'allele',
                FeatRelationship.rank == 3,
                FeatRelationship.parent_feature_no.in_(batch)
            )
            .all()
        )
        exclude_set.update(r[0] for r in alleles_of_a21)

    return exclude_set


def _find_feature_with_homology(db: Session, gene_id: str) -> Optional[Feature]:
    """
    Find a feature by gene ID with homology relationships eagerly loaded.
    Filters out Assembly 21 features and prefers features with homology data.
    """
    gene_id = gene_id.strip()
    if not gene_id:
        return None

    # Get Assembly 21 exclusion set
    a21_exclude = _get_a21_exclusion_set(db)

    # Load ALL matching features with homology relationships
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
                func.upper(Feature.gene_name) == func.upper(gene_id),
                func.upper(Feature.feature_name) == func.upper(gene_id),
                func.upper(Feature.dbxref_id) == func.upper(gene_id),
            )
        )
        .filter(func.lower(Feature.feature_type) != 'allele')
        .all()
    )

    if not features:
        return None

    # Filter out Assembly 21 features
    valid_features = [f for f in features if f.feature_no not in a21_exclude]

    if not valid_features:
        # If all filtered out, fall back to original list
        valid_features = features

    # Prefer feature with homology data (has feat_homology entries)
    for feat in valid_features:
        if feat.feat_homology and len(feat.feat_homology) > 0:
            return feat

    # Fall back to first valid feature
    return valid_features[0] if valid_features else None


def _get_ortholog_groups_for_feature(feature: Feature) -> list[HomologyGroup]:
    """
    Get all ortholog homology groups for a feature.
    Prioritizes CGOB method, then BLAST RBH, then BLAST.
    Feature must have been loaded with homology relationships.
    """
    # Filter for ortholog type groups, prioritize by method
    cgob_groups = []
    blast_rbh_groups = []
    blast_groups = []
    seen_groups = set()

    for fh in feature.feat_homology:
        hg = fh.homology_group
        if hg is None:
            continue
        # Skip if already processed (avoid duplicates)
        if hg.homology_group_no in seen_groups:
            continue
        seen_groups.add(hg.homology_group_no)

        if hg.homology_group_type == 'ortholog':
            if hg.method == 'CGOB':
                cgob_groups.append(hg)
            elif hg.method == 'BLAST RBH':
                blast_rbh_groups.append(hg)
            elif hg.method == 'BLAST':
                blast_groups.append(hg)

    # Return all groups in priority order: CGOB first, then BLAST RBH, then BLAST
    # This allows fallback to BLAST if CGOB/BLAST RBH don't have target organism
    all_groups = cgob_groups + blast_rbh_groups + blast_groups
    return all_groups


def _find_cgd_ortholog_in_group(
    homology_group: HomologyGroup,
    target_organism_name: str,
    source_feature_no: int,
) -> list[tuple[Feature, str]]:
    """
    Find CGD orthologs in a homology group for the target organism.
    Returns list of (feature, cluster_id) tuples.
    """
    results = []
    cluster_id = homology_group.homology_group_id

    for fh in homology_group.feat_homology:
        feat = fh.feature
        if feat and feat.feature_no != source_feature_no:
            org_name = _get_organism_name(feat)
            if org_name and org_name == target_organism_name:
                results.append((feat, cluster_id))

    return results


def _find_external_ortholog_in_group(
    homology_group: HomologyGroup,
    target_organism_name: str,
    external_source: str,
) -> list[tuple[str, str, str, str]]:
    """
    Find external orthologs (SGD, POMBASE, etc.) in a homology group.
    Returns list of (dbxref_id, organism_name, url, cluster_id) tuples.
    """
    results = []
    cluster_id = homology_group.homology_group_id

    for dh in homology_group.dbxref_homology:
        dbxref = dh.dbxref
        if not dbxref:
            continue

        # dh.name contains the organism name
        ext_org = (dh.name or '').strip()

        # Check if this is the target organism
        if target_organism_name.lower() in ext_org.lower():
            dbxref_id = dbxref.dbxref_id

            # Build URL based on source
            if external_source == 'SGD':
                url = f"https://www.yeastgenome.org/locus/{dbxref_id}"
            elif external_source == 'POMBASE':
                url = f"https://www.pombase.org/gene/{dbxref_id}"
            elif external_source == 'AspGD':
                url = f"http://www.aspgd.org/cgi-bin/locus.pl?locus={dbxref_id}"
            else:
                url = None

            results.append((dbxref_id, ext_org, url, cluster_id))

    return results


def _count_source_genes_in_cluster(
    homology_group: HomologyGroup,
    source_organism_name: str,
) -> int:
    """Count how many genes from the source organism are in this cluster."""
    count = 0
    for fh in homology_group.feat_homology:
        feat = fh.feature
        if feat:
            org_name = _get_organism_name(feat)
            if org_name == source_organism_name:
                count += 1
    return count


def _determine_relationship(source_count: int, target_count: int) -> str:
    """Determine the ortholog relationship type."""
    if target_count == 0:
        return "no_ortholog"
    elif source_count == 1 and target_count == 1:
        return "1:1"
    elif source_count == 1 and target_count > 1:
        return "1:many"
    elif source_count > 1 and target_count == 1:
        return "many:1"
    else:
        return "many:many"


def _find_sgd_gene_homology_groups(
    db: Session,
    gene_id: str,
) -> list[tuple[HomologyGroup, Dbxref, DbxrefHomology]]:
    """
    Find homology groups for an SGD gene ID.
    Returns list of (HomologyGroup, Dbxref, DbxrefHomology) tuples.

    Searches DbxrefHomology entries that link to S. cerevisiae orthologs.
    S. cerevisiae genes in CGOB are stored with systematic names (YFL039C),
    so users should enter systematic names for best results.
    """
    gene_id = gene_id.strip().upper()
    if not gene_id:
        return []

    dbxref_homologies = []

    # Strategy 1: Match dbxref_id directly (systematic names like YFL039C)
    # Source is typically "CGOB (Saccharomyces cerevisiae S288C)" for CGOB entries
    dbxref_homologies = (
        db.query(DbxrefHomology)
        .join(Dbxref, DbxrefHomology.dbxref_no == Dbxref.dbxref_no)
        .options(
            joinedload(DbxrefHomology.dbxref),
            joinedload(DbxrefHomology.homology_group)
                .joinedload(HomologyGroup.feat_homology)
                .joinedload(FeatHomology.feature)
                .joinedload(Feature.organism),
        )
        .filter(
            func.upper(Dbxref.dbxref_id) == gene_id,
            func.lower(Dbxref.source).contains('cerevisiae'),
        )
        .all()
    )

    # Strategy 2: Match dbxref_id with source='SGD' (for SGD ortholog pairs)
    if not dbxref_homologies:
        dbxref_homologies = (
            db.query(DbxrefHomology)
            .join(Dbxref, DbxrefHomology.dbxref_no == Dbxref.dbxref_no)
            .options(
                joinedload(DbxrefHomology.dbxref),
                joinedload(DbxrefHomology.homology_group)
                    .joinedload(HomologyGroup.feat_homology)
                    .joinedload(FeatHomology.feature)
                    .joinedload(Feature.organism),
            )
            .filter(
                func.upper(Dbxref.dbxref_id) == gene_id,
                func.upper(Dbxref.source) == 'SGD',
            )
            .all()
        )

    # Strategy 3: Match description field (gene name might be stored there)
    if not dbxref_homologies:
        dbxref_homologies = (
            db.query(DbxrefHomology)
            .join(Dbxref, DbxrefHomology.dbxref_no == Dbxref.dbxref_no)
            .options(
                joinedload(DbxrefHomology.dbxref),
                joinedload(DbxrefHomology.homology_group)
                    .joinedload(HomologyGroup.feat_homology)
                    .joinedload(FeatHomology.feature)
                    .joinedload(Feature.organism),
            )
            .filter(
                func.upper(Dbxref.description) == gene_id,
                func.lower(Dbxref.source).contains('cerevisiae'),
            )
            .all()
        )

    # Strategy 4: Match description with source='SGD'
    if not dbxref_homologies:
        dbxref_homologies = (
            db.query(DbxrefHomology)
            .join(Dbxref, DbxrefHomology.dbxref_no == Dbxref.dbxref_no)
            .options(
                joinedload(DbxrefHomology.dbxref),
                joinedload(DbxrefHomology.homology_group)
                    .joinedload(HomologyGroup.feat_homology)
                    .joinedload(FeatHomology.feature)
                    .joinedload(Feature.organism),
            )
            .filter(
                func.upper(Dbxref.description) == gene_id,
                func.upper(Dbxref.source) == 'SGD',
            )
            .all()
        )

    results = []
    for dh in dbxref_homologies:
        if dh.homology_group and dh.homology_group.homology_group_type == 'ortholog':
            results.append((dh.homology_group, dh.dbxref, dh))

    return results


def _count_sgd_genes_in_cluster(
    db: Session,
    homology_group: HomologyGroup,
) -> int:
    """Count how many S. cerevisiae genes are in this cluster."""
    count = (
        db.query(DbxrefHomology)
        .join(Dbxref)
        .filter(
            DbxrefHomology.homology_group_no == homology_group.homology_group_no,
            or_(
                func.upper(Dbxref.source) == 'SGD',
                func.lower(Dbxref.source).contains('cerevisiae'),
            ),
        )
        .count()
    )
    return count


def convert_orthologs_from_sgd(
    db: Session,
    gene_ids: list[str],
    target_organism: TargetOrganism,
) -> OrthologConvertResponse:
    """
    Convert a list of S. cerevisiae gene IDs to orthologs in a CGD organism.
    This is the reverse lookup: SGD → CGD.
    """
    target_display_name = TARGET_ORGANISM_DISPLAY_NAMES.get(target_organism, str(target_organism))
    source_display_name = SOURCE_ORGANISM_DISPLAY_NAMES[SourceOrganism.S_CEREVISIAE]

    # S. cerevisiae can only be converted to CGD species
    if target_organism in EXTERNAL_ORGANISM_SOURCES:
        return OrthologConvertResponse(
            source_organism=source_display_name,
            target_organism=target_display_name,
            total_input=len([g for g in gene_ids if g.strip()]),
            found_count=0,
            converted_count=0,
            results=[OrthologResult(
                input_id=gene_id,
                found=False,
                relationship="not_found",
                notes="Cannot convert S. cerevisiae to another external organism",
            ) for gene_id in gene_ids if gene_id.strip()],
        )

    results = []
    found_count = 0
    converted_count = 0

    for gene_id in gene_ids:
        gene_id = gene_id.strip()
        if not gene_id:
            continue

        # Find homology groups for this SGD gene
        hg_dbxref_pairs = _find_sgd_gene_homology_groups(db, gene_id)

        if not hg_dbxref_pairs:
            results.append(OrthologResult(
                input_id=gene_id,
                input_organism="Saccharomyces cerevisiae",
                found=False,
                relationship="not_found",
                notes="Gene not found. Please use systematic names (e.g., YFL039C, not ACT1)",
            ))
            continue

        found_count += 1
        dbxref = hg_dbxref_pairs[0][1]  # Get the matched Dbxref for input info

        # Search for CGD orthologs in the target organism
        all_orthologs = []
        cluster_id = None

        for hg, _, _ in hg_dbxref_pairs:
            # Find CGD features in this homology group for the target organism
            for fh in hg.feat_homology:
                feat = fh.feature
                if feat:
                    org_name = _get_organism_name(feat)
                    if org_name and org_name == target_display_name:
                        all_orthologs.append({
                            'id': feat.feature_name,
                            'gene_name': feat.gene_name,
                            'feature_name': feat.feature_name,
                            'description': feat.headline,
                            'organism': org_name,
                            'url': f"/locus/{feat.feature_name}",
                            'cluster_id': hg.homology_group_id,
                        })
                        cluster_id = hg.homology_group_id

            if all_orthologs:
                break

        if not all_orthologs:
            results.append(OrthologResult(
                input_id=gene_id,
                input_gene_name=dbxref.description,  # SGD gene name might be in description
                input_feature_name=dbxref.dbxref_id,
                input_organism="Saccharomyces cerevisiae",
                found=True,
                cluster_id=hg_dbxref_pairs[0][0].homology_group_id if hg_dbxref_pairs else None,
                relationship="no_ortholog",
                notes=f"No ortholog found in {target_display_name}",
            ))
            continue

        # Determine relationship
        source_count = _count_sgd_genes_in_cluster(db, hg_dbxref_pairs[0][0])
        target_count = len(all_orthologs)
        relationship = _determine_relationship(source_count, target_count)

        converted_count += 1

        # If multiple orthologs, add the first one as main result with note
        first_orth = all_orthologs[0]
        notes = None
        if len(all_orthologs) > 1:
            other_ids = [o['id'] for o in all_orthologs[1:]]
            notes = f"Multiple orthologs: {', '.join(other_ids)}"

        results.append(OrthologResult(
            input_id=gene_id,
            input_gene_name=dbxref.description,
            input_feature_name=dbxref.dbxref_id,
            input_organism="Saccharomyces cerevisiae",
            found=True,
            ortholog_id=first_orth['id'],
            ortholog_gene_name=first_orth['gene_name'],
            ortholog_feature_name=first_orth['feature_name'],
            ortholog_description=first_orth['description'],
            target_organism=first_orth['organism'],
            relationship=relationship,
            cluster_id=first_orth['cluster_id'],
            ortholog_url=first_orth['url'],
            notes=notes,
        ))

    return OrthologConvertResponse(
        source_organism=source_display_name,
        target_organism=target_display_name,
        total_input=len([g for g in gene_ids if g.strip()]),
        found_count=found_count,
        converted_count=converted_count,
        results=results,
    )


def convert_orthologs(
    db: Session,
    gene_ids: list[str],
    target_organism: TargetOrganism,
    source_organism: SourceOrganism = SourceOrganism.CGD,
) -> OrthologConvertResponse:
    """
    Convert a list of gene IDs to orthologs in the target organism.
    Supports both CGD → target and S. cerevisiae → CGD conversions.
    """
    # Dispatch to reverse lookup if source is S. cerevisiae
    if source_organism == SourceOrganism.S_CEREVISIAE:
        return convert_orthologs_from_sgd(db, gene_ids, target_organism)

    target_display_name = TARGET_ORGANISM_DISPLAY_NAMES.get(target_organism, str(target_organism))
    source_display_name = SOURCE_ORGANISM_DISPLAY_NAMES[SourceOrganism.CGD]
    is_external = target_organism in EXTERNAL_ORGANISM_SOURCES
    external_source = EXTERNAL_ORGANISM_SOURCES.get(target_organism)

    results = []
    found_count = 0
    converted_count = 0

    for gene_id in gene_ids:
        gene_id = gene_id.strip()
        if not gene_id:
            continue

        # Find the input feature with homology data
        feature = _find_feature_with_homology(db, gene_id)

        if not feature:
            # Gene not found in CGD
            results.append(OrthologResult(
                input_id=gene_id,
                found=False,
                relationship="not_found",
                notes="Gene not found in CGD",
            ))
            continue

        found_count += 1
        source_organism_name = _get_organism_name(feature)

        # Check if source is same as target
        if source_organism_name == target_display_name:
            results.append(OrthologResult(
                input_id=gene_id,
                input_gene_name=feature.gene_name,
                input_feature_name=feature.feature_name,
                input_organism=source_organism_name,
                found=True,
                ortholog_id=feature.feature_name,
                ortholog_gene_name=feature.gene_name,
                ortholog_feature_name=feature.feature_name,
                ortholog_description=feature.headline,
                target_organism=target_display_name,
                relationship="same_organism",
                notes="Input gene is already in target organism",
            ))
            converted_count += 1
            continue

        # Get ortholog groups
        homology_groups = _get_ortholog_groups_for_feature(feature)

        if not homology_groups:
            results.append(OrthologResult(
                input_id=gene_id,
                input_gene_name=feature.gene_name,
                input_feature_name=feature.feature_name,
                input_organism=source_organism_name,
                found=True,
                relationship="no_ortholog",
                notes="No ortholog cluster found for this gene",
            ))
            continue

        # Search for orthologs in target organism
        all_orthologs = []
        cluster_id = None

        for hg in homology_groups:
            if is_external:
                # Search external orthologs
                ext_orthologs = _find_external_ortholog_in_group(
                    hg, target_display_name, external_source
                )
                for dbxref_id, org_name, url, cid in ext_orthologs:
                    all_orthologs.append({
                        'id': dbxref_id,
                        'gene_name': None,  # External orthologs don't have gene names in our DB
                        'feature_name': dbxref_id,
                        'description': None,  # External orthologs don't have descriptions in our DB
                        'organism': org_name,
                        'url': url,
                        'cluster_id': cid,
                    })
                    cluster_id = cid
            else:
                # Search CGD orthologs
                cgd_orthologs = _find_cgd_ortholog_in_group(
                    hg, target_display_name, feature.feature_no
                )
                for orth_feat, cid in cgd_orthologs:
                    all_orthologs.append({
                        'id': orth_feat.feature_name,
                        'gene_name': orth_feat.gene_name,
                        'feature_name': orth_feat.feature_name,
                        'description': orth_feat.headline,
                        'organism': _get_organism_name(orth_feat),
                        'url': f"/locus/{orth_feat.feature_name}",
                        'cluster_id': cid,
                    })
                    cluster_id = cid

            # Use the first cluster that has orthologs
            if all_orthologs:
                break

        if not all_orthologs:
            results.append(OrthologResult(
                input_id=gene_id,
                input_gene_name=feature.gene_name,
                input_feature_name=feature.feature_name,
                input_organism=source_organism_name,
                found=True,
                cluster_id=cluster_id or (homology_groups[0].homology_group_id if homology_groups else None),
                relationship="no_ortholog",
                notes=f"No ortholog found in {target_display_name}",
            ))
            continue

        # Determine relationship
        source_count = 1
        if homology_groups:
            source_count = _count_source_genes_in_cluster(
                homology_groups[0], source_organism_name
            )
        target_count = len(all_orthologs)
        relationship = _determine_relationship(source_count, target_count)

        converted_count += 1

        # If multiple orthologs, add the first one as main result with note
        first_orth = all_orthologs[0]
        notes = None
        if len(all_orthologs) > 1:
            other_ids = [o['id'] for o in all_orthologs[1:]]
            notes = f"Multiple orthologs: {', '.join(other_ids)}"

        results.append(OrthologResult(
            input_id=gene_id,
            input_gene_name=feature.gene_name,
            input_feature_name=feature.feature_name,
            input_organism=source_organism_name,
            found=True,
            ortholog_id=first_orth['id'],
            ortholog_gene_name=first_orth['gene_name'],
            ortholog_feature_name=first_orth['feature_name'],
            ortholog_description=first_orth['description'],
            target_organism=first_orth['organism'],
            relationship=relationship,
            cluster_id=first_orth['cluster_id'],
            ortholog_url=first_orth['url'],
            notes=notes,
        ))

    return OrthologConvertResponse(
        source_organism=source_display_name,
        target_organism=target_display_name,
        total_input=len([g for g in gene_ids if g.strip()]),
        found_count=found_count,
        converted_count=converted_count,
        results=results,
    )


def get_available_targets() -> AvailableTargetsResponse:
    """Return list of available target and source organisms."""
    targets = []
    sources = []

    # CGD species (can be both source and target)
    cgd_species = [
        TargetOrganism.C_ALBICANS,
        TargetOrganism.C_DUBLINIENSIS,
        TargetOrganism.C_TROPICALIS,
        TargetOrganism.C_PARAPSILOSIS,
        TargetOrganism.C_AURIS,
        TargetOrganism.C_GLABRATA,
    ]

    for org in cgd_species:
        targets.append(TargetOrganismInfo(
            id=org.value,
            name=TARGET_ORGANISM_DISPLAY_NAMES[org],
            source="CGD",
            is_external=False,
        ))

    # External species (only S. cerevisiae has ortholog data in CGD)
    external_species = [
        TargetOrganism.S_CEREVISIAE,
    ]

    for org in external_species:
        targets.append(TargetOrganismInfo(
            id=org.value,
            name=TARGET_ORGANISM_DISPLAY_NAMES[org],
            source=EXTERNAL_ORGANISM_SOURCES[org],
            is_external=True,
        ))

    # Source organisms
    sources.append(SourceOrganismInfo(
        id=SourceOrganism.CGD.value,
        name=SOURCE_ORGANISM_DISPLAY_NAMES[SourceOrganism.CGD],
        description="Enter CGD gene names, systematic names, or CGD IDs",
    ))
    sources.append(SourceOrganismInfo(
        id=SourceOrganism.S_CEREVISIAE.value,
        name=SOURCE_ORGANISM_DISPLAY_NAMES[SourceOrganism.S_CEREVISIAE],
        description="Enter S. cerevisiae systematic names (e.g., YFL039C, YNL055C)",
    ))

    return AvailableTargetsResponse(targets=targets, sources=sources)
