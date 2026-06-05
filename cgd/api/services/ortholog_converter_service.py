"""Service for ortholog converter endpoint."""
from __future__ import annotations

import logging
from typing import Optional

import httpx
from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from cgd.models.models import (
    Feature,
    FeatHomology,
    FeatRelationship,
    HomologyGroup,
    DbxrefHomology,
    DbxrefFeat,
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

# SGD REST API for resolving S. cerevisiae identifiers (gene name, systematic/
# ORF name, alias, or SGDID) to the canonical SGDID and standard gene name.
SGD_ENTITY_API_URL = "https://backend.yeastgenome.org/entity/locus/"
SGD_ENTITY_TIMEOUT = 10.0


def _resolve_sgd_ids(gene_ids: list[str]) -> dict[str, dict]:
    """
    Resolve S. cerevisiae identifiers to canonical SGDID and gene name via the
    SGD REST API.

    CGD links S. cerevisiae orthologs through DbxrefFeat -> Dbxref
    (source='SGD'), where dbxref_id holds the SGDID (e.g. S000002708) and
    description holds the standard gene name (e.g. PRO1). Systematic/ORF names
    (e.g. YDR300C) are not stored, so input using ORF names cannot be matched
    directly. This resolver maps any accepted SGD identifier to the SGDID and
    gene name that CGD does store.

    Returns a dict keyed by the upper-cased identifier (the API query echo plus
    the systematic name, gene name, SGDID, and prefixed modEntityId) with
    values::

        {'sgdid': 'S000002708', 'gene_name': 'PRO1', 'systematic_name': 'YDR300C'}

    Best-effort: returns an empty dict if the SGD API is unavailable, leaving
    direct CGD lookups (by gene name or SGDID) unaffected.
    """
    # De-duplicate while preserving order.
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in gene_ids:
        gene = (raw or "").strip()
        if gene and gene.upper() not in seen:
            seen.add(gene.upper())
            cleaned.append(gene)
    if not cleaned:
        return {}

    try:
        # The endpoint accepts pipe-separated identifiers; httpx percent-encodes
        # the path for us.
        response = httpx.get(
            SGD_ENTITY_API_URL + "|".join(cleaned),
            timeout=SGD_ENTITY_TIMEOUT,
        )
        if response.status_code != 200:
            logger.warning(
                "SGD entity lookup returned status %s", response.status_code
            )
            return {}
        entries = response.json()
    except (httpx.RequestError, ValueError) as exc:
        logger.warning("SGD entity lookup failed: %s", exc)
        return {}

    resolution: dict[str, dict] = {}
    for entry in entries or []:
        mod_id = (entry.get("modEntityId") or "").strip()
        # Strip the "SGD:" prefix to match the dbxref_id stored in CGD.
        sgdid = mod_id.split(":", 1)[1] if ":" in mod_id else mod_id
        gene_name = (entry.get("display_name") or "").strip() or None
        systematic_name = (entry.get("format_name") or "").strip() or None
        info = {
            "sgdid": sgdid or None,
            "gene_name": gene_name,
            "systematic_name": systematic_name,
        }
        # Key by every identifier form so a match succeeds regardless of which
        # the user supplied.
        for key in (entry.get("query"), systematic_name, gene_name, sgdid, mod_id):
            if key and key.strip():
                resolution[key.strip().upper()] = info

    return resolution


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
) -> list[dict]:
    """
    Find external orthologs (SGD, POMBASE, etc.) in a homology group.
    Returns list of dicts with id, gene_name, description, organism, url, cluster_id.
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
            # Note: For DbxrefHomology entries, dbxref.description contains the organism name,
            # not the gene name. Gene name will be looked up separately from DbxrefFeat.
            gene_name = None

            # Build URL based on source
            if external_source == 'SGD':
                url = f"https://www.yeastgenome.org/locus/{dbxref_id}"
            elif external_source == 'POMBASE':
                url = f"https://www.pombase.org/gene/{dbxref_id}"
            elif external_source == 'AspGD':
                url = f"http://www.aspgd.org/cgi-bin/locus.pl?locus={dbxref_id}"
            else:
                url = None

            results.append({
                'id': dbxref_id,
                'gene_name': gene_name,
                'feature_name': dbxref_id,
                'description': None,  # Will be looked up from DbxrefFeat if available
                'organism': ext_org,
                'url': url,
                'cluster_id': cluster_id,
            })

    return results


def _get_sgd_gene_info_from_feature(
    db: Session,
    feature_no: int,
) -> tuple[Optional[str], Optional[str]]:
    """
    Get SGD gene name and description from a CGD feature's DbxrefFeat entries.
    Returns (gene_name, description) tuple.
    """
    result = (
        db.query(Dbxref)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .filter(
            DbxrefFeat.feature_no == feature_no,
            func.upper(Dbxref.source) == 'SGD',
        )
        .first()
    )
    if result:
        # For SGD DbxrefFeat entries:
        # - description contains the gene name (e.g., ACT1)
        # - We don't have the SGD gene's functional description in our DB
        return result.description, None
    return None, None


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


def _find_cgd_feature_for_sgd_gene(
    db: Session,
    gene_id: str,
) -> tuple[Optional[Feature], Optional[Dbxref]]:
    """
    Find CGD feature linked to an SGD gene via DbxrefFeat.
    Returns (Feature, Dbxref) tuple or (None, None) if not found.

    SGD genes are stored in Dbxref with source='SGD':
    - dbxref_id = systematic name (e.g., YFL039C)
    - description = gene name (e.g., ACT1)

    The link to CGD features is through DbxrefFeat.
    """
    gene_id = gene_id.strip().upper()
    if not gene_id:
        return None, None

    # Strategy 1: Match by systematic name (dbxref_id) with source='SGD'
    result = (
        db.query(Feature, Dbxref)
        .select_from(Dbxref)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
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
            func.upper(Dbxref.dbxref_id) == gene_id,
            func.upper(Dbxref.source) == 'SGD',
        )
        .first()
    )

    if result:
        return result

    # Strategy 2: Match by gene name (description) with source='SGD'
    result = (
        db.query(Feature, Dbxref)
        .select_from(Dbxref)
        .join(DbxrefFeat, Dbxref.dbxref_no == DbxrefFeat.dbxref_no)
        .join(Feature, DbxrefFeat.feature_no == Feature.feature_no)
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
            func.upper(Dbxref.description) == gene_id,
            func.upper(Dbxref.source) == 'SGD',
        )
        .first()
    )

    return result if result else (None, None)


def convert_orthologs_from_sgd(
    db: Session,
    gene_ids: list[str],
    target_organism: TargetOrganism,
) -> OrthologConvertResponse:
    """
    Convert a list of S. cerevisiae gene IDs to orthologs in a CGD organism.
    This is the reverse lookup: SGD → CGD.

    The approach:
    1. Find the CGD Feature linked to the SGD gene via DbxrefFeat
    2. Use the CGD Feature's homology groups to find orthologs in the target organism
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

    # Source is S. cerevisiae: always resolve every input via the SGD API to get
    # the canonical gene name, systematic/ORF name, and SGDID. CGD stores its
    # S. cerevisiae orthologs by SGDID and gene name (never by ORF name), so this
    # both lets ORF-name input be matched and supplies the ORF name for display.
    sgd_resolution = _resolve_sgd_ids([g for g in gene_ids if g and g.strip()])

    for gene_id in gene_ids:
        gene_id_stripped = gene_id.strip()
        if not gene_id_stripped:
            continue

        resolved = sgd_resolution.get(gene_id_stripped.upper()) or {}
        input_gene_name = resolved.get("gene_name")
        input_orf_name = resolved.get("systematic_name")
        input_sgdid = resolved.get("sgdid")

        # Find the CGD feature linked to this SGD gene via DbxrefFeat. Prefer the
        # SGD-resolved SGDID (stable), then the gene name, then the raw input.
        feature, dbxref = (None, None)
        for candidate in (input_sgdid, input_gene_name, gene_id_stripped):
            if not candidate:
                continue
            feature, dbxref = _find_cgd_feature_for_sgd_gene(db, candidate)
            if feature and dbxref:
                break

        # Prefer the SGD-resolved names for display; fall back to CGD's dbxref.
        if dbxref:
            input_gene_name = input_gene_name or dbxref.description
            input_sgdid = input_sgdid or dbxref.dbxref_id

        if not feature or not dbxref:
            results.append(OrthologResult(
                input_id=gene_id_stripped,
                input_gene_name=input_gene_name,
                input_feature_name=input_orf_name,
                input_sgdid=input_sgdid,
                input_organism="Saccharomyces cerevisiae",
                found=False,
                relationship="not_found",
                notes="Gene not found in SGD ortholog data",
            ))
            continue

        found_count += 1

        # Get organism of the CGD feature we found
        cgd_feature_org = _get_organism_name(feature)

        # If the CGD feature is already in the target organism, return it
        if cgd_feature_org == target_display_name:
            converted_count += 1
            results.append(OrthologResult(
                input_id=gene_id_stripped,
                input_gene_name=input_gene_name,
                input_feature_name=input_orf_name,
                input_sgdid=input_sgdid,
                input_organism="Saccharomyces cerevisiae",
                found=True,
                ortholog_id=feature.feature_name,
                ortholog_gene_name=feature.gene_name,
                ortholog_feature_name=feature.feature_name,
                ortholog_description=feature.headline,
                target_organism=cgd_feature_org,
                relationship="1:1",
                ortholog_url=f"/locus/{feature.feature_name}",
                notes=None,
            ))
            continue

        # Search for orthologs in target organism using the CGD feature's homology groups
        all_orthologs = []
        cluster_id = None
        homology_groups = _get_ortholog_groups_for_feature(feature)

        for hg in homology_groups:
            # Find features in this homology group for the target organism
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

            if all_orthologs:
                break

        if not all_orthologs:
            results.append(OrthologResult(
                input_id=gene_id_stripped,
                input_gene_name=input_gene_name,
                input_feature_name=input_orf_name,
                input_sgdid=input_sgdid,
                input_organism="Saccharomyces cerevisiae",
                found=True,
                cluster_id=cluster_id or (homology_groups[0].homology_group_id if homology_groups else None),
                relationship="no_ortholog",
                notes=f"No ortholog found in {target_display_name}",
            ))
            continue

        # Determine relationship
        source_count = 1
        if homology_groups:
            source_count = _count_source_genes_in_cluster(homology_groups[0], cgd_feature_org)
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
            input_id=gene_id_stripped,
            input_gene_name=input_gene_name,
            input_feature_name=input_orf_name,
            input_sgdid=input_sgdid,
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

        # For SGD target, try to get gene name from the input feature's DbxrefFeat
        # and use the CGD feature's headline as the description
        sgd_gene_name = None
        sgd_description = None
        if is_external and external_source == 'SGD':
            sgd_gene_name, _ = _get_sgd_gene_info_from_feature(
                db, feature.feature_no
            )
            # Use the CGD feature's headline as the SGD ortholog description
            # (the headline is derived from SGD ortholog annotations)
            sgd_description = feature.headline

        for hg in homology_groups:
            if is_external:
                # Search external orthologs
                ext_orthologs = _find_external_ortholog_in_group(
                    hg, target_display_name, external_source
                )
                for orth in ext_orthologs:
                    # Enrich with gene name from DbxrefFeat if available
                    if external_source == 'SGD' and sgd_gene_name:
                        orth['gene_name'] = sgd_gene_name
                        orth['description'] = sgd_description
                    all_orthologs.append(orth)
                    cluster_id = orth['cluster_id']
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
        description="Enter S. cerevisiae gene names (e.g., ACT1, ERG11) or systematic names (e.g., YFL039C)",
    ))

    return AvailableTargetsResponse(targets=targets, sources=sources)
