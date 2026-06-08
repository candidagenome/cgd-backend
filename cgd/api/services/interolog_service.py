"""
On-the-fly orthology-inferred (interolog) interactions.

For a gene in a non-C.-albicans Candida species, transfer C. albicans curated
interactions across CGD's CGOB orthologs:

    target gene G  --ortholog-->  C. albicans gene G_ca
    G_ca's curated partners P_ca  --ortholog-->  target-species genes P_t
    => inferred (G, P_t), labeled predicted, with the C. albicans pair as evidence.

Computed live (read-only). C. albicans curated interaction data is small (~1,806
interactions, a handful per gene), so the per-page cost is only a few batched
queries — no precompute table. Inferred data is never written to INTERACTION.
"""
import logging
from typing import Optional

from sqlalchemy.orm import Session, joinedload

from cgd.models.models import (
    Feature, FeatHomology, HomologyGroup, FeatInteract, Interaction, RefLink,
    Reference, RefUrl,
)
from cgd.schemas.interaction_schema import (
    InferredInteractionOut, InteractionReferenceOut,
)
from cgd.api.services.ortholog_converter_service import (
    _get_ortholog_groups_for_feature,
    _find_cgd_ortholog_in_group,
)

logger = logging.getLogger(__name__)

# Source species for v1 interolog transfer.
SOURCE_ORGANISM = "Candida albicans SC5314"

# Genetic interaction experiment types; everything else is physical.
GENETIC_TYPES = {
    'Dosage Lethality', 'Dosage Rescue', 'Dosage Growth Defect',
    'Negative Genetic', 'Positive Genetic', 'Phenotypic Enhancement',
    'Phenotypic Suppression', 'Synthetic Growth Defect',
    'Synthetic Haploinsufficiency', 'Synthetic Lethality', 'Synthetic Rescue',
}

# Eager-load the ortholog (CGOB) homology neighborhood needed by the
# ortholog-converter helpers.
_HOMOLOGY_OPTS = (
    joinedload(Feature.organism),
    joinedload(Feature.feat_homology)
        .joinedload(FeatHomology.homology_group)
        .joinedload(HomologyGroup.feat_homology)
        .joinedload(FeatHomology.feature)
        .joinedload(Feature.organism),
)


def _load_with_homology(db: Session, feature_nos: set[int]) -> dict[int, Feature]:
    if not feature_nos:
        return {}
    feats = (
        db.query(Feature)
        .options(*_HOMOLOGY_OPTS)
        .filter(Feature.feature_no.in_(list(feature_nos)))
        .all()
    )
    return {f.feature_no: f for f in feats}


def _ortholog_in(feature: Feature, target_org_name: str) -> tuple[Optional[Feature], Optional[str]]:
    """Return (ortholog Feature in target organism, method) for `feature`
    (CGOB preferred), or (None, None). `feature` must be loaded with homology."""
    for hg in _get_ortholog_groups_for_feature(feature):
        matches = _find_cgd_ortholog_in_group(hg, target_org_name, feature.feature_no)
        if matches:
            return matches[0][0], hg.method
    return None, None


def _build_reference_map(db: Session, interaction_nos: list[int]) -> dict[int, list]:
    """Map interaction_no -> [InteractionReferenceOut] via REF_LINK (batched)."""
    if not interaction_nos:
        return {}
    ref_map: dict[int, list] = {}
    ref_links = (
        db.query(RefLink)
        .options(joinedload(RefLink.reference))
        .filter(
            RefLink.tab_name == "INTERACTION",
            RefLink.primary_key.in_(interaction_nos),
        )
        .all()
    )
    for rl in ref_links:
        ref = rl.reference
        if not ref:
            continue
        ref_map.setdefault(rl.primary_key, []).append(
            InteractionReferenceOut(
                dbxref_id=ref.dbxref_id,
                pubmed=ref.pubmed,
                citation=ref.citation,
            )
        )
    return ref_map


def get_inferred_interactions(
    db: Session,
    target_feature: Feature,
    target_organism_name: str,
) -> list[InferredInteractionOut]:
    """Inferred interactions for a target-species gene, transferred from the
    C. albicans ortholog's curated interactions. Empty for C. albicans itself."""
    if not target_feature or target_organism_name == SOURCE_ORGANISM:
        return []

    # 1. C. albicans ortholog of the query gene.
    q = _load_with_homology(db, {target_feature.feature_no}).get(target_feature.feature_no)
    if not q:
        return []
    ca_gene, _ = _ortholog_in(q, SOURCE_ORGANISM)
    if not ca_gene:
        return []

    # 2. The C. albicans ortholog's curated interactions (+ C. albicans partners).
    ca_gene = (
        db.query(Feature)
        .options(
            joinedload(Feature.feat_interact)
            .joinedload(FeatInteract.interaction)
            .joinedload(Interaction.feat_interact)
            .joinedload(FeatInteract.feature)
        )
        .filter(Feature.feature_no == ca_gene.feature_no)
        .first()
    )
    if not ca_gene or not ca_gene.feat_interact:
        return []

    interactions: dict[int, tuple] = {}  # interaction_no -> (interaction, [partner Feature])
    for fi in ca_gene.feat_interact:
        inter = fi.interaction
        if not inter or inter.interaction_no in interactions:
            continue
        partners = [
            ofi.feature for ofi in inter.feat_interact
            if ofi.feature and ofi.feature.feature_no != ca_gene.feature_no
        ]
        if not partners:
            partners = [ca_gene]  # self-interaction
        interactions[inter.interaction_no] = (inter, partners)

    if not interactions:
        return []

    # 3. Batch-map every C. albicans partner to its ortholog in the target species.
    partner_nos = {p.feature_no for (_, ps) in interactions.values() for p in ps}
    partner_feats = _load_with_homology(db, partner_nos)
    target_ortholog: dict[int, tuple] = {}  # ca partner feature_no -> (Feature, method)
    for pno, pf in partner_feats.items():
        ortho, method = _ortholog_in(pf, target_organism_name)
        if ortho:
            target_ortholog[pno] = (ortho, method)

    # 4. Batch references for all source interactions.
    ref_map = _build_reference_map(db, list(interactions.keys()))

    ca_display = ca_gene.gene_name or ca_gene.feature_name
    results: list[InferredInteractionOut] = []
    seen_pairs = set()
    for inter, partners in interactions.values():
        itype = 'genetic' if (inter.experiment_type or '') in GENETIC_TYPES else 'physical'
        refs = ref_map.get(inter.interaction_no, [])
        for p_ca in partners:
            mapped = target_ortholog.get(p_ca.feature_no)
            if not mapped:
                continue
            p_t, method = mapped
            # Dedup identical inferred edges (same partner + experiment type).
            key = (p_t.feature_no, inter.experiment_type)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            results.append(InferredInteractionOut(
                interactor_feature_name=p_t.feature_name,
                interactor_gene_name=p_t.gene_name,
                interaction_type=itype,
                experiment_type=inter.experiment_type,
                description=inter.description,
                source=inter.source,
                source_organism=SOURCE_ORGANISM,
                source_gene_name=ca_display,
                source_gene_feature_name=ca_gene.feature_name,
                source_partner_name=p_ca.gene_name or p_ca.feature_name,
                source_partner_feature_name=p_ca.feature_name,
                ortholog_method=method,
                references=refs,
            ))
    return results
