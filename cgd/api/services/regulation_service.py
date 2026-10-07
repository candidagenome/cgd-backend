"""
Regulation Service.

Serves transcriptional regulatory associations from PathoYeastract for the
locus page Regulation tab. The data are per-gene JSON files written by
scripts/regulation/harvest_pathoyeastract.py under
<regulation data dir>/<organism_abbrev>/<ORF>.json; this service joins them
to CGD: TF and target names are resolved to CGD features (so they link to
locus pages) and PubMed IDs to CGD references.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urlencode

from sqlalchemy import func, or_
from sqlalchemy.orm import Session, joinedload

from cgd.api.services.locus_service import _filter_features_by_preference
from cgd.core.settings import settings
from cgd.models.models import Feature, Organism, Reference
from cgd.schemas.regulation_schema import (
    BindingSite,
    Promoter,
    RegulationDetailsResponse,
    RegulationEvidence,
    RegulationForOrganism,
    RegulationGene,
    RegulationReference,
    RegulationSource,
    RegulationTarget,
    Regulator,
    TargetCounts,
    TfTargets,
)

logger = logging.getLogger(__name__)

PATHOYEASTRACT_URL = "https://yeastract-plus.org/pathoyeastract"

# CGD ORGANISM.organism_abbrev -> PathoYeastract species slug.
# C. dubliniensis is not covered by PathoYeastract.
PATHOYEASTRACT_SPECIES = {
    "C_albicans_SC5314": "calbicans",
    "C_glabrata_CBS138": "cglabrata",
    "C_parapsilosis_CDC317": "cparapsilosis",
    "C_tropicalis": "ctropicalis",
    "C_auris_B8441": "cauris",
}

# PathoYeastract lists C. albicans targets on both haplotypes; CGD locus
# pages are keyed on the A haplotype.
_ALBICANS_B_HAPLOTYPE = re.compile(r"^(C[1-7R]_\d{5}[WC])_B$")


def _data_dir() -> Path:
    if settings.regulation_data_dir:
        return Path(settings.regulation_data_dir)
    return Path(settings.cgd_data_dir) / "regulation" / "pathoyeastract"


def _load_gene_file(organism_abbrev: str, feature_name: str) -> Optional[dict]:
    path = _data_dir() / organism_abbrev / f"{feature_name}.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        logger.error("Unreadable regulation file %s: %s", path, exc)
        return None


_NAME_INDEX_CACHE: dict[int, dict[str, tuple[str, Optional[str]]]] = {}


def _name_index(db: Session, organism_no: int) -> dict[str, tuple[str, Optional[str]]]:
    """Upper-cased feature name / gene name -> (feature_name, gene_name) for one organism.

    Built once per organism per process; target lists run to thousands of
    genes, so they are resolved in memory rather than with IN queries.
    """
    index = _NAME_INDEX_CACHE.get(organism_no)
    if index is not None:
        return index
    rows = (
        db.query(Feature.feature_name, Feature.gene_name)
        .filter(Feature.organism_no == organism_no)
        .filter(func.lower(Feature.feature_type) != "allele")
        .all()
    )
    index = {}
    for feature_name, gene_name in rows:
        index[feature_name.upper()] = (feature_name, gene_name)
    for feature_name, gene_name in rows:
        # Systematic names win over gene names; legacy orf19 rows never win a gene name
        if gene_name and not feature_name.startswith(("orf19.", "orf21.")):
            index.setdefault(gene_name.upper(), (feature_name, gene_name))
    _NAME_INDEX_CACHE[organism_no] = index
    return index


def _resolve_gene(name: str, orf: Optional[str], index: dict) -> RegulationGene:
    """Resolve a PathoYeastract TF/target to a CGD feature.

    `orf` is the systematic name when known; otherwise `name` may be a
    systematic name, a gene name, or a protein name such as 'Mrr1p'.
    """
    candidates = [orf] if orf else []
    candidates.append(name)
    haplotype_a = _ALBICANS_B_HAPLOTYPE.match(orf or name)
    if haplotype_a:
        candidates.append(f"{haplotype_a.group(1)}_A")
    if name.endswith("p") and len(name) > 2:
        candidates.append(name[:-1])
    for candidate in candidates:
        hit = index.get(candidate.upper())
        if hit:
            feature_name, gene_name = hit
            return RegulationGene(name=name, feature_name=feature_name, gene_name=gene_name,
                                  display_name=gene_name or feature_name)
    return RegulationGene(name=name, display_name=name)


def _references_by_pubmed(db: Session, pmids: Iterable[int]) -> dict[int, Reference]:
    pmids = sorted({p for p in pmids if p})
    out: dict[int, Reference] = {}
    for start in range(0, len(pmids), 900):
        chunk = pmids[start:start + 900]
        for ref in db.query(Reference).filter(Reference.pubmed.in_(chunk)).all():
            out[ref.pubmed] = ref
    return out


def _build_regulators(entries: list[dict], orf: str, slug: str, index: dict,
                      refs: dict[int, Reference]) -> list[Regulator]:
    regulators = []
    for entry in entries:
        if not entry.get("evidence") and entry.get("potential_status") != "found":
            continue
        evidence = []
        for row in entry.get("evidence") or []:
            ref = refs.get(row.get("pubmed"))
            evidence.append(RegulationEvidence(
                reference=RegulationReference(
                    pubmed=row.get("pubmed"),
                    citation=ref.citation if ref else row.get("citation"),
                    dbxref_id=ref.dbxref_id if ref else None,
                ),
                evidence_code=row.get("evidence_code"),
                experiment=row.get("experiment"),
                association_type=row.get("association_type"),
                strain=row.get("strain"),
                environmental_group=row.get("environmental_group"),
                environmental_condition=row.get("environmental_condition"),
                log2fc=row.get("log2fc"),
            ))
        sites = []
        for strand, key in (("+", "plus"), ("-", "minus")):
            for start, end in (entry.get("sites") or {}).get(key) or []:
                sites.append(BindingSite(strand=strand, start=start, end=end))
        protein = entry["tf_protein"]
        codes = {e.evidence_code for e in evidence}
        signs = {e.association_type for e in evidence}
        regulators.append(Regulator(
            tf=_resolve_gene(protein, entry.get("tf_orf"), index),
            documented=bool(evidence),
            binding_evidence="Direct" in codes,
            expression_evidence="Indirect" in codes,
            activator="Positive" in signs,
            repressor="Negative" in signs,
            reference_count=len({e.reference.pubmed for e in evidence if e.reference.pubmed}),
            evidence=evidence,
            potential=entry.get("potential_status") == "found",
            consensus=entry.get("consensus") or [],
            sites=sorted(sites, key=lambda s: s.start),
            pathoyeastract_url=(
                f"{PATHOYEASTRACT_URL}/{slug}/view.php?"
                + urlencode({"existing": "regulation", "proteinname": protein, "orfname": orf})
                if evidence else None
            ),
        ))
    # Best-supported first: binding evidence, then both documented and predicted,
    # then by number of papers
    regulators.sort(key=lambda r: (not r.binding_evidence, not (r.documented and r.potential),
                                   not r.documented, -r.reference_count, r.tf.display_name.upper()))
    return regulators


def _build_tf_targets(tf: dict, index: dict) -> TfTargets:
    merged: dict[str, RegulationTarget] = {}
    for row in tf.get("targets") or []:
        gene = _resolve_gene(row["orf"], None, index)
        key = gene.feature_name or gene.name
        target = merged.get(key)
        if target is None:
            target = merged[key] = RegulationTarget(gene=gene)
        # A and B haplotype rows collapse onto one CGD locus
        for flag in ("documented", "binding", "expression", "activated", "repressed", "potential"):
            if row.get(flag):
                setattr(target, flag, True)
    targets = sorted(merged.values(), key=lambda t: (not t.binding, not t.documented,
                                                     t.gene.display_name.upper()))
    counts = TargetCounts(
        documented=sum(t.documented for t in targets),
        binding=sum(t.binding for t in targets),
        expression=sum(t.expression for t in targets),
        activated=sum(t.activated for t in targets),
        repressed=sum(t.repressed for t in targets),
        potential=sum(t.potential for t in targets),
        documented_and_potential=sum(t.documented and t.potential for t in targets),
    )
    return TfTargets(tf_protein=tf["protein"], consensus=tf.get("consensus") or [],
                     counts=counts, targets=targets)


def _regulation_for_feature(db: Session, feature: Feature) -> RegulationForOrganism:
    organism = feature.organism
    abbrev = organism.organism_abbrev if organism else None
    slug = PATHOYEASTRACT_SPECIES.get(abbrev)
    out = RegulationForOrganism(
        locus_display_name=feature.gene_name or feature.feature_name,
        feature_name=feature.feature_name,
        gene_name=feature.gene_name,
        covered=slug is not None,
    )
    if slug is None:
        return out
    out.source = RegulationSource(
        url=f"{PATHOYEASTRACT_URL}/{slug}/",
        gene_url=f"{PATHOYEASTRACT_URL}/{slug}/view.php?"
                 + urlencode({"existing": "locus", "orfname": feature.feature_name}),
    )
    data = _load_gene_file(abbrev, feature.feature_name)
    if not data:
        return out

    index = _name_index(db, feature.organism_no)
    pmids = [row.get("pubmed") for entry in data.get("regulators") or [] for row in entry.get("evidence") or []]
    refs = _references_by_pubmed(db, pmids)
    source = data.get("source") or {}

    out.has_data = True
    out.source.citation_doi = source.get("citation_doi")
    out.source.retrieved = source.get("retrieved")
    out.regulators = _build_regulators(data.get("regulators") or [], feature.feature_name, slug, index, refs)
    if data.get("promoter"):
        out.promoter = Promoter(**data["promoter"])
    if data.get("tf"):
        out.tf_targets = _build_tf_targets(data["tf"], index)
    return out


def get_regulation_details(db: Session, name: str) -> RegulationDetailsResponse:
    """Regulation data for a locus, keyed by organism name."""
    n = name.strip()
    features = (
        db.query(Feature)
        .options(joinedload(Feature.organism))
        .filter(
            or_(
                func.upper(Feature.gene_name) == func.upper(n),
                func.upper(Feature.feature_name) == func.upper(n),
                func.upper(Feature.dbxref_id) == func.upper(n),
            )
        )
        .filter(func.lower(Feature.feature_type) != "allele")
        .all()
    )
    features = _filter_features_by_preference(db, features)

    results: dict[str, RegulationForOrganism] = {}
    for feature in features:
        organism: Optional[Organism] = feature.organism
        if organism is None or organism.organism_name in results:
            continue
        results[organism.organism_name] = _regulation_for_feature(db, feature)
    return RegulationDetailsResponse(results=results)
