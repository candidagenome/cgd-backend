"""
Regulation Service.

Serves transcriptional regulatory associations for the locus page Regulation
tab from two sources:

  * PathoYeastract: per-gene JSON files written by
    scripts/regulation/load_pathoyeastract_flatfiles.py (documented
    associations) and harvest_pathoyeastract.py (predicted sites) under
    <regulation data dir>/<organism_abbrev>/<ORF>.json;
  * CGD curation: MULTI.REGULATION rows entered with the Curate Regulation
    tool, merged in as evidence rows with source "CGD", and RNA accumulation
    phenotype annotations of regulators (source "CGD phenotype", see
    regulation_phenotype.py).

TF and target names are resolved to CGD features (so they link to locus
pages) and PubMed IDs to CGD references.
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

from cgd.api.services import regulation_phenotype
from cgd.api.services.locus_service import _filter_features_by_preference
from cgd.core.settings import settings
from cgd.models.models import Feature, Organism, Reference, Regulation
from cgd.schemas.regulation_schema import (
    BindingSite,
    MatchedConsensus,
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


def _site_summary(sites: list[BindingSite], positions: dict) -> tuple[int, int, list[MatchedConsensus]]:
    """Distinct site locations, those on both strands, and per-motif hit counts.

    PathoYeastract lists a site once per strand, so a palindromic match (e.g.
    CANNTG) appears on both; count locations instead. Its per-motif positions
    give a + strand match by its start and a - strand match by its end + 1;
    a hit inside a merged block of overlapping matches has no site of its own
    and counts as its own location.
    """
    strands_at: dict[tuple[int, int], set] = {}
    for site in sites:
        strands_at.setdefault((site.start, site.end), set()).add(site.strand)
    by_start = {start: (start, end) for (start, end), st in strands_at.items() if "+" in st}
    by_end = {end + 1: (start, end) for (start, end), st in strands_at.items() if "-" in st}
    matched = []
    for motif, hits in (positions or {}).items():
        locations = ({by_start.get(p, ("+", p)) for p in hits.get("+") or []}
                     | {by_end.get(p, ("-", p)) for p in hits.get("-") or []})
        if locations:
            matched.append(MatchedConsensus(motif=motif, sites=len(locations)))
    matched.sort(key=lambda m: (-m.sites, m.motif))
    return len(strands_at), sum(len(st) > 1 for st in strands_at.values()), matched


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
                mutant_type=row.get("mutant_type"),
                source=row.get("source") or "PathoYeastract",
                curation_note=row.get("curation_note"),
            ))
        sites = []
        for strand, key in (("+", "plus"), ("-", "minus")):
            for start, end in (entry.get("sites") or {}).get(key) or []:
                sites.append(BindingSite(strand=strand, start=start, end=end))
        site_count, both_strand_count, matched = _site_summary(
            sites, (entry.get("sites") or {}).get("consensus_positions"))
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
            site_count=site_count,
            both_strand_count=both_strand_count,
            matched_consensus=matched,
            pathoyeastract_url=(
                f"{PATHOYEASTRACT_URL}/{slug}/view.php?"
                + urlencode({"existing": "regulation", "proteinname": protein, "orfname": orf})
                if slug and any(e.source == "PathoYeastract" for e in evidence) else None
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
        for flag in ("documented", "binding", "expression", "activated", "repressed", "potential",
                     "cgd_curated"):
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


def _curated_evidence(row: Regulation) -> dict:
    """A CGD-curated REGULATION row in the per-gene JSON evidence format."""
    ref = row.reference
    return {
        "pubmed": ref.pubmed if ref else None,
        "citation": ref.citation if ref else None,
        "evidence_code": row.evidence_code,
        "experiment": row.experiment,
        "mutant_type": row.mutant_type,
        "association_type": row.association_type,
        "strain": row.strain_background,
        "environmental_group": row.condition_group,
        "environmental_condition": row.environmental_condition,
        "log2fc": row.log2fc,
        "source": "CGD",
        "curation_note": row.regulation_note,
    }


def _covered_by_pathoyeastract(entry: dict, pubmed: Optional[int]) -> bool:
    """Whether PathoYeastract's entry for this TF already cites the paper.

    The gene page then shows only PathoYeastract's copy; a CGD row stays in
    the database and is flagged on the curation page for review.
    """
    return bool(pubmed) and any(
        e.get("pubmed") == pubmed and (e.get("source") or "PathoYeastract") == "PathoYeastract"
        for e in entry.get("evidence") or []
    )


def _cgd_evidence(db: Session, feature: Feature) -> tuple[list[tuple], list[tuple]]:
    """CGD evidence for a gene: curated REGULATION rows and RNA accumulation phenotypes.

    Returns (as_target, as_tf): as_target items are ((tf_name, tf_gene), evidence)
    for regulators of this gene; as_tf items are ((target_name, target_gene),
    evidence) for genes this one regulates. Phenotype rows count only when the
    mutated gene is a regulator (regulation_phenotype.regulator_sources).
    """
    as_target, as_tf = [], []
    for row in (
        db.query(Regulation)
        .options(joinedload(Regulation.tf_feature), joinedload(Regulation.reference))
        .filter(Regulation.target_feature_no == feature.feature_no)
        .all()
    ):
        as_target.append(((row.tf_feature.feature_name, row.tf_feature.gene_name), _curated_evidence(row)))
    for row in (
        db.query(Regulation)
        .options(joinedload(Regulation.target_feature), joinedload(Regulation.reference))
        .filter(Regulation.tf_feature_no == feature.feature_no)
        .all()
    ):
        as_tf.append(((row.target_feature.feature_name, row.target_feature.gene_name), _curated_evidence(row)))

    organism = feature.organism
    if organism is None:
        return as_target, as_tf
    rows = [row for row in regulation_phenotype.phenotype_rows(db, feature.organism_no)
            if regulation_phenotype.shown(row)]
    if not rows:
        return as_target, as_tf
    me = regulation_phenotype.locus_name(feature.feature_name)
    regulators = set().union(*regulation_phenotype.regulator_sources(
        db, feature.organism_no, organism.organism_abbrev).values())
    for row in rows:
        regulator = row["regulator"]
        if regulator[0] not in regulators:
            continue
        if regulator[0] == me:
            for target in row["targets"]:
                for ref in row["references"]:
                    as_tf.append((target, regulation_phenotype.phenotype_evidence(row, ref)))
        elif any(t[0] == me for t in row["targets"]):
            for ref in row["references"]:
                as_target.append((regulator, regulation_phenotype.phenotype_evidence(row, ref)))
    return as_target, as_tf


def _merge_curated(db: Session, feature: Feature, data: dict) -> int:
    """Add CGD evidence (curated rows, RNA accumulation phenotypes) to a gene's PathoYeastract data.

    Evidence where the gene is the target joins its regulators (appended to
    the same TF's PathoYeastract evidence when there is any); evidence where
    it is the regulator joins its target list. Evidence whose TF, target and
    paper PathoYeastract already has is left out. Returns the number of CGD
    evidence rows shown.
    """
    as_target, as_tf = _cgd_evidence(db, feature)
    shown = 0
    if as_target:
        entries = data.setdefault("regulators", [])
        by_tf = {e.get("tf_orf"): e for e in entries if e.get("tf_orf")}
        for (tf_name, tf_gene), evidence in as_target:
            entry = by_tf.get(tf_name)
            if entry and _covered_by_pathoyeastract(entry, evidence["pubmed"]):
                continue
            if entry is None:
                entry = by_tf[tf_name] = {
                    "tf_protein": tf_gene or tf_name,
                    "tf_orf": tf_name,
                    "evidence": [],
                    "potential_status": "not_scanned",
                    "consensus": None,
                    "sites": None,
                }
                entries.append(entry)
            entry["evidence"] = list(entry.get("evidence") or []) + [evidence]
            shown += 1
    if as_tf:
        tf = data.get("tf") or {"protein": feature.gene_name or feature.feature_name,
                                "consensus": None, "targets": []}
        targets = list(tf.get("targets") or [])
        abbrev = feature.organism.organism_abbrev if feature.organism else None
        target_files: dict[str, dict] = {}
        for (target_name, _gene), evidence in as_tf:
            if target_name not in target_files:
                target_files[target_name] = _load_gene_file(abbrev, target_name) or {}
            entry = next((e for e in target_files[target_name].get("regulators") or []
                          if e.get("tf_orf") == feature.feature_name), None)
            if entry and _covered_by_pathoyeastract(entry, evidence["pubmed"]):
                continue
            shown += 1
            targets.append({
                "orf": target_name,
                "documented": True,
                "binding": evidence["evidence_code"] == "Direct",
                "expression": evidence["evidence_code"] == "Indirect",
                "activated": evidence["association_type"] == "Positive",
                "repressed": evidence["association_type"] == "Negative",
                "potential": False,
                "cgd_curated": True,
            })
        data["tf"] = {**tf, "targets": targets}
    return shown


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
    if slug is not None:
        out.source = RegulationSource(
            url=f"{PATHOYEASTRACT_URL}/{slug}/",
            gene_url=f"{PATHOYEASTRACT_URL}/{slug}/view.php?"
                     + urlencode({"existing": "locus", "orfname": feature.feature_name}),
        )
    data = (_load_gene_file(abbrev, feature.feature_name) if slug else None) or {}
    out.cgd_curated_count = _merge_curated(db, feature, data)
    if not data.get("regulators") and not data.get("tf"):
        return out

    index = _name_index(db, feature.organism_no)
    pmids = [row.get("pubmed") for entry in data.get("regulators") or [] for row in entry.get("evidence") or []]
    refs = _references_by_pubmed(db, pmids)
    source = data.get("source") or {}

    out.has_data = True
    if out.source:
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
