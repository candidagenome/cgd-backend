"""
Regulators and phenotype-derived expression evidence for the Regulation tab.

Which genes count as regulators (curator decision, 2026-11-04): a gene with a
DNA-binding transcription factor GO annotation (GO:0003700 or a descendant,
not NOT-qualified), a PathoYeastract regulator (the loader's
_regulators.json), or a gene a curator admitted (MULTI.REGULATOR_ALLOW).

CGD's "RNA accumulation" phenotype annotations record expression changes in
single-gene mutants, with the affected genes as Reporter experiment
properties. Rows whose mutated gene is a regulator are shown on the tab as
expression evidence (source "CGD phenotype"), so curators do not enter them
twice. Reporter text is free ("CDR2 mRNA", "CDR1-RLUC", "ERG6|ERG11|CDR1");
it is split into gene names, and words that are not genes are reported for
review rather than guessed.
"""
from __future__ import annotations

import json
import logging
import re
import time
from collections import defaultdict
from typing import Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from cgd.models.models import (
    Alias,
    ExptExptprop,
    ExptProperty,
    FeatAlias,
    Feature,
    PhenoAnnotation,
    Phenotype,
    Reference,
    RefLink,
    RegulatorAllow,
)

logger = logging.getLogger(__name__)

OBSERVABLE = "RNA accumulation"
TF_GOID = 3700  # DNA-binding transcription factor activity
CACHE_SECONDS = 600

# Mutant types that remove or reduce the regulator, and those that add to it
LOSS_OF_FUNCTION = {"null", "reduction of function", "repressible", "conditional", "dominant negative"}
GAIN_OF_FUNCTION = {"overexpression", "gain of function", "activation"}
MUTANT_TYPES = ["null", "reduction of function", "repressible", "conditional", "overexpression",
                "gain of function", "activation", "unspecified"]

# Phenotype qualifier -> change in the reporter's expression
_QUALIFIER_CHANGE = {"increased": "up", "decreased": "down", "absent": "down", "abnormal": "changed"}

# Reporter words that are not genes: molecule types, tags and connectives
_NON_GENE_WORDS = {
    "MRNA", "RNA", "RRNA", "TRNA", "TRANSCRIPT", "TRANSCRIPTS", "UNSPLICED", "SPLICED", "TOTAL", "LEVEL",
    "LEVELS", "PROMOTER", "FUSION", "REPORTER", "AND", "RLUC", "LUC", "LUCIFERASE", "GFP", "YFP", "RFP",
    "MCHERRY", "LACZ", "HA", "MYC", "FLAG", "TAP",
}
_ALBICANS_B = re.compile(r"^(C[1-7R]_\d{5}[WC])_B$")

_cache: dict[tuple, tuple[float, object]] = {}


def _cached(key: tuple, build):
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < CACHE_SECONDS:
        return hit[1]
    value = build()
    _cache[key] = (time.monotonic(), value)
    return value


def clear_cache() -> None:
    _cache.clear()


def infer_association(mutant_type: Optional[str], change: Optional[str]) -> str:
    """Regulator effect from how it was mutated and how the target's expression changed.

    Losing an activator lowers its targets; overexpressing it raises them.
    """
    mutant = (mutant_type or "").lower()
    if change not in ("up", "down"):
        return "N/A"
    if mutant in LOSS_OF_FUNCTION:
        return "Positive" if change == "down" else "Negative"
    if mutant in GAIN_OF_FUNCTION:
        return "Positive" if change == "up" else "Negative"
    return "N/A"


def locus_name(feature_name: str) -> str:
    """C. albicans B allele -> its A-allele locus name (the name tab data is keyed on)."""
    match = _ALBICANS_B.match(feature_name)
    return f"{match.group(1)}_A" if match else feature_name


# ----------------------------------------------------------------------
# Regulators
# ----------------------------------------------------------------------
def _go_transcription_factors(db: Session, organism_no: int) -> set[str]:
    rows = db.execute(text("""
        SELECT DISTINCT f.feature_name
        FROM go_annotation ga
        JOIN feature f ON f.feature_no = ga.feature_no
        WHERE f.organism_no = :organism_no
          AND (ga.go_no IN (SELECT go_no FROM go WHERE goid = :goid)
               OR ga.go_no IN (SELECT gp.child_go_no FROM go_path gp JOIN go a ON a.go_no = gp.ancestor_go_no
                               WHERE a.goid = :goid))
          AND EXISTS (SELECT 1 FROM go_ref gr WHERE gr.go_annotation_no = ga.go_annotation_no
                      AND NOT EXISTS (SELECT 1 FROM go_qualifier gq WHERE gq.go_ref_no = gr.go_ref_no
                                      AND UPPER(gq.qualifier) = 'NOT'))
    """), {"organism_no": organism_no, "goid": TF_GOID})
    return {locus_name(r[0]) for r in rows}


def _pathoyeastract_regulators(organism_abbrev: str) -> set[str]:
    from cgd.api.services.regulation_service import _data_dir

    path = _data_dir() / organism_abbrev / "_regulators.json"
    try:
        return {locus_name(name) for name in json.loads(path.read_text())}
    except FileNotFoundError:
        return set()
    except (OSError, ValueError) as exc:
        logger.error("Unreadable regulator list %s: %s", path, exc)
        return set()


def admitted_regulators(db: Session, organism_no: int) -> dict[str, dict]:
    """Curator-admitted regulators: locus name -> {feature_name, gene_name, note, created_by, date_created}."""
    rows = (
        db.query(RegulatorAllow, Feature)
        .join(Feature, Feature.feature_no == RegulatorAllow.feature_no)
        .filter(Feature.organism_no == organism_no)
        .all()
    )
    return {
        locus_name(f.feature_name): {
            "feature_name": f.feature_name, "gene_name": f.gene_name, "note": allow.allow_note,
            "created_by": allow.created_by,
            "date_created": allow.date_created.date().isoformat() if allow.date_created else None,
        }
        for allow, f in rows
    }


def regulator_sources(db: Session, organism_no: int, organism_abbrev: str) -> dict[str, set[str]]:
    """{'go': ..., 'pathoyeastract': ..., 'admitted': ...} sets of locus names.

    The GO and PathoYeastract sets are cached; admissions are read each time,
    so they count at once in every API worker.
    """
    cached = _cached(("regulators", organism_no), lambda: {
        "go": _go_transcription_factors(db, organism_no),
        "pathoyeastract": _pathoyeastract_regulators(organism_abbrev),
    })
    return {**cached, "admitted": set(admitted_regulators(db, organism_no))}


def is_regulator(db: Session, organism_no: int, organism_abbrev: str, feature_name: str) -> bool:
    name = locus_name(feature_name)
    return any(name in names for names in regulator_sources(db, organism_no, organism_abbrev).values())


# ----------------------------------------------------------------------
# RNA accumulation phenotypes
# ----------------------------------------------------------------------
def split_reporter(reporter: str) -> list[str]:
    """Candidate gene names in a Reporter value, in order ('CDR1 and CDR2' -> [CDR1, CDR2]).

    Hyphenated words are kept whole ('CDR1-RLUC', 'MT-II'); reporter_genes()
    splits them when the whole word is not a gene.
    """
    value = re.sub(r"\s+and\s+", "|", reporter or "", flags=re.I)
    tokens = []
    for part in re.split(r"[|,;/]", value):
        for token in part.split():
            token = token.strip("()[]{}.:'\"-")
            if token and token.upper() not in _NON_GENE_WORDS and token not in tokens:
                tokens.append(token)
    return tokens


def reporter_genes(reporter: str, resolve) -> tuple[list, list[str]]:
    """Genes named in a Reporter value, and the words that are not genes.

    `resolve(name)` returns a gene or None. A hyphenated word that is not a
    gene is split ('CDR1-RLUC' -> CDR1; RLUC is a tag, not reported).
    """
    genes, unresolved = [], []
    for word in split_reporter(reporter):
        hit = resolve(word)
        parts = [(word, hit)] if hit or "-" not in word else [
            (p, resolve(p)) for p in word.split("-") if p and p.upper() not in _NON_GENE_WORDS]
        for part, gene in parts:
            if gene is None:
                unresolved.append(part)
            elif gene not in genes:
                genes.append(gene)
    return genes, unresolved


class _GeneNames:
    """Resolve reporter tokens to A-locus (feature_name, gene_name) within one organism."""

    def __init__(self, db: Session, organism_no: int):
        from cgd.api.services.regulation_service import _name_index

        self.db = db
        self.organism_no = organism_no
        self.index = _name_index(db, organism_no)
        self.aliases: dict[str, list[tuple[str, Optional[str]]]] = {}

    def load_aliases(self, tokens: set[str]) -> None:
        wanted = sorted({t.upper() for t in tokens if t.upper() not in self.index})
        for start in range(0, len(wanted), 900):
            rows = (
                self.db.query(func.upper(Alias.alias_name), Feature.feature_name, Feature.gene_name)
                .join(FeatAlias, FeatAlias.alias_no == Alias.alias_no)
                .join(Feature, Feature.feature_no == FeatAlias.feature_no)
                .filter(Feature.organism_no == self.organism_no,
                        func.upper(Alias.alias_name).in_(wanted[start:start + 900]))
                .all()
            )
            for alias, feature_name, gene_name in rows:
                if not feature_name.startswith(("orf19.", "orf21.")):
                    hits = self.aliases.setdefault(alias, [])
                    if (feature_name, gene_name) not in hits:
                        hits.append((feature_name, gene_name))

    def resolve(self, name: str) -> Optional[tuple[str, Optional[str]]]:
        hit = self.index.get(name.upper())
        if hit is None:
            hits = self.aliases.get(name.upper()) or []
            hit = hits[0] if len(hits) == 1 else None
        if hit is None:
            return None
        feature_name, gene_name = hit
        if feature_name.startswith(("orf19.", "orf21.")):
            # A legacy orf19 row: use the current locus with the same gene name
            current = self.index.get((gene_name or "").upper())
            if not current or current[0].startswith(("orf19.", "orf21.")):
                return None
            feature_name, gene_name = current
        name_a = locus_name(feature_name)
        if name_a != feature_name:
            feature_name, gene_name = self.index.get(name_a.upper(), (name_a, gene_name))
        return feature_name, gene_name


def phenotype_rows(db: Session, organism_no: int) -> list[dict]:
    """The organism's RNA accumulation annotations, with reporters resolved to genes.

    Each row: pheno_annotation_no, regulator (feature_name, gene_name), mutant_type,
    qualifier, change (up/down/changed/None), association_type, reporter,
    targets [(feature_name, gene_name)], unresolved [tokens], strain, allele,
    condition, details, references [{pubmed, citation, dbxref_id}].
    """
    return _cached(("phenotype", organism_no), lambda: _build_phenotype_rows(db, organism_no))


def _build_phenotype_rows(db: Session, organism_no: int) -> list[dict]:
    annotations = (
        db.query(PhenoAnnotation.pheno_annotation_no, PhenoAnnotation.experiment_no, Feature.feature_name,
                 Feature.gene_name, Phenotype.mutant_type, Phenotype.qualifier, Phenotype.experiment_type)
        .join(Phenotype, Phenotype.phenotype_no == PhenoAnnotation.phenotype_no)
        .join(Feature, Feature.feature_no == PhenoAnnotation.feature_no)
        .filter(Feature.organism_no == organism_no, func.lower(Phenotype.observable) == OBSERVABLE.lower())
        .all()
    )
    if not annotations:
        return []
    experiment_nos = sorted({a.experiment_no for a in annotations if a.experiment_no})
    props: dict[int, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for start in range(0, len(experiment_nos), 900):
        for experiment_no, ptype, value in (
            db.query(ExptExptprop.experiment_no, ExptProperty.property_type, ExptProperty.property_value)
            .join(ExptProperty, ExptProperty.expt_property_no == ExptExptprop.expt_property_no)
            .filter(ExptExptprop.experiment_no.in_(experiment_nos[start:start + 900]))
            .all()
        ):
            props[experiment_no][ptype].append(value)
    annotation_nos = [a.pheno_annotation_no for a in annotations]
    refs: dict[int, list[dict]] = defaultdict(list)
    for start in range(0, len(annotation_nos), 900):
        for primary_key, ref in (
            db.query(RefLink.primary_key, Reference)
            .join(Reference, Reference.reference_no == RefLink.reference_no)
            .filter(RefLink.tab_name == "PHENO_ANNOTATION",
                    RefLink.primary_key.in_(annotation_nos[start:start + 900]))
            .all()
        ):
            refs[primary_key].append({"pubmed": ref.pubmed, "citation": ref.citation, "dbxref_id": ref.dbxref_id})

    names = _GeneNames(db, organism_no)
    reporters = {a.pheno_annotation_no: props[a.experiment_no].get("Reporter", []) for a in annotations}
    names.load_aliases({part for values in reporters.values() for v in values for word in split_reporter(v)
                        for part in [word, *word.split("-")] if part})

    rows = []
    for a in annotations:
        regulator = names.resolve(a.feature_name) or (locus_name(a.feature_name), a.gene_name)
        p = props[a.experiment_no]
        change = _QUALIFIER_CHANGE.get((a.qualifier or "").lower())
        for reporter in reporters[a.pheno_annotation_no]:
            targets, unresolved = reporter_genes(reporter, names.resolve)
            rows.append({
                "pheno_annotation_no": a.pheno_annotation_no,
                "regulator": regulator,
                "mutant_type": a.mutant_type,
                "qualifier": a.qualifier,
                "experiment_type": a.experiment_type,
                "change": change,
                "association_type": infer_association(a.mutant_type, change),
                "reporter": reporter,
                "targets": targets,
                "unresolved": unresolved,
                "strain": "; ".join(p.get("strain_background", [])) or None,
                "allele": "; ".join(p.get("Allele", [])) or None,
                "condition": "; ".join(p.get("Condition", [])) or None,
                "details": "; ".join(p.get("Details", [])) or None,
                "references": sorted(refs.get(a.pheno_annotation_no, []), key=lambda r: r["pubmed"] or 0),
            })
    return rows


def shown(row: dict) -> bool:
    """Whether a phenotype row is expression evidence: a direction and at least one target gene."""
    return row["change"] is not None and bool(row["targets"])


def phenotype_evidence(row: dict, ref: dict) -> dict:
    """A phenotype row in the per-gene JSON evidence format, for one of its references."""
    qualifier = (row["qualifier"] or "").lower()
    notes = [f"Reporter: {row['reporter']}"]
    if row["allele"]:
        notes.append(f"Allele: {row['allele']}")
    if row["details"]:
        notes.append(row["details"])
    return {
        "pubmed": ref["pubmed"],
        "citation": ref["citation"],
        "evidence_code": "Indirect",
        "experiment": f"Phenotype: {OBSERVABLE} {qualifier}".strip(),
        "mutant_type": row["mutant_type"],
        "association_type": row["association_type"],
        "strain": row["strain"],
        "environmental_group": None,
        "environmental_condition": row["condition"],
        "log2fc": None,
        "source": "CGD phenotype",
        "curation_note": "; ".join(notes),
    }


__all__ = [
    "MUTANT_TYPES",
    "admitted_regulators",
    "clear_cache",
    "infer_association",
    "is_regulator",
    "locus_name",
    "phenotype_evidence",
    "phenotype_rows",
    "regulator_sources",
    "reporter_genes",
    "shown",
    "split_reporter",
]
