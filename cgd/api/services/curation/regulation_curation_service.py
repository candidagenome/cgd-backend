"""
Regulation Curation Service.

Curator entry of transcriptional regulatory associations (regulator -> target
gene) from papers PathoYeastract has not covered yet. Rows live in
MULTI.REGULATION (scripts/regulation/create_regulation_table.py), one row per
evidence observation, with the fields PathoYeastract records, and are merged
into the locus Regulation tab by cgd/api/services/regulation_service.py.

Two kinds of entry (curator decision, 2026-11-04); large-scale expression
studies are left to PathoYeastract:

  * DNA binding (Direct): targeted assays (EMSA, ChIP-qPCR, footprinting, ...)
    and genome-wide binding (ChIP-seq, ChIP-chip, ChEC-seq, ...), whose peak
    files assign_peaks() turns into a target list;
  * expression change in a single-gene mutant (Indirect): the regulator is
    mutated (null, overexpression, ...) and a target's expression measured by
    a targeted assay; the effect follows from the mutant type and the change.

The regulator must be a regulator (regulation_phenotype.is_regulator: a GO
DNA-binding transcription factor, a PathoYeastract regulator, or a gene a
curator admitted with allow_regulator()).

A batch entry is one regulator, one paper and one experiment/condition, with a
pasted list of target genes; each target line may carry its own effect or
direction of change and log2 fold change. Names are resolved within one
organism; C. albicans B alleles and legacy orf19 names resolve to the Assembly
22 A-allele locus.
"""
from __future__ import annotations

import bisect
import logging
import re
from collections import defaultdict
from typing import Iterable, Optional

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, aliased, joinedload

from cgd.api.services import regulation_phenotype, regulation_service
from cgd.models.models import (
    Alias,
    FeatAlias,
    Feature,
    FeatLocation,
    Organism,
    Reference,
    Regulation,
    RegulatorAllow,
    Seq,
)

logger = logging.getLogger(__name__)

EVIDENCE_CODES = {
    "Direct": "DNA binding",
    "Indirect": "Expression change in a single-gene mutant",
}

# Experiment vocabulary, after PathoYeastract's names. Expression is targeted
# assays only: genome-wide expression studies are left to PathoYeastract.
EXPERIMENTS = {
    "Direct": [
        "ChIP",
        "EMSA",
        "DNA footprinting",
        "Yeast one-hybrid",
        "Reporter - wt vs mutant promoter sequence",
        "ChIP-chip",
        "ChIP-seq",
        "ChIP-exo",
        "ChEC-seq",
        "CUT&RUN",
    ],
    "Indirect": [
        "RT-qPCR",
        "Northern blot",
        "Reporter (lacZ/GFP/luciferase)",
        "Western blot",
    ],
}

# Binding experiments whose targets come from a peak file (assign_peaks)
GENOME_WIDE_EXPERIMENTS = ["ChIP-chip", "ChIP-seq", "ChIP-exo", "ChEC-seq", "CUT&RUN"]

EXPRESSION_CHANGES = {"up": "Increased in the mutant", "down": "Decreased in the mutant"}

# PathoYeastract's environmental condition groups
CONDITION_GROUPS = [
    "Unstressed log-phase growth (control)",
    "Biofilm formation",
    "Carbon source quality/availability",
    "Cell cycle/morphology",
    "Human niche conditions",
    "Ion/metal/phosphate/sulfur/vitamin availability",
    "Lipid supplementation",
    "Nitrogen source quality/availability",
    "Oxygen availability",
    "Pathogenicity",
    "Stress",
    "Unassigned",
]

ASSOCIATION_TYPES = ["Positive", "Negative", "N/A"]

_EFFECT_TOKENS = {
    "positive": "Positive", "activation": "Positive", "activated": "Positive", "activator": "Positive",
    "+": "Positive",
    "negative": "Negative", "repression": "Negative", "repressed": "Negative", "repressor": "Negative",
    "-": "Negative",
    "n/a": "N/A", "na": "N/A", "none": "N/A",
}

# Direction of a target's expression change in the mutant
_CHANGE_TOKENS = {
    "up": "up", "increased": "up", "increase": "up", "induced": "up", "higher": "up",
    "down": "down", "decreased": "down", "decrease": "down", "reduced": "down", "lower": "down",
    "absent": "down",
}

# Features that can be a TF or a target (not CDS, introns, UTRs, ...)
GENE_FEATURE_TYPES = (
    "ORF", "allele", "pseudogene", "blocked_reading_frame", "uORF",
    "tRNA", "rRNA", "snoRNA", "snRNA", "ncRNA",
)

MAX_TARGETS = 5000
MAX_PEAKS = 200000
DEFAULT_PROMOTER_WINDOW = 1000


class RegulationCurationError(Exception):
    """Raised for curator-facing regulation curation errors (HTTP 400)."""


def _clean(value: Optional[str], limit: int) -> Optional[str]:
    value = re.sub(r"\s+", " ", value or "").strip()
    if len(value) > limit:
        raise RegulationCurationError(f"'{value[:40]}...' is longer than {limit} characters.")
    return value or None


class RegulationCurationService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Vocabulary
    # ------------------------------------------------------------------
    @staticmethod
    def get_vocabulary() -> dict:
        return {
            "evidence_codes": [{"code": c, "label": label} for c, label in EVIDENCE_CODES.items()],
            "experiments": EXPERIMENTS,
            "genome_wide_experiments": GENOME_WIDE_EXPERIMENTS,
            "mutant_types": regulation_phenotype.MUTANT_TYPES,
            "expression_changes": [{"code": c, "label": label} for c, label in EXPRESSION_CHANGES.items()],
            "condition_groups": CONDITION_GROUPS,
            "association_types": ASSOCIATION_TYPES,
            "promoter_window": DEFAULT_PROMOTER_WINDOW,
        }

    # ------------------------------------------------------------------
    # Name resolution
    # ------------------------------------------------------------------
    def _organism(self, organism_abbrev: str) -> Organism:
        organism = (
            self.db.query(Organism)
            .filter(func.upper(Organism.organism_abbrev) == (organism_abbrev or "").upper())
            .first()
        )
        if not organism:
            raise RegulationCurationError(f"Unknown organism '{organism_abbrev}'.")
        return organism

    def resolve_names(self, organism: Organism, names: Iterable[str]) -> dict[str, dict]:
        """Resolve gene names within one organism.

        Returns {name: {"status": "ok"|"not_found"|"ambiguous", "feature": Feature|None,
        "candidates": [feature names]}}. Systematic names win, then standard
        gene names, then aliases (which carry orf19 IDs and B-allele names).
        """
        wanted = {n.strip().upper(): n.strip() for n in names if n and n.strip()}
        if not wanted:
            return {}
        features = (
            self.db.query(Feature)
            .filter(Feature.organism_no == organism.organism_no, Feature.feature_type.in_(GENE_FEATURE_TYPES))
            .all()
        )
        current = [f for f in features if not f.feature_name.upper().startswith(("ORF19.", "ORF21."))]
        by_name = {f.feature_name.upper(): f for f in current}

        def locus(f: Feature) -> Feature:
            # C. albicans B allele -> its A-allele locus, when there is one
            return by_name.get(f.feature_name[:-2].upper() + "_A", f) if f.feature_name.endswith("_B") else f

        by_gene: dict[str, dict[int, Feature]] = {}
        for f in current:
            if f.gene_name:
                by_gene.setdefault(f.gene_name.upper(), {})[f.feature_no] = f
        by_alias: dict[str, dict[int, Feature]] = {}
        upper_names = list(wanted)
        current_nos = {f.feature_no: f for f in current}
        for start in range(0, len(upper_names), 900):
            chunk = upper_names[start:start + 900]
            rows = (
                self.db.query(func.upper(Alias.alias_name), FeatAlias.feature_no)
                .join(FeatAlias, FeatAlias.alias_no == Alias.alias_no)
                .filter(func.upper(Alias.alias_name).in_(chunk))
                .all()
            )
            for alias, feature_no in rows:
                f = current_nos.get(feature_no)
                if f:
                    by_alias.setdefault(alias, {})[f.feature_no] = f

        out = {}
        for upper, original in wanted.items():
            hits: dict[int, Feature] = {}
            if upper in by_name:
                f = locus(by_name[upper])
                hits = {f.feature_no: f}
            for table in (by_gene, by_alias):
                if hits:
                    break
                for f in table.get(upper, {}).values():
                    f = locus(f)
                    hits[f.feature_no] = f
            if len(hits) == 1:
                out[original] = {"status": "ok", "feature": next(iter(hits.values())), "candidates": []}
            elif hits:
                out[original] = {"status": "ambiguous", "feature": None,
                                 "candidates": sorted(f.feature_name for f in hits.values())}
            else:
                out[original] = {"status": "not_found", "feature": None, "candidates": []}
        return out

    def _resolve_one(self, organism: Organism, name: str, role: str) -> Feature:
        result = self.resolve_names(organism, [name]).get(name.strip())
        if not result or result["status"] == "not_found":
            raise RegulationCurationError(f"{role} '{name}' not found in {organism.organism_name}.")
        if result["status"] == "ambiguous":
            raise RegulationCurationError(
                f"{role} '{name}' is ambiguous in {organism.organism_name}: {', '.join(result['candidates'])}."
            )
        return result["feature"]

    # ------------------------------------------------------------------
    # References
    # ------------------------------------------------------------------
    def _reference(self, pubmed) -> Reference:
        try:
            pubmed = int(str(pubmed).strip())
        except (TypeError, ValueError):
            raise RegulationCurationError("A numeric PubMed ID is required.")
        ref = self.db.query(Reference).filter(Reference.pubmed == pubmed).first()
        if not ref:
            raise RegulationCurationError(
                f"PubMed {pubmed} is not in the CGD reference database. "
                "Add the reference first, then curate its regulation data."
            )
        return ref

    @staticmethod
    def _reference_out(ref: Reference) -> dict:
        return {"reference_no": ref.reference_no, "pubmed": ref.pubmed, "dbxref_id": ref.dbxref_id,
                "citation": ref.citation}

    def get_reference(self, pubmed) -> dict:
        ref = self._reference(pubmed)
        return {**self._reference_out(ref), "associations": self._rows(Regulation.reference_no == ref.reference_no)}

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------
    @staticmethod
    def _gene_out(f: Feature) -> dict:
        return {"feature_name": f.feature_name, "gene_name": f.gene_name,
                "display_name": f.gene_name or f.feature_name}

    def _rows(self, *criteria) -> list[dict]:
        rows = (
            self.db.query(Regulation)
            .options(
                joinedload(Regulation.tf_feature).joinedload(Feature.organism),
                joinedload(Regulation.target_feature),
                joinedload(Regulation.reference),
            )
            .filter(*criteria)
            .all()
        )
        out = []
        for r in rows:
            organism = r.tf_feature.organism
            out.append({
                "regulation_no": r.regulation_no,
                "organism": organism.organism_abbrev if organism else None,
                "tf": self._gene_out(r.tf_feature),
                "target": self._gene_out(r.target_feature),
                "reference": self._reference_out(r.reference),
                "evidence_code": r.evidence_code,
                "experiment": r.experiment,
                "mutant_type": r.mutant_type,
                "association_type": r.association_type,
                "strain_background": r.strain_background,
                "condition_group": r.condition_group,
                "environmental_condition": r.environmental_condition,
                "log2fc": r.log2fc,
                "regulation_note": r.regulation_note,
                "created_by": r.created_by,
                "date_created": r.date_created.date().isoformat() if r.date_created else None,
                "in_pathoyeastract": self._in_pathoyeastract(organism, r),
            })
        out.sort(key=lambda x: (x["tf"]["display_name"].upper(), x["experiment"],
                                x["environmental_condition"] or "", x["target"]["display_name"].upper()))
        return out

    @staticmethod
    def _in_pathoyeastract(organism: Optional[Organism], r: Regulation) -> bool:
        """Whether PathoYeastract already lists this TF -> target for the same paper."""
        if organism is None or not r.reference or not r.reference.pubmed:
            return False
        data = regulation_service._load_gene_file(organism.organism_abbrev, r.target_feature.feature_name)
        for entry in (data or {}).get("regulators") or []:
            if entry.get("tf_orf") == r.tf_feature.feature_name:
                return any(e.get("pubmed") == r.reference.pubmed for e in entry.get("evidence") or [])
        return False

    def get_gene(self, name: str, organism_abbrev: str) -> dict:
        organism = self._organism(organism_abbrev)
        feature = self._resolve_one(organism, name, "Gene")
        return {
            "gene": self._gene_out(feature),
            "organism": organism.organism_abbrev,
            "as_tf": self._rows(Regulation.tf_feature_no == feature.feature_no),
            "as_target": self._rows(Regulation.target_feature_no == feature.feature_no),
        }

    # ------------------------------------------------------------------
    # Batch entry
    # ------------------------------------------------------------------
    @staticmethod
    def parse_target_lines(text: str) -> list[dict]:
        """Parse 'GENE [effect | up/down] [log2FC]' lines (tab, comma or space separated)."""
        out = []
        for number, raw in enumerate((text or "").splitlines(), start=1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            tokens = [t for t in re.split(r"[\t,;]+|\s+", line) if t]
            entry = {"line": number, "name": tokens[0], "effect": None, "change": None, "log2fc": None,
                     "error": None}
            for token in tokens[1:]:
                effect = _EFFECT_TOKENS.get(token.lower())
                if effect:
                    entry["effect"] = effect
                    continue
                change = _CHANGE_TOKENS.get(token.lower())
                if change:
                    entry["change"] = change
                    continue
                try:
                    value = float(token)
                except ValueError:
                    entry["error"] = f"Unrecognized value '{token}'"
                    continue
                if value != value or value in (float("inf"), float("-inf")):
                    entry["error"] = f"Log2FC '{token}' is not a finite number"
                else:
                    entry["log2fc"] = value
            out.append(entry)
        return out

    def _validate_shared(self, data: dict, current_experiment: Optional[str] = None) -> dict:
        """Check the fields a batch shares. `current_experiment` (edits) may stay as it was."""
        evidence_code = data.get("evidence_code")
        if evidence_code not in EVIDENCE_CODES:
            raise RegulationCurationError("Evidence type must be Direct (DNA binding) or Indirect (expression).")
        default_effect = data.get("association_type") or "N/A"
        if default_effect not in ASSOCIATION_TYPES and default_effect != "infer":
            raise RegulationCurationError(f"Unknown effect '{default_effect}'.")
        experiment = _clean(data.get("experiment"), 240)
        if not experiment:
            raise RegulationCurationError("Experiment is required.")
        if experiment not in EXPERIMENTS[evidence_code] and experiment != current_experiment:
            raise RegulationCurationError(
                f"'{experiment}' is not a {EVIDENCE_CODES[evidence_code].lower()} experiment. "
                f"Choose one of: {', '.join(EXPERIMENTS[evidence_code])}."
            )
        mutant_type = _clean(data.get("mutant_type"), 40)
        if evidence_code == "Indirect":
            if mutant_type not in regulation_phenotype.MUTANT_TYPES:
                raise RegulationCurationError(
                    "Choose how the regulator was mutated: " + ", ".join(regulation_phenotype.MUTANT_TYPES) + "."
                )
        else:
            mutant_type = None
        default_change = data.get("expression_change") or None
        if default_change not in (None, "up", "down", "infer"):
            raise RegulationCurationError(f"Unknown expression change '{default_change}'.")
        condition_group = _clean(data.get("condition_group"), 100)
        if condition_group and condition_group not in CONDITION_GROUPS:
            raise RegulationCurationError(f"Unknown condition group '{condition_group}'.")
        return {
            "evidence_code": evidence_code,
            "default_effect": default_effect,
            "default_change": default_change if evidence_code == "Indirect" else None,
            "experiment": experiment,
            "mutant_type": mutant_type,
            "strain_background": _clean(data.get("strain_background"), 100),
            "condition_group": condition_group,
            "environmental_condition": _clean(data.get("environmental_condition"), 1000),
            "regulation_note": _clean(data.get("regulation_note"), 1000),
        }

    def preview(self, data: dict) -> dict:
        """Resolve and check a batch without saving it."""
        organism = self._organism(data.get("organism"))
        ref = self._reference(data.get("pubmed"))
        shared = self._validate_shared(data)
        tf = self._resolve_one(organism, data.get("tf") or "", "Regulator")
        regulator = self.regulator_status(organism, tf)
        lines = self.parse_target_lines(data.get("targets") or "")
        if not lines:
            raise RegulationCurationError("Enter at least one target gene.")
        if len(lines) > MAX_TARGETS:
            raise RegulationCurationError(f"At most {MAX_TARGETS} targets per batch.")

        resolved = self.resolve_names(organism, [ln["name"] for ln in lines])
        existing = {
            (r.target_feature_no, r.experiment, r.mutant_type, r.strain_background,
             r.environmental_condition): r.regulation_no
            for r in self.db.query(Regulation).filter(
                Regulation.tf_feature_no == tf.feature_no, Regulation.reference_no == ref.reference_no
            )
        }
        seen = set()
        rows = []
        for ln in lines:
            res = resolved.get(ln["name"], {"status": "not_found", "feature": None, "candidates": []})
            change, effect = self._line_effect(ln, shared)
            row = {"line": ln["line"], "name": ln["name"], "change": change, "effect": effect,
                   "log2fc": ln["log2fc"], "status": res["status"], "message": None, "target": None}
            feature = res["feature"]
            if ln["error"]:
                row["status"], row["message"] = "error", ln["error"]
            elif shared["evidence_code"] == "Indirect" and change is None and ln["effect"] is None:
                row["status"] = "error"
                row["message"] = "Say whether expression went up or down in the mutant (or give a log2FC)"
            elif res["status"] == "ambiguous":
                row["message"] = "Matches " + ", ".join(res["candidates"])
            elif res["status"] == "not_found":
                row["message"] = f"Not found in {organism.organism_name}"
            else:
                row["target"] = self._gene_out(feature)
                key = (feature.feature_no, shared["experiment"], shared["mutant_type"],
                       shared["strain_background"], shared["environmental_condition"])
                if key in seen:
                    row["status"], row["message"] = "duplicate", "Listed twice in this batch"
                elif key in existing:
                    row["status"] = "exists"
                    row["message"] = f"Already curated (regulation {existing[key]})"
                seen.add(key)
                row["_feature_no"] = feature.feature_no
            rows.append(row)

        counts = {}
        for row in rows:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        return {
            "organism": organism.organism_abbrev,
            "reference": self._reference_out(ref),
            "tf": self._gene_out(tf),
            "regulator": regulator,
            "shared": shared,
            "rows": rows,
            "counts": counts,
            "_tf_no": tf.feature_no,
            "_ref_no": ref.reference_no,
        }

    def create(self, data: dict, curator_userid: str) -> dict:
        """Save the resolvable, new rows of a batch; report what was skipped."""
        result = self.preview(data)
        if not result["regulator"]["is_regulator"]:
            raise RegulationCurationError(
                f"{result['tf']['display_name']} is not a recognized regulator. Admit it as a regulator first."
            )
        shared = result["shared"]
        to_add = [r for r in result["rows"] if r["status"] == "ok"]
        if not to_add:
            raise RegulationCurationError("Nothing to save: no target resolved to a new association.")
        user = curator_userid[:12].upper()
        try:
            for row in to_add:
                self.db.add(Regulation(
                    tf_feature_no=result["_tf_no"],
                    target_feature_no=row["_feature_no"],
                    reference_no=result["_ref_no"],
                    evidence_code=shared["evidence_code"],
                    experiment=shared["experiment"],
                    mutant_type=shared["mutant_type"],
                    association_type=row["effect"],
                    strain_background=shared["strain_background"],
                    condition_group=shared["condition_group"],
                    environmental_condition=shared["environmental_condition"],
                    log2fc=row["log2fc"],
                    regulation_note=shared["regulation_note"],
                    created_by=user,
                ))
            self.db.commit()
        except IntegrityError:
            self.db.rollback()
            logger.exception("Regulation batch hit a constraint")
            raise RegulationCurationError(
                "The database rejected the batch (a duplicate or invalid value); nothing was saved."
            )
        except SQLAlchemyError:
            self.db.rollback()
            logger.exception("Failed to save regulation batch")
            raise RegulationCurationError("Could not save the batch; nothing was saved.")
        skipped = len(result["rows"]) - len(to_add)
        logger.info("Curated %d regulation rows (%s -> targets, PMID %s) by %s; %d skipped",
                    len(to_add), result["tf"]["feature_name"], result["reference"]["pubmed"], user, skipped)
        return {"added": len(to_add), "skipped": skipped, "counts": result["counts"]}

    # ------------------------------------------------------------------
    # Update / delete
    # ------------------------------------------------------------------
    def update(self, regulation_no: int, data: dict, curator_userid: str) -> dict:
        row = self.db.query(Regulation).filter(Regulation.regulation_no == regulation_no).first()
        if not row:
            raise RegulationCurationError(f"Regulation {regulation_no} not found.")
        shared = self._validate_shared({**data, "association_type": data.get("association_type") or "N/A"},
                                       current_experiment=row.experiment)
        if shared["default_effect"] == "infer":
            raise RegulationCurationError("Choose an effect.")
        log2fc = data.get("log2fc")
        if log2fc in ("", None):
            log2fc = None
        else:
            try:
                log2fc = float(log2fc)
            except (TypeError, ValueError):
                raise RegulationCurationError(f"Log2FC '{log2fc}' is not a number.")
        row.evidence_code = shared["evidence_code"]
        row.experiment = shared["experiment"]
        row.mutant_type = shared["mutant_type"]
        row.association_type = shared["default_effect"]
        row.strain_background = shared["strain_background"]
        row.condition_group = shared["condition_group"]
        row.environmental_condition = shared["environmental_condition"]
        row.regulation_note = shared["regulation_note"]
        row.log2fc = log2fc
        try:
            self.db.commit()
        except IntegrityError:
            self.db.rollback()
            raise RegulationCurationError(
                "Another row already has this regulator, target, paper, experiment, mutant and condition."
            )
        logger.info("Updated regulation %s by %s", regulation_no, curator_userid)
        return self._rows(Regulation.regulation_no == regulation_no)[0]

    def delete(self, regulation_nos: list[int], curator_userid: str) -> int:
        nos = sorted({int(n) for n in regulation_nos or []})
        if not nos:
            raise RegulationCurationError("No rows selected.")
        deleted = 0
        for start in range(0, len(nos), 900):
            deleted += (
                self.db.query(Regulation)
                .filter(Regulation.regulation_no.in_(nos[start:start + 900]))
                .delete(synchronize_session=False)
            )
        self.db.commit()
        logger.info("Deleted %d regulation rows by %s", deleted, curator_userid)
        return deleted

    # ------------------------------------------------------------------
    # Effects
    # ------------------------------------------------------------------
    @staticmethod
    def _line_effect(line: dict, shared: dict) -> tuple[Optional[str], str]:
        """(expression change, effect) for one target line.

        Binding rows take the line's effect or the batch default. Expression
        rows take the line's direction of change (else the batch default, or
        the log2FC sign when the default is 'infer') and derive the effect from
        the mutant type; an explicit effect on the line wins.
        """
        if shared["evidence_code"] == "Direct":
            effect = line["effect"]
            if effect is None:
                if shared["default_effect"] == "infer":
                    fc = line["log2fc"] or 0
                    effect = "Positive" if fc > 0 else "Negative" if fc < 0 else "N/A"
                else:
                    effect = shared["default_effect"]
            return None, effect
        change = line["change"]
        if change is None:
            if shared["default_change"] == "infer":
                fc = line["log2fc"] or 0
                change = "up" if fc > 0 else "down" if fc < 0 else None
            else:
                change = shared["default_change"]
        effect = line["effect"] or regulation_phenotype.infer_association(shared["mutant_type"], change)
        return change, effect

    # ------------------------------------------------------------------
    # Regulators (who may regulate)
    # ------------------------------------------------------------------
    def regulator_status(self, organism: Organism, feature: Feature) -> dict:
        """Why a gene counts as a regulator, or that it does not."""
        sources = regulation_phenotype.regulator_sources(self.db, organism.organism_no, organism.organism_abbrev)
        name = regulation_phenotype.locus_name(feature.feature_name)
        labels = {"go": "GO DNA-binding transcription factor", "pathoyeastract": "PathoYeastract regulator",
                  "admitted": "admitted by a curator"}
        reasons = [label for key, label in labels.items() if name in sources[key]]
        return {"is_regulator": bool(reasons), "reasons": reasons}

    def allow_regulator(self, organism_abbrev: str, name: str, note: Optional[str], curator_userid: str) -> dict:
        organism = self._organism(organism_abbrev)
        feature = self._resolve_one(organism, name, "Gene")
        note = _clean(note, 1000)
        if not note:
            raise RegulationCurationError("Say why this gene is a regulator (the note is shown to curators).")
        existing = self.db.query(RegulatorAllow).filter(RegulatorAllow.feature_no == feature.feature_no).first()
        if existing:
            existing.allow_note = note
        else:
            self.db.add(RegulatorAllow(feature_no=feature.feature_no, allow_note=note,
                                       created_by=curator_userid[:12].upper()))
        try:
            self.db.commit()
        except SQLAlchemyError:
            self.db.rollback()
            logger.exception("Failed to admit regulator %s", feature.feature_name)
            raise RegulationCurationError("Could not save; nothing was changed.")
        regulation_phenotype.clear_cache()
        logger.info("Admitted %s as a regulator by %s", feature.feature_name, curator_userid)
        return {"gene": self._gene_out(feature), **self.regulator_status(organism, feature)}

    def remove_regulator(self, organism_abbrev: str, name: str, curator_userid: str) -> dict:
        organism = self._organism(organism_abbrev)
        feature = self._resolve_one(organism, name, "Gene")
        deleted = (self.db.query(RegulatorAllow).filter(RegulatorAllow.feature_no == feature.feature_no)
                   .delete(synchronize_session=False))
        self.db.commit()
        regulation_phenotype.clear_cache()
        logger.info("Removed %s from admitted regulators by %s (%d row)", feature.feature_name, curator_userid,
                    deleted)
        return {"gene": self._gene_out(feature), **self.regulator_status(organism, feature)}

    def review(self, organism_abbrev: str) -> dict:
        """What curators may need to act on for one organism.

        * admitted: genes admitted as regulators;
        * excluded_regulators: mutated genes of RNA accumulation phenotypes that
          are not regulators, so their phenotypes are not on the Regulation tab;
        * unresolved_reporters: phenotype reporters with words that are not
          CGD gene names (shown with the genes that did resolve).
        """
        organism = self._organism(organism_abbrev)
        sources = regulation_phenotype.regulator_sources(self.db, organism.organism_no, organism.organism_abbrev)
        regulators = set().union(*sources.values())
        excluded: dict[str, dict] = {}
        unresolved = []
        for row in regulation_phenotype.phenotype_rows(self.db, organism.organism_no):
            name, gene = row["regulator"]
            if row["change"] is not None and name not in regulators:
                entry = excluded.setdefault(name, {"feature_name": name, "gene_name": gene,
                                                   "display_name": gene or name, "annotations": 0,
                                                   "reporters": []})
                entry["annotations"] += 1
                if row["reporter"] not in entry["reporters"]:
                    entry["reporters"].append(row["reporter"])
            if row["unresolved"]:
                unresolved.append({
                    "pheno_annotation_no": row["pheno_annotation_no"],
                    "regulator": {"feature_name": name, "gene_name": gene, "display_name": gene or name},
                    "reporter": row["reporter"],
                    "unresolved": row["unresolved"],
                    "targets": [{"feature_name": f, "gene_name": g, "display_name": g or f}
                                for f, g in row["targets"]],
                    "regulator_shown": name in regulators,
                })
        return {
            "organism": organism.organism_abbrev,
            "admitted": sorted(
                ({"display_name": a["gene_name"] or a["feature_name"], **a}
                 for a in regulation_phenotype.admitted_regulators(self.db, organism.organism_no).values()),
                key=lambda a: a["display_name"].upper()),
            "excluded_regulators": sorted(excluded.values(), key=lambda e: (-e["annotations"],
                                                                            e["display_name"].upper())),
            "unresolved_reporters": sorted(unresolved, key=lambda u: (u["regulator"]["display_name"].upper(),
                                                                      u["reporter"])),
            "regulator_counts": {key: len(names) for key, names in sources.items()},
        }

    # ------------------------------------------------------------------
    # Genome-wide binding: peaks -> promoters
    # ------------------------------------------------------------------
    @staticmethod
    def parse_peaks(text: str) -> tuple[list[tuple[str, int, int]], int]:
        """Peaks from BED / narrowPeak / a chrom,start,end table, or 'chrom:start-end' lines.

        Returns ([(chrom, start, end)] 1-based inclusive, skipped line count).
        A narrowPeak summit (column 10) narrows the peak to its summit.
        """
        peaks, skipped = [], 0
        for raw in (text or "").splitlines():
            line = raw.strip()
            if not line or line.startswith(("#", "track", "browser")):
                continue
            match = re.match(r"^([^:\s,]+):([\d,]+)-([\d,]+)(?:\s|$)", line)
            fields = [f.strip() for f in re.split(r"\t|,", line)]
            if len(fields) < 3:
                fields = line.split()
            try:
                if match:
                    chrom = match.group(1)
                    start, end = (int(match.group(i).replace(",", "")) for i in (2, 3))
                else:
                    chrom, start, end = fields[0], int(fields[1]) + 1, int(fields[2])
                    if len(fields) >= 10 and fields[9].lstrip("-").isdigit() and int(fields[9]) >= 0:
                        start = end = start + int(fields[9])
            except (IndexError, ValueError):
                skipped += 1  # header or malformed line
                continue
            peaks.append((chrom, min(start, end), max(start, end)))
            if len(peaks) > MAX_PEAKS:
                raise RegulationCurationError(f"At most {MAX_PEAKS} peaks per file.")
        return peaks, skipped

    @staticmethod
    def _chromosome_keys(name: str, organism_abbrev: str) -> list[str]:
        """Names a peak file may use for a CGD chromosome (e.g. Ca22chr1A, chr1A, ChrA)."""
        short = name[:-len(organism_abbrev) - 1] if name.endswith("_" + organism_abbrev) else name
        keys = [name.lower(), short.lower()]
        assembly = re.match(r"^Ca\d+(chr.+)$", short)
        if assembly:
            keys.append(assembly.group(1).lower())
        return keys

    def assign_peaks(self, organism_abbrev: str, text: str, window: Optional[int] = None) -> dict:
        """Assign binding peaks to the genes whose promoters they fall in.

        A gene's promoter is the `window` bp upstream of its start (strand
        aware); a peak between two divergent genes counts for both. Genes are
        reported as current loci (orf19 and B-allele hits map to the A22 locus).
        """
        organism = self._organism(organism_abbrev)
        window = int(window or DEFAULT_PROMOTER_WINDOW)
        if not 100 <= window <= 5000:
            raise RegulationCurationError("The promoter window must be 100-5000 bp.")
        peaks, skipped = self.parse_peaks(text)
        if not peaks:
            raise RegulationCurationError("No peaks found. Expected BED/narrowPeak, a chrom,start,end table, "
                                          "or chrom:start-end lines.")

        chrom = aliased(Feature)
        gene = aliased(Feature)
        locations = (
            self.db.query(chrom.feature_name, gene.feature_name, FeatLocation.start_coord, FeatLocation.strand)
            .select_from(FeatLocation)
            .join(Seq, Seq.seq_no == FeatLocation.root_seq_no)
            .join(chrom, chrom.feature_no == Seq.feature_no)
            .join(gene, gene.feature_no == FeatLocation.feature_no)
            .filter(gene.organism_no == organism.organism_no, gene.feature_type.in_(GENE_FEATURE_TYPES),
                    FeatLocation.is_loc_current == "Y", Seq.is_seq_current == "Y")
            .all()
        )
        promoters: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
        for chrom_name, gene_name, start, strand in locations:
            if strand in ("C", "-"):
                promoters[chrom_name].append((start + 1, start + window, gene_name))
            else:
                promoters[chrom_name].append((max(1, start - window), start - 1, gene_name))
        # Newer assemblies sort last, so their short names (chr1A) win
        by_key: dict[str, str] = {}
        for chrom_name in sorted(promoters):
            for key in self._chromosome_keys(chrom_name, organism.organism_abbrev):
                by_key[key] = chrom_name
        index = {}
        for chrom_name, rows in promoters.items():
            rows.sort()
            index[chrom_name] = ([r[0] for r in rows], rows)

        hits: dict[str, int] = defaultdict(int)
        unmatched: dict[str, int] = defaultdict(int)
        assigned = 0
        for chrom_name, start, end in peaks:
            cgd_chrom = by_key.get(chrom_name.lower())
            if cgd_chrom is None:
                unmatched[chrom_name] += 1
                continue
            starts, rows = index[cgd_chrom]
            genes = set()
            for i in range(bisect.bisect_left(starts, start - window), bisect.bisect_right(starts, end)):
                p_start, p_end, name = rows[i]
                if p_start <= end and p_end >= start:
                    genes.add(name)
            assigned += bool(genes)
            for name in genes:
                hits[name] += 1

        resolved = self.resolve_names(organism, list(hits))
        by_locus: dict[int, dict] = {}
        for name, count in hits.items():
            res = resolved.get(name)
            if not res or res["status"] != "ok":
                continue
            f = res["feature"]
            entry = by_locus.setdefault(f.feature_no, {**self._gene_out(f), "peaks": 0})
            entry["peaks"] = max(entry["peaks"], count)  # A and B alleles see the same peaks
        targets = sorted(by_locus.values(), key=lambda t: (-t["peaks"], t["display_name"].upper()))
        return {
            "organism": organism.organism_abbrev,
            "window": window,
            "peaks": len(peaks),
            "peaks_assigned": assigned,
            "skipped_lines": skipped,
            "unmatched_chromosomes": [{"name": n, "peaks": c} for n, c in sorted(unmatched.items())],
            "targets": targets,
            "target_lines": "\n".join(
                f"{t['feature_name']}  # {t['display_name']}, {t['peaks']} peak{'s' if t['peaks'] != 1 else ''}"
                for t in targets),
        }


def strip_private(result: dict) -> dict:
    """Drop the service's internal keys (leading underscore) before returning JSON."""
    out = {k: v for k, v in result.items() if not k.startswith("_")}
    if "rows" in out:
        out["rows"] = [{k: v for k, v in r.items() if not k.startswith("_")} for r in out["rows"]]
    return out


__all__ = ["RegulationCurationService", "RegulationCurationError", "strip_private"]
