"""
Regulation Curation Service.

Curator entry of transcriptional regulatory associations (transcription factor
-> target gene) from papers PathoYeastract has not covered yet. Rows live in
MULTI.REGULATION (scripts/regulation/create_regulation_table.py), one row per
evidence observation, with the fields PathoYeastract records, and are merged
into the locus Regulation tab by cgd/api/services/regulation_service.py.

A batch entry is one TF, one paper and one experiment/condition, with a pasted
list of target genes; each target line may carry its own effect and log2 fold
change. Names are resolved within one organism; C. albicans B alleles and
legacy orf19 names resolve to the Assembly 22 A-allele locus.
"""
from __future__ import annotations

import logging
import re
from typing import Iterable, Optional

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, joinedload

from cgd.api.services import regulation_service
from cgd.models.models import Alias, FeatAlias, Feature, Organism, Reference, Regulation

logger = logging.getLogger(__name__)

EVIDENCE_CODES = {
    "Direct": "DNA binding",
    "Indirect": "Expression",
}

# Experiment vocabulary, after PathoYeastract's (curators may also type their own)
EXPERIMENTS = {
    "Direct": [
        "ChIP",
        "ChIP-chip",
        "ChIP-exo",
        "ChIP-seq",
        "ChEC-seq",
        "CUT&RUN",
        "DNA footprinting",
        "EMSA",
        "Yeast one-hybrid",
        "Reporter - wt vs mutant promoter sequence",
    ],
    "Indirect": [
        "RNA-seq analysis - WT vs TF Deletion",
        "RNA-seq analysis - TF Over-expression vs WT",
        "RNA-seq analysis - WT vs TF Under-expression",
        "RNA-seq analysis - TF GOF vs WT",
        "Microarray analysis - WT vs TF Deletion",
        "Microarray analysis - TF Over-expression vs WT",
        "NanoString - WT vs TF mutant",
        "RT-qPCR - WT vs TF mutant",
        "RT-qPCR - TF overexpression",
        "Northern blot - WT vs TF mutant",
        "Reporter (lacZ/GFP/luciferase) - WT vs TF mutant",
        "Proteomics - WT vs TF mutant",
        "Western blot - WT vs TF mutant",
    ],
}

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
    "up": "Positive", "induced": "Positive", "+": "Positive",
    "negative": "Negative", "repression": "Negative", "repressed": "Negative", "repressor": "Negative",
    "down": "Negative", "-": "Negative",
    "n/a": "N/A", "na": "N/A", "none": "N/A",
}

# Features that can be a TF or a target (not CDS, introns, UTRs, ...)
GENE_FEATURE_TYPES = (
    "ORF", "allele", "pseudogene", "blocked_reading_frame", "uORF",
    "tRNA", "rRNA", "snoRNA", "snRNA", "ncRNA",
)

MAX_TARGETS = 5000


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
            "condition_groups": CONDITION_GROUPS,
            "association_types": ASSOCIATION_TYPES,
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
        """Parse 'GENE [effect] [log2FC]' lines (tab, comma or space separated)."""
        out = []
        for number, raw in enumerate((text or "").splitlines(), start=1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            tokens = [t for t in re.split(r"[\t,;]+|\s+", line) if t]
            entry = {"line": number, "name": tokens[0], "effect": None, "log2fc": None, "error": None}
            for token in tokens[1:]:
                effect = _EFFECT_TOKENS.get(token.lower())
                if effect:
                    entry["effect"] = effect
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

    def _validate_shared(self, data: dict) -> dict:
        evidence_code = data.get("evidence_code")
        if evidence_code not in EVIDENCE_CODES:
            raise RegulationCurationError("Evidence type must be Direct (DNA binding) or Indirect (expression).")
        default_effect = data.get("association_type") or "N/A"
        if default_effect not in ASSOCIATION_TYPES and default_effect != "infer":
            raise RegulationCurationError(f"Unknown effect '{default_effect}'.")
        experiment = _clean(data.get("experiment"), 240)
        if not experiment:
            raise RegulationCurationError("Experiment is required.")
        condition_group = _clean(data.get("condition_group"), 100)
        if condition_group and condition_group not in CONDITION_GROUPS:
            raise RegulationCurationError(f"Unknown condition group '{condition_group}'.")
        return {
            "evidence_code": evidence_code,
            "default_effect": default_effect,
            "experiment": experiment,
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
        tf = self._resolve_one(organism, data.get("tf") or "", "Transcription factor")
        lines = self.parse_target_lines(data.get("targets") or "")
        if not lines:
            raise RegulationCurationError("Enter at least one target gene.")
        if len(lines) > MAX_TARGETS:
            raise RegulationCurationError(f"At most {MAX_TARGETS} targets per batch.")

        resolved = self.resolve_names(organism, [ln["name"] for ln in lines])
        existing = {
            (r.target_feature_no, r.experiment, r.strain_background, r.environmental_condition): r.regulation_no
            for r in self.db.query(Regulation).filter(
                Regulation.tf_feature_no == tf.feature_no, Regulation.reference_no == ref.reference_no
            )
        }
        seen = set()
        rows = []
        for ln in lines:
            res = resolved.get(ln["name"], {"status": "not_found", "feature": None, "candidates": []})
            effect = ln["effect"]
            if effect is None:
                if shared["default_effect"] == "infer":
                    effect = ("Positive" if (ln["log2fc"] or 0) > 0
                              else "Negative" if (ln["log2fc"] or 0) < 0 else "N/A")
                else:
                    effect = shared["default_effect"]
            row = {"line": ln["line"], "name": ln["name"], "effect": effect, "log2fc": ln["log2fc"],
                   "status": res["status"], "message": None, "target": None}
            feature = res["feature"]
            if ln["error"]:
                row["status"], row["message"] = "error", ln["error"]
            elif res["status"] == "ambiguous":
                row["message"] = "Matches " + ", ".join(res["candidates"])
            elif res["status"] == "not_found":
                row["message"] = f"Not found in {organism.organism_name}"
            else:
                row["target"] = self._gene_out(feature)
                key = (feature.feature_no, shared["experiment"], shared["strain_background"],
                       shared["environmental_condition"])
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
            "shared": shared,
            "rows": rows,
            "counts": counts,
            "_tf_no": tf.feature_no,
            "_ref_no": ref.reference_no,
        }

    def create(self, data: dict, curator_userid: str) -> dict:
        """Save the resolvable, new rows of a batch; report what was skipped."""
        result = self.preview(data)
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
        shared = self._validate_shared({**data, "association_type": data.get("association_type") or "N/A"})
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
            raise RegulationCurationError("Another row already has this TF, target, paper, experiment and condition.")
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


def strip_private(result: dict) -> dict:
    """Drop the service's internal keys (leading underscore) before returning JSON."""
    out = {k: v for k, v in result.items() if not k.startswith("_")}
    if "rows" in out:
        out["rows"] = [{k: v for k, v in r.items() if not k.startswith("_")} for r in out["rows"]]
    return out


__all__ = ["RegulationCurationService", "RegulationCurationError", "strip_private"]
