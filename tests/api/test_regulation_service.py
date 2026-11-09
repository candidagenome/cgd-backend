"""
Tests for Regulation Service (locus Regulation tab, PathoYeastract data).

Tests cover:
- Resolving PathoYeastract TF/target names to CGD features
- Regulator flags (binding/expression evidence, activator/repressor), CGD
  reference substitution, and ordering
- TF target lists: B-haplotype rows collapse onto the A locus; counts
- Per-organism coverage (C. dubliniensis is not in PathoYeastract) and
  genes without a data file
- CGD-curated rows: shown alongside PathoYeastract's, hidden when
  PathoYeastract already has the same TF, target and paper
- Rows transferred from RNA accumulation phenotypes: shown as "CGD phenotype",
  and hidden like curated rows when PathoYeastract has the paper
- Reference page: a paper's PathoYeastract rows (per-PMID index) and CGD rows,
  with the same hide rule
"""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from cgd.api.services import regulation_service
from cgd.api.services.regulation_service import (
    _build_regulators,
    _build_tf_targets,
    _regulation_for_feature,
    _resolve_gene,
    get_reference_regulation,
)
from cgd.models.models import Organism

INDEX = {
    "C3_05230W_A": ("C3_05230W_A", "SAP3"),
    "SAP3": ("C3_05230W_A", "SAP3"),
    "CR_07890W_A": ("CR_07890W_A", "EFG1"),
    "EFG1": ("CR_07890W_A", "EFG1"),
    "C3_03320W_A": ("C3_03320W_A", None),
    "C2_08890W_A": ("C2_08890W_A", "IRF1"),
}


def _evidence(pubmed, code, sign):
    return {"pubmed": pubmed, "citation": f"Paper {pubmed}", "evidence_code": code,
            "experiment": "ChIP-seq", "association_type": sign, "strain": "SC5314",
            "environmental_group": "Stress", "environmental_condition": "37C", "log2fc": 1.5}


class TestResolveGene:
    def test_resolves_by_systematic_name(self):
        gene = _resolve_gene("Efg1p", "CR_07890W_A", INDEX)
        assert (gene.feature_name, gene.gene_name, gene.display_name) == ("CR_07890W_A", "EFG1", "EFG1")
        assert gene.name == "Efg1p"

    def test_resolves_protein_name_without_orf(self):
        assert _resolve_gene("Efg1p", None, INDEX).feature_name == "CR_07890W_A"

    def test_b_haplotype_maps_to_a_locus(self):
        gene = _resolve_gene("C3_03320W_B", None, INDEX)
        assert gene.feature_name == "C3_03320W_A"
        assert gene.display_name == "C3_03320W_A"

    def test_unknown_name_is_kept_unlinked(self):
        gene = _resolve_gene("PIS49793", None, INDEX)
        assert gene.feature_name is None
        assert gene.display_name == "PIS49793"


class TestBuildRegulators:
    ENTRIES = [
        {"tf_protein": "Rob1p", "tf_orf": None, "potential_status": "not_found", "consensus": None,
         "sites": None, "evidence": [_evidence(111, "Indirect", "Positive")]},
        {"tf_protein": "Efg1p", "tf_orf": "CR_07890W_A", "potential_status": "found",
         "consensus": ["TATGCA"], "sites": {"plus": [[-500, -495]], "minus": [[-900, -895]]},
         "evidence": [_evidence(222, "Direct", "Negative"), _evidence(333, "Indirect", "Positive")]},
        {"tf_protein": "Cta8p", "tf_orf": None, "potential_status": "found", "consensus": ["GAA"],
         "sites": {"plus": [[-20, -18]], "minus": []}, "evidence": []},
        # Neither evidence nor a promoter match: nothing to show
        {"tf_protein": "Ada2p", "tf_orf": None, "potential_status": "not_found", "consensus": None,
         "sites": None, "evidence": []},
    ]

    def test_flags_sites_and_ordering(self):
        refs = {222: SimpleNamespace(citation="Smith J (2020) CGD title", dbxref_id="CAL0000000222")}
        regulators = _build_regulators(self.ENTRIES, "C3_05230W_A", "calbicans", INDEX, refs)

        # Binding evidence first, then documented, then potential-only
        assert [r.tf.name for r in regulators] == ["Efg1p", "Rob1p", "Cta8p"]
        efg1, rob1, cta8 = regulators
        assert efg1.binding_evidence and efg1.expression_evidence
        assert efg1.activator and efg1.repressor
        assert efg1.reference_count == 2
        assert efg1.potential and efg1.consensus == ["TATGCA"]
        assert [(s.strand, s.start, s.end) for s in efg1.sites] == [("-", -900, -895), ("+", -500, -495)]
        assert "proteinname=Efg1p" in efg1.pathoyeastract_url
        assert "orfname=C3_05230W_A" in efg1.pathoyeastract_url

        assert rob1.documented and not rob1.potential and not rob1.binding_evidence
        assert not cta8.documented and cta8.potential and cta8.pathoyeastract_url is None

    def test_cgd_reference_replaces_pathoyeastract_citation(self):
        refs = {222: SimpleNamespace(citation="Smith J (2020) CGD title", dbxref_id="CAL0000000222")}
        efg1 = _build_regulators(self.ENTRIES, "C3_05230W_A", "calbicans", INDEX, refs)[0]

        cgd_ref, external_ref = (e.reference for e in efg1.evidence)
        assert (cgd_ref.citation, cgd_ref.dbxref_id) == ("Smith J (2020) CGD title", "CAL0000000222")
        assert (external_ref.citation, external_ref.dbxref_id) == ("Paper 333", None)


class TestBuildTfTargets:
    def test_haplotypes_merge_and_counts(self):
        tf = {"protein": "Mrr1p", "consensus": ["DCSGHD"], "targets": [
            {"orf": "C3_05230W_A", "documented": True, "binding": True, "expression": False,
             "activated": True, "repressed": False, "potential": False},
            {"orf": "C3_03320W_A", "documented": True, "binding": False, "expression": True,
             "activated": False, "repressed": True, "potential": False},
            {"orf": "C3_03320W_B", "documented": False, "binding": False, "expression": False,
             "activated": False, "repressed": False, "potential": True},
            {"orf": "C9_99999W_A", "documented": False, "binding": False, "expression": False,
             "activated": False, "repressed": False, "potential": True},
        ]}
        result = _build_tf_targets(tf, INDEX)

        assert result.tf_protein == "Mrr1p"
        assert [t.gene.display_name for t in result.targets] == ["SAP3", "C3_03320W_A", "C9_99999W_A"]
        merged = result.targets[1]
        assert merged.documented and merged.expression and merged.repressed and merged.potential
        assert result.counts.model_dump() == {
            "documented": 2, "binding": 1, "expression": 1, "activated": 1, "repressed": 1,
            "potential": 2, "documented_and_potential": 1,
        }


class TestRegulationForFeature:
    @pytest.fixture
    def data_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(regulation_service.settings, "regulation_data_dir", str(tmp_path))
        monkeypatch.setattr(regulation_service, "_name_index", lambda db, organism_no: INDEX)
        monkeypatch.setattr(regulation_service, "_references_by_pubmed", lambda db, pmids: {})
        return tmp_path

    @staticmethod
    def _feature(abbrev, feature_name="C3_05230W_A", gene_name="SAP3"):
        organism = SimpleNamespace(organism_abbrev=abbrev, organism_name=abbrev)
        return SimpleNamespace(feature_no=30, feature_name=feature_name, gene_name=gene_name,
                               organism=organism, organism_no=3)

    @staticmethod
    def _db(as_target=(), as_tf=()):
        """A session whose REGULATION queries return these rows (gene as target, then as TF)."""
        db = MagicMock()
        db.query.return_value.options.return_value.filter.return_value.all.side_effect = [
            list(as_target), list(as_tf)]
        return db

    @staticmethod
    def _curated(tf_name, tf_gene, pubmed, code="Direct", sign="Positive", target=None):
        return SimpleNamespace(
            tf_feature=SimpleNamespace(feature_name=tf_name, gene_name=tf_gene),
            target_feature=SimpleNamespace(feature_name=target[0], gene_name=target[1]) if target else None,
            reference=SimpleNamespace(pubmed=pubmed, citation=f"Paper {pubmed}"),
            evidence_code=code, experiment="EMSA", mutant_type=None, association_type=sign, strain_background=None,
            condition_group=None, environmental_condition=None, log2fc=None, regulation_note=None,
            pheno_annotation_no=None)

    def test_dubliniensis_is_not_covered(self, data_dir):
        out = _regulation_for_feature(self._db(), self._feature("C_dubliniensis_CD36", "Cd36_00010", None))
        assert out.covered is False and out.has_data is False and out.source is None
        assert out.locus_display_name == "Cd36_00010"

    def test_covered_gene_without_file(self, data_dir):
        out = _regulation_for_feature(self._db(), self._feature("C_albicans_SC5314"))
        assert out.covered is True and out.has_data is False and out.regulators == []
        # Still links out so readers can check PathoYeastract directly
        assert out.source.gene_url.endswith("calbicans/view.php?existing=locus&orfname=C3_05230W_A")

    def test_gene_file_is_served(self, data_dir):
        (data_dir / "C_albicans_SC5314").mkdir()
        (data_dir / "C_albicans_SC5314" / "C3_05230W_A.json").write_text(json.dumps({
            "orf": "C3_05230W_A",
            "source": {"url": "https://yeastract-plus.org/pathoyeastract/calbicans/", "retrieved": "2026-10-07"},
            "regulators": TestBuildRegulators.ENTRIES,
            "promoter": {"start": -1000, "end": -1, "sequence": "ACGT"},
        }))
        out = _regulation_for_feature(self._db(), self._feature("C_albicans_SC5314"))

        assert out.has_data is True
        assert len(out.regulators) == 3
        assert out.promoter.sequence == "ACGT"
        assert out.tf_targets is None
        assert out.source.retrieved == "2026-10-07"
        assert out.source.gene_url.endswith("calbicans/view.php?existing=locus&orfname=C3_05230W_A")

    def _write_gene_file(self, data_dir):
        (data_dir / "C_albicans_SC5314").mkdir()
        (data_dir / "C_albicans_SC5314" / "C3_05230W_A.json").write_text(json.dumps({
            "orf": "C3_05230W_A",
            "source": {"url": "https://yeastract-plus.org/pathoyeastract/calbicans/", "retrieved": "2026-10-07"},
            "regulators": TestBuildRegulators.ENTRIES,
        }))

    def test_curated_row_shown_for_uncovered_species(self, data_dir):
        db = self._db(as_target=[self._curated("Cd36_12340", "EFG1", 444)])
        out = _regulation_for_feature(db, self._feature("C_dubliniensis_CD36", "Cd36_00010", None))

        assert out.covered is False and out.has_data is True and out.cgd_curated_count == 1
        evidence = out.regulators[0].evidence
        assert [(e.reference.pubmed, e.source) for e in evidence] == [(444, "CGD")]

    def test_curated_row_joins_pathoyeastract_tf(self, data_dir):
        self._write_gene_file(data_dir)
        db = self._db(as_target=[self._curated("CR_07890W_A", "EFG1", 444)])
        out = _regulation_for_feature(db, self._feature("C_albicans_SC5314"))

        assert out.cgd_curated_count == 1
        efg1 = next(r for r in out.regulators if r.tf.feature_name == "CR_07890W_A")
        assert sorted((e.reference.pubmed, e.source) for e in efg1.evidence) == [
            (222, "PathoYeastract"), (333, "PathoYeastract"), (444, "CGD")]

    def test_curated_row_hidden_when_pathoyeastract_has_paper(self, data_dir):
        self._write_gene_file(data_dir)
        db = self._db(as_target=[self._curated("CR_07890W_A", "EFG1", 222)])
        out = _regulation_for_feature(db, self._feature("C_albicans_SC5314"))

        assert out.cgd_curated_count == 0
        efg1 = next(r for r in out.regulators if r.tf.feature_name == "CR_07890W_A")
        assert all(e.source == "PathoYeastract" for e in efg1.evidence)

    @classmethod
    def _phenotype(cls, pubmed, sign="Positive", target=None):
        """A REGULATION row transferred from an RNA accumulation phenotype."""
        row = cls._curated("CR_07890W_A", "EFG1", pubmed, code="Indirect", sign=sign, target=target)
        row.experiment = "Phenotype: RNA accumulation decreased"
        row.mutant_type = "null"
        row.regulation_note = "Reporter: SAP3 mRNA"
        row.pheno_annotation_no = 1
        return row

    def test_phenotype_row_is_shown_as_cgd_phenotype(self, data_dir):
        self._write_gene_file(data_dir)
        out = _regulation_for_feature(self._db(as_target=[self._phenotype(555)]), self._feature("C_albicans_SC5314"))

        assert out.cgd_curated_count == 1
        efg1 = next(r for r in out.regulators if r.tf.feature_name == "CR_07890W_A")
        phenotype = next(e for e in efg1.evidence if e.source == "CGD phenotype")
        assert phenotype.reference.pubmed == 555 and phenotype.mutant_type == "null"
        assert phenotype.experiment == "Phenotype: RNA accumulation decreased"
        assert phenotype.association_type == "Positive" and phenotype.evidence_code == "Indirect"

    def test_phenotype_row_hidden_when_pathoyeastract_has_paper(self, data_dir):
        self._write_gene_file(data_dir)
        out = _regulation_for_feature(self._db(as_target=[self._phenotype(222)]), self._feature("C_albicans_SC5314"))
        assert out.cgd_curated_count == 0

    def test_regulator_lists_phenotype_targets(self, data_dir):
        db = self._db(as_tf=[self._phenotype(555, sign="Negative", target=("C3_05230W_A", "SAP3"))])
        out = _regulation_for_feature(db, self._feature("C_albicans_SC5314", "CR_07890W_A", "EFG1"))

        assert out.cgd_curated_count == 1
        [target] = out.tf_targets.targets
        assert target.gene.feature_name == "C3_05230W_A"
        assert target.expression and target.repressed and target.cgd_curated and not target.binding


class TestReferenceRegulation:
    ALBICANS = SimpleNamespace(organism_abbrev="C_albicans_SC5314", organism_name="Candida albicans SC5314",
                               organism_no=3)

    @pytest.fixture
    def data_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(regulation_service.settings, "regulation_data_dir", str(tmp_path))
        monkeypatch.setattr(regulation_service, "_name_index", lambda db, organism_no: INDEX)
        return tmp_path

    @classmethod
    def _db(cls, cgd_rows=()):
        """A session returning C. albicans for the Organism query and these REGULATION rows."""
        db = MagicMock()

        def query(model):
            q = MagicMock()
            if model is Organism:
                q.filter.return_value.all.return_value = [cls.ALBICANS]
            else:
                q.options.return_value.filter.return_value.all.return_value = list(cgd_rows)
            return q

        db.query.side_effect = query
        return db

    @staticmethod
    def _write_paper(data_dir, pubmed, rows):
        paper_dir = data_dir / "C_albicans_SC5314" / "_by_pubmed"
        paper_dir.mkdir(parents=True)
        (paper_dir / f"{pubmed}.json").write_text(json.dumps({
            "pubmed": pubmed, "source": {"retrieved": "2026-10-07"}, "rows": rows}))

    @classmethod
    def _cgd_row(cls, tf, target, pheno_annotation_no=None):
        organism = SimpleNamespace(organism_name=cls.ALBICANS.organism_name)
        return SimpleNamespace(
            tf_feature=SimpleNamespace(feature_name=tf[0], gene_name=tf[1], organism=organism, organism_no=3),
            target_feature=SimpleNamespace(feature_name=target[0], gene_name=target[1]),
            evidence_code="Indirect", experiment="Northern blot", association_type="Positive",
            mutant_type="null", strain_background=None, environmental_condition=None, log2fc=None,
            regulation_note="note", pheno_annotation_no=pheno_annotation_no)

    PY_ROWS = [
        {"tf_orf": "CR_07890W_A", "tf_protein": "Efg1p", "target_orf": "C3_03320W_B",
         "evidence_code": "Indirect", "experiment": "Microarray", "association_type": "Negative",
         "strain": "SC5314", "environmental_condition": "37C", "log2fc": -1.2},
        {"tf_orf": "CR_07890W_A", "tf_protein": "Efg1p", "target_orf": "C3_05230W_A",
         "evidence_code": "Direct", "experiment": "ChIP-chip", "association_type": None,
         "strain": None, "environmental_condition": None, "log2fc": None},
    ]

    def test_pathoyeastract_and_cgd_rows(self, data_dir):
        self._write_paper(data_dir, 222, self.PY_ROWS)
        cgd = self._cgd_row(("C2_08890W_A", "IRF1"), ("C3_05230W_A", "SAP3"), pheno_annotation_no=9)
        out = get_reference_regulation(self._db([cgd]), SimpleNamespace(reference_no=1, pubmed=222))

        assert out.pathoyeastract_release == "2026-10-07"
        assert [(r.regulator.display_name, r.target.display_name, r.evidence_code, r.source)
                for r in out.regulations] == [
            ("EFG1", "SAP3", "Direct", "PathoYeastract"),
            ("EFG1", "C3_03320W_A", "Indirect", "PathoYeastract"),  # B haplotype -> A locus
            ("IRF1", "SAP3", "Indirect", "CGD phenotype"),
        ]
        assert out.regulations[1].log2fc == -1.2
        assert out.regulations[2].mutant_type == "null" and out.regulations[2].note == "note"

    def test_cgd_row_hidden_when_pathoyeastract_has_it(self, data_dir):
        self._write_paper(data_dir, 222, self.PY_ROWS)
        cgd = self._cgd_row(("CR_07890W_A", "EFG1"), ("C3_05230W_A", "SAP3"))
        out = get_reference_regulation(self._db([cgd]), SimpleNamespace(reference_no=1, pubmed=222))
        assert [r.source for r in out.regulations] == ["PathoYeastract", "PathoYeastract"]

    def test_paper_without_pathoyeastract_data(self, data_dir):
        cgd = self._cgd_row(("CR_07890W_A", "EFG1"), ("C3_05230W_A", "SAP3"))
        out = get_reference_regulation(self._db([cgd]), SimpleNamespace(reference_no=1, pubmed=None))
        assert out.pathoyeastract_release is None
        assert [(r.regulator.feature_name, r.source) for r in out.regulations] == [("CR_07890W_A", "CGD")]
