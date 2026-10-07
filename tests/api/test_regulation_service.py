"""
Tests for Regulation Service (locus Regulation tab, PathoYeastract data).

Tests cover:
- Resolving PathoYeastract TF/target names to CGD features
- Regulator flags (binding/expression evidence, activator/repressor), CGD
  reference substitution, and ordering
- TF target lists: B-haplotype rows collapse onto the A locus; counts
- Per-organism coverage (C. dubliniensis is not in PathoYeastract) and
  genes without a data file
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
)

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
        return SimpleNamespace(feature_name=feature_name, gene_name=gene_name, organism=organism,
                               organism_no=3)

    def test_dubliniensis_is_not_covered(self, data_dir):
        out = _regulation_for_feature(MagicMock(), self._feature("C_dubliniensis_CD36", "Cd36_00010", None))
        assert out.covered is False and out.has_data is False and out.source is None
        assert out.locus_display_name == "Cd36_00010"

    def test_covered_gene_without_file(self, data_dir):
        out = _regulation_for_feature(MagicMock(), self._feature("C_albicans_SC5314"))
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
        out = _regulation_for_feature(MagicMock(), self._feature("C_albicans_SC5314"))

        assert out.has_data is True
        assert len(out.regulators) == 3
        assert out.promoter.sequence == "ACGT"
        assert out.tf_targets is None
        assert out.source.retrieved == "2026-10-07"
        assert out.source.gene_url.endswith("calbicans/view.php?existing=locus&orfname=C3_05230W_A")
