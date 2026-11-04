"""
Tests for regulation curation (Curate Regulation tool) and phenotype-derived regulation.

Tests cover:
- Reporter text from RNA accumulation phenotypes split into gene names
- Regulator effect inferred from mutant type and direction of change
- Target lines: effects, directions of change and log2FC
- Batch validation: binding vs single-gene-mutant expression experiments
- Peak files (BED, narrowPeak summits, chrom:start-end) and chromosome names
"""
import pytest

from cgd.api.services.curation.regulation_curation_service import (
    RegulationCurationError,
    RegulationCurationService,
)
from cgd.api.services.regulation_phenotype import infer_association, locus_name, reporter_genes, split_reporter


class TestSplitReporter:
    @pytest.mark.parametrize("reporter, genes", [
        ("CDR2 mRNA", ["CDR2"]),
        ("CDR1-RLUC", ["CDR1-RLUC"]),
        ("EPA1 transcript", ["EPA1"]),
        ("ERG6|ERG11|CDR1", ["ERG6", "ERG11", "CDR1"]),
        ("CDR1 and CDR2", ["CDR1", "CDR2"]),
        ("EED1/DEF1 mRNA", ["EED1", "DEF1"]),
        ("NAD6-NAD1", ["NAD6-NAD1"]),
        ("HAC1 unspliced", ["HAC1"]),
        ("total RNA", []),
        ("C1_07530W_A", ["C1_07530W_A"]),
    ])
    def test_split(self, reporter, genes):
        assert split_reporter(reporter) == genes

    def test_left_over_words_are_kept_for_review(self):
        # "2" in "TNR1,2" is not a gene: it must reach the review list, not be dropped
        assert split_reporter("TNR1,2 mRNA") == ["TNR1", "2"]


class TestReporterGenes:
    GENES = {"CDR1", "CDR2", "NAD1", "NAD6", "MT-II", "TNR1"}

    def _genes(self, reporter):
        return reporter_genes(reporter, lambda name: name.upper() if name.upper() in self.GENES else None)

    @pytest.mark.parametrize("reporter, genes, unresolved", [
        ("CDR1-RLUC", ["CDR1"], []),                 # a tag is not reported
        ("NAD6-NAD1", ["NAD6", "NAD1"], []),
        ("MT-II mRNA", ["MT-II"], []),               # a hyphenated gene name stays whole
        ("CDR1 and CDR2", ["CDR1", "CDR2"], []),
        ("TNR1,2 mRNA", ["TNR1"], ["2"]),
        ("lariat splicing intermediates", [], ["lariat", "splicing", "intermediates"]),
    ])
    def test_genes(self, reporter, genes, unresolved):
        assert self._genes(reporter) == (genes, unresolved)


class TestInferAssociation:
    @pytest.mark.parametrize("mutant, change, effect", [
        ("null", "down", "Positive"),            # losing an activator lowers its targets
        ("null", "up", "Negative"),
        ("repressible", "down", "Positive"),
        ("reduction of function", "up", "Negative"),
        ("overexpression", "up", "Positive"),    # more activator raises its targets
        ("overexpression", "down", "Negative"),
        ("gain of function", "up", "Positive"),
        ("unspecified", "up", "N/A"),
        ("null", "changed", "N/A"),
        ("null", None, "N/A"),
    ])
    def test_effect(self, mutant, change, effect):
        assert infer_association(mutant, change) == effect


def test_locus_name_maps_b_allele_to_a():
    assert locus_name("CR_02190C_B") == "CR_02190C_A"
    assert locus_name("CAGL0A00451g") == "CAGL0A00451g"


class TestTargetLines:
    def test_effects_changes_and_log2fc(self):
        lines = RegulationCurationService.parse_target_lines(
            "CDR1 up 2.5\nCDR2\tdecreased\nERG11, activator\n# comment\nSNQ2 -1.2  # note\nPDR16 maybe")
        assert [(ln["name"], ln["change"], ln["effect"], ln["log2fc"]) for ln in lines] == [
            ("CDR1", "up", None, 2.5),
            ("CDR2", "down", None, None),
            ("ERG11", None, "Positive", None),
            ("SNQ2", None, None, -1.2),
            ("PDR16", None, None, None),
        ]
        assert lines[-1]["error"] == "Unrecognized value 'maybe'"


class TestLineEffect:
    @staticmethod
    def _line(change=None, effect=None, log2fc=None):
        return {"change": change, "effect": effect, "log2fc": log2fc}

    def test_binding_uses_default_effect(self):
        shared = {"evidence_code": "Direct", "default_effect": "Positive", "default_change": None,
                  "mutant_type": None}
        assert RegulationCurationService._line_effect(self._line(), shared) == (None, "Positive")
        assert RegulationCurationService._line_effect(self._line(effect="Negative"), shared) == (None, "Negative")

    def test_expression_effect_follows_mutant_and_change(self):
        shared = {"evidence_code": "Indirect", "default_effect": "N/A", "default_change": "down",
                  "mutant_type": "null"}
        assert RegulationCurationService._line_effect(self._line(), shared) == ("down", "Positive")
        assert RegulationCurationService._line_effect(self._line(change="up"), shared) == ("up", "Negative")
        # An explicit effect on the line wins
        assert RegulationCurationService._line_effect(self._line(change="up", effect="Positive"),
                                                      shared) == ("up", "Positive")

    def test_expression_change_inferred_from_log2fc(self):
        shared = {"evidence_code": "Indirect", "default_effect": "N/A", "default_change": "infer",
                  "mutant_type": "overexpression"}
        assert RegulationCurationService._line_effect(self._line(log2fc=1.4), shared) == ("up", "Positive")
        assert RegulationCurationService._line_effect(self._line(log2fc=-2), shared) == ("down", "Negative")
        assert RegulationCurationService._line_effect(self._line(), shared) == (None, "N/A")


class TestValidateShared:
    @staticmethod
    def _validate(**data):
        return RegulationCurationService(None)._validate_shared(data)

    def test_binding(self):
        shared = self._validate(evidence_code="Direct", experiment="EMSA", mutant_type="null")
        assert shared["experiment"] == "EMSA" and shared["mutant_type"] is None

    def test_expression_needs_a_mutant_type(self):
        with pytest.raises(RegulationCurationError, match="how the regulator was mutated"):
            self._validate(evidence_code="Indirect", experiment="RT-qPCR")
        shared = self._validate(evidence_code="Indirect", experiment="RT-qPCR", mutant_type="null",
                                expression_change="down")
        assert shared["mutant_type"] == "null" and shared["default_change"] == "down"

    def test_genome_wide_expression_is_not_curated(self):
        with pytest.raises(RegulationCurationError, match="not a expression change"):
            self._validate(evidence_code="Indirect", experiment="RNA-seq analysis - WT vs TF Deletion",
                           mutant_type="null")

    def test_an_edit_may_keep_an_older_experiment(self):
        shared = RegulationCurationService(None)._validate_shared(
            {"evidence_code": "Indirect", "experiment": "Microarray analysis - WT vs TF Deletion",
             "mutant_type": "null"},
            current_experiment="Microarray analysis - WT vs TF Deletion")
        assert shared["experiment"] == "Microarray analysis - WT vs TF Deletion"


class TestPeaks:
    def test_formats(self):
        text = "\n".join([
            "track name=peaks",
            "chrom\tstart\tend",                                         # header: skipped
            "Ca22chr1A_C_albicans_SC5314\t999\t1100",                    # BED: 0-based start
            "chr1A\t1999\t2300\tp2\t50\t.\t5.1\t10\t8\t100",             # narrowPeak: summit at 2100
            "Ca22chr2A:5,001-5,200",
            "ChrA,10,20",
        ])
        peaks, skipped = RegulationCurationService.parse_peaks(text)
        assert peaks == [("Ca22chr1A_C_albicans_SC5314", 1000, 1100), ("chr1A", 2100, 2100),
                         ("Ca22chr2A", 5001, 5200), ("ChrA", 11, 20)]
        assert skipped == 1

    def test_chromosome_keys(self):
        keys = RegulationCurationService._chromosome_keys
        assert keys("Ca22chr1A_C_albicans_SC5314", "C_albicans_SC5314") == [
            "ca22chr1a_c_albicans_sc5314", "ca22chr1a", "chr1a"]
        assert keys("ChrA_C_glabrata_CBS138", "C_glabrata_CBS138") == ["chra_c_glabrata_cbs138", "chra"]
