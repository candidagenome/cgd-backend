"""Tests for multi-target ortholog conversion (request contract + merge)."""
import pytest
from pydantic import ValidationError

from cgd.api.services import ortholog_converter_service as svc
from cgd.schemas.ortholog_converter_schema import (
    OrthologConvertRequest,
    OrthologConvertResponse,
    OrthologResult,
    TargetOrganism,
)


class TestRequestContract:
    def test_single_target_legacy_form(self):
        req = OrthologConvertRequest(
            gene_ids=["ACT1"], target_organism=TargetOrganism.C_GLABRATA)
        assert req.targets == [TargetOrganism.C_GLABRATA]

    def test_multi_target_form(self):
        req = OrthologConvertRequest(
            gene_ids=["ACT1"],
            target_organisms=[TargetOrganism.C_GLABRATA,
                              TargetOrganism.C_AURIS])
        assert req.targets == [TargetOrganism.C_GLABRATA,
                               TargetOrganism.C_AURIS]

    def test_multi_target_deduplicates_preserving_order(self):
        req = OrthologConvertRequest(
            gene_ids=["ACT1"],
            target_organisms=[TargetOrganism.C_AURIS,
                              TargetOrganism.C_GLABRATA,
                              TargetOrganism.C_AURIS])
        assert req.targets == [TargetOrganism.C_AURIS,
                               TargetOrganism.C_GLABRATA]

    def test_rejects_both_target_forms(self):
        with pytest.raises(ValidationError):
            OrthologConvertRequest(
                gene_ids=["ACT1"],
                target_organism=TargetOrganism.C_AURIS,
                target_organisms=[TargetOrganism.C_GLABRATA])

    def test_rejects_neither_target_form(self):
        with pytest.raises(ValidationError):
            OrthologConvertRequest(gene_ids=["ACT1"])


def _fake_response(target_display, rows):
    return OrthologConvertResponse(
        source_organism="CGD Species",
        target_organism=target_display,
        total_input=len(rows),
        found_count=sum(1 for r in rows if r.found),
        converted_count=sum(1 for r in rows if r.ortholog_id),
        results=rows,
    )


class TestMultiTargetMerge:
    def test_gene_major_merge_with_no_ortholog_rows(self, monkeypatch):
        def fake_convert(db, gene_ids, target_organism,
                         source_organism=None, feature_cache=None):
            display = {"C_glabrata_CBS138": "Candida glabrata CBS138",
                       "C_auris_B8441": "Candida auris B8441"}[target_organism.value]
            rows = []
            for g in gene_ids:
                has = not (g == "ORPHAN" and "auris" in display)
                rows.append(OrthologResult(
                    input_id=g, found=True,
                    ortholog_id=f"{g}_orth" if has else None,
                    target_organism=display,
                    relationship="1:1" if has else "no_ortholog"))
            return _fake_response(display, rows)

        monkeypatch.setattr(svc, "convert_orthologs", fake_convert)
        out = svc.convert_orthologs_multi(
            db=None, gene_ids=["ACT1", "ORPHAN"],
            target_organisms=[TargetOrganism.C_GLABRATA,
                              TargetOrganism.C_AURIS])

        assert out.target_organisms == ["Candida glabrata CBS138",
                                        "Candida auris B8441"]
        # gene-major: ACT1 x both targets, then ORPHAN x both targets
        assert [(r.input_id, r.target_organism) for r in out.results] == [
            ("ACT1", "Candida glabrata CBS138"),
            ("ACT1", "Candida auris B8441"),
            ("ORPHAN", "Candida glabrata CBS138"),
            ("ORPHAN", "Candida auris B8441"),
        ]
        orphan_auris = out.results[3]
        assert orphan_auris.relationship == "no_ortholog"
        assert orphan_auris.target_organism == "Candida auris B8441"
        assert out.converted_count == 3

    def test_single_target_passthrough_sets_plural_field(self, monkeypatch):
        def fake_convert(db, gene_ids, target_organism,
                         source_organism=None, feature_cache=None):
            return _fake_response("Candida auris B8441", [OrthologResult(
                input_id="ACT1", found=True, ortholog_id="X",
                target_organism="Candida auris B8441", relationship="1:1")])

        monkeypatch.setattr(svc, "convert_orthologs", fake_convert)
        out = svc.convert_orthologs_multi(
            db=None, gene_ids=["ACT1"],
            target_organisms=[TargetOrganism.C_AURIS])
        assert out.target_organisms == ["Candida auris B8441"]
        assert len(out.results) == 1
