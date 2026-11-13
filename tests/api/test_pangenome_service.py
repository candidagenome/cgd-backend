"""Tests for the pangenome viewer service (cgd/api/services/pangenome_service.py)."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from cgd.api.services import pangenome_service as svc

STUDY = {
    "study_id": "demo2025", "organism_abbrev": "C_parapsilosis_CDC317", "species": "Candida parapsilosis",
    "reference_strain": "CDC317", "title": "Demo pangenome", "citation": "Doe J et al. (2025) Demo. J 1:1",
    "pubmed": 1, "isolate_count": 10, "loaded": "2026-11-12",
    "counts": {"genes": 4, "core": 2, "accessory": 2, "not_in_reference": 1, "with_stop_codons": 1,
               "isolates_listed": 2},
    "notes": ["demo"], "divergent_strains": ["S1"],
    "isolates": [{"name": "A", "origin": "Ireland"}, {"name": "B", "origin": "USA"}],
}
STOP = {"chromosome": "chr1", "position": 50, "aa_position": 10, "isolates": 2, "homozygous_isolates": 1,
        "heterozygous": ["A"], "homozygous": ["B"]}
GENES = [
    {"id": "CPAR2_100010", "category": "core", "in_reference": True, "type": "CDC317 Reference Gene",
     "reference_gene": "CPAR2_100010", "chromosome": "chr1", "start": 1, "end": 300, "stop_codons": [STOP],
     "divergent_strains": {"S1": {"presence": "Present", "identity": 99.5, "hit_status": "MBH"}}},
    {"id": "CPAR2_100020", "category": "accessory", "in_reference": True, "type": "CDC317 Reference Gene",
     "presence": 7, "reference_gene": "CPAR2_100020", "ortholog": "SIT1", "chromosome": "chr1", "start": 400,
     "end": 900, "stop_codons": []},
    {"id": "PARALOG_100020_1", "category": "accessory", "in_reference": False, "type": "Tandem Duplicate",
     "presence": 3, "reference_gene": "CPAR2_100020", "stop_codons": []},
    {"id": "HYPOTHETICAL_000001", "category": "core", "in_reference": False, "type": "Not in CDC317",
     "reference_gene": None, "stop_codons": []},
]


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    study_dir = tmp_path / "C_parapsilosis_CDC317" / "demo2025"
    study_dir.mkdir(parents=True)
    (study_dir / "study.json").write_text(json.dumps(STUDY))
    (study_dir / "genes.json").write_text(json.dumps(GENES))
    # a half-written reload must be ignored
    (tmp_path / "C_parapsilosis_CDC317" / "demo2025.new").mkdir()
    monkeypatch.setattr(svc.settings, "pangenome_data_dir", str(tmp_path))
    monkeypatch.setattr(svc, "_studies", {})
    monkeypatch.setattr(svc, "_gene_name_map", lambda db, abbrev: {"CPAR2_100020": "SIT1"})
    return tmp_path


@pytest.fixture
def db():
    db = MagicMock()
    db.query.return_value.filter.return_value.scalar.return_value = "CAL0000271000"
    return db


def test_list_studies(data_dir, db):
    studies = svc.list_studies(db).studies
    assert [s.study_id for s in studies] == ["demo2025"]
    assert studies[0].reference_dbxref_id == "CAL0000271000"
    assert studies[0].counts.accessory == 2


def test_get_study_has_isolates(data_dir, db):
    study = svc.get_study(db, "demo2025")
    assert [i.name for i in study.isolates] == ["A", "B"]
    with pytest.raises(svc.PangenomeNotFound):
        svc.get_study(db, "nope")


def test_list_genes_filters(data_dir, db):
    assert svc.list_genes(db, "demo2025").total == 4
    accessory = svc.list_genes(db, "demo2025", category="accessory", sort="presence").genes
    assert [g.id for g in accessory] == ["PARALOG_100020_1", "CPAR2_100020"]
    assert {g.id for g in svc.list_genes(db, "demo2025", reference="novel").genes} == {
        "PARALOG_100020_1", "HYPOTHETICAL_000001"}
    assert [g.id for g in svc.list_genes(db, "demo2025", stops=True).genes] == ["CPAR2_100010"]
    # search matches the current CGD gene name, not just study IDs
    found = {g.id for g in svc.list_genes(db, "demo2025", query="sit1").genes}
    assert found == {"CPAR2_100020", "PARALOG_100020_1"}


def test_list_genes_pages_without_stop_detail(data_dir, db):
    page = svc.list_genes(db, "demo2025", page=2, page_size=3)
    assert page.total == 4 and len(page.genes) == 1
    first = svc.list_genes(db, "demo2025", stops=True).genes[0]
    assert first.stop_codon_count == 1 and first.stop_codon_isolates == 2 and first.stop_codons == []


def test_locus_includes_derived_accessory_genes(data_dir, db):
    feature = SimpleNamespace(feature_name="CPAR2_100020", gene_name="SIT1",
                              organism=SimpleNamespace(organism_abbrev="C_parapsilosis_CDC317",
                                                       organism_name="Candida parapsilosis CDC317"))
    db.query.return_value.options.return_value.join.return_value.filter.return_value \
        .filter.return_value.filter.return_value.all.return_value = [feature]
    with patch.object(svc, "_filter_features_by_preference", lambda db, features: features):
        result = svc.get_locus_pangenome(db, "SIT1")
    assert result.feature_name == "CPAR2_100020"
    genes = result.studies[0].genes
    assert [g.id for g in genes] == ["CPAR2_100020", "PARALOG_100020_1"]  # the gene itself first
    assert genes[0].gene_name == "SIT1" and result.studies[0].isolate_count == 10


def test_locus_without_studies_is_empty(tmp_path, monkeypatch, db):
    monkeypatch.setattr(svc.settings, "pangenome_data_dir", str(tmp_path / "missing"))
    assert svc.get_locus_pangenome(db, "ACT1").studies == []


def test_downloads(data_dir, db):
    name, body = svc.download_table(db, "demo2025", "accessory")
    lines = [line for line in body.splitlines() if not line.startswith("#")]
    assert name == "demo2025_accessory.tsv"
    assert lines[0].split("\t")[4] == "presence_of_10" and len(lines) == 3
    _, stops = svc.download_table(db, "demo2025", "stop_codons")
    assert "CPAR2_100010\tCPAR2_100010\t\tchr1\t50\t10\t2\t1\tA\tB" in stops
    with pytest.raises(svc.PangenomeNotFound):
        svc.download_table(db, "demo2025", "everything")


def test_get_gene_includes_stops(data_dir, db):
    gene = svc.get_gene(db, "demo2025", "cpar2_100010")
    assert gene.stop_codons[0].homozygous == ["B"] and gene.divergent_strains["S1"].identity == 99.5
    with pytest.raises(svc.PangenomeNotFound):
        svc.get_gene(db, "demo2025", "CPAR2_999999")


def test_map_places_variants_at_their_reference_gene(data_dir, db):
    result = svc.get_map(db, "demo2025")
    placed = {g.id: g for g in result.genes}
    assert [c.name for c in result.chromosomes] == ["chr1"] and result.chromosomes[0].length == 900
    paralog = placed["PARALOG_100020_1"]
    assert (paralog.start, paralog.end, paralog.placed_by) == (400, 900, "reference_gene")
    assert placed["CPAR2_100020"].placed_by == "own" and placed["CPAR2_100020"].gene_name == "SIT1"
    assert placed["CPAR2_100010"].stops[0].isolates == 2 and placed["CPAR2_100010"].stops[0].homozygous == 1
    assert [g.id for g in result.unplaced] == ["HYPOTHETICAL_000001"]
