"""Tests for the C. glabrata parser in scripts/pangenome/load_pangenome_study.py."""
import importlib.util
from pathlib import Path
from unittest.mock import patch

import openpyxl

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "pangenome" / "load_pangenome_study.py"
spec = importlib.util.spec_from_file_location("load_pangenome_study", SCRIPT)
loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loader)

FEATURES = (
    "! header\n"
    "CAGL0A00110g\t\tOLD_A00110\tORF|Verified\tChrA_C_glabrata_CBS138\t900\t100\tC\n"
    "CAGL0B00154g\t\t\tORF|Verified\tChrB_C_glabrata_CBS138\t2000\t3000\tW\n"
)


def write_tables(path: Path) -> None:
    wb = openpyxl.Workbook()
    wb.active.title = "Supplementary_table_1"
    wb["Supplementary_table_1"].append(["Strain", "Genome size (Mb)", "Number of scaffolds", "N50"])
    wb["Supplementary_table_1"].append(["S1", 12.4, 15, 1049635])
    s3 = wb.create_sheet("Supplementary_table_3")
    s3.append(["Strain", "Number of genes"])
    s3.append(["S1 (Cg16)", 5292])
    s4 = wb.create_sheet("Supplementary_table_4")
    s4.append(["OrthoGroup", "Tag", "Filtering", "Reference", "S1", "S2"])
    s4.append(["OG1", "Core", "-", "CAGL0A00110g, CAGL0B00116g", "a1, a2", "b1, b2"])
    s4.append(["OG2", "Accessory", "Genuine", "-", "a3", "-"])
    s4.append(["OG3", "Accessory", "Sequencing error", "CAGL0C00100g", "a4", "-"])
    s4.append(["OG4", "Accessory", "Unclear", "CAGL0K07348g", "-", "b4"])
    s4.append(["OG5", "Accessory", "Miss-prediction", "-", "a5", "-"])
    wb.save(path)


def test_parse_marcet2022(tmp_path):
    write_tables(tmp_path / loader.MARCET_FILES["tables"])
    (tmp_path / loader.MARCET_FILES["features"]).write_text(FEATURES)
    # CAGL0B00116g was renamed in CGD; CAGL0K07348g was merged and keeps its name
    renamed = {"CAGL0B00116g": "CAGL0B00154g", "CAGL0K07348g": "CAGL0K07348g", "CAGL0C00100g": "CAGL0C00100g"}
    with patch.object(loader, "resolve_retired_names", lambda names, abbrev: {n: renamed[n] for n in names}):
        study, genes = loader.parse_marcet2022(tmp_path)
    by_id = {g["id"]: g for g in genes}
    # one record per CBS138 gene, family-level presence; artefact families' CBS138 genes are core,
    # flagged, and artefact families without a CBS138 gene are dropped
    assert set(by_id) == {"CAGL0A00110g", "CAGL0B00116g", "OG2", "CAGL0C00100g", "CAGL0K07348g"}
    artefact = by_id["CAGL0C00100g"]
    assert (artefact["category"], artefact["type"], artefact["presence"]) == (
        "core", "Core (gap: sequencing error)", 1)
    a = by_id["CAGL0A00110g"]
    assert (a["category"], a["presence"], a["chromosome"], a["start"], a["end"]) == (
        "core", 2, "ChrA_C_glabrata_CBS138", 100, 900)
    assert a["isolate_genes"] == {"S1": ["a1", "a2"], "S2": ["b1", "b2"]}
    assert "1 other CBS138 gene: CAGL0B00116g" in a["note"]
    assert (by_id["CAGL0B00116g"]["reference_gene"], by_id["CAGL0B00116g"]["start"]) == ("CAGL0B00154g", 2000)
    merged = by_id["CAGL0K07348g"]
    assert (merged["type"], merged["presence"], merged["chromosome"]) == ("Accessory (unclear)", 1, None)
    assert (by_id["OG2"]["in_reference"], by_id["OG2"]["reference_gene"]) == (False, None)
    assert study["isolate_count"] == 2 and study["stop_codons_reported"] is False
    assert study["isolates"][0] == {"name": "S1", "assembly_level": "Chromosome-level",
                                    "platform": "long + short reads", "contigs": 15, "n50": 1049635,
                                    "gene_count": 5292}
    assert "its 1 CBS138 genes are listed as core" in study["notes"][1]
    assert "the 1 such families with no CBS138 gene are not shown" in study["notes"][1]


CPAR_FEATURES = (
    "! header\n"
    "CPAR2_100010\t\t\tORF|Verified\tContig005569_C_parapsilosis_CDC317\t100\t400\tW\n"
    "CPAR2_100020\t\t\tORF|Verified\tContig005569_C_parapsilosis_CDC317\t900\t500\tC\n"
)


def test_parse_bergin2024(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "ORF"
    ws.append(["Strain", "CPAR2_100010", "CPAR2_100020"])
    ws.append(["Cp1", 2.04, 0.01])
    ws.append(["Cp2", 41.83, 1.97])
    ws.append([None, None, None])  # trailing blank rows in the sheet
    wb.save(tmp_path / loader.BERGIN_FILES["copy_number"])
    (tmp_path / loader.BERGIN_FILES["features"]).write_text(CPAR_FEATURES)
    with patch.object(loader, "resolve_retired_names", lambda names, abbrev: {n: n for n in names}):
        study, genes = loader.parse_bergin2024(tmp_path)
    by_id = {g["id"]: g for g in genes}
    core, absent = by_id["CPAR2_100010"], by_id["CPAR2_100020"]
    assert (core["category"], core["presence"], core["copy_number_max"]) == ("core", 2, 41.83)
    assert core["copy_number"] == {"Cp1": 2.04, "Cp2": 41.83}
    # contigs are labelled by the chromosome their CPAR2 genes are numbered for
    assert (core["chromosome"], absent["start"], absent["end"]) == ("cpar_Chr_1", 500, 900)
    assert (absent["category"], absent["presence"], absent["type"], absent["note"]) == (
        "accessory", 1, "Absent from 1 strain", "Absent from Cp1")
    assert study["isolate_count"] == 2 and study["copy_number_reported"] is True
    assert [i["name"] for i in study["isolates"]] == ["Cp1", "Cp2"]


def test_parse_wang2025(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["PG_ID", "BMU_1", "BMU_2", "CBS138_homolog", "PG_type"])
    ws.append(["2PG1", "1_g", "7_g|9_g", "CAGL0A00110g", "Core"])
    ws.append(["1PG2", "2_g", "-", "CAGL0A00110g", "Cloud"])  # a split piece of the same CBS138 gene
    ws.append(["1PG3", "-", "8_g", "-", "Shell"])
    ws.append(["1PG4", "3_g", "-", "CAGL0B00116g", "Softcore"])
    wb.save(tmp_path / loader.WANG_FILES["pangenome"])
    (tmp_path / loader.WANG_FILES["features"]).write_text(FEATURES)
    with patch.object(loader, "resolve_retired_names", lambda names, abbrev: {n: "CAGL0B00154g" for n in names}):
        study, genes = loader.parse_wang2025(tmp_path)
    by_id = {g["id"]: g for g in genes}
    assert set(by_id) == {"CAGL0A00110g", "1PG2", "1PG3", "CAGL0B00116g"}
    named = by_id["CAGL0A00110g"]
    assert (named["category"], named["type"], named["presence"], named["start"]) == ("core", "Core", 2, 100)
    assert named["isolate_genes"] == {"BMU_1": ["1_g"], "BMU_2": ["7_g", "9_g"]}
    # the less widespread family matching the same gene is listed by family ID, drawn at the gene
    piece = by_id["1PG2"]
    assert (piece["reference_gene"], piece["chromosome"], piece["category"]) == ("CAGL0A00110g", None, "accessory")
    assert "named for family 2PG1" in piece["note"]
    assert (by_id["1PG3"]["in_reference"], by_id["1PG3"]["reference_gene"]) == (False, None)
    soft = by_id["CAGL0B00116g"]
    assert (soft["category"], soft["type"], soft["reference_gene"]) == ("core", "Soft-core", "CAGL0B00154g")
    assert study["isolate_count"] == 2 and [i["name"] for i in study["isolates"]] == ["BMU_1", "BMU_2"]


AURIS_FEATURES = (
    "! header\n"
    "B9J08_000010\t\t\tORF|Verified\tChr1_C_auris_B8441\t500\t100\tC\n"
)


def test_parse_xu2025(tmp_path):
    wb = openpyxl.Workbook()
    s1 = wb.active
    s1.title = "Table S1"
    s1.append(["Table S1. Isolates"])
    s1.append(["ID", "Strain", "Country", "Organization", "BioProject", "Strategy", "Selection", "Source",
               "Layout", "Platform", "Model", "Sample type", "Isolation Source"])
    s1.append(["SRR1", "CBS1", "Korea", "x", "PRJ", "WGS", "PCR", "GENOMIC", "PAIRED", "ILLUMINA", "NovaSeq",
               "unknown", "blood"])
    s1.append(["SRR2", 9109611, "USA", "x", "PRJ", "WGS", "PCR", "GENOMIC", "PAIRED", "ILLUMINA", "NovaSeq",
               "unknown", "NA"])
    s3 = wb.create_sheet("Table S3")
    s3.append(["Table S3. Assemblies"])
    s3.append(["ID", "Total length", "Number of contigs", "N50"])
    s3.append(["SRR1", 12000000, 120, 180000])
    s3.append(["BJCA001", 12300000, 97, 240058])  # an assembly Table S1 does not list
    s4 = wb.create_sheet("Table S4")
    s4.append(["Table S4. Genes"])
    s4.append(["Gene name", "Genome compartment", "Number of isolates", "Percent"])
    s4.append(["B9J08_000010", "Core", 2, 1])
    s4.append(["B9J08_000299", "Softcore", 2, 1])
    s4.append(["CA_000001", "Cloud", 1, 0.5])
    wb.save(tmp_path / loader.XU_FILES["tables"])
    (tmp_path / loader.XU_FILES["features"]).write_text(AURIS_FEATURES)
    with patch.object(loader, "resolve_retired_names", lambda names, abbrev: {n: n for n in names}):
        study, genes = loader.parse_xu2025(tmp_path)
    by_id = {g["id"]: g for g in genes}
    assert (by_id["B9J08_000010"]["chromosome"], by_id["B9J08_000010"]["start"]) == ("Chr1_C_auris_B8441", 100)
    # a B8441 gene missing from the feature file keeps its name and is not drawn
    assert (by_id["B9J08_000299"]["type"], by_id["B9J08_000299"]["category"]) == ("Soft-core", "core")
    assert by_id["B9J08_000299"]["chromosome"] is None
    novel = by_id["CA_000001"]
    assert (novel["category"], novel["in_reference"], novel["reference_gene"]) == ("accessory", False, None)
    assert study["isolate_count"] == 2
    # a strain named by a number in the sheet keeps it as text
    assert [i["name"] for i in study["isolates"]] == ["CBS1", "9109611", "BJCA001"]
    assert study["isolates"][0]["origin"] == "Korea, blood" and study["isolates"][0]["n50"] == 180000


def test_parse_wang2025_cnv(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["PG_ID", "BMU_1", "BMU_2", "CBS138_homolog", "PG_type"])
    ws.append(["2PG1", "1_g", "7_g|9_g", "CAGL0A00110g", "Core"])
    ws.append(["1PG3", "-", "8_g", "-", "Shell"])
    wb.save(tmp_path / loader.WANG_CNV_FILES["pangenome"])
    cnv = openpyxl.Workbook()
    sheet = cnv.active
    sheet.title = "Copy_number_varied_PG_families"
    sheet.append(["PG_ID", "BMU_1", "BMU_2"])
    sheet.append(["2PG1", "1_g", "7_g|9_g|10_g"])
    sheet.append(["1PG3", "-", "8_g|11_g"])
    cnv.save(tmp_path / loader.WANG_CNV_FILES["copy_number"])
    (tmp_path / loader.WANG_CNV_FILES["features"]).write_text(FEATURES)
    with patch.object(loader, "resolve_retired_names", lambda names, abbrev: {n: n for n in names}):
        study, genes = loader.parse_wang2025_cnv(tmp_path)
    by_id = {g["id"]: g for g in genes}
    # copy number is the count of an isolate's genes in the family; families are named as in the pangenome
    named = by_id["CAGL0A00110g"]
    assert named["copy_number"] == {"BMU_1": 1, "BMU_2": 3} and named["category"] == "core"
    assert (named["chromosome"], named["type"]) == ("ChrA_C_glabrata_CBS138", "Core family")
    unnamed = by_id["1PG3"]
    assert (unnamed["copy_number_min"], unnamed["copy_number_max"], unnamed["presence"]) == (0, 2, 1)
    assert unnamed["type"] == "Absent from 1 isolate"
    assert study["copy_number_baseline"] == 1 and study["isolate_count"] == 2
