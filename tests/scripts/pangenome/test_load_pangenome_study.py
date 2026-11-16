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


def test_parse_munoz2021(tmp_path):
    s1 = openpyxl.Workbook()
    s1.active.title = "S1. Sequencing"
    s1.active.append(["Supplementary Table 1."])
    s1.active.append(["Strain", "Clade", "Country"])
    s1.active.append(["B11220", 2, "Japan"])
    s1.save(tmp_path / loader.MUNOZ_FILES["isolates"])
    s2 = openpyxl.Workbook()
    s2.active.title = "S2. Genes loss"
    s2.active.append(["Supplementary Table 2."])
    s2.active.append(["B8441 regions not present in B11220 of length > 5kb"])
    s2.active.append(["Region ID", "B8441 scaffold", "Chromosome", "Start", "End", "Length", "# genes",
                      "# multi copy", "Notes", "IDs"])
    s2.active.append(["del1", "scaffold00001", "chr1", 100, 1000, 901, 1, 0, "2 adhesins", "B9J08_000010"])
    s2.save(tmp_path / loader.MUNOZ_FILES["regions"])
    s3 = openpyxl.Workbook()
    s3.active.title = "S3. DeletedinOtherClade2"
    s3.active.append(["Supplementary Table 3."])
    s3.active.append(["Region ID", "strain", "type", "coordinates", "length", "normalizedRD", "p-val"])
    s3.active.append(["del1", "B11220", "deletion", "scaffold00001:1-1000", 1000, 0.0469, 1e-12])
    s3.active.append(["del1", "B14308", "deletion", "scaffold00001:1-1000", 1000, 0.0042, 1e-12])
    s3.active.append(["*Manual annotation", None, None, None, None, None, None])
    s3.save(tmp_path / loader.MUNOZ_FILES["deletions"])
    (tmp_path / loader.MUNOZ_FILES["features"]).write_text(AURIS_FEATURES)
    with patch.object(loader, "resolve_retired_names", lambda names, abbrev: {n: n for n in names}):
        study, genes = loader.parse_munoz2021(tmp_path)
    gene = genes[0]
    assert (gene["id"], gene["category"], gene["presence"], gene["chromosome"]) == (
        "B9J08_000010", "accessory", 0, "Chr1_C_auris_B8441")
    assert gene["copy_number"] == {"B11220": 0.05, "B14308": 0.0} and gene["type"] == "Lost in clade II (del1)"
    assert gene["note"] == "Region del1 (chr1:100-1,000, 0.9 kb): 2 adhesins"
    assert study["isolates"] == [{"name": "B11220", "origin": "clade II, Japan"},
                                 {"name": "B14308", "origin": "clade II"}]
    assert study["copy_number_baseline"] == 1 and loader.STUDIES["munoz2021"]["section"] == "strain_variation"


def test_ryan2025_split(tmp_path):
    het = {"chromosome": "cpar_Chr_1", "position": 50, "heterozygous": ["A"], "homozygous": [], "isolates": 1}
    hom = {"chromosome": "cpar_Chr_1", "position": 90, "heterozygous": [], "homozygous": ["B"], "isolates": 1}
    nobody = {"chromosome": "cpar_Chr_1", "position": 70, "heterozygous": [], "homozygous": [], "isolates": 0}
    genes = [
        {"id": "CPAR2_100010", "category": "core", "in_reference": True, "reference_gene": "CPAR2_100010",
         "presence": None, "chromosome": "cpar_Chr_1", "start": 1, "end": 300, "stop_codons": [het, hom, nobody]},
        {"id": "CPAR2_100020", "category": "core", "in_reference": True, "reference_gene": "CPAR2_100020",
         "presence": None, "stop_codons": [nobody]},
        {"id": "CPAR2_100030", "category": "accessory", "in_reference": True, "presence": 7, "stop_codons": []},
    ]
    tables = {"isolates": [{"name": "A", "origin": "Ireland"}, {"name": "B"}, {"name": "C"}],
              "stop_codon_isolates": ["A", "B"]}
    with patch.object(loader, "ryan2025_tables", lambda src: (tables, [dict(g) for g in genes])):
        pangenome, pan_genes = loader.parse_ryan2025(tmp_path)
        stops, stop_genes = loader.parse_ryan2025_stops(tmp_path)
    # the pangenome keeps every gene but no stop codons
    assert len(pan_genes) == 3 and all(g["stop_codons"] == [] for g in pan_genes)
    assert pangenome["stop_codons_reported"] is False and pangenome["isolate_count"] == 376
    # strain variation: genes with a stop some isolate carries, without the sites nobody carries
    assert [g["id"] for g in stop_genes] == ["CPAR2_100010"]
    gene = stop_genes[0]
    assert gene["type"] == "Stop in 2 isolates, homozygous in 1 (2 sites)" and len(gene["stop_codons"]) == 2
    assert stops["presence_reported"] is False and stops["isolate_count"] == 2
    assert [i["name"] for i in stops["isolates"]] == ["A", "B"]
    assert "2 stop sites that no isolate carries" in stops["notes"][2]


TROP_FEATURES = (
    "! header\n"
    "CTRG_00005\t\t\tORF|Verified\tSupercontig_3.1_C_tropicalis_MYA-3404\t1000\t4012\tW\n"
    "CTRG_00009\t\t\tORF|Verified\tSupercontig_3.1_C_tropicalis_MYA-3404\t9000\t6001\tC\n"
)


def test_parse_obrien2021_stops(tmp_path):
    import gzip
    ann = "T|stop_gained|HIGH|CTRG_00005|CTRG_00005|transcript|t|protein_coding|1/1|c.2702C>A|p.Ser901*|x|x|901/1337||"
    ann2 = "G|stop_gained|HIGH|CTRG_00009|CTRG_00009|transcript|t|protein_coding|1/1|c.4G>T|p.Glu2*|x|x|2/999||"
    lines = [
        "##fileformat=VCFv4.2",
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tct01.ct01\tct02.ct02\tct03.ct03",
        f"ctro_c_1.2\t9672\t.\tC\tT\t100\tPASS\tAC=3;ANN={ann}\tGT:DP\t0/1:20\t1/1:30\t./.:0",
        f"ctro_c_1.2\t500\t.\tA\tG\t100\tPASS\tAC=0;ANN={ann2}\tGT\t0/0\t0/0\t0/0",  # carried by no isolate
        f"ctro_c_1.2\t800\t.\tA\tG\t100\tLowQual\tANN={ann2}\tGT\t1/1\t1/1\t1/1",  # filtered out
    ]
    with gzip.open(tmp_path / loader.OBRIEN_FILES["variants"], "wt") as f:
        f.write("\n".join(lines) + "\n")
    wb = openpyxl.Workbook()
    wb.active.append(["Table S1. List of strains"])
    wb.active.append(["Strain name", "Strain code", "Strain description", "Source"])
    wb.active.append(["SJH2014 ct01", "ct01", "Clinical", "Dublin"])
    wb.save(tmp_path / loader.OBRIEN_FILES["isolates"])
    (tmp_path / loader.OBRIEN_FILES["features"]).write_text(TROP_FEATURES)
    with patch.object(loader, "resolve_retired_names", lambda names, abbrev: {n: n for n in names}):
        study, genes = loader.parse_obrien2021_stops(tmp_path)
    assert [g["id"] for g in genes] == ["CTRG_00005"]
    stop = genes[0]["stop_codons"][0]
    assert (stop["heterozygous"], stop["homozygous"]) == (["SJH2014 ct01"], ["ct02"])
    # drawn on CGD's gene from the protein position: 1000 + 3 * 900
    assert (stop["position"], stop["protein_change"], stop["aa_position"]) == (3700, "p.Ser901*", 901)
    assert stop["source_position"] == "ctro_c_1.2:9,672 (Assembly B)" and stop["position_approximate"]
    assert genes[0]["type"] == "Stop in 2 isolates, homozygous in 1"
    assert study["isolate_count"] == 3 and study["presence_reported"] is False
    assert study["isolates"][0] == {"name": "SJH2014 ct01", "origin": "Clinical, Dublin"}
