"""Tests for the PathoYeastract HTML parsers used by the regulation harvester.

The fixtures are trimmed copies of real PathoYeastract result pages
(2026-10), keeping the structures the parsers depend on: TF rows that embed
javascript menu tables, rowspanned evidence tables, and highlighted promoter
sequences.
"""
from scripts.regulation.harvest_pathoyeastract import (
    parse_associations,
    parse_documented_pairs,
    parse_evidence,
    parse_promoter,
    parse_tf_orf,
)

MENU = (
    '<div id="dropdocX" style="display:none;"><table class="jsmenu"><tr><td class="jsmenu">Rank by TF</td>'
    '</tr></table></div>'
)

ASSOCIATIONS_PAGE = f"""
<td class="content" colspan="2">
<table border="1" summary="main">
<tr><th>Transcription Factors</th><th>Documented</th><th>Potential</th></tr>
<tr>
  <td class="align"><a name="Ace2p" href="view.php?existing=protein&amp;proteinname=Ace2p">Ace2p</a></td>
  <td class="align">{MENU}
    <a href="view.php?existing=locus&amp;orfname=SAP3">SAP3</a> -
    <a href=view.php?existing=regulation&amp;proteinname=Ace2p&amp;orfname=C3_05230W_A>Reference</a><br/></td>
  <td class="align"><a href="view.php?existing=locus&amp;orfname=C3_05230W_A">SAP3</a> -
    <a href="viewconsensusinpromoter.php?orfname=C3_05230W_A&amp;consensus=CCAGC|MMCCASC&amp;subst=0">Promoter</a>
  </td>
</tr>
<tr>
  <td class="align"><a name="Ada2p" href="view.php?existing=protein&amp;proteinname=Ada2p">Ada2p</a></td>
  <td class="align">{MENU}
    <a href=view.php?existing=regulation&amp;proteinname=Ada2p&amp;orfname=C3_05230W_A>Reference</a></td>
  <td class="align"><span class="error">Not found!</span></td>
</tr>
<tr>
  <td class="align"><a name="Hot1p" href="view.php?existing=protein&amp;proteinname=Hot1p">Hot1p</a></td>
  <td class="align"><span class="error">Not found!</span></td>
  <td class="align"></td>
</tr>
<tr>
  <td class="align"><a name="Znc1p" href="view.php?existing=protein&amp;proteinname=Znc1p">Znc1p</a></td>
  <td class="align">{MENU}
    <a href=view.php?existing=regulation&amp;proteinname=Znc1p&amp;orfname=C3_05230W_A>Reference</a></td>
  <td class="align"></td>
</tr>
</table>
"""

EVIDENCE_PAGE = """
<td class="content"><table><tr><th>Transcription Factor</th><th>Log2FC</th></tr>
<tr>
 <td class="align" rowspan="3"><a href="view.php?existing=protein&amp;proteinname=Efg1p">Efg1p</a></td>
 <td rowspan="3" class="align"><a href="view.php?existing=locus&amp;orfname=C3_05230W_A">C3_05230W_A</a></td>
 <td rowspan="1" class="align">
 <a href="https://www.ncbi.nlm.nih.gov/pubmed/33168610" >PubMed&nbsp;<img src="x.png" /></a>
 Toth Hervay N et al., Yeast, 2020<span style="display:none">Toth Hervay N et al., Yeast, 2020</span> </td>
 <td class="align">Indirect</td>
 <td class="align">RNA-seq analysis - WT vs TF Deletion</td>
 <td class="align">Positive</td>
 <td class="align">SN250</td>
 <td class="align">Stress</td>
 <td class="align">Caspofungin 125&nbsp;ng/ml at 30&#176;C</td>
 <td class="align">1.12751855251835</td></tr>
 <tr> <td rowspan="2" class="align">
 <a href="https://www.ncbi.nlm.nih.gov/pubmed/41170998" >PubMed</a>
 Wang Z-H et al., mSphere, 2025</td>
 <td class="align">Direct</td>
 <td class="align">ChIP-seq</td>
 <td class="align">Negative</td>
 <td class="align">SC5314</td>
 <td class="align">Cell cycle/morphology</td>
 <td class="align">Hyphal induction</td>
 <td class="align">-</td></tr>
 <tr> <td class="align">Indirect</td>
 <td class="align">RNA-seq analysis - WT vs TF Deletion</td>
 <td class="align">Negative</td>
 <td class="align">SN250</td>
 <td class="align">Cell cycle/morphology</td>
 <td class="align">Hyphal induction</td>
 <td class="align">-1.493647</td></tr>
</table>
"""

PROMOTER_PAGE = """
<td class="content">
<table border="1" summary="orf">
 <tr><th class="align">Promoter Sequence<br/><br/><small>No match found!</small></th>
  <td class="align"><pre>>SAP3    upstream sequence, from -1000 to -1, size 1000<br/>
AAAACCGTTT<br/>GGGG</pre></td></tr>
 <tr><th class="align">Promoter Sequence<br/><small>Complementary Strand</small><br/><br/>
  Consensus<small><br/>MMCCASC: -861 -898 </small></th>
  <td class="align"><pre>>SAP3	Upstream Sequence Complementary Strand<br/>
TT<span style="background-color:#00FF00">TTGG</span>CAAA<br/>
CC<span style="background-color:#00FF00">CC</span></pre></td></tr>
</table>
"""


def test_parse_associations_classifies_documented_and_potential():
    rows = {row["tf"]: row for row in parse_associations(ASSOCIATIONS_PAGE)}

    assert set(rows) == {"Ace2p", "Ada2p", "Hot1p", "Znc1p"}
    assert rows["Ace2p"]["documented_targets"] == ["C3_05230W_A"]
    assert rows["Ace2p"]["potential"] == {"C3_05230W_A": "CCAGC|MMCCASC"}
    assert rows["Ace2p"]["potential_status"] == "found"
    # TF has a known consensus, but it is absent from this promoter
    assert rows["Ada2p"]["potential_status"] == "not_found"
    # Neither documented nor a known consensus
    assert rows["Hot1p"]["documented_targets"] == []
    assert rows["Hot1p"]["potential_status"] == "no_consensus"


def test_parse_associations_keeps_last_row_despite_nested_menu_tables():
    rows = parse_associations(ASSOCIATIONS_PAGE)

    assert rows[-1]["tf"] == "Znc1p"
    assert rows[-1]["documented_targets"] == ["C3_05230W_A"]


def test_parse_associations_strips_stray_marker_from_target_names():
    page = (
        '<td class="content"><tr><td class="align"><a name="Mrr1p" '
        'href="view.php?existing=protein&amp;proteinname=Mrr1p">Mrr1p</a></td>'
        '<td class="align"></td><td class="align">'
        '<a href="viewconsensusinpromoter.php?orfname=>C3_03320W_B&amp;consensus=TCCGA&amp;subst=0">P</a>'
        '</td></tr>'
    )
    assert parse_associations(page)[0]["potential"] == {"C3_03320W_B": "TCCGA"}


def test_parse_evidence_carries_rowspanned_reference_to_following_rows():
    evidence = parse_evidence(EVIDENCE_PAGE)

    assert [e["pubmed"] for e in evidence] == [33168610, 41170998, 41170998]
    assert evidence[0]["citation"] == "Toth Hervay N et al., Yeast, 2020"
    assert evidence[0]["evidence_code"] == "Indirect"
    assert evidence[0]["association_type"] == "Positive"
    assert evidence[0]["environmental_condition"] == "Caspofungin 125 ng/ml at 30°C"
    assert evidence[0]["log2fc"] == 1.12751855251835
    assert evidence[1]["evidence_code"] == "Direct"
    assert evidence[1]["log2fc"] is None
    assert evidence[2]["association_type"] == "Negative"


def test_parse_promoter_maps_highlights_to_upstream_coordinates():
    promoter = parse_promoter(PROMOTER_PAGE)

    assert promoter["sequence"] == "AAAACCGTTTGGGG"
    assert promoter["sites"]["+"] == []
    # Column 2..5 and 12..13 of the complement, counted from -1000
    assert promoter["sites"]["-"] == [[-998, -995], [-988, -987]]
    assert promoter["consensus_positions"] == {"MMCCASC": {"-": [-861, -898]}}


def test_parse_tf_orf_picks_systematic_name_from_fasta_header():
    named = '<td class="content"><td class="align"><pre>&gt;MRR1 C3_05920W_A<br/>MSIATT</pre>'
    unnamed = '<td class="content"><td class="align"><pre>&gt;B9J08_004820 PIS49793<br/>MSAVV</pre>'

    assert parse_tf_orf(named, "calbicans") == "C3_05920W_A"
    assert parse_tf_orf(unnamed, "cauris") == "B9J08_004820"
    assert parse_tf_orf(unnamed, "calbicans") is None


def test_parse_documented_pairs_reads_evidence_links():
    page = (
        '<td class="content"><a href="view.php?existing=protein&amp;proteinname=Mrr1p">Mrr1p</a>'
        '<a href=view.php?existing=locus&amp;orfname=C3_06470W_A> AAF1</a> - '
        '<a href=view.php?existing=regulation&amp;proteinname=Mrr1p&amp;orfname=C3_06470W_A>Reference</a>'
        '<a href="view.php?existing=regulation&amp;proteinname=Mrr1p&amp;orfname=>C2_06970W_A">Reference</a>'
    )
    assert parse_documented_pairs(page) == {("Mrr1p", "C3_06470W_A"), ("Mrr1p", "C2_06970W_A")}
