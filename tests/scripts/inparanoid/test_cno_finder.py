#!/usr/bin/env python3
"""
Unit tests for scripts/inparanoid/cno_finder.py

Tests the closest non-ortholog (CNO) finding functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from inparanoid.cno_finder import (
    parse_cluster_xml,
    read_blast_file,
    find_cno,
    Cluster,
    SCORE_CUTOFF,
    SEQ_OVERLAP_CUTOFF,
    SEGMENT_COVERAGE_CUTOFF,
)


class TestConstants:
    """Tests for module constants."""

    def test_score_cutoff(self):
        """Test that score cutoff is reasonable."""
        assert SCORE_CUTOFF == 50
        assert SCORE_CUTOFF > 0

    def test_seq_overlap_cutoff(self):
        """Test that sequence overlap cutoff is reasonable."""
        assert SEQ_OVERLAP_CUTOFF == 0.5
        assert 0 < SEQ_OVERLAP_CUTOFF <= 1

    def test_segment_coverage_cutoff(self):
        """Test that segment coverage cutoff is reasonable."""
        assert SEGMENT_COVERAGE_CUTOFF == 0.25
        assert 0 < SEGMENT_COVERAGE_CUTOFF <= 1


class TestCluster:
    """Tests for Cluster dataclass."""

    def test_cluster_creation(self):
        """Test creating a Cluster."""
        cluster = Cluster(cluster_id="1")
        assert cluster.cluster_id == "1"
        assert cluster.orthologs_a == []
        assert cluster.orthologs_b == []

    def test_cluster_with_orthologs(self):
        """Test creating a Cluster with orthologs."""
        cluster = Cluster(
            cluster_id="2",
            orthologs_a=["gene1", "gene2"],
            orthologs_b=["gene3"]
        )
        assert len(cluster.orthologs_a) == 2
        assert len(cluster.orthologs_b) == 1


class TestParseClusterXml:
    """Tests for parse_cluster_xml function."""

    def test_parse_basic_xml(self, temp_file):
        """Test parsing basic cluster XML."""
        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER CLUSTERNO="1">
        <GENE SPECIES="Candida_albicans" PROTID="orf19.1"/>
        <GENE SPECIES="Candida_albicans" PROTID="orf19.2"/>
        <GENE SPECIES="S_cerevisiae" PROTID="YAL001C"/>
    </CLUSTER>
</CLUSTERS>
"""
        xml_file = temp_file("clusters.xml", xml_content)
        clusters, ortholog_table = parse_cluster_xml(
            xml_file, "Candida_albicans", "S_cerevisiae"
        )

        assert len(clusters) == 1
        assert clusters[0].cluster_id == "1"
        assert len(clusters[0].orthologs_a) == 2
        assert len(clusters[0].orthologs_b) == 1
        assert "orf19.1" in clusters[0].orthologs_a
        assert "YAL001C" in clusters[0].orthologs_b

    def test_parse_multiple_clusters(self, temp_file):
        """Test parsing multiple clusters."""
        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER CLUSTERNO="1">
        <GENE SPECIES="orgA" PROTID="gene1"/>
        <GENE SPECIES="orgB" PROTID="gene2"/>
    </CLUSTER>
    <CLUSTER CLUSTERNO="2">
        <GENE SPECIES="orgA" PROTID="gene3"/>
        <GENE SPECIES="orgB" PROTID="gene4"/>
    </CLUSTER>
</CLUSTERS>
"""
        xml_file = temp_file("clusters.xml", xml_content)
        clusters, ortholog_table = parse_cluster_xml(xml_file, "orgA", "orgB")

        assert len(clusters) == 2
        assert clusters[0].cluster_id == "1"
        assert clusters[1].cluster_id == "2"

    def test_ortholog_table_populated(self, temp_file):
        """Test that ortholog table is populated."""
        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER CLUSTERNO="1">
        <GENE SPECIES="orgA" PROTID="gene1"/>
        <GENE SPECIES="orgB" PROTID="gene2"/>
    </CLUSTER>
</CLUSTERS>
"""
        xml_file = temp_file("clusters.xml", xml_content)
        clusters, ortholog_table = parse_cluster_xml(xml_file, "orgA", "orgB")

        assert "gene1" in ortholog_table
        assert "gene2" in ortholog_table

    def test_fallback_to_geneid(self, temp_file):
        """Test fallback to GENEID when PROTID is missing."""
        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER CLUSTERNO="1">
        <GENE SPECIES="orgA" GENEID="gene1"/>
    </CLUSTER>
</CLUSTERS>
"""
        xml_file = temp_file("clusters.xml", xml_content)
        clusters, ortholog_table = parse_cluster_xml(xml_file, "orgA", "orgB")

        assert "gene1" in clusters[0].orthologs_a

    def test_empty_xml(self, temp_file):
        """Test parsing empty clusters XML."""
        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
</CLUSTERS>
"""
        xml_file = temp_file("clusters.xml", xml_content)
        clusters, ortholog_table = parse_cluster_xml(xml_file, "orgA", "orgB")

        assert len(clusters) == 0
        assert len(ortholog_table) == 0


class TestReadBlastFile:
    """Tests for read_blast_file function."""

    def test_read_basic_blast(self, temp_file):
        """Test reading basic BLAST output."""
        # Format: query match score q_len h_len mrq mrh tmq tmh
        blast_content = """gene1\thit1\t100\t200\t200\t150\t150\t100\t100
gene1\thit2\t80\t200\t180\t140\t130\t90\t85
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table)

        assert "hit1" in ortholog_table["gene1"]
        assert "hit2" in ortholog_table["gene1"]
        assert ortholog_table["gene1"]["hit1"] == 100
        assert ortholog_table["gene1"]["hit2"] == 80

    def test_skip_low_score_hits(self, temp_file):
        """Test that low score hits are skipped."""
        blast_content = """gene1\thit1\t30\t200\t200\t150\t150\t100\t100
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table, score_cutoff=50)

        assert "hit1" not in ortholog_table["gene1"]

    def test_skip_non_orthologs(self, temp_file):
        """Test that hits for non-orthologs are skipped."""
        blast_content = """gene_not_in_table\thit1\t100\t200\t200\t150\t150\t100\t100
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table)

        assert "gene_not_in_table" not in ortholog_table

    def test_skip_short_lines(self, temp_file):
        """Test that short lines are skipped."""
        blast_content = """gene1\thit1\t100
gene1\thit2\t100\t200\t200\t150\t150\t100\t100
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table)

        assert "hit1" not in ortholog_table["gene1"]
        assert "hit2" in ortholog_table["gene1"]

    def test_seq_overlap_filter_query_longer(self, temp_file):
        """Test sequence overlap filter when query is longer."""
        # Query 200, Hit 100. Match region query = 50 < 0.5 * 200 = 100
        blast_content = """gene1\thit1\t100\t200\t100\t50\t50\t100\t100
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table, seq_overlap_cutoff=0.5)

        assert "hit1" not in ortholog_table["gene1"]

    def test_seq_overlap_filter_hit_longer(self, temp_file):
        """Test sequence overlap filter when hit is longer."""
        # Query 100, Hit 200. Match region hit = 50 < 0.5 * 200 = 100
        blast_content = """gene1\thit1\t100\t100\t200\t100\t50\t100\t100
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table, seq_overlap_cutoff=0.5)

        assert "hit1" not in ortholog_table["gene1"]

    def test_segment_coverage_filter(self, temp_file):
        """Test segment coverage filter."""
        # Query 200, total_match_query = 40 < 0.25 * 200 = 50
        blast_content = """gene1\thit1\t100\t200\t200\t150\t150\t40\t40
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table, segment_coverage_cutoff=0.25)

        assert "hit1" not in ortholog_table["gene1"]

    def test_score_rounding(self, temp_file):
        """Test that scores are rounded to integers."""
        blast_content = """gene1\thit1\t100.4\t200\t200\t150\t150\t100\t100
gene1\thit2\t100.6\t200\t200\t150\t150\t100\t100
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table)

        # 100.4 + 0.5 = 100.9 -> 100, 100.6 + 0.5 = 101.1 -> 101
        assert ortholog_table["gene1"]["hit1"] == 100
        assert ortholog_table["gene1"]["hit2"] == 101

    def test_custom_cutoffs(self, temp_file):
        """Test with custom cutoff values."""
        blast_content = """gene1\thit1\t30\t100\t100\t80\t80\t50\t50
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        # With lower cutoffs, hit should pass
        read_blast_file(
            blast_file,
            ortholog_table,
            score_cutoff=20,
            seq_overlap_cutoff=0.3,
            segment_coverage_cutoff=0.2
        )

        assert "hit1" in ortholog_table["gene1"]


class TestFindCno:
    """Tests for find_cno function."""

    def test_find_basic_cno(self):
        """Test finding basic CNO."""
        hit_table = {"hit1": 100, "hit2": 80, "hit3": 60}
        orthologs = ["ortholog1", "ortholog2"]

        cno_id, score = find_cno("ortholog1", orthologs, hit_table)

        assert cno_id == "hit1"
        assert score == 100

    def test_skip_orthologs_in_cluster(self):
        """Test that orthologs in same cluster are skipped."""
        hit_table = {"ortholog2": 100, "hit1": 80}
        orthologs = ["ortholog1", "ortholog2"]

        cno_id, score = find_cno("ortholog1", orthologs, hit_table)

        # Should skip ortholog2 and return hit1
        assert cno_id == "hit1"
        assert score == 80

    def test_no_cno_found(self):
        """Test when no CNO is found."""
        hit_table = {"ortholog2": 100}  # Only contains cluster member
        orthologs = ["ortholog1", "ortholog2"]

        cno_id, score = find_cno("ortholog1", orthologs, hit_table)

        assert cno_id is None
        assert score is None

    def test_empty_hit_table(self):
        """Test with empty hit table."""
        hit_table = {}
        orthologs = ["ortholog1"]

        cno_id, score = find_cno("ortholog1", orthologs, hit_table)

        assert cno_id is None
        assert score is None

    def test_sorted_by_score(self):
        """Test that hits are sorted by score (highest first)."""
        hit_table = {"hit_low": 50, "hit_high": 100, "hit_mid": 75}
        orthologs = ["ortholog1"]

        cno_id, score = find_cno("ortholog1", orthologs, hit_table)

        assert cno_id == "hit_high"
        assert score == 100

    def test_multiple_orthologs_skipped(self):
        """Test that all cluster orthologs are skipped."""
        hit_table = {"orth2": 100, "orth3": 90, "cno": 80}
        orthologs = ["orth1", "orth2", "orth3"]

        cno_id, score = find_cno("orth1", orthologs, hit_table)

        assert cno_id == "cno"
        assert score == 80


class TestMainFunction:
    """Tests for the main function."""

    def test_main_basic(self, temp_dir, capsys):
        """Test basic main function execution."""
        from inparanoid.cno_finder import main

        # Create cluster file
        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER CLUSTERNO="1">
        <GENE SPECIES="orgA" PROTID="gene1"/>
        <GENE SPECIES="orgB" PROTID="gene2"/>
    </CLUSTER>
</CLUSTERS>
"""
        cluster_file = temp_dir / "clusters.xml"
        cluster_file.write_text(xml_content)

        # Create blast files
        blast_aa = temp_dir / "blast_aa.txt"
        blast_aa.write_text("gene1\thit1\t100\t200\t200\t150\t150\t100\t100\n")

        blast_bb = temp_dir / "blast_bb.txt"
        blast_bb.write_text("gene2\thit2\t90\t200\t200\t140\t140\t95\t95\n")

        with patch.object(sys, 'argv', [
            'prog', 'orgA', 'orgB',
            str(cluster_file), str(blast_aa), str(blast_bb)
        ]):
            main()

        captured = capsys.readouterr()
        assert ">1" in captured.out  # Cluster ID
        assert "orgA" in captured.out
        assert "orgB" in captured.out

    def test_main_missing_file(self, temp_dir):
        """Test main with missing file."""
        from inparanoid.cno_finder import main

        with patch.object(sys, 'argv', [
            'prog', 'orgA', 'orgB',
            str(temp_dir / 'nonexistent.xml'),
            str(temp_dir / 'blast_aa.txt'),
            str(temp_dir / 'blast_bb.txt')
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    def test_main_with_custom_cutoffs(self, temp_dir, capsys):
        """Test main with custom cutoff options."""
        from inparanoid.cno_finder import main

        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER CLUSTERNO="1">
        <GENE SPECIES="orgA" PROTID="gene1"/>
    </CLUSTER>
</CLUSTERS>
"""
        cluster_file = temp_dir / "clusters.xml"
        cluster_file.write_text(xml_content)

        blast_aa = temp_dir / "blast_aa.txt"
        blast_aa.write_text("")

        blast_bb = temp_dir / "blast_bb.txt"
        blast_bb.write_text("")

        with patch.object(sys, 'argv', [
            'prog', 'orgA', 'orgB',
            str(cluster_file), str(blast_aa), str(blast_bb),
            '--score-cutoff', '30',
            '--seq-overlap', '0.3',
            '--segment-coverage', '0.2'
        ]):
            main()

        # Should complete without error
        captured = capsys.readouterr()
        assert ">1" in captured.out

    def test_main_output_format(self, temp_dir, capsys):
        """Test main output format."""
        from inparanoid.cno_finder import main

        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER CLUSTERNO="42">
        <GENE SPECIES="Candida" PROTID="orf19.1"/>
        <GENE SPECIES="Yeast" PROTID="YAL001C"/>
    </CLUSTER>
</CLUSTERS>
"""
        cluster_file = temp_dir / "clusters.xml"
        cluster_file.write_text(xml_content)

        blast_aa = temp_dir / "blast_aa.txt"
        blast_aa.write_text("orf19.1\tcno1\t85\t200\t200\t150\t150\t100\t100\n")

        blast_bb = temp_dir / "blast_bb.txt"
        blast_bb.write_text("")

        with patch.object(sys, 'argv', [
            'prog', 'Candida', 'Yeast',
            str(cluster_file), str(blast_aa), str(blast_bb)
        ]):
            main()

        captured = capsys.readouterr()
        lines = captured.out.strip().split('\n')

        assert lines[0] == ">42"  # Cluster header
        assert "Candida\torf19.1\t" in captured.out
        assert "Yeast\tYAL001C\t" in captured.out


class TestEdgeCases:
    """Tests for edge cases."""

    def test_same_length_sequences(self, temp_file):
        """Test filtering when query and hit have same length."""
        # Both sequences 200, both overlap checks must pass
        blast_content = """gene1\thit1\t100\t200\t200\t80\t80\t100\t100
"""
        blast_file = temp_file("blast.txt", blast_content)
        ortholog_table = {"gene1": {}}

        # Match region 80 < 0.5 * 200 = 100, should be filtered
        read_blast_file(blast_file, ortholog_table, seq_overlap_cutoff=0.5)

        assert "hit1" not in ortholog_table["gene1"]

    def test_equal_score_hits(self):
        """Test CNO selection with equal scores."""
        hit_table = {"hit1": 100, "hit2": 100, "hit3": 100}
        orthologs = ["ortholog1"]

        cno_id, score = find_cno("ortholog1", orthologs, hit_table)

        # Should return one of them (first in sorted order)
        assert cno_id in ["hit1", "hit2", "hit3"]
        assert score == 100

    def test_xml_missing_cluster_number(self, temp_file):
        """Test XML with missing cluster number."""
        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER>
        <GENE SPECIES="orgA" PROTID="gene1"/>
    </CLUSTER>
</CLUSTERS>
"""
        xml_file = temp_file("clusters.xml", xml_content)
        clusters, _ = parse_cluster_xml(xml_file, "orgA", "orgB")

        # Should have empty string for cluster_id
        assert clusters[0].cluster_id == ""

    def test_organism_not_matching(self, temp_file):
        """Test when organism doesn't match either specified."""
        xml_content = """<?xml version="1.0"?>
<CLUSTERS>
    <CLUSTER CLUSTERNO="1">
        <GENE SPECIES="other_org" PROTID="gene1"/>
    </CLUSTER>
</CLUSTERS>
"""
        xml_file = temp_file("clusters.xml", xml_content)
        clusters, _ = parse_cluster_xml(xml_file, "orgA", "orgB")

        # Gene should go to orthologs_b (else branch)
        assert len(clusters[0].orthologs_a) == 0
        assert len(clusters[0].orthologs_b) == 1

    def test_empty_blast_files(self, temp_file):
        """Test with empty BLAST files."""
        blast_file = temp_file("blast.txt", "")
        ortholog_table = {"gene1": {}}

        read_blast_file(blast_file, ortholog_table)

        assert ortholog_table["gene1"] == {}
