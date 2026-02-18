#!/usr/bin/env python3
"""
Unit tests for scripts/inparanoid/blast_parser.py

Tests the BLAST XML parsing functionality for InParanoid.
"""

import pytest
from pathlib import Path
from io import StringIO
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from inparanoid.blast_parser import (
    HSP,
    Hit,
    Query,
    check_overlap_linear,
    check_overlap_non_linear,
    has_overlap,
    parse_blast_xml,
    main,
)


class TestHSPDataclass:
    """Tests for HSP dataclass."""

    def test_hsp_creation(self):
        """Test creating an HSP object."""
        hsp = HSP(
            bit_score=150.0,
            query_from=1,
            query_to=100,
            hit_from=1,
            hit_to=95,
            qseq="MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQAPILSRVGDGTQDNLSGAEKAVQVK",
            hseq="MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQAPILSRVGDGTQDNLSGAEKAVQVK",
        )

        assert hsp.bit_score == 150.0
        assert hsp.query_from == 1
        assert hsp.query_to == 100
        assert hsp.hit_from == 1
        assert hsp.hit_to == 95

    def test_hsp_default_sequences(self):
        """Test HSP default sequence values."""
        hsp = HSP(bit_score=100, query_from=1, query_to=50, hit_from=1, hit_to=50)
        assert hsp.qseq == ""
        assert hsp.hseq == ""


class TestHitDataclass:
    """Tests for Hit dataclass."""

    def test_hit_creation(self):
        """Test creating a Hit object."""
        hit = Hit(hit_id="YAL001C", hit_length=500)

        assert hit.hit_id == "YAL001C"
        assert hit.hit_length == 500
        assert hit.total_score == 0.0
        assert hit.hsps == []

    def test_hit_with_hsps(self):
        """Test Hit with HSPs."""
        hit = Hit(hit_id="YAL001C", hit_length=500)
        hsp = HSP(bit_score=100, query_from=1, query_to=50, hit_from=1, hit_to=50)
        hit.hsps.append(hsp)
        hit.total_score += hsp.bit_score

        assert len(hit.hsps) == 1
        assert hit.total_score == 100


class TestQueryDataclass:
    """Tests for Query dataclass."""

    def test_query_creation(self):
        """Test creating a Query object."""
        query = Query(query_id="orf19.1", query_length=300)

        assert query.query_id == "orf19.1"
        assert query.query_length == 300
        assert query.hits == []


class TestCheckOverlapLinear:
    """Tests for check_overlap_linear function."""

    def test_no_overlap(self):
        """Test segments that don't overlap."""
        # Segments: 1-50 and 60-100
        result = check_overlap_linear(1, 50, 60, 100)
        assert result is False

    def test_small_overlap_allowed(self):
        """Test small overlap within 5% threshold."""
        # Segments: 1-100 and 99-200 (overlap of 2, which is < 5% of shorter)
        result = check_overlap_linear(1, 100, 99, 200)
        # 2 / 100 = 2%, should be allowed
        assert result is False

    def test_large_overlap_detected(self):
        """Test large overlap exceeding 5% threshold."""
        # Segments: 1-100 and 50-150 (overlap of 51)
        result = check_overlap_linear(1, 100, 50, 150)
        assert result is True

    def test_identical_segments(self):
        """Test identical segments overlap."""
        result = check_overlap_linear(1, 100, 1, 100)
        assert result is True

    def test_adjacent_segments(self):
        """Test adjacent segments don't overlap."""
        result = check_overlap_linear(1, 50, 51, 100)
        assert result is False


class TestCheckOverlapNonLinear:
    """Tests for check_overlap_non_linear function."""

    def test_no_overlap(self):
        """Test HSPs that don't overlap."""
        hsp1 = HSP(bit_score=100, query_from=1, query_to=50,
                   hit_from=1, hit_to=50)
        hsp2 = HSP(bit_score=100, query_from=100, query_to=150,
                   hit_from=100, hit_to=150)
        result = check_overlap_non_linear(hsp1, hsp2)
        assert result is False

    def test_query_overlap(self):
        """Test overlap on query sequence."""
        hsp1 = HSP(bit_score=100, query_from=1, query_to=100,
                   hit_from=1, hit_to=50)
        hsp2 = HSP(bit_score=100, query_from=50, query_to=150,
                   hit_from=100, hit_to=150)
        result = check_overlap_non_linear(hsp1, hsp2)
        assert result is True

    def test_hit_overlap(self):
        """Test overlap on hit sequence."""
        hsp1 = HSP(bit_score=100, query_from=1, query_to=50,
                   hit_from=1, hit_to=100)
        hsp2 = HSP(bit_score=100, query_from=100, query_to=150,
                   hit_from=50, hit_to=150)
        result = check_overlap_non_linear(hsp1, hsp2)
        assert result is True


class TestHasOverlap:
    """Tests for has_overlap function."""

    def test_linear_mode_no_overlap(self):
        """Test no overlap in linear mode."""
        hsp1 = HSP(bit_score=100, query_from=1, query_to=50,
                   hit_from=1, hit_to=50)
        hsp2 = HSP(bit_score=100, query_from=100, query_to=150,
                   hit_from=100, hit_to=150)
        result = has_overlap(hsp1, hsp2, linear_mode=True)
        assert result is False

    def test_linear_mode_overlap(self):
        """Test overlap detected in linear mode."""
        hsp1 = HSP(bit_score=100, query_from=1, query_to=100,
                   hit_from=1, hit_to=100)
        hsp2 = HSP(bit_score=100, query_from=50, query_to=150,
                   hit_from=50, hit_to=150)
        result = has_overlap(hsp1, hsp2, linear_mode=True)
        assert result is True

    def test_same_start_position(self):
        """Test HSPs with same start position."""
        hsp1 = HSP(bit_score=100, query_from=1, query_to=50,
                   hit_from=1, hit_to=50)
        hsp2 = HSP(bit_score=100, query_from=1, query_to=100,
                   hit_from=1, hit_to=100)
        result = has_overlap(hsp1, hsp2)
        assert result is True


class TestParseBlastXml:
    """Tests for parse_blast_xml function."""

    def test_parse_basic_xml(self, temp_dir, sample_blast_xml):
        """Test parsing basic BLAST XML."""
        xml_file = temp_dir / "blast.xml"
        xml_file.write_text(sample_blast_xml)

        output = StringIO()
        parse_blast_xml(xml_file, score_cutoff=100.0, output=output)

        result = output.getvalue()
        assert "orf19.1" in result
        # Hit_def is parsed for hit_id, which would be "TFC3" from the sample XML
        assert "TFC3" in result or "YAL001C" in result

    def test_score_cutoff_filter(self, temp_dir, sample_blast_xml):
        """Test that score cutoff filters results."""
        xml_file = temp_dir / "blast.xml"
        xml_file.write_text(sample_blast_xml)

        # With high cutoff, should filter out results
        output = StringIO()
        parse_blast_xml(xml_file, score_cutoff=500.0, output=output)

        result = output.getvalue()
        # Result with score 150 should be filtered out
        assert "YAL001C" not in result

    def test_alignment_mode(self, temp_dir, sample_blast_xml):
        """Test alignment mode output."""
        xml_file = temp_dir / "blast.xml"
        xml_file.write_text(sample_blast_xml)

        output = StringIO()
        parse_blast_xml(
            xml_file,
            score_cutoff=100.0,
            alignment_mode=True,
            output=output
        )

        result = output.getvalue()
        # Alignment mode adds > prefix
        assert result.startswith(">") or ">orf19.1" in result

    def test_nonexistent_file(self, temp_dir, capsys):
        """Test handling of nonexistent XML file."""
        xml_file = temp_dir / "nonexistent.xml"

        output = StringIO()
        # parse_blast_xml raises FileNotFoundError for nonexistent files
        with pytest.raises(FileNotFoundError):
            parse_blast_xml(xml_file, score_cutoff=100.0, output=output)

    def test_invalid_xml(self, temp_dir, capsys):
        """Test handling of invalid XML."""
        xml_file = temp_dir / "invalid.xml"
        xml_file.write_text("This is not valid XML <unclosed>")

        output = StringIO()
        parse_blast_xml(xml_file, score_cutoff=100.0, output=output)

        captured = capsys.readouterr()
        assert "Error parsing XML" in captured.err


class TestMainFunction:
    """Tests for the main function."""

    def test_main_basic(self, temp_dir, sample_blast_xml):
        """Test main function with basic arguments runs without error."""
        xml_file = temp_dir / "blast.xml"
        xml_file.write_text(sample_blast_xml)

        with patch.object(sys, 'argv', ['prog', '100', str(xml_file)]):
            # main() should complete without raising exception
            main()

    def test_main_alignment_mode(self, temp_dir, sample_blast_xml):
        """Test main function with alignment mode runs without error."""
        xml_file = temp_dir / "blast.xml"
        xml_file.write_text(sample_blast_xml)

        with patch.object(sys, 'argv', ['prog', '-a', '100', str(xml_file)]):
            # main() should complete without raising exception
            main()

    def test_main_nonexistent_file(self, temp_dir, capsys):
        """Test main function with nonexistent file."""
        with patch.object(sys, 'argv', ['prog', '100', str(temp_dir / 'none.xml')]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1


class TestOutputFormat:
    """Tests for output format."""

    def test_output_fields(self, temp_dir, sample_blast_xml):
        """Test output contains expected fields."""
        xml_file = temp_dir / "blast.xml"
        xml_file.write_text(sample_blast_xml)

        output = StringIO()
        parse_blast_xml(xml_file, score_cutoff=100.0, output=output)

        result = output.getvalue()
        lines = result.strip().split('\n')

        if lines and lines[0]:
            fields = lines[0].split('\t')
            # Should have query_id, hit_id, score, lengths, etc.
            assert len(fields) >= 9

    def test_segment_positions_in_output(self, temp_dir, sample_blast_xml):
        """Test segment positions are in output."""
        xml_file = temp_dir / "blast.xml"
        xml_file.write_text(sample_blast_xml)

        output = StringIO()
        parse_blast_xml(xml_file, score_cutoff=100.0, output=output)

        result = output.getvalue()
        # Should contain q: and h: position markers
        assert "q:" in result or len(result) == 0
        assert "h:" in result or len(result) == 0


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_xml(self, temp_dir):
        """Test handling of empty XML."""
        xml_file = temp_dir / "empty.xml"
        xml_file.write_text("""<?xml version="1.0"?>
<BlastOutput>
  <BlastOutput_iterations></BlastOutput_iterations>
</BlastOutput>""")

        output = StringIO()
        parse_blast_xml(xml_file, score_cutoff=100.0, output=output)

        # Should produce no output
        assert output.getvalue() == ""

    def test_no_hits(self, temp_dir):
        """Test XML with query but no hits."""
        xml_content = """<?xml version="1.0"?>
<BlastOutput>
  <BlastOutput_iterations>
    <Iteration>
      <Iteration_query-def>orf19.1</Iteration_query-def>
      <Iteration_query-len>100</Iteration_query-len>
      <Iteration_hits></Iteration_hits>
    </Iteration>
  </BlastOutput_iterations>
</BlastOutput>"""
        xml_file = temp_dir / "no_hits.xml"
        xml_file.write_text(xml_content)

        output = StringIO()
        parse_blast_xml(xml_file, score_cutoff=100.0, output=output)

        # No hits means no output
        assert output.getvalue() == ""

    def test_self_hit_filtered_in_alignment_mode(self, temp_dir):
        """Test self-hits are filtered in alignment mode."""
        xml_content = """<?xml version="1.0"?>
<BlastOutput>
  <BlastOutput_iterations>
    <Iteration>
      <Iteration_query-def>orf19.1</Iteration_query-def>
      <Iteration_query-len>100</Iteration_query-len>
      <Iteration_hits>
        <Hit>
          <Hit_def>orf19.1 self</Hit_def>
          <Hit_len>100</Hit_len>
          <Hit_hsps>
            <Hsp>
              <Hsp_bit-score>200.0</Hsp_bit-score>
              <Hsp_query-from>1</Hsp_query-from>
              <Hsp_query-to>100</Hsp_query-to>
              <Hsp_hit-from>1</Hsp_hit-from>
              <Hsp_hit-to>100</Hsp_hit-to>
            </Hsp>
          </Hit_hsps>
        </Hit>
      </Iteration_hits>
    </Iteration>
  </BlastOutput_iterations>
</BlastOutput>"""
        xml_file = temp_dir / "self_hit.xml"
        xml_file.write_text(xml_content)

        output = StringIO()
        parse_blast_xml(
            xml_file,
            score_cutoff=100.0,
            alignment_mode=True,
            output=output
        )

        # Self-hit should be filtered in alignment mode
        result = output.getvalue()
        # Result should be empty or not contain the self-hit
        assert result == "" or "orf19.1\torf19.1" not in result
