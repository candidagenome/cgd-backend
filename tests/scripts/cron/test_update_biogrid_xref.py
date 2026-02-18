#!/usr/bin/env python3
"""
Unit tests for scripts/cron/update_biogrid_xref.py

Tests the BioGRID cross-reference update functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

# Skip if requests not installed
pytest.importorskip("requests")

from cron.update_biogrid_xref import (
    get_strain_config,
    get_valid_features,
    download_biogrid_data,
    parse_biogrid_data,
    write_xref_file,
    delete_defunct_refs,
)


class TestGetStrainConfig:
    """Tests for get_strain_config function."""

    def test_strain_found(self, mock_db_session):
        """Test when strain is found."""
        mock_db_session.execute.return_value.first.return_value = (1, 5476)

        result = get_strain_config(mock_db_session, "C_albicans_SC5314")

        assert result is not None
        assert result["organism_no"] == 1
        assert result["taxon_id"] == 5476

    def test_strain_not_found(self, mock_db_session):
        """Test when strain is not found."""
        mock_db_session.execute.return_value.first.return_value = None

        result = get_strain_config(mock_db_session, "NonexistentStrain")

        assert result is None


class TestGetValidFeatures:
    """Tests for get_valid_features function."""

    def test_returns_features(self, mock_db_session):
        """Test returning valid features."""
        mock_db_session.execute.return_value = iter([
            ("orf19.1",),
            ("orf19.2",),
            ("orf19.3",),
        ])

        result = get_valid_features(mock_db_session, "C_albicans_SC5314")

        assert len(result) == 3
        assert "orf19.1" in result
        assert "orf19.2" in result
        assert "orf19.3" in result

    def test_empty_features(self, mock_db_session):
        """Test with no valid features."""
        mock_db_session.execute.return_value = iter([])

        result = get_valid_features(mock_db_session, "C_albicans_SC5314")

        assert result == set()

    def test_filters_none_values(self, mock_db_session):
        """Test that None values are filtered."""
        mock_db_session.execute.return_value = iter([
            ("orf19.1",),
            (None,),
            ("orf19.2",),
        ])

        result = get_valid_features(mock_db_session, "C_albicans_SC5314")

        assert len(result) == 2
        assert None not in result


class TestDownloadBiogridData:
    """Tests for download_biogrid_data function."""

    @patch('cron.update_biogrid_xref.requests.get')
    @patch('cron.update_biogrid_xref.BIOGRID_API_KEY', 'test_key')
    def test_successful_download(self, mock_get, temp_dir):
        """Test successful download."""
        mock_response = MagicMock()
        mock_response.text = "data1\tdata2\n"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        output_file = temp_dir / "biogrid_data.tab"

        result = download_biogrid_data(5476, output_file)

        assert result is True
        assert output_file.exists()
        assert output_file.read_text() == "data1\tdata2\n"

    @patch('cron.update_biogrid_xref.BIOGRID_API_KEY', '')
    def test_missing_api_key(self, temp_dir):
        """Test with missing API key."""
        output_file = temp_dir / "biogrid_data.tab"

        result = download_biogrid_data(5476, output_file)

        assert result is False

    @patch('cron.update_biogrid_xref.requests.get')
    @patch('cron.update_biogrid_xref.BIOGRID_API_KEY', 'test_key')
    def test_request_failure(self, mock_get, temp_dir):
        """Test with request failure."""
        import requests
        mock_get.side_effect = requests.RequestException("Connection failed")

        output_file = temp_dir / "biogrid_data.tab"

        result = download_biogrid_data(5476, output_file)

        assert result is False


class TestParseBiogridData:
    """Tests for parse_biogrid_data function."""

    def test_parse_valid_data(self, temp_file):
        """Test parsing valid BioGRID data."""
        data = "col1\tcol2\tcol3\t100\t200\torf19.1\torf19.2\n"
        data_file = temp_file("biogrid.tab", data)

        valid_features = {"orf19.1", "orf19.2", "orf19.3"}

        feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

        assert "orf19.1" in feat_to_bgid
        assert feat_to_bgid["orf19.1"] == 100
        assert "orf19.2" in feat_to_bgid
        assert feat_to_bgid["orf19.2"] == 200

    def test_parse_filters_invalid_features(self, temp_file):
        """Test that invalid features are filtered."""
        data = "col1\tcol2\tcol3\t100\t200\tinvalid1\tinvalid2\n"
        data_file = temp_file("biogrid.tab", data)

        valid_features = {"orf19.1", "orf19.2"}

        feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

        assert len(feat_to_bgid) == 0

    def test_parse_short_lines(self, temp_file):
        """Test that short lines are skipped."""
        data = "col1\tcol2\tcol3\n"  # Less than 7 columns
        data_file = temp_file("biogrid.tab", data)

        valid_features = {"orf19.1"}

        feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

        assert len(feat_to_bgid) == 0

    def test_parse_empty_file(self, temp_file):
        """Test parsing empty file."""
        data_file = temp_file("biogrid.tab", "")

        valid_features = {"orf19.1"}

        feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

        assert feat_to_bgid == {}
        assert bgid_to_feat == {}

    def test_parse_nonexistent_file(self, temp_dir):
        """Test parsing nonexistent file."""
        data_file = temp_dir / "nonexistent.tab"

        valid_features = {"orf19.1"}

        feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

        assert feat_to_bgid == {}
        assert bgid_to_feat == {}

    def test_parse_invalid_bgid(self, temp_file):
        """Test parsing with invalid BioGRID IDs."""
        data = "col1\tcol2\tcol3\tnotanint\t200\torf19.1\torf19.2\n"
        data_file = temp_file("biogrid.tab", data)

        valid_features = {"orf19.1", "orf19.2"}

        feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

        # Should skip lines with invalid integers
        assert len(feat_to_bgid) == 0


class TestWriteXrefFile:
    """Tests for write_xref_file function."""

    def test_write_basic(self, temp_dir):
        """Test writing basic xref file."""
        feat_to_bgid = {"orf19.1": 100, "orf19.2": 200}
        output_file = temp_dir / "xref.tab"

        write_xref_file(feat_to_bgid, output_file)

        assert output_file.exists()
        content = output_file.read_text()
        assert "CGD\tBioGRID" in content
        assert "orf19.1\t100" in content
        assert "orf19.2\t200" in content

    def test_write_creates_directory(self, temp_dir):
        """Test that output directory is created."""
        feat_to_bgid = {"orf19.1": 100}
        output_file = temp_dir / "subdir" / "xref.tab"

        write_xref_file(feat_to_bgid, output_file)

        assert output_file.exists()

    def test_write_empty(self, temp_dir):
        """Test writing with no features."""
        feat_to_bgid = {}
        output_file = temp_dir / "xref.tab"

        write_xref_file(feat_to_bgid, output_file)

        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 1  # Just the header

    def test_write_sorted_output(self, temp_dir):
        """Test that output is sorted."""
        feat_to_bgid = {"zzz": 300, "aaa": 100, "mmm": 200}
        output_file = temp_dir / "xref.tab"

        write_xref_file(feat_to_bgid, output_file)

        content = output_file.read_text()
        lines = content.strip().split("\n")[1:]  # Skip header
        features = [line.split("\t")[0] for line in lines]
        assert features == sorted(features)


class TestDeleteDefunctRefs:
    """Tests for delete_defunct_refs function."""

    def test_no_deletions_needed(self, mock_db_session):
        """Test when no deletions are needed."""
        mock_db_session.execute.return_value = iter([
            (1, "orf19.1", "100"),
        ])

        feat_to_bgid = {"orf19.1": 100}

        count = delete_defunct_refs(mock_db_session, feat_to_bgid)

        assert count == 0

    def test_feature_removed_from_biogrid(self, mock_db_session):
        """Test when feature is removed from BioGRID."""
        mock_db_session.execute.return_value = iter([
            (1, "orf19.removed", "100"),
        ])

        feat_to_bgid = {"orf19.1": 100}  # orf19.removed not in new data

        count = delete_defunct_refs(mock_db_session, feat_to_bgid)

        assert count == 1
        mock_db_session.commit.assert_called()

    def test_id_changed(self, mock_db_session):
        """Test when BioGRID ID has changed."""
        mock_db_session.execute.return_value = iter([
            (1, "orf19.1", "100"),  # Old ID is 100
        ])

        feat_to_bgid = {"orf19.1": 200}  # New ID is 200

        count = delete_defunct_refs(mock_db_session, feat_to_bgid)

        assert count == 1


class TestMainFunction:
    """Tests for the main function."""

    @patch('cron.update_biogrid_xref.update_biogrid_xref')
    def test_main_success(self, mock_update):
        """Test main with successful execution."""
        from cron.update_biogrid_xref import main

        mock_update.return_value = True

        with patch.object(sys, 'argv', ['prog', '--strain', 'C_albicans_SC5314']):
            result = main()

        assert result == 0
        mock_update.assert_called_with("C_albicans_SC5314")

    @patch('cron.update_biogrid_xref.update_biogrid_xref')
    def test_main_failure(self, mock_update):
        """Test main with failed execution."""
        from cron.update_biogrid_xref import main

        mock_update.return_value = False

        with patch.object(sys, 'argv', ['prog', '--strain', 'C_albicans_SC5314']):
            result = main()

        assert result == 1

    def test_main_missing_strain(self):
        """Test main with missing strain argument."""
        from cron.update_biogrid_xref import main

        with patch.object(sys, 'argv', ['prog']):
            with pytest.raises(SystemExit):
                main()


class TestEdgeCases:
    """Tests for edge cases."""

    def test_parse_multiple_interactions_same_feature(self, temp_file):
        """Test parsing multiple interactions for same feature."""
        data = """col1\tcol2\tcol3\t100\t200\torf19.1\torf19.2
col1\tcol2\tcol3\t100\t300\torf19.1\torf19.3
"""
        data_file = temp_file("biogrid.tab", data)

        valid_features = {"orf19.1", "orf19.2", "orf19.3"}

        feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

        # First occurrence should be kept
        assert feat_to_bgid["orf19.1"] == 100

    def test_parse_blank_lines(self, temp_file):
        """Test parsing with blank lines."""
        data = """col1\tcol2\tcol3\t100\t200\torf19.1\torf19.2

col1\tcol2\tcol3\t300\t400\torf19.3\torf19.4
"""
        data_file = temp_file("biogrid.tab", data)

        valid_features = {"orf19.1", "orf19.2", "orf19.3", "orf19.4"}

        feat_to_bgid, bgid_to_feat = parse_biogrid_data(data_file, valid_features)

        assert len(feat_to_bgid) == 4
