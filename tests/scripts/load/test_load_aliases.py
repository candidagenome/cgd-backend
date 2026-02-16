#!/usr/bin/env python3
"""
Unit tests for scripts/load/load_aliases.py

Tests the alias loading functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from load.load_aliases import (
    determine_alias_type,
    parse_input_file,
)


class TestDetermineAliasType:
    """Tests for determine_alias_type function."""

    def test_uniform_alias(self):
        """Test detection of uniform alias (3 letters + digits)."""
        assert determine_alias_type("ACT1") == "Uniform"
        assert determine_alias_type("TUB2") == "Uniform"
        assert determine_alias_type("CDC42") == "Uniform"
        assert determine_alias_type("HIS100") == "Uniform"

    def test_non_uniform_alias(self):
        """Test detection of non-uniform aliases."""
        assert determine_alias_type("ACTB") == "Non-uniform"
        assert determine_alias_type("ACT") == "Non-uniform"
        assert determine_alias_type("A1") == "Non-uniform"
        assert determine_alias_type("ABCD1") == "Non-uniform"
        assert determine_alias_type("orf19.123") == "Non-uniform"

    def test_case_insensitive(self):
        """Test that detection is case-insensitive."""
        assert determine_alias_type("act1") == "Uniform"
        assert determine_alias_type("Act1") == "Uniform"
        assert determine_alias_type("ACT1") == "Uniform"

    def test_edge_cases(self):
        """Test edge cases for alias type detection."""
        assert determine_alias_type("") == "Non-uniform"
        assert determine_alias_type("123") == "Non-uniform"
        assert determine_alias_type("ABC") == "Non-uniform"


class TestParseInputFile:
    """Tests for parse_input_file function."""

    def test_parse_basic_file(self, temp_file):
        """Test parsing basic input file."""
        content = """CAL0001\t123456\talias1, alias2, alias3
CAL0002\t234567\tgene1
"""
        input_file = temp_file("input.txt", content)
        entries = parse_input_file(input_file)

        assert len(entries) == 2
        assert entries[0]["cgdid"] == "CAL0001"
        assert entries[0]["aliases"] == ["alias1", "alias2", "alias3"]
        assert entries[1]["cgdid"] == "CAL0002"
        assert entries[1]["aliases"] == ["gene1"]

    def test_skip_empty_lines(self, temp_file):
        """Test that empty lines are skipped."""
        content = """CAL0001\t123456\talias1

CAL0002\t234567\talias2
"""
        input_file = temp_file("input.txt", content)
        entries = parse_input_file(input_file)

        assert len(entries) == 2

    def test_skip_lines_with_missing_columns(self, temp_file):
        """Test that lines with fewer than 3 columns are skipped."""
        content = """CAL0001\t123456\talias1
CAL0002
CAL0003\t234567\talias2
"""
        input_file = temp_file("input.txt", content)
        entries = parse_input_file(input_file)

        assert len(entries) == 2

    def test_skip_entries_without_aliases(self, temp_file):
        """Test that entries without aliases are skipped."""
        content = """CAL0001\t123456\talias1
CAL0002\t234567\t
CAL0003\t345678\talias2
"""
        input_file = temp_file("input.txt", content)
        entries = parse_input_file(input_file)

        assert len(entries) == 2

    def test_trim_whitespace(self, temp_file):
        """Test that whitespace is trimmed."""
        content = """  CAL0001  \t  123456  \t  alias1 , alias2  \n"""
        input_file = temp_file("input.txt", content)
        entries = parse_input_file(input_file)

        assert len(entries) == 1
        assert entries[0]["cgdid"] == "CAL0001"
        assert entries[0]["aliases"] == ["alias1", "alias2"]

    def test_empty_file(self, temp_file):
        """Test parsing empty file."""
        input_file = temp_file("input.txt", "")
        entries = parse_input_file(input_file)

        assert len(entries) == 0


class TestGetFeatureByDbxrefId:
    """Tests for get_feature_by_dbxref_id function."""

    def test_feature_found(self, mock_db_session):
        """Test finding a feature by dbxref_id."""
        from load.load_aliases import get_feature_by_dbxref_id

        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_feature.dbxref_id = "CAL0001"
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_feature

        result = get_feature_by_dbxref_id(mock_db_session, "CAL0001")

        assert result is not None
        assert result.feature_no == 123

    def test_feature_not_found(self, mock_db_session):
        """Test when feature is not found."""
        from load.load_aliases import get_feature_by_dbxref_id

        mock_db_session.query.return_value.filter.return_value.first.return_value = None

        result = get_feature_by_dbxref_id(mock_db_session, "NONEXISTENT")

        assert result is None


class TestGetExistingAliases:
    """Tests for get_existing_aliases function."""

    def test_get_existing_aliases(self, mock_db_session):
        """Test getting existing aliases for a feature."""
        from load.load_aliases import get_existing_aliases

        # Mock the feature query
        mock_feature = MagicMock()
        mock_feature.feature_name = "orf19.1"
        mock_feature.gene_name = "ACT1"

        # Set up the query chain for feature
        mock_feature_query = MagicMock()
        mock_feature_query.filter.return_value.first.return_value = mock_feature

        # Set up the query chain for feat_aliases
        mock_alias = MagicMock()
        mock_alias.alias_no = 1

        mock_feat_aliases_query = MagicMock()
        mock_feat_aliases_query.filter.return_value.all.return_value = [mock_alias]

        # Set up the query chain for alias
        mock_alias_obj = MagicMock()
        mock_alias_obj.alias_name = "ALIAS1"

        mock_alias_query = MagicMock()
        mock_alias_query.filter.return_value.first.return_value = mock_alias_obj

        # Make query return different results based on the model
        def query_side_effect(model):
            from load.load_aliases import Feature, FeatAlias, Alias
            if model == Feature:
                return mock_feature_query
            elif model == FeatAlias:
                return mock_feat_aliases_query
            elif model == Alias:
                return mock_alias_query
            return MagicMock()

        mock_db_session.query.side_effect = query_side_effect

        result = get_existing_aliases(mock_db_session, 123)

        assert "orf19.1" in result
        assert "ACT1" in result
        assert "ALIAS1" in result


class TestGetOrCreateAlias:
    """Tests for get_or_create_alias function."""

    def test_get_existing_alias(self, mock_db_session):
        """Test getting an existing alias."""
        from load.load_aliases import get_or_create_alias

        mock_alias = MagicMock()
        mock_alias.alias_no = 42
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_alias

        result = get_or_create_alias(
            mock_db_session, "ALIAS1", "Uniform", "SCRIPT"
        )

        assert result == 42
        mock_db_session.add.assert_not_called()

    def test_create_new_alias(self, mock_db_session):
        """Test creating a new alias."""
        from load.load_aliases import get_or_create_alias

        mock_db_session.query.return_value.filter.return_value.first.return_value = None

        # Mock the flush to set alias_no
        def flush_side_effect():
            for call_args in mock_db_session.add.call_args_list:
                alias_obj = call_args[0][0]
                alias_obj.alias_no = 100

        mock_db_session.flush.side_effect = flush_side_effect

        result = get_or_create_alias(
            mock_db_session, "NEWALIAS", "Non-uniform", "SCRIPT"
        )

        mock_db_session.add.assert_called_once()
        mock_db_session.flush.assert_called_once()


class TestLoadAliases:
    """Tests for load_aliases function."""

    def test_load_aliases_basic(self, mock_db_session):
        """Test basic alias loading."""
        from load.load_aliases import load_aliases

        # Mock feature lookup
        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_feature.feature_name = "orf19.1"
        mock_feature.gene_name = "ACT1"

        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_feature
        mock_db_session.query.return_value.filter.return_value.all.return_value = []

        entries = [
            {"cgdid": "CAL0001", "aliases": ["NEWALIAS"]},
        ]

        with patch('load.load_aliases.get_feature_by_dbxref_id', return_value=mock_feature):
            with patch('load.load_aliases.get_existing_aliases', return_value={"orf19.1", "ACT1"}):
                with patch('load.load_aliases.get_or_create_alias', return_value=1):
                    with patch('load.load_aliases.create_feat_alias_if_not_exists', return_value=1):
                        with patch('load.load_aliases.create_ref_link_if_not_exists', return_value=True):
                            stats = load_aliases(
                                mock_db_session,
                                entries,
                                reference_no=1000,
                                created_by="SCRIPT"
                            )

        assert stats["features_processed"] == 1
        assert stats["aliases_created"] == 1

    def test_load_aliases_skip_existing(self, mock_db_session):
        """Test that existing aliases are skipped."""
        from load.load_aliases import load_aliases

        mock_feature = MagicMock()
        mock_feature.feature_no = 123
        mock_feature.feature_name = "orf19.1"

        entries = [
            {"cgdid": "CAL0001", "aliases": ["orf19.1"]},  # Already exists
        ]

        with patch('load.load_aliases.get_feature_by_dbxref_id', return_value=mock_feature):
            with patch('load.load_aliases.get_existing_aliases', return_value={"orf19.1"}):
                stats = load_aliases(
                    mock_db_session,
                    entries,
                    reference_no=1000,
                    created_by="SCRIPT"
                )

        assert stats["aliases_skipped"] == 1

    def test_load_aliases_feature_not_found(self, mock_db_session):
        """Test handling of missing features."""
        from load.load_aliases import load_aliases

        entries = [
            {"cgdid": "NONEXISTENT", "aliases": ["ALIAS"]},
        ]

        with patch('load.load_aliases.get_feature_by_dbxref_id', return_value=None):
            stats = load_aliases(
                mock_db_session,
                entries,
                reference_no=1000,
                created_by="SCRIPT"
            )

        assert stats["features_not_found"] == 1


class TestMainFunction:
    """Tests for the main function."""

    def test_main_dry_run(self, temp_dir, capfd):
        """Test main function with dry-run option."""
        from load.load_aliases import main

        input_file = temp_dir / "input.txt"
        input_file.write_text("CAL0001\t123456\talias1, alias2\n")

        with patch.object(sys, 'argv', [
            'prog', str(input_file), '1000', '--dry-run'
        ]):
            main()

        # Dry run should complete without error - the message goes to logger
        # Just verify the function runs successfully
        assert True  # If we get here, dry run worked

    def test_main_missing_input_file(self, temp_dir, capsys):
        """Test main with missing input file."""
        from load.load_aliases import main

        with patch.object(sys, 'argv', [
            'prog', str(temp_dir / 'nonexistent.txt'), '1000'
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1


class TestEdgeCases:
    """Tests for edge cases."""

    def test_alias_with_special_characters(self, temp_file):
        """Test parsing aliases with special characters."""
        content = """CAL0001\t123456\talias-1, alias_2, alias.3\n"""
        input_file = temp_file("input.txt", content)
        entries = parse_input_file(input_file)

        assert entries[0]["aliases"] == ["alias-1", "alias_2", "alias.3"]

    def test_created_by_truncation(self):
        """Test that created_by is truncated to 12 characters."""
        from load.load_aliases import get_or_create_alias

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = None

        get_or_create_alias(
            mock_session,
            "ALIAS",
            "Uniform",
            "VERYLONGUSERNAME"  # > 12 chars
        )

        # Check that the Alias was created with truncated created_by
        add_call = mock_session.add.call_args[0][0]
        assert len(add_call.created_by) <= 12
