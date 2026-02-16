#!/usr/bin/env python3
"""
Unit tests for scripts/data/dump_phenotype_data.py

Tests the phenotype data dumping functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from data.dump_phenotype_data import (
    get_organism_info,
    get_species_abbrev,
    dump_phenotype_data,
)


class TestGetOrganismInfo:
    """Tests for get_organism_info function."""

    def test_organism_found(self, mock_db_session):
        """Test when organism is found."""
        mock_db_session.execute.return_value.first.return_value = (
            1, "Candida albicans SC5314", "C_albicans_SC5314"
        )

        result = get_organism_info(mock_db_session, "C_albicans_SC5314")

        assert result is not None
        assert result["organism_no"] == 1
        assert result["organism_name"] == "Candida albicans SC5314"
        assert result["organism_abbrev"] == "C_albicans_SC5314"

    def test_organism_not_found(self, mock_db_session):
        """Test when organism is not found."""
        mock_db_session.execute.return_value.first.return_value = None

        result = get_organism_info(mock_db_session, "NonexistentOrganism")

        assert result is None


class TestGetSpeciesAbbrev:
    """Tests for get_species_abbrev function."""

    def test_species_parent_found(self, mock_db_session):
        """Test when species parent is found."""
        mock_db_session.execute.return_value.first.return_value = ("C_albicans",)

        result = get_species_abbrev(mock_db_session, 1)

        assert result == "C_albicans"

    def test_is_species_itself(self, mock_db_session):
        """Test when organism is species itself."""
        # First query returns None (no parent)
        # Second query returns the organism as species
        mock_db_session.execute.return_value.first.side_effect = [
            None,
            ("C_albicans",),
        ]

        result = get_species_abbrev(mock_db_session, 1)

        assert result == "C_albicans"

    def test_no_species_found(self, mock_db_session):
        """Test when no species is found."""
        mock_db_session.execute.return_value.first.return_value = None

        result = get_species_abbrev(mock_db_session, 1)

        assert result is None


class TestDumpPhenotypeData:
    """Tests for dump_phenotype_data function."""

    def test_dump_basic_data(self, temp_dir, mock_db_session):
        """Test dumping basic phenotype data."""
        mock_db_session.execute.return_value = iter([
            ("orf19.1", "ACT1", "colony morphology", "abnormal", "null",
             "SC5314", "colony spot assay", "Visible phenotype", None,
             None, None, None, "Smooth colonies", "12345678", "Smith et al."),
        ])

        output_file = temp_dir / "phenotype.tab"

        count = dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        assert count == 1
        assert output_file.exists()

        content = output_file.read_text()
        lines = content.strip().split("\n")

        # Check header
        assert "Feature_Name" in lines[0]
        assert "Observable" in lines[0]

        # Check data line
        assert "orf19.1" in lines[1]
        assert "colony morphology" in lines[1]

    def test_dump_empty_data(self, temp_dir, mock_db_session):
        """Test dumping with no phenotype data."""
        mock_db_session.execute.return_value = iter([])

        output_file = temp_dir / "phenotype.tab"

        count = dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        assert count == 0
        assert output_file.exists()

        content = output_file.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 1  # Just header

    def test_dump_handles_none_values(self, temp_dir, mock_db_session):
        """Test that None values are handled correctly."""
        mock_db_session.execute.return_value = iter([
            ("orf19.1", None, "phenotype", None, None, None, None, None,
             None, None, None, None, None, None, None),
        ])

        output_file = temp_dir / "phenotype.tab"

        count = dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        assert count == 1

        content = output_file.read_text()
        # None should be converted to empty string
        assert "None" not in content.split("\n")[1]

    def test_dump_multiple_records(self, temp_dir, mock_db_session):
        """Test dumping multiple phenotype records."""
        mock_db_session.execute.return_value = iter([
            ("orf19.1", "ACT1", "morphology", "abnormal", "null",
             "SC5314", "assay1", "comment1", None, None, None, None,
             "details1", "111", "Ref1"),
            ("orf19.2", "TUB1", "growth", "normal", "null",
             "SC5314", "assay2", "comment2", None, None, None, None,
             "details2", "222", "Ref2"),
            ("orf19.3", None, "virulence", "reduced", "deletion",
             "SC5314", "assay3", "comment3", None, None, None, None,
             "details3", "333", "Ref3"),
        ])

        output_file = temp_dir / "phenotype.tab"

        count = dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        assert count == 3

        content = output_file.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 4  # Header + 3 records


class TestMainFunction:
    """Tests for the main function."""

    @patch('data.dump_phenotype_data.SessionLocal')
    @patch('data.dump_phenotype_data.LOG_DIR', Path('/tmp'))
    @patch('data.dump_phenotype_data.HTML_ROOT_DIR')
    def test_main_success(self, mock_html_dir, mock_session_local, temp_dir):
        """Test main with successful execution."""
        from data.dump_phenotype_data import main

        mock_html_dir.__truediv__ = lambda self, x: temp_dir / x
        mock_html_dir.mkdir = MagicMock()

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        # Mock organism found
        mock_session.execute.return_value.first.return_value = (
            1, "Candida albicans", "C_albicans_SC5314"
        )

        # Mock phenotype data
        mock_session.execute.return_value = iter([])

        with patch.object(sys, 'argv', ['prog', 'C_albicans_SC5314']):
            with patch('data.dump_phenotype_data.get_organism_info') as mock_get_org:
                mock_get_org.return_value = {
                    "organism_no": 1,
                    "organism_name": "Candida albicans",
                    "organism_abbrev": "C_albicans_SC5314",
                }
                with patch('data.dump_phenotype_data.get_species_abbrev') as mock_species:
                    mock_species.return_value = "C_albicans"
                    with patch('data.dump_phenotype_data.dump_phenotype_data') as mock_dump:
                        mock_dump.return_value = 10
                        result = main()

        assert result == 0

    @patch('data.dump_phenotype_data.SessionLocal')
    @patch('data.dump_phenotype_data.LOG_DIR', Path('/tmp'))
    def test_main_organism_not_found(self, mock_session_local):
        """Test main when organism not found."""
        from data.dump_phenotype_data import main

        mock_session = MagicMock()
        mock_session_local.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_session_local.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(sys, 'argv', ['prog', 'NonexistentOrganism']):
            with patch('data.dump_phenotype_data.get_organism_info') as mock_get_org:
                mock_get_org.return_value = None
                result = main()

        assert result == 1

    @patch('data.dump_phenotype_data.SessionLocal')
    @patch('data.dump_phenotype_data.LOG_DIR', Path('/tmp'))
    def test_main_handles_exception(self, mock_session_local):
        """Test main handles exceptions."""
        from data.dump_phenotype_data import main

        mock_session_local.side_effect = Exception("Database error")

        with patch.object(sys, 'argv', ['prog', 'C_albicans_SC5314']):
            result = main()

        assert result == 1

    def test_main_missing_organism(self):
        """Test main with missing organism argument."""
        from data.dump_phenotype_data import main

        with patch.object(sys, 'argv', ['prog']):
            with pytest.raises(SystemExit):
                main()


class TestHeaderFormat:
    """Tests for output file header format."""

    def test_header_columns(self, temp_dir, mock_db_session):
        """Test that header has all required columns."""
        mock_db_session.execute.return_value = iter([])

        output_file = temp_dir / "phenotype.tab"

        dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        content = output_file.read_text()
        header = content.split("\n")[0]
        columns = header.split("\t")

        expected_columns = [
            "Feature_Name",
            "Gene_Name",
            "Observable",
            "Qualifier",
            "Mutant_Type",
            "Strain_Background",
            "Experiment_Type",
            "Experiment_Comment",
            "Allele",
            "Allele_Comment",
            "Reporter",
            "Reporter_Comment",
            "Details",
            "PubMed_ID",
            "Citation",
        ]

        for col in expected_columns:
            assert col in columns


class TestEdgeCases:
    """Tests for edge cases."""

    def test_special_characters_in_data(self, temp_dir, mock_db_session):
        """Test handling of special characters in data."""
        mock_db_session.execute.return_value = iter([
            ("orf19.1", "Gene/Name", "phenotype\twith\ttabs", "qualifier",
             "null", "strain", "type", "comment with \"quotes\"", None,
             None, None, None, "details <with> special &chars;",
             "12345", "Citation"),
        ])

        output_file = temp_dir / "phenotype.tab"

        count = dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        assert count == 1

    def test_unicode_in_data(self, temp_dir, mock_db_session):
        """Test handling of unicode characters."""
        mock_db_session.execute.return_value = iter([
            ("orf19.1", "Gène", "phénotype", "α-factor", "null",
             "strain", "type", "comment", None, None, None, None,
             "détails", "12345", "Müller et al."),
        ])

        output_file = temp_dir / "phenotype.tab"

        count = dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        assert count == 1

        content = output_file.read_text()
        assert "Gène" in content
        assert "Müller" in content

    def test_empty_string_vs_none(self, temp_dir, mock_db_session):
        """Test distinction between empty string and None."""
        mock_db_session.execute.return_value = iter([
            ("orf19.1", "", "phenotype", None, "null",
             "strain", "type", "", None, None, None, None,
             None, "12345", ""),
        ])

        output_file = temp_dir / "phenotype.tab"

        count = dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        assert count == 1
        # Both empty strings and None should result in empty fields

    def test_very_long_details(self, temp_dir, mock_db_session):
        """Test handling of very long details field."""
        long_details = "x" * 10000
        mock_db_session.execute.return_value = iter([
            ("orf19.1", "ACT1", "phenotype", "qualifier", "null",
             "strain", "type", "comment", None, None, None, None,
             long_details, "12345", "Citation"),
        ])

        output_file = temp_dir / "phenotype.tab"

        count = dump_phenotype_data(mock_db_session, "C_albicans_SC5314", output_file)

        assert count == 1

        content = output_file.read_text()
        assert long_details in content
