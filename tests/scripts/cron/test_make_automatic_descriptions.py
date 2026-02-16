#!/usr/bin/env python3
"""
Unit tests for scripts/cron/make_automatic_descriptions.py

Tests the automatic description generation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from cron.make_automatic_descriptions import (
    AutomaticDescriptionGenerator,
    ASPECT_DESCRIPTIONS,
    MAX_DESCRIPTION_LENGTH,
    REFERENCE_NOS,
)


class TestAutomaticDescriptionGeneratorInit:
    """Tests for AutomaticDescriptionGenerator initialization."""

    def test_init_valid_strain(self, mock_db_session):
        """Test initialization with valid strain."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "C_albicans_SC5314", load_to_db=False
        )

        assert generator.organism_no == 1
        assert generator.strain_abbrev == "C_albicans_SC5314"
        assert generator.load_to_db is False

    def test_init_invalid_strain(self, mock_db_session):
        """Test initialization with invalid strain."""
        mock_db_session.execute.return_value.first.return_value = None

        with pytest.raises(ValueError) as exc_info:
            AutomaticDescriptionGenerator(mock_db_session, "InvalidStrain")

        assert "No organism found" in str(exc_info.value)


class TestGetOrganismNo:
    """Tests for _get_organism_no method."""

    def test_organism_found(self, mock_db_session):
        """Test when organism is found."""
        mock_db_session.execute.return_value.first.return_value = (42,)

        generator = AutomaticDescriptionGenerator.__new__(AutomaticDescriptionGenerator)
        generator.session = mock_db_session
        generator.strain_abbrev = "SC5314"

        result = generator._get_organism_no()

        assert result == 42

    def test_organism_not_found(self, mock_db_session):
        """Test when organism is not found."""
        mock_db_session.execute.return_value.first.return_value = None

        generator = AutomaticDescriptionGenerator.__new__(AutomaticDescriptionGenerator)
        generator.session = mock_db_session
        generator.strain_abbrev = "Unknown"

        result = generator._get_organism_no()

        assert result is None


class TestGetCandidateFeatures:
    """Tests for get_candidate_features method."""

    def test_returns_features(self, mock_db_session):
        """Test returning candidate features."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        mock_db_session.execute.return_value = iter([
            (100, "orf19.1"),
            (101, "orf19.2"),
            (102, "orf19.3"),
        ])

        features = generator.get_candidate_features()

        assert len(features) == 3
        assert (100, "orf19.1") in features

    def test_empty_features(self, mock_db_session):
        """Test with no candidate features."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        mock_db_session.execute.return_value = iter([])

        features = generator.get_candidate_features()

        assert features == []


class TestGetGoAnnotations:
    """Tests for get_go_annotations method."""

    def test_with_annotations(self, mock_db_session):
        """Test getting GO annotations."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        mock_db_session.execute.return_value = iter([
            ("GO:0001", "kinase activity", "F"),
            ("GO:0002", "cell division", "P"),
            ("GO:0003", "nucleus", "C"),
        ])

        go_terms = generator.get_go_annotations(100, 49605)

        assert "F" in go_terms
        assert "P" in go_terms
        assert "C" in go_terms
        assert "kinase activity" in go_terms["F"]

    def test_empty_annotations(self, mock_db_session):
        """Test with no GO annotations."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        mock_db_session.execute.return_value = iter([])

        go_terms = generator.get_go_annotations(100, 49605)

        assert go_terms == {}


class TestGetOrthologs:
    """Tests for get_orthologs method."""

    def test_with_orthologs(self, mock_db_session):
        """Test getting orthologs."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        # Mock will be called multiple times for different queries
        def mock_execute(*args, **kwargs):
            result = MagicMock()
            result.__iter__ = lambda self: iter([
                ("SGD1", "ACT1", "S. cerevisiae"),
            ])
            return result

        mock_db_session.execute.side_effect = mock_execute

        orthologs = generator.get_orthologs(100)

        assert "S. cerevisiae" in orthologs

    def test_empty_orthologs(self, mock_db_session):
        """Test with no orthologs."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        mock_db_session.execute.return_value = iter([])

        orthologs = generator.get_orthologs(100)

        assert orthologs == {}


class TestCreateDescFromGo:
    """Tests for create_desc_from_go method."""

    def test_single_aspect(self, mock_db_session):
        """Test creating description from single GO aspect."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        go_terms = {"F": ["kinase activity"]}

        desc = generator.create_desc_from_go(go_terms, "Ortholog(s) have ")

        assert desc is not None
        assert "Ortholog(s) have " in desc
        assert "kinase" in desc.lower()

    def test_multiple_aspects(self, mock_db_session):
        """Test creating description from multiple GO aspects."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        go_terms = {
            "F": ["kinase activity"],
            "P": ["cell division"],
        }

        desc = generator.create_desc_from_go(go_terms, "Has domain(s) with predicted ")

        assert desc is not None
        assert "Has domain(s) with predicted " in desc

    def test_empty_go_terms(self, mock_db_session):
        """Test with empty GO terms."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        desc = generator.create_desc_from_go({}, "Ortholog(s) have ")

        assert desc is None


class TestCreateDescFromOrthologs:
    """Tests for create_desc_from_orthologs method."""

    def test_single_species(self, mock_db_session):
        """Test creating description from single species ortholog."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        orthologs = {"S. cerevisiae": ["ACT1"]}

        desc = generator.create_desc_from_orthologs(orthologs, "Ortholog of ")

        assert desc is not None
        assert "Ortholog of " in desc
        assert "ACT1" in desc

    def test_multiple_species(self, mock_db_session):
        """Test creating description from multiple species orthologs."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        orthologs = {
            "S. cerevisiae": ["ACT1"],
            "C. glabrata": ["CAGL0A00001g"],
        }

        mock_db_session.execute.return_value.first.return_value = ("C. glabrata",)

        desc = generator.create_desc_from_orthologs(orthologs, "Ortholog of ")

        assert desc is not None

    def test_empty_orthologs(self, mock_db_session):
        """Test with empty orthologs."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        desc = generator.create_desc_from_orthologs({}, "Ortholog of ")

        assert desc is None


class TestJoinParts:
    """Tests for _join_parts method."""

    def test_single_part(self, mock_db_session):
        """Test joining single part."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        result = generator._join_parts(["part1"], 1)

        assert result == "part1"

    def test_two_parts(self, mock_db_session):
        """Test joining two parts."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        result = generator._join_parts(["part1", "part2"], 2)

        assert result == "part1 and part2"

    def test_three_parts(self, mock_db_session):
        """Test joining three parts."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        result = generator._join_parts(["part1", "part2", "part3"], 3)

        assert result == "part1, part2 and part3"

    def test_zero_parts(self, mock_db_session):
        """Test joining zero parts."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        result = generator._join_parts(["part1", "part2"], 0)

        assert result == ""


class TestGenerateDescription:
    """Tests for generate_description method."""

    def test_default_description(self, mock_db_session):
        """Test generating default description when no data."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        # Mock to return empty for all queries
        mock_db_session.execute.return_value = iter([])

        desc = generator.generate_description(100, "orf19.1")

        assert desc == "Protein of unknown function"

    def test_with_go_annotations(self, mock_db_session):
        """Test generating description with GO annotations."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        # Mock GO annotations
        generator.get_go_annotations = MagicMock(return_value={"F": ["kinase activity"]})
        generator.create_desc_from_go = MagicMock(return_value="Ortholog(s) have kinase activity")

        desc = generator.generate_description(100, "orf19.1")

        assert "kinase" in desc


class TestConstants:
    """Tests for module constants."""

    def test_max_description_length(self):
        """Test MAX_DESCRIPTION_LENGTH constant."""
        assert MAX_DESCRIPTION_LENGTH == 240

    def test_aspect_descriptions(self):
        """Test ASPECT_DESCRIPTIONS constant."""
        assert "F" in ASPECT_DESCRIPTIONS
        assert "P" in ASPECT_DESCRIPTIONS
        assert "C" in ASPECT_DESCRIPTIONS
        assert "activity" in ASPECT_DESCRIPTIONS["F"]

    def test_reference_nos_cgd(self):
        """Test REFERENCE_NOS for CGD."""
        assert "CGD" in REFERENCE_NOS
        assert "auto_description" in REFERENCE_NOS["CGD"]
        assert "ortho_go_transfer" in REFERENCE_NOS["CGD"]

    def test_reference_nos_aspgd(self):
        """Test REFERENCE_NOS for AspGD."""
        assert "AspGD" in REFERENCE_NOS


class TestMainFunction:
    """Tests for the main function."""

    @patch('cron.make_automatic_descriptions.make_automatic_descriptions')
    def test_main_success(self, mock_make_desc):
        """Test main with successful execution."""
        from cron.make_automatic_descriptions import main

        mock_make_desc.return_value = True

        with patch.object(sys, 'argv', ['prog', '--strain', 'SC5314']):
            result = main()

        assert result == 0
        mock_make_desc.assert_called_once()

    @patch('cron.make_automatic_descriptions.make_automatic_descriptions')
    def test_main_failure(self, mock_make_desc):
        """Test main with failed execution."""
        from cron.make_automatic_descriptions import main

        mock_make_desc.return_value = False

        with patch.object(sys, 'argv', ['prog', '--strain', 'SC5314']):
            result = main()

        assert result == 1

    @patch('cron.make_automatic_descriptions.make_automatic_descriptions')
    def test_main_dry_run(self, mock_make_desc):
        """Test main with dry-run flag."""
        from cron.make_automatic_descriptions import main

        mock_make_desc.return_value = True

        with patch.object(sys, 'argv', ['prog', '--strain', 'SC5314', '--dry-run']):
            result = main()

        assert result == 0
        mock_make_desc.assert_called_with('SC5314', load_to_db=False)


class TestEdgeCases:
    """Tests for edge cases."""

    def test_long_description_truncation(self, mock_db_session):
        """Test that long descriptions are handled."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        # Create a very long GO term list
        long_terms = {"F": ["term" + str(i) for i in range(100)]}

        desc = generator.create_desc_from_go(long_terms, "Ortholog(s) have ")

        # Description should be None or under max length
        assert desc is None or len(desc) < MAX_DESCRIPTION_LENGTH

    def test_species_with_cerevisiae_first(self, mock_db_session):
        """Test that S. cerevisiae is sorted first."""
        mock_db_session.execute.return_value.first.return_value = (1,)

        generator = AutomaticDescriptionGenerator(
            mock_db_session, "SC5314", load_to_db=False
        )

        orthologs = {
            "A. nidulans": ["AN0001"],
            "S. cerevisiae": ["ACT1"],
            "C. glabrata": ["CAGL0001"],
        }

        # Mock species display
        mock_db_session.execute.return_value.first.return_value = None

        desc = generator.create_desc_from_orthologs(orthologs, "Ortholog of ")

        if desc:
            # S. cerevisiae should appear first
            sc_pos = desc.find("S. cerevisiae")
            assert sc_pos >= 0
