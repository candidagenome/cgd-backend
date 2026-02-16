#!/usr/bin/env python3
"""
Unit tests for scripts/utilities/recompute_locations_after_seq_update.py

Tests the genomic location recomputation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from utilities.recompute_locations_after_seq_update import (
    parse_seq_changes,
    compute_new_location,
    SeqChange,
    UpdateType,
    FileFormat,
    LocationStats,
)


class TestParseSeqChanges:
    """Tests for parse_seq_changes function."""

    def test_parse_insertion(self, temp_file):
        """Test parsing insertion change."""
        content = "chr1\tinsertion\t100:50\n"
        change_file = temp_file("changes.txt", content)

        result = parse_seq_changes(change_file)

        assert "chr1" in result
        assert result["chr1"].update_type == UpdateType.INSERTION
        assert result["chr1"].details == "100:50"

    def test_parse_deletion(self, temp_file):
        """Test parsing deletion change."""
        content = "chr1\tdeletion\t100:150\n"
        change_file = temp_file("changes.txt", content)

        result = parse_seq_changes(change_file)

        assert result["chr1"].update_type == UpdateType.DELETION
        assert result["chr1"].details == "100:150"

    def test_parse_substitution(self, temp_file):
        """Test parsing substitution change."""
        content = "chr1\tsubstitution\t100:150:60\n"
        change_file = temp_file("changes.txt", content)

        result = parse_seq_changes(change_file)

        assert result["chr1"].update_type == UpdateType.SUBSTITUTION
        assert result["chr1"].details == "100:150:60"

    def test_parse_multiple_chromosomes(self, temp_file):
        """Test parsing changes for multiple chromosomes."""
        content = """chr1\tinsertion\t100:50
chr2\tdeletion\t200:250
"""
        change_file = temp_file("changes.txt", content)

        result = parse_seq_changes(change_file)

        assert len(result) == 2
        assert "chr1" in result
        assert "chr2" in result

    def test_skip_empty_lines(self, temp_file):
        """Test that empty lines are skipped."""
        content = """chr1\tinsertion\t100:50

chr2\tdeletion\t200:250
"""
        change_file = temp_file("changes.txt", content)

        result = parse_seq_changes(change_file)

        assert len(result) == 2

    def test_invalid_format(self, temp_file):
        """Test error on invalid line format."""
        content = "chr1\tinsertion\n"  # Missing details
        change_file = temp_file("changes.txt", content)

        with pytest.raises(ValueError) as exc_info:
            parse_seq_changes(change_file)

        assert "Invalid format" in str(exc_info.value)

    def test_invalid_update_type(self, temp_file):
        """Test error on invalid update type."""
        content = "chr1\tinvalid_type\t100:50\n"
        change_file = temp_file("changes.txt", content)

        with pytest.raises(ValueError) as exc_info:
            parse_seq_changes(change_file)

        assert "not recognized" in str(exc_info.value)

    def test_duplicate_chromosome(self, temp_file):
        """Test error on duplicate chromosome."""
        content = """chr1\tinsertion\t100:50
chr1\tdeletion\t200:250
"""
        change_file = temp_file("changes.txt", content)

        with pytest.raises(ValueError) as exc_info:
            parse_seq_changes(change_file)

        assert "Only one change per chromosome" in str(exc_info.value)

    def test_case_insensitive_update_type(self, temp_file):
        """Test that update type is case-insensitive."""
        content = "chr1\tINSERTION\t100:50\n"
        change_file = temp_file("changes.txt", content)

        result = parse_seq_changes(change_file)

        assert result["chr1"].update_type == UpdateType.INSERTION


class TestComputeNewLocationInsertion:
    """Tests for compute_new_location with insertions."""

    def test_downstream_shift(self):
        """Test regions downstream of insertion are shifted."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.INSERTION, "100:50",
            start=200, stop=300, line_num=1, line_content=""
        )

        assert new_start == 250  # 200 + 50
        assert new_stop == 350   # 300 + 50
        assert change_type == "downstream"

    def test_unaffected_upstream(self):
        """Test regions upstream of insertion are unaffected."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.INSERTION, "200:50",
            start=50, stop=100, line_num=1, line_content=""
        )

        assert new_start == 50
        assert new_stop == 100
        assert change_type == "unaffected"

    def test_overlap_warning(self):
        """Test overlapping regions generate warning."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.INSERTION, "100:50",
            start=80, stop=120, line_num=1, line_content=""
        )

        assert change_type == "overlap"
        assert new_start == 80  # Unchanged
        assert new_stop == 120  # Unchanged


class TestComputeNewLocationDeletion:
    """Tests for compute_new_location with deletions."""

    def test_downstream_shift(self):
        """Test regions downstream of deletion are shifted back."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.DELETION, "100:150",  # Deleting 51 bases
            start=200, stop=300, line_num=1, line_content=""
        )

        assert new_start == 149  # 200 - 51
        assert new_stop == 249   # 300 - 51
        assert change_type == "downstream"

    def test_unaffected_upstream(self):
        """Test regions upstream of deletion are unaffected."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.DELETION, "200:250",
            start=50, stop=100, line_num=1, line_content=""
        )

        assert new_start == 50
        assert new_stop == 100
        assert change_type == "unaffected"

    def test_encompassing_region(self):
        """Test regions that contain the deletion."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.DELETION, "100:150",  # Deleting 51 bases
            start=50, stop=200, line_num=1, line_content=""
        )

        assert new_start == 50
        assert new_stop == 149   # 200 - 51
        assert change_type == "encompassing"

    def test_overlap_warning(self):
        """Test overlapping regions generate warning."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.DELETION, "100:150",
            start=120, stop=200, line_num=1, line_content=""
        )

        assert change_type == "overlap"


class TestComputeNewLocationSubstitution:
    """Tests for compute_new_location with substitutions."""

    def test_downstream_shift_larger(self):
        """Test downstream shift when substitution is larger."""
        # Substituting 51 bases (100-150) with 100 bases = +49 offset
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.SUBSTITUTION, "100:150:100",
            start=200, stop=300, line_num=1, line_content=""
        )

        assert new_start == 249  # 200 + 49
        assert new_stop == 349   # 300 + 49
        assert change_type == "downstream"

    def test_downstream_shift_smaller(self):
        """Test downstream shift when substitution is smaller."""
        # Substituting 51 bases (100-150) with 20 bases = -31 offset
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.SUBSTITUTION, "100:150:20",
            start=200, stop=300, line_num=1, line_content=""
        )

        assert new_start == 169  # 200 - 31
        assert new_stop == 269   # 300 - 31
        assert change_type == "downstream"

    def test_unaffected_upstream(self):
        """Test regions upstream of substitution are unaffected."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.SUBSTITUTION, "200:250:60",
            start=50, stop=100, line_num=1, line_content=""
        )

        assert new_start == 50
        assert new_stop == 100
        assert change_type == "unaffected"

    def test_encompassing_region(self):
        """Test regions that contain the substitution."""
        # Substituting 51 bases with 100 = +49 offset
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.SUBSTITUTION, "100:150:100",
            start=50, stop=200, line_num=1, line_content=""
        )

        assert new_start == 50
        assert new_stop == 249   # 200 + 49
        assert change_type == "encompassing"


class TestDataClasses:
    """Tests for dataclasses."""

    def test_seq_change_creation(self):
        """Test SeqChange dataclass."""
        change = SeqChange(
            update_type=UpdateType.INSERTION,
            details="100:50"
        )

        assert change.update_type == UpdateType.INSERTION
        assert change.details == "100:50"

    def test_location_stats_defaults(self):
        """Test LocationStats dataclass defaults."""
        stats = LocationStats()

        assert stats.downstream == 0
        assert stats.unaffected == 0
        assert stats.overlap == 0
        assert stats.encompassing == 0

    def test_location_stats_update(self):
        """Test LocationStats can be updated."""
        stats = LocationStats()
        stats.downstream = 10
        stats.overlap = 2

        assert stats.downstream == 10
        assert stats.overlap == 2


class TestEnums:
    """Tests for enums."""

    def test_file_format_values(self):
        """Test FileFormat enum values."""
        assert FileFormat.GFF.value == "GFF"
        assert FileFormat.VCF.value == "VCF"

    def test_update_type_values(self):
        """Test UpdateType enum values."""
        assert UpdateType.INSERTION.value == "insertion"
        assert UpdateType.DELETION.value == "deletion"
        assert UpdateType.SUBSTITUTION.value == "substitution"


class TestMainFunction:
    """Tests for the main function."""

    def test_main_missing_input(self, temp_dir):
        """Test main with missing input file."""
        from utilities.recompute_locations_after_seq_update import main

        changes_file = temp_dir / "changes.txt"
        changes_file.write_text("chr1\tinsertion\t100:50\n")

        with patch.object(sys, 'argv', [
            'prog',
            str(temp_dir / 'nonexistent.gff'),
            'GFF',
            str(changes_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1

    def test_main_missing_changes(self, temp_dir):
        """Test main with missing changes file."""
        from utilities.recompute_locations_after_seq_update import main

        input_file = temp_dir / "input.gff"
        input_file.write_text("header\nchr1\t.\tgene\t100\t200\n")

        with patch.object(sys, 'argv', [
            'prog',
            str(input_file),
            'GFF',
            str(temp_dir / 'nonexistent.txt')
        ]):
            with pytest.raises(SystemExit) as exc_info:
                main()

        assert exc_info.value.code == 1


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_changes_file(self, temp_file):
        """Test empty changes file."""
        change_file = temp_file("changes.txt", "")

        result = parse_seq_changes(change_file)

        assert len(result) == 0

    def test_single_base_deletion(self):
        """Test single base deletion."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.DELETION, "100:100",  # Single base
            start=200, stop=300, line_num=1, line_content=""
        )

        assert new_start == 199  # 200 - 1
        assert new_stop == 299   # 300 - 1
        assert change_type == "downstream"

    def test_zero_length_insertion(self):
        """Test zero-length insertion (edge case)."""
        new_start, new_stop, change_type = compute_new_location(
            UpdateType.INSERTION, "100:0",
            start=200, stop=300, line_num=1, line_content=""
        )

        # No change expected
        assert new_start == 200
        assert new_stop == 300
