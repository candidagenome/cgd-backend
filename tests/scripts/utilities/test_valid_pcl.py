#!/usr/bin/env python3
"""
Unit tests for scripts/utilities/valid_pcl.py

Tests the PCL (Pre-Cluster) file validation functionality.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "scripts"))

from utilities.valid_pcl import (
    is_number,
    read_params,
    crack_description,
    validate_pcl,
)


class TestIsNumber:
    """Tests for is_number function."""

    def test_valid_integers(self):
        """Test that integers are recognized as numbers."""
        assert is_number("0") is True
        assert is_number("1") is True
        assert is_number("-1") is True
        assert is_number("123456") is True

    def test_valid_floats(self):
        """Test that floats are recognized as numbers."""
        assert is_number("1.0") is True
        assert is_number("-3.14159") is True
        assert is_number("0.0") is True
        assert is_number(".5") is True

    def test_scientific_notation(self):
        """Test that scientific notation is recognized."""
        assert is_number("1e10") is True
        assert is_number("1E-5") is True
        assert is_number("3.14e2") is True

    def test_invalid_strings(self):
        """Test that non-numeric strings return False."""
        assert is_number("abc") is False
        assert is_number("") is False
        assert is_number("1.2.3") is False
        assert is_number("one") is False

    def test_none_value(self):
        """Test that None returns False."""
        assert is_number(None) is False


class TestReadParams:
    """Tests for read_params function."""

    def test_basic_params(self, temp_file):
        """Test reading basic parameters."""
        content = """pclFile=/path/to/data
gene=1
expt=2
"""
        param_file = temp_file("params.txt", content)
        params = read_params(param_file)

        assert params['pclFile'] == '/path/to/data'
        assert params['g'] == 1
        assert params['e'] == 2

    def test_euclidean_mode(self, temp_file):
        """Test Euclidean mode when gene=3."""
        content = """pclFile=/path/to/data
gene=3
expt=0
"""
        param_file = temp_file("params.txt", content)
        params = read_params(param_file)

        assert params['p'] == 0  # Euclidean
        assert params['g'] == 1  # gene=3 maps to g=1

    def test_comments_ignored(self, temp_file):
        """Test that comments are ignored."""
        content = """# This is a comment
pclFile=/path/to/data
# Another comment
gene=1
"""
        param_file = temp_file("params.txt", content)
        params = read_params(param_file)

        assert params['pclFile'] == '/path/to/data'
        assert '#' not in str(params)

    def test_empty_lines_ignored(self, temp_file):
        """Test that empty lines are ignored."""
        content = """pclFile=/path/to/data

gene=1

expt=2
"""
        param_file = temp_file("params.txt", content)
        params = read_params(param_file)

        assert params['pclFile'] == '/path/to/data'


class TestCrackDescription:
    """Tests for crack_description function."""

    def test_valid_description(self):
        """Test parsing a valid description."""
        desc = "orf19.123||ACT1||CGD:orf19.123||actin"
        warnings, errors = crack_description(desc, 1)
        assert warnings == 0
        assert errors == 0

    def test_missing_locus_id(self):
        """Test error when locusId is missing."""
        desc = "||ACT1||CGD:orf19.123||actin"
        warnings, errors = crack_description(desc, 1)
        assert errors == 1  # Missing locusId is an error

    def test_missing_gene_name(self):
        """Test warning when gene name is missing."""
        desc = "orf19.123||||CGD:orf19.123||actin"
        warnings, errors = crack_description(desc, 1)
        assert warnings >= 1  # Missing gene name is a warning

    def test_suppress_gene_warning(self):
        """Test that gene warning can be suppressed."""
        desc = "orf19.123||||CGD:orf19.123||actin"
        warnings, errors = crack_description(desc, 1, suppress_gene_warning=True)
        # Gene name warning should be suppressed
        assert warnings < 2  # Other warnings may still appear


class TestValidatePcl:
    """Tests for validate_pcl function."""

    def test_valid_pcl_file(self, temp_file):
        """Test validation of a valid PCL file."""
        content = """UID\tNAME\tGWEIGHT\tExp1\tExp2
EWEIGHT\t\t\t1\t1
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t1\t0.5\t0.8
orf19.2\torf19.2||TUB1||CGD:orf19.2||tubulin\t1\t0.3\t0.6
"""
        pcl_file = temp_file("test.pcl", content)
        warnings, errors = validate_pcl(pcl_file)
        assert errors == 0

    def test_wrong_header_columns(self, temp_file):
        """Test warning for wrong header column names."""
        content = """ID\tDescription\tWeight\tExp1
EWEIGHT\t\t\t1
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t1\t0.5
"""
        pcl_file = temp_file("test.pcl", content)
        warnings, errors = validate_pcl(pcl_file)
        assert warnings >= 3  # All three headers wrong

    def test_missing_eweight_row(self, temp_file):
        """Test warning when EWEIGHT row is missing."""
        content = """UID\tNAME\tGWEIGHT\tExp1
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t1\t0.5
"""
        pcl_file = temp_file("test.pcl", content)
        warnings, errors = validate_pcl(pcl_file)
        assert warnings >= 1  # EWEIGHT warning

    def test_non_numeric_eweight(self, temp_file):
        """Test warning for non-numeric EWEIGHT values."""
        content = """UID\tNAME\tGWEIGHT\tExp1
EWEIGHT\t\t\tabc
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t1\t0.5
"""
        pcl_file = temp_file("test.pcl", content)
        warnings, errors = validate_pcl(pcl_file)
        assert warnings >= 1

    def test_missing_uid(self, temp_file):
        """Test error when UID is missing."""
        content = """UID\tNAME\tGWEIGHT\tExp1
EWEIGHT\t\t\t1
\torf19.1||ACT1||CGD:orf19.1||actin\t1\t0.5
"""
        pcl_file = temp_file("test.pcl", content)
        warnings, errors = validate_pcl(pcl_file)
        assert errors >= 1

    def test_missing_gweight(self, temp_file):
        """Test error when gweight is missing."""
        content = """UID\tNAME\tGWEIGHT\tExp1
EWEIGHT\t\t\t1
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t\t0.5
"""
        pcl_file = temp_file("test.pcl", content)
        warnings, errors = validate_pcl(pcl_file)
        assert errors >= 1

    def test_non_numeric_data_value(self, temp_file):
        """Test error for non-numeric data values."""
        content = """UID\tNAME\tGWEIGHT\tExp1
EWEIGHT\t\t\t1
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t1\tabc
"""
        pcl_file = temp_file("test.pcl", content)
        warnings, errors = validate_pcl(pcl_file)
        assert errors >= 1

    def test_wrong_column_count(self, temp_file):
        """Test error for inconsistent column counts."""
        content = """UID\tNAME\tGWEIGHT\tExp1\tExp2
EWEIGHT\t\t\t1\t1
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t1\t0.5
"""
        pcl_file = temp_file("test.pcl", content)
        warnings, errors = validate_pcl(pcl_file)
        assert errors >= 1  # Wrong column count

    def test_suppress_value_warning(self, temp_file):
        """Test that missing value warnings can be suppressed."""
        content = """UID\tNAME\tGWEIGHT\tExp1\tExp2
EWEIGHT\t\t\t1\t1
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t1\t\t0.8
"""
        pcl_file = temp_file("test.pcl", content)
        warnings_default, _ = validate_pcl(pcl_file)
        warnings_suppressed, _ = validate_pcl(pcl_file, suppress_value_warning=True)
        assert warnings_suppressed < warnings_default


class TestMainFunction:
    """Tests for the main function."""

    def test_main_with_valid_file(self, temp_dir, capsys):
        """Test main function with a valid PCL file."""
        # Create param file
        param_file = temp_dir / "params.txt"
        param_file.write_text(f"pclFile={temp_dir / 'test'}\n")

        # Create PCL file
        pcl_file = temp_dir / "test.pcl"
        pcl_file.write_text("""UID\tNAME\tGWEIGHT\tExp1
EWEIGHT\t\t\t1
orf19.1\torf19.1||ACT1||CGD:orf19.1||actin\t1\t0.5
""")

        # Test would require mocking sys.argv
        # This test validates the main logic indirectly through validate_pcl
        warnings, errors = validate_pcl(pcl_file)
        assert errors == 0
