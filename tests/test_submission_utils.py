"""
Tests for Submission Utilities.

Tests cover:
- Submission directory handling
- Filename generation
- Colleague submission writing
- Gene registry submission writing
- Text formatting for colleague submissions
- Text formatting for gene registry submissions
"""
import pytest
import json
import os
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import datetime

from cgd.api.services.submission_utils import (
    _get_submission_dir,
    _ensure_submission_dir,
    _generate_filename,
    write_colleague_submission,
    write_gene_registry_submission,
    format_colleague_submission_text,
    format_gene_registry_text,
)


class TestGetSubmissionDir:
    """Tests for _get_submission_dir."""

    def test_returns_env_var_when_set(self):
        """Should return env var value when set."""
        with patch.dict(os.environ, {'CGD_SUBMISSION_DIR': '/custom/path'}):
            result = _get_submission_dir()
            assert result == '/custom/path'

    def test_returns_default_when_no_env_var(self):
        """Should return default path when no env var."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove the env var if it exists
            os.environ.pop('CGD_SUBMISSION_DIR', None)
            result = _get_submission_dir()
            assert '/tmp' in result or 'cgd_submissions' in result


class TestGenerateFilename:
    """Tests for _generate_filename."""

    def test_includes_prefix(self):
        """Should include prefix in filename."""
        result = _generate_filename("test_prefix")
        assert result.startswith("test_prefix_")

    def test_includes_pid(self):
        """Should include PID in filename."""
        result = _generate_filename("prefix", pid=12345)
        assert "12345" in result

    def test_ends_with_json(self):
        """Should end with .json extension."""
        result = _generate_filename("prefix")
        assert result.endswith(".json")

    def test_includes_timestamp(self):
        """Should include timestamp in filename."""
        result = _generate_filename("prefix")
        # Should contain date pattern YYYYMMDD
        import re
        assert re.search(r'\d{8}_\d{6}', result)


class TestWriteColleagueSubmission:
    """Tests for write_colleague_submission."""

    @patch('cgd.api.services.submission_utils._ensure_submission_dir')
    def test_writes_json_file(self, mock_ensure_dir, tmp_path):
        """Should write JSON file with submission data."""
        mock_ensure_dir.return_value = tmp_path

        data = {
            "last_name": "Smith",
            "first_name": "John",
            "email": "john@example.com",
        }

        filepath = write_colleague_submission(None, data)

        assert os.path.exists(filepath)
        with open(filepath) as f:
            content = json.load(f)
        assert content["data"]["last_name"] == "Smith"

    @patch('cgd.api.services.submission_utils._ensure_submission_dir')
    def test_marks_new_submission(self, mock_ensure_dir, tmp_path):
        """Should mark as new submission when no colleague_no."""
        mock_ensure_dir.return_value = tmp_path

        filepath = write_colleague_submission(None, {"last_name": "Test"})

        with open(filepath) as f:
            content = json.load(f)
        assert content["submission_type"] == "colleague_new"
        assert content["colleague_no"] is None

    @patch('cgd.api.services.submission_utils._ensure_submission_dir')
    def test_marks_update_submission(self, mock_ensure_dir, tmp_path):
        """Should mark as update when colleague_no provided."""
        mock_ensure_dir.return_value = tmp_path

        filepath = write_colleague_submission(123, {"last_name": "Test"})

        with open(filepath) as f:
            content = json.load(f)
        assert content["submission_type"] == "colleague_update"
        assert content["colleague_no"] == 123

    @patch('cgd.api.services.submission_utils._ensure_submission_dir')
    def test_includes_remote_addr(self, mock_ensure_dir, tmp_path):
        """Should include remote address when provided."""
        mock_ensure_dir.return_value = tmp_path

        filepath = write_colleague_submission(
            None, {"last_name": "Test"}, remote_addr="192.168.1.1"
        )

        with open(filepath) as f:
            content = json.load(f)
        assert content["remote_addr"] == "192.168.1.1"

    @patch('cgd.api.services.submission_utils._ensure_submission_dir')
    def test_includes_submitted_at(self, mock_ensure_dir, tmp_path):
        """Should include submission timestamp."""
        mock_ensure_dir.return_value = tmp_path

        filepath = write_colleague_submission(None, {"last_name": "Test"})

        with open(filepath) as f:
            content = json.load(f)
        assert "submitted_at" in content


class TestWriteGeneRegistrySubmission:
    """Tests for write_gene_registry_submission."""

    @patch('cgd.api.services.submission_utils._ensure_submission_dir')
    def test_writes_json_file(self, mock_ensure_dir, tmp_path):
        """Should write JSON file with gene registry data."""
        mock_ensure_dir.return_value = tmp_path

        data = {
            "gene_name": "ALS1",
            "orf_name": "CAL0001",
            "organism": "C_albicans_SC5314",
        }

        filepath = write_gene_registry_submission(data)

        assert os.path.exists(filepath)
        with open(filepath) as f:
            content = json.load(f)
        assert content["gene_name"] == "ALS1"

    @patch('cgd.api.services.submission_utils._ensure_submission_dir')
    def test_marks_as_gene_registry(self, mock_ensure_dir, tmp_path):
        """Should mark submission type as gene_registry."""
        mock_ensure_dir.return_value = tmp_path

        filepath = write_gene_registry_submission({"gene_name": "TEST1"})

        with open(filepath) as f:
            content = json.load(f)
        assert content["submission_type"] == "gene_registry"

    @patch('cgd.api.services.submission_utils._ensure_submission_dir')
    def test_includes_all_gene_fields(self, mock_ensure_dir, tmp_path):
        """Should include all gene-related fields."""
        mock_ensure_dir.return_value = tmp_path

        data = {
            "gene_name": "ALS1",
            "orf_name": "CAL0001",
            "organism": "C_albicans_SC5314",
            "colleague_no": 123,
        }

        filepath = write_gene_registry_submission(data)

        with open(filepath) as f:
            content = json.load(f)
        assert content["gene_name"] == "ALS1"
        assert content["orf_name"] == "CAL0001"
        assert content["organism"] == "C_albicans_SC5314"
        assert content["colleague_no"] == 123


class TestFormatColleagueSubmissionText:
    """Tests for format_colleague_submission_text."""

    def test_includes_header(self):
        """Should include COLLEAGUE SUBMISSION header."""
        result = format_colleague_submission_text(None, {"last_name": "Test"})
        assert "COLLEAGUE SUBMISSION" in result

    def test_marks_new_registration(self):
        """Should mark as New Registration when no colleague_no."""
        result = format_colleague_submission_text(None, {"last_name": "Test"})
        assert "New Registration" in result

    def test_marks_update(self):
        """Should mark as Update when colleague_no provided."""
        result = format_colleague_submission_text(123, {"last_name": "Test"})
        assert "Update" in result
        assert "Colleague ID: 123" in result

    def test_includes_name(self):
        """Should include formatted name."""
        data = {"first_name": "John", "last_name": "Smith"}
        result = format_colleague_submission_text(None, data)
        assert "John Smith" in result

    def test_includes_email(self):
        """Should include email."""
        data = {"last_name": "Test", "email": "test@example.com"}
        result = format_colleague_submission_text(None, data)
        assert "test@example.com" in result

    def test_includes_organization(self):
        """Should include organization."""
        data = {"last_name": "Test", "institution": "MIT"}
        result = format_colleague_submission_text(None, data)
        assert "MIT" in result

    def test_includes_address(self):
        """Should include address fields."""
        data = {
            "last_name": "Test",
            "address1": "123 Main St",
            "city": "Cambridge",
            "state": "MA",
            "postal_code": "02139",
            "country": "USA",
        }
        result = format_colleague_submission_text(None, data)
        assert "123 Main St" in result
        assert "Cambridge" in result
        assert "USA" in result

    def test_includes_phone(self):
        """Should include phone numbers."""
        data = {
            "last_name": "Test",
            "work_phone": "555-1234",
            "fax": "555-5678",
        }
        result = format_colleague_submission_text(None, data)
        assert "555-1234" in result
        assert "555-5678" in result

    def test_includes_research_interests(self):
        """Should include research interests."""
        data = {
            "last_name": "Test",
            "research_interests": "Fungal genetics",
        }
        result = format_colleague_submission_text(None, data)
        assert "Fungal genetics" in result

    def test_includes_urls(self):
        """Should include URLs."""
        data = {
            "last_name": "Test",
            "urls": [{"url": "http://example.com", "url_type": "Lab"}],
        }
        result = format_colleague_submission_text(None, data)
        assert "http://example.com" in result


class TestFormatGeneRegistryText:
    """Tests for format_gene_registry_text."""

    def test_includes_header(self):
        """Should include GENE REGISTRY header."""
        result = format_gene_registry_text({"gene_name": "ALS1"})
        assert "GENE REGISTRY SUBMISSION" in result

    def test_includes_gene_name(self):
        """Should include gene name."""
        result = format_gene_registry_text({"gene_name": "ALS1"})
        assert "Gene Name: ALS1" in result

    def test_includes_orf_name(self):
        """Should include ORF name when provided."""
        result = format_gene_registry_text({
            "gene_name": "ALS1",
            "orf_name": "CAL0001",
        })
        assert "ORF Name: CAL0001" in result

    def test_includes_organism(self):
        """Should include organism."""
        result = format_gene_registry_text({
            "gene_name": "ALS1",
            "organism": "C_albicans_SC5314",
        })
        assert "Organism: C_albicans_SC5314" in result

    def test_includes_colleague_no_when_provided(self):
        """Should include colleague ID when provided."""
        result = format_gene_registry_text({
            "gene_name": "ALS1",
            "colleague_no": 123,
        })
        assert "Colleague ID: 123" in result

    def test_includes_new_colleague_info(self):
        """Should include new colleague info when no colleague_no."""
        result = format_gene_registry_text({
            "gene_name": "ALS1",
            "first_name": "John",
            "last_name": "Smith",
            "email": "john@example.com",
            "institution": "MIT",
        })
        assert "John Smith" in result
        assert "john@example.com" in result
        assert "MIT" in result

    def test_includes_description(self):
        """Should include description when provided."""
        result = format_gene_registry_text({
            "gene_name": "ALS1",
            "description": "Cell surface adhesin",
        })
        assert "Description: Cell surface adhesin" in result

    def test_includes_reference(self):
        """Should include reference when provided."""
        result = format_gene_registry_text({
            "gene_name": "ALS1",
            "reference": "PMID:12345678",
        })
        assert "Reference: PMID:12345678" in result

    def test_includes_comments(self):
        """Should include comments when provided."""
        result = format_gene_registry_text({
            "gene_name": "ALS1",
            "comments": "Additional notes",
        })
        assert "Comments: Additional notes" in result
