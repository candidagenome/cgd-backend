"""
Tests for BLAST service functionality.

Tests cover:
- Task auto-selection
- JBrowse URL generation
- orf19 ID mapping
- Multi-database search
- Download format generation
- Genetic code handling
"""
import pytest
from unittest.mock import MagicMock, patch
from typing import Optional

from cgd.schemas.blast_schema import (
    BlastProgram,
    BlastTask,
    BlastDatabase,
    DownloadFormat,
    BlastSearchRequest,
    BlastSearchResult,
    BlastHit,
    BlastHsp,
)
from cgd.api.services.blast_service import (
    _select_blast_task,
    _generate_jbrowse_url,
    _map_to_orf19_id,
    get_blast_organisms,
    get_tasks_for_program,
    get_genetic_codes,
    generate_fasta_download,
    generate_tab_download,
    generate_raw_download,
    format_blast_results_text,
)
from cgd.core.blast_config import (
    BLAST_ORGANISMS,
    BLAST_TASKS,
    GENETIC_CODES,
    get_organism_for_database,
    extract_organism_tag_from_database,
    load_blast_clade_conf,
)


class TestBlastTaskSelection:
    """Tests for BLAST task auto-selection."""

    def test_blastn_short_query_selects_blastn_short(self):
        """Short nucleotide queries (<50 bp) should use blastn-short."""
        task = _select_blast_task(BlastProgram.BLASTN, query_length=30)
        assert task == "blastn-short"

    def test_blastn_long_query_selects_megablast(self):
        """Long nucleotide queries (>=50 bp) should use megablast."""
        task = _select_blast_task(BlastProgram.BLASTN, query_length=100)
        assert task == "megablast"

    def test_blastp_short_query_selects_blastp_short(self):
        """Short protein queries (<30 aa) should use blastp-short."""
        task = _select_blast_task(BlastProgram.BLASTP, query_length=20)
        assert task == "blastp-short"

    def test_blastp_long_query_selects_blastp(self):
        """Long protein queries (>=30 aa) should use standard blastp."""
        task = _select_blast_task(BlastProgram.BLASTP, query_length=100)
        assert task == "blastp"

    def test_user_specified_task_takes_precedence(self):
        """User-specified task should override auto-selection."""
        task = _select_blast_task(
            BlastProgram.BLASTN,
            query_length=30,
            user_task=BlastTask.DC_MEGABLAST
        )
        assert task == "dc-megablast"

    def test_blastx_returns_none(self):
        """BLASTX has no task variants, should return None."""
        task = _select_blast_task(BlastProgram.BLASTX, query_length=100)
        assert task is None

    def test_tblastn_returns_none(self):
        """TBLASTN has no task variants, should return None."""
        task = _select_blast_task(BlastProgram.TBLASTN, query_length=100)
        assert task is None


class TestJBrowseUrlGeneration:
    """Tests for JBrowse URL generation."""

    @patch('cgd.api.services.blast_service.settings')
    @patch('cgd.api.services.blast_service.get_all_blast_organisms')
    def test_generates_url_for_known_organism(
        self, mock_get_orgs, mock_settings
    ):
        """Should generate JBrowse URL for known organism."""
        mock_settings.jbrowse_base_url = "http://test.jbrowse.org/index.html"
        mock_settings.jbrowse_flank = 1000
        mock_settings.blast_clade_conf = None
        mock_get_orgs.return_value = {
            "C_albicans_SC5314_A22": {
                "full_name": "Candida albicans SC5314 (Assembly 22)",
                "jbrowse_data": "cgd_data/C_albicans_SC5314",
            }
        }

        url = _generate_jbrowse_url(
            organism_tag="C_albicans_SC5314_A22",
            chromosome="Ca22chr1A_C_albicans_SC5314",
            start=10000,
            end=11000,
        )

        assert url is not None
        assert "test.jbrowse.org" in url
        assert "cgd_data" in url
        assert "Ca22chr1A_C_albicans_SC5314" in url

    @patch('cgd.api.services.blast_service.settings')
    @patch('cgd.api.services.blast_service.get_all_blast_organisms')
    def test_returns_none_for_unknown_organism(
        self, mock_get_orgs, mock_settings
    ):
        """Should return None for unknown organism."""
        mock_settings.blast_clade_conf = None
        mock_get_orgs.return_value = {}

        url = _generate_jbrowse_url(
            organism_tag="Unknown_organism",
            chromosome="chr1",
            start=1000,
            end=2000,
        )

        assert url is None

    @patch('cgd.api.services.blast_service.settings')
    @patch('cgd.api.services.blast_service.get_all_blast_organisms')
    def test_returns_none_for_organism_without_jbrowse(
        self, mock_get_orgs, mock_settings
    ):
        """Should return None for organism without JBrowse data."""
        mock_settings.blast_clade_conf = None
        mock_get_orgs.return_value = {
            "S_cerevisiae": {
                "full_name": "S. cerevisiae",
                "jbrowse_data": None,  # No JBrowse
            }
        }

        url = _generate_jbrowse_url(
            organism_tag="S_cerevisiae",
            chromosome="chr1",
            start=1000,
            end=2000,
        )

        assert url is None


class TestOrf19Mapping:
    """Tests for Assembly 22 to orf19 ID mapping."""

    def test_returns_none_for_non_a22_organism(self):
        """Should return None for non-A22 organisms."""
        mock_db = MagicMock()

        result = _map_to_orf19_id(
            db=mock_db,
            feature_name="CAGL0A01234g",
            organism_tag="C_glabrata_CBS138"
        )

        assert result is None
        mock_db.query.assert_not_called()

    def test_returns_none_when_feature_not_found(self):
        """Should return None when feature is not found."""
        mock_db = MagicMock()
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = None
        mock_db.query.return_value = mock_query

        result = _map_to_orf19_id(
            db=mock_db,
            feature_name="C1_00010W_A",
            organism_tag="C_albicans_SC5314_A22"
        )

        assert result is None


class TestBlastOrganismConfig:
    """Tests for organism configuration."""

    def test_blast_organisms_contains_c_albicans(self):
        """BLAST_ORGANISMS should contain C. albicans entries."""
        assert "C_albicans_SC5314_A22" in BLAST_ORGANISMS
        assert "C_albicans_SC5314_A21" in BLAST_ORGANISMS

    def test_c_albicans_uses_genetic_code_12(self):
        """C. albicans should use genetic code 12 (CTG clade)."""
        config = BLAST_ORGANISMS["C_albicans_SC5314_A22"]
        assert config["trans_table"] == 12

    def test_c_glabrata_uses_genetic_code_1(self):
        """C. glabrata should use standard genetic code (1)."""
        config = BLAST_ORGANISMS["C_glabrata_CBS138"]
        assert config["trans_table"] == 1

    def test_get_organism_for_database(self):
        """Should return organism config for database name."""
        # New naming convention
        config = get_organism_for_database("default_genomic_C_albicans_SC5314_A22")
        assert config is not None
        assert config["tag"] == "C_albicans_SC5314_A22"

        # Non-default prefix
        config = get_organism_for_database("genomic_C_albicans_SC5314_A21")
        assert config is not None
        assert config["tag"] == "C_albicans_SC5314_A21"

    def test_extract_organism_tag_from_database(self):
        """Should extract organism tag from database name."""
        # New naming convention
        tag = extract_organism_tag_from_database("default_genomic_C_albicans_SC5314_A22")
        assert tag == "C_albicans_SC5314_A22"

        tag = extract_organism_tag_from_database("default_protein_C_albicans_SC5314_A22")
        assert tag == "C_albicans_SC5314_A22"

        # Non-default prefix
        tag = extract_organism_tag_from_database("genomic_C_albicans_SC5314_A21")
        assert tag == "C_albicans_SC5314_A21"

        # Legacy naming still works
        tag = extract_organism_tag_from_database("C_glabrata_CBS138_protein")
        assert tag == "C_glabrata_CBS138"


class TestBlastTasks:
    """Tests for BLAST task configuration."""

    def test_blastn_has_multiple_tasks(self):
        """BLASTN should have multiple task options."""
        tasks = BLAST_TASKS.get("blastn", [])
        assert len(tasks) >= 4
        task_names = [t["name"] for t in tasks]
        assert "megablast" in task_names
        assert "blastn-short" in task_names

    def test_blastp_has_multiple_tasks(self):
        """BLASTP should have multiple task options."""
        tasks = BLAST_TASKS.get("blastp", [])
        assert len(tasks) >= 2
        task_names = [t["name"] for t in tasks]
        assert "blastp" in task_names
        assert "blastp-short" in task_names

    def test_get_tasks_for_program(self):
        """Should return task list for program."""
        tasks = get_tasks_for_program(BlastProgram.BLASTN)
        assert len(tasks) >= 1
        assert all(hasattr(t, "name") for t in tasks)


class TestGeneticCodes:
    """Tests for genetic code configuration."""

    def test_genetic_codes_contains_standard(self):
        """Should contain standard genetic code."""
        assert 1 in GENETIC_CODES
        assert GENETIC_CODES[1]["name"] == "Standard"

    def test_genetic_codes_contains_yeast_nuclear(self):
        """Should contain yeast nuclear code (CTG clade)."""
        assert 12 in GENETIC_CODES
        assert "Yeast Nuclear" in GENETIC_CODES[12]["name"]

    def test_get_genetic_codes(self):
        """Should return list of genetic code info."""
        codes = get_genetic_codes()
        assert len(codes) > 0
        assert all(hasattr(c, "code") for c in codes)


class TestDownloadGeneration:
    """Tests for BLAST result download generation."""

    @pytest.fixture
    def sample_blast_result(self):
        """Create sample BLAST result for testing."""
        hsp = BlastHsp(
            hsp_num=1,
            bit_score=100.0,
            score=200,
            evalue=1e-20,
            query_start=1,
            query_end=100,
            hit_start=1,
            hit_end=100,
            query_frame=None,
            hit_frame=None,
            identity=95,
            positive=None,
            gaps=2,
            align_len=100,
            query_seq="ATGC" * 25,
            hit_seq="ATGC" * 25,
            midline="||||" * 25,
            percent_identity=95.0,
            percent_positive=None,
        )

        hit = BlastHit(
            num=1,
            id="Ca22chr1A_C_albicans_SC5314",
            accession="Ca22chr1A",
            description="Candida albicans SC5314 chromosome 1A",
            length=1000000,
            hsps=[hsp],
            best_evalue=1e-20,
            best_bit_score=100.0,
            total_score=200,
            query_cover=100.0,
            locus_link="/locus/orf19.1",
            jbrowse_url="http://jbrowse.test/",
            organism_name="Candida albicans SC5314",
            organism_tag="C_albicans_SC5314_A22",
            orf19_id="orf19.1234",
        )

        return BlastSearchResult(
            query_id="Query_1",
            query_length=100,
            query_def="Test query",
            database="default_genomic_C_albicans_SC5314_A22",
            database_length=14000000,
            database_sequences=8,
            program="blastn",
            version="BLAST+ 2.12.0",
            parameters={},
            hits=[hit],
            search_time=0.5,
            warnings=[],
        )

    def test_generate_fasta_download(self, sample_blast_result):
        """Should generate valid FASTA download."""
        download = generate_fasta_download(sample_blast_result)

        assert download.format == DownloadFormat.FASTA
        assert download.content_type == "text/plain"
        assert download.filename.endswith(".fasta")
        assert ">" in download.content
        assert "Candida albicans" in download.content

    def test_generate_tab_download(self, sample_blast_result):
        """Should generate valid tab-delimited download."""
        download = generate_tab_download(sample_blast_result)

        assert download.format == DownloadFormat.TAB
        assert download.content_type == "text/tab-separated-values"
        assert download.filename.endswith(".tsv")
        assert "Query" in download.content
        assert "\t" in download.content
        # Check header fields
        assert "Identity%" in download.content
        assert "E-value" in download.content
        assert "orf19_ID" in download.content

    def test_generate_raw_download(self, sample_blast_result):
        """Should generate valid raw text download."""
        download = generate_raw_download(sample_blast_result)

        assert download.format == DownloadFormat.RAW
        assert download.content_type == "text/plain"
        assert download.filename.endswith(".txt")
        assert "BLAST" in download.content
        assert "Database:" in download.content

    def test_format_blast_results_text(self, sample_blast_result):
        """Should format BLAST results as text."""
        text = format_blast_results_text(sample_blast_result)

        assert "BLAST blastn" in text
        assert "Database:" in text
        assert "Query:" in text
        assert "Score" in text
        assert "E-value" in text


class TestBlastCladeConf:
    """Tests for blast_clade.conf parsing."""

    def test_load_nonexistent_file_returns_empty(self):
        """Should return empty dict for nonexistent file."""
        result = load_blast_clade_conf("/nonexistent/path/blast_clade.conf")
        assert result == {}


class TestGetBlastOrganisms:
    """Tests for get_blast_organisms function."""

    def test_returns_list_of_organism_configs(self):
        """Should return list of BlastOrganismConfig objects."""
        organisms = get_blast_organisms()

        assert len(organisms) > 0
        for org in organisms:
            assert hasattr(org, "tag")
            assert hasattr(org, "full_name")
            assert hasattr(org, "trans_table")
            assert hasattr(org, "seq_sets")

    def test_includes_c_albicans(self):
        """Should include C. albicans organisms."""
        organisms = get_blast_organisms()
        tags = [o.tag for o in organisms]

        assert "C_albicans_SC5314_A22" in tags
        assert "C_albicans_SC5314_A21" in tags


class TestBlastSearchRequestValidation:
    """Tests for BlastSearchRequest model validation."""

    def test_accepts_single_database(self):
        """Should accept single database specification."""
        request = BlastSearchRequest(
            sequence="ATGCATGCATGC",
            program=BlastProgram.BLASTN,
            database=BlastDatabase.CA22_GENOME,
        )
        assert request.database == BlastDatabase.CA22_GENOME
        assert request.databases is None

    def test_accepts_multiple_databases(self):
        """Should accept multiple databases specification."""
        request = BlastSearchRequest(
            sequence="ATGCATGCATGC",
            program=BlastProgram.BLASTN,
            databases=["default_genomic_C_albicans_SC5314_A22", "genomic_C_albicans_SC5314_A21"],
        )
        assert len(request.databases) == 2

    def test_accepts_task_specification(self):
        """Should accept task specification."""
        request = BlastSearchRequest(
            sequence="ATGCATGCATGC",
            program=BlastProgram.BLASTN,
            database=BlastDatabase.CA22_GENOME,
            task=BlastTask.DC_MEGABLAST,
        )
        assert request.task == BlastTask.DC_MEGABLAST

    def test_accepts_genetic_codes(self):
        """Should accept genetic code specifications."""
        request = BlastSearchRequest(
            sequence="ATGCATGCATGC",
            program=BlastProgram.BLASTX,
            database=BlastDatabase.CA22_PROTEIN,
            query_gencode=12,
        )
        assert request.query_gencode == 12

    def test_accepts_nucleotide_scoring(self):
        """Should accept nucleotide match/mismatch scoring."""
        request = BlastSearchRequest(
            sequence="ATGCATGCATGC",
            program=BlastProgram.BLASTN,
            database=BlastDatabase.CA22_GENOME,
            reward=1,
            penalty=-4,
        )
        assert request.reward == 1
        assert request.penalty == -4

    def test_accepts_ungapped_flag(self):
        """Should accept ungapped alignment flag."""
        request = BlastSearchRequest(
            sequence="ATGCATGCATGC",
            program=BlastProgram.BLASTN,
            database=BlastDatabase.CA22_GENOME,
            ungapped=True,
        )
        assert request.ungapped is True
