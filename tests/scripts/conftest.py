"""
Pytest fixtures for CGD script tests.

Provides common fixtures for testing CGD backend scripts including:
- Temporary directories and files
- Mock database sessions
- Sample test data
- Environment configuration
"""
import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_file(temp_dir):
    """Create a temporary file."""
    def _create_file(name: str, content: str = "") -> Path:
        file_path = temp_dir / name
        file_path.write_text(content)
        return file_path
    return _create_file


@pytest.fixture
def mock_db_session():
    """Create a mock database session for testing."""
    session = MagicMock()
    session.execute = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    return session


@pytest.fixture
def mock_session_local(mock_db_session):
    """Mock the SessionLocal context manager."""
    with patch('cgd.db.engine.SessionLocal') as mock:
        mock.return_value.__enter__ = MagicMock(return_value=mock_db_session)
        mock.return_value.__exit__ = MagicMock(return_value=False)
        yield mock


@pytest.fixture
def sample_fasta_content():
    """Sample FASTA content for testing."""
    return """>seq1 Sample sequence 1
ATGCGATCGATCGATCGATCG
>seq2 Sample sequence 2
GCTAGCTAGCTAGCTAGCTA
>seq3 Sample sequence 3
TTTTAAAACCCCGGGG
"""


@pytest.fixture
def sample_gff_content():
    """Sample GFF3 content for testing."""
    return """##gff-version 3
##sequence-region Ca22chr1 1 3188363
Ca22chr1\tCGD\tgene\t1000\t2000\t.\t+\t.\tID=orf19.1;Name=ACT1
Ca22chr1\tCGD\tCDS\t1000\t2000\t.\t+\t0\tID=orf19.1_CDS;Parent=orf19.1
Ca22chr1\tCGD\tgene\t3000\t4000\t.\t-\t.\tID=orf19.2;Name=TUB1
Ca22chr1\tCGD\tCDS\t3000\t4000\t.\t-\t0\tID=orf19.2_CDS;Parent=orf19.2
"""


@pytest.fixture
def sample_tab_content():
    """Sample tab-delimited content for testing."""
    return """feature_name\tgene_name\tdescription
orf19.1\tACT1\tActin
orf19.2\tTUB1\tTubulin
orf19.3\tCDC42\tGTPase
"""


@pytest.fixture
def env_vars():
    """Set up environment variables for testing."""
    original_env = os.environ.copy()
    test_env = {
        'DB_SCHEMA': 'TEST',
        'DATABASE_URL': 'sqlite:///:memory:',
        'DATA_DIR': '/tmp/test_data',
        'LOG_DIR': '/tmp/test_logs',
        'TMP_DIR': '/tmp/test_tmp',
    }
    os.environ.update(test_env)
    yield test_env
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def capture_stdout(capsys):
    """Fixture to capture stdout for testing print output."""
    return capsys


@pytest.fixture
def sample_go_obo_content():
    """Sample GO OBO content for testing."""
    return """format-version: 1.2
ontology: go

[Term]
id: GO:0008150
name: biological_process
namespace: biological_process
def: "A biological process represents a specific objective."

[Term]
id: GO:0006412
name: translation
namespace: biological_process
def: "The cellular metabolic process in which a protein is formed."
is_a: GO:0008150 ! biological_process

[Term]
id: GO:0003674
name: molecular_function
namespace: molecular_function
def: "A molecular process that can be carried out by a gene product."
"""


@pytest.fixture
def sample_blast_xml():
    """Sample BLAST XML output for testing."""
    return """<?xml version="1.0"?>
<!DOCTYPE BlastOutput PUBLIC "-//NCBI//NCBI BlastOutput/EN" "http://www.ncbi.nlm.nih.gov/dtd/NCBI_BlastOutput.dtd">
<BlastOutput>
  <BlastOutput_program>blastp</BlastOutput_program>
  <BlastOutput_version>BLASTP 2.12.0+</BlastOutput_version>
  <BlastOutput_iterations>
    <Iteration>
      <Iteration_iter-num>1</Iteration_iter-num>
      <Iteration_query-def>orf19.1</Iteration_query-def>
      <Iteration_query-len>100</Iteration_query-len>
      <Iteration_hits>
        <Hit>
          <Hit_num>1</Hit_num>
          <Hit_id>YAL001C</Hit_id>
          <Hit_def>TFC3</Hit_def>
          <Hit_len>95</Hit_len>
          <Hit_hsps>
            <Hsp>
              <Hsp_bit-score>150.0</Hsp_bit-score>
              <Hsp_score>380</Hsp_score>
              <Hsp_evalue>1e-40</Hsp_evalue>
              <Hsp_identity>80</Hsp_identity>
              <Hsp_positive>85</Hsp_positive>
              <Hsp_align-len>95</Hsp_align-len>
            </Hsp>
          </Hit_hsps>
        </Hit>
      </Iteration_hits>
    </Iteration>
  </BlastOutput_iterations>
</BlastOutput>
"""
