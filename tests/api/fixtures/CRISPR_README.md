# CRISPR Guide Designer Test Suite

This directory contains test fixtures and documentation for validating the CGD CRISPR Guide RNA Designer against established tools like CRISPOR.

## Overview

The CRISPR test suite provides:
- **Unit tests** for core algorithm functions (GC content, PAM finding, etc.)
- **Integration tests** for the full guide design pipeline
- **Validation tests** comparing our results against CRISPOR predictions

## Directory Structure

```
tests/api/
├── test_crispr_service.py      # Main test file
├── fixtures/
│   ├── README.md               # This file
│   ├── crispr_test_genes.json  # Gene sequences and expected guides
│   └── crispr_test_sequences.fasta  # FASTA for CRISPOR submission
```

## Running Tests

```bash
# From cgd-backend directory
source .venv/bin/activate  # or 'venv/bin/activate' on server

# Run all CRISPR tests
pytest tests/api/test_crispr_service.py -v

# Run only unit tests (skip CRISPOR validation)
pytest tests/api/test_crispr_service.py -v -k "not CRISPOR"

# Run with coverage
pytest tests/api/test_crispr_service.py --cov=cgd.api.services.crispr_service
```

## Test Categories

### 1. Helper Function Tests (Unit Tests)

| Test Class | Description | Tests |
|------------|-------------|-------|
| `TestReverseComplement` | DNA reverse complement | 5 |
| `TestGCContent` | GC percentage calculation | 5 |
| `TestPolyT` | Poly-T terminator detection | 4 |
| `TestRestrictionSites` | Restriction enzyme site finding | 3 |
| `TestEfficiencyScore` | Guide efficiency scoring | 3 |

### 2. PAM Site Finding Tests

| Test Class | Description | Tests |
|------------|-------------|-------|
| `TestPAMSiteFinding` | NGG/TTTV PAM detection | 5 |
| `TestTargetRegionFiltering` | 5'/3'/upstream region filtering | 4 |

### 3. Integration Tests

| Test Class | Description | Tests |
|------------|-------------|-------|
| `TestDesignGuidesIntegration` | Full pipeline with mock DB | 3 |

### 4. CRISPOR Validation Tests

| Test Class | Description | Tests |
|------------|-------------|-------|
| `TestCRISPORValidation` | Compare against CRISPOR results | 20 (skipped until populated) |

## Test Genes

We selected 20 well-characterized *C. albicans* genes representing diverse functional categories:

### Virulence & Adhesion (4 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| ALS1 | C1_13700C_A | Agglutinin-like sequence protein |
| ALS3 | C6_01030W_A | Agglutinin-like protein, invasion |
| HWP1 | C1_06250C_A | Hyphal wall protein 1 |
| ECE1 | C3_05610W_A | Candidalysin precursor |

### Secreted Proteases (2 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| SAP1 | C6_02460W_A | Secreted aspartyl protease 1 |
| SAP2 | C6_02480W_A | Secreted aspartyl protease 2 |

### Transcription Factors (4 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| EFG1 | CR_07890W_A | bHLH transcription factor |
| CPH1 | C4_03540C_A | Mating/filamentation regulator |
| WOR1 | C1_11000C_A | White-opaque switching master regulator |
| BCR1 | C3_04800W_A | Biofilm transcription factor |

### Signaling Pathway (4 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| HOG1 | C1_05270W_A | MAP kinase, stress response |
| RAS1 | C2_05700W_A | Ras-family GTPase |
| CDC42 | C5_02460C_A | Rho-type GTPase |
| CEK1 | C2_00410C_A | MAP kinase |

### Housekeeping (2 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| ACT1 | C1_06310W_A | Actin |
| TUB1 | CR_02550C_A | Alpha-tubulin |

### Cell Wall (2 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| PHR1 | C5_01020C_A | pH-responsive glycosidase |
| CHT2 | C1_04170C_A | Chitinase |

### Drug Resistance (2 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| CDR1 | C3_02280C_A | ABC transporter, azole resistance |
| ERG11 | C5_00660C_A | Lanosterol 14-alpha-demethylase |

## Populating CRISPOR Fixtures

The CRISPOR validation tests require expected guide sequences from CRISPOR. Follow these steps to populate them:

### Step 1: Submit Sequences to CRISPOR

1. Go to https://crispor.tefor.net/
2. For each gene, paste the sequence from `crispr_test_sequences.fasta`
3. Settings:
   - **PAM**: NGG (SpCas9)
   - **Genome**: Select "No genome" or paste sequence
4. Click "Submit"
5. Copy the top 5-10 guide sequences (20bp, without PAM)

### Step 2: Update the JSON Fixture

Edit `crispr_test_genes.json` and add guides to `expected_guides_5prime`:

```json
{
  "gene_name": "HOG1",
  "feature_name": "C1_05270W_A",
  "cds_first_500bp": "ATGTCTGCA...",
  "expected_guides_5prime": [
    "ATGTCTGCAGATGGAGAATT",
    "TCTGCAGATGGAGAATTTAC",
    "GCAGATGGAGAATTTACAAG"
  ]
}
```

**Important**: Guide sequences should be:
- Exactly 20bp long
- Without the PAM sequence (no trailing NGG)
- In 5' to 3' orientation

### Step 3: Generate Test Code (Optional)

```bash
python scripts/update_crispr_test_fixtures.py
```

This validates the fixtures and generates Python code for the test file.

### Step 4: Run Validation Tests

```bash
# Remove the skip decorator from TestCRISPORValidation, then:
pytest tests/api/test_crispr_service.py::TestCRISPORValidation -v
```

## What the Validation Tests Check

The CRISPOR validation tests verify that:

1. **Guide Discovery**: Guides found by CRISPOR are also found by our tool
2. **Sequence Accuracy**: Guide sequences match exactly (or reverse complement)
3. **Position Validity**: Guides are in the correct target region (5' first 20%)

**Note**: Our tool may find additional guides not reported by CRISPOR due to different filtering criteria. The tests verify that CRISPOR's guides are a subset of ours.

## Adding New Test Genes

To add a new gene to the test suite:

1. Add the gene to `TEST_GENES` in `scripts/fetch_crispr_test_fixtures.py`:
   ```python
   {"gene_name": "NEW_GENE", "feature_name": "C1_XXXXX_A", "description": "..."},
   ```

2. Run the fetch script:
   ```bash
   python scripts/fetch_crispr_test_fixtures.py
   ```

3. Submit the new sequence to CRISPOR and collect expected guides

4. Update `crispr_test_genes.json` with the expected guides

5. Add the gene to `CRISPR_TEST_GENES` in `test_crispr_service.py`

## Troubleshooting

### Tests fail with "Module not found"
```bash
# Ensure you're in the virtual environment
source .venv/bin/activate  # or venv/bin/activate
```

### CRISPOR validation tests are skipped
The tests are skipped by default until fixtures are populated. After adding expected guides:
1. Remove `@pytest.mark.skip` from `TestCRISPORValidation`
2. Or run with: `pytest -k "CRISPOR" --runskipped`

### Guide not found in our results
If a CRISPOR guide isn't found by our tool:
1. Check if it's in a filtered region (outside 5' 20%)
2. Verify the PAM sequence is valid NGG
3. Check for sequence errors in the fixture

## References

- **CRISPOR**: Concordet & Haeussler, 2018. https://crispor.tefor.net/
- **Rule Set 2**: Doench et al., 2016. Optimized sgRNA design.
- **CFD Score**: Doench et al., 2016. Off-target scoring.

## Maintenance

| Task | Frequency | Script |
|------|-----------|--------|
| Fetch new gene sequences | As needed | `fetch_crispr_test_fixtures.py` |
| Update expected guides | After CRISPOR submission | Manual edit of JSON |
| Validate fixture format | Before commit | `update_crispr_test_fixtures.py` |
