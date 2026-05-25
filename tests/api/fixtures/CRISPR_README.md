# CRISPR Guide Designer Test Suite

This directory contains test fixtures and documentation for validating the CGD CRISPR Guide RNA Designer against CHOPCHOP reference guides.

## Overview

The CRISPR test suite provides:
- **Unit tests** for core algorithm functions (GC content, PAM finding, etc.)
- **Integration tests** for the full guide design pipeline
- **Validation tests** comparing our results against external tool predictions

### External Validation Tool

| Tool | URL | Status |
|------|-----|--------|
| **CHOPCHOP** | https://chopchop.cbu.uib.no/ | Reference source |

## Directory Structure

```
tests/api/
├── test_crispr_service.py      # Main test file
├── fixtures/
│   ├── README.md               # This file
│   ├── crispr_test_genes.json  # Gene sequences and expected guides
│   └── crispr_test_sequences.fasta  # FASTA for CHOPCHOP submission
```

## Running Tests

```bash
# From cgd-backend directory
source .venv/bin/activate  # or 'venv/bin/activate' on server

# Run all CRISPR tests
pytest tests/api/test_crispr_service.py -v

# Run only unit tests (skip CHOPCHOP validation)
pytest tests/api/test_crispr_service.py -v -k "not CHOPCHOP"

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

### 4. CHOPCHOP Validation Tests

| Test Class | Description | Tests |
|------------|-------------|-------|
| `TestCHOPCHOPValidation` | Compare against CHOPCHOP results | 20 |

## Test Genes

We selected 20 well-characterized *C. albicans* genes representing diverse functional categories:

### Virulence & Adhesion (4 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| ALS1 | C6_03700W_A | Agglutinin-like sequence protein |
| ALS3 | CR_07070C_A | Agglutinin-like protein, invasion |
| HWP1 | C4_03570W_A | Hyphal wall protein 1 |
| ECE1 | C4_03470C_A | Candidalysin precursor |

### Secreted Proteases (2 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| SAP1 | C6_03490C_A | Secreted aspartyl protease 1 |
| SAP2 | CR_07800W_A | Secreted aspartyl protease 2 |

### Transcription Factors (4 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| EFG1 | CR_07890W_A | bHLH transcription factor |
| CPH1 | C1_07370C_A | Mating/filamentation regulator |
| WOR1 | C1_10150W_A | White-opaque switching master regulator |
| BCR1 | CR_06440C_A | Biofilm transcription factor |

### Signaling Pathway (4 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| HOG1 | C2_03330C_A | MAP kinase, stress response |
| RAS1 | C2_10210C_A | Ras-family GTPase |
| CDC42 | C1_08450C_A | Rho-type GTPase |
| CEK1 | C4_06480C_A | MAP kinase |

### Housekeeping (2 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| ACT1 | C1_13700W_A | Actin |
| TUB1 | CR_09120C_A | Alpha-tubulin |

### Cell Wall (2 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| PHR1 | C4_04530C_A | pH-responsive glycosidase |
| CHT2 | C5_04130C_A | Chitinase |

### Drug Resistance (2 genes)
| Gene | ORF | Description |
|------|-----|-------------|
| CDR1 | C3_05220W_A | ABC transporter, azole resistance |
| ERG11 | C5_00660C_A | Lanosterol 14-alpha-demethylase |

## Populating Validation Fixtures

The validation tests use expected guide sequences from CHOPCHOP.

1. Go to https://chopchop.cbu.uib.no/
2. Select **"Target"** tab → **"Paste your own sequence"**
3. Paste the sequence from `crispr_test_sequences.fasta`
4. Settings:
   - **In**: Select "Other" or leave default
   - **Using**: SpCas9
   - **For**: Knock-out
5. Click "Find Target Sites"
6. Copy the top 5-10 guide sequences from the results table (20bp, without PAM)

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
pytest tests/api/test_crispr_service.py::TestCHOPCHOPValidation -v
```

## What the Validation Tests Check

The CHOPCHOP validation tests verify that:

1. **Guide Discovery**: Guides found by CHOPCHOP are also found by our tool
2. **Sequence Accuracy**: Guide sequences match exactly (or reverse complement)
3. **Position Validity**: Guides are in the correct target region (5' first 20%)

**Note**: Our tool may find additional guides not reported by CHOPCHOP due to different filtering criteria. The tests verify that CHOPCHOP's guides are a subset of ours.

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

3. Submit the new sequence to CHOPCHOP and collect expected guides

4. Update `crispr_test_genes.json` with the expected guides

5. Add the gene to `CRISPR_TEST_GENES` in `test_crispr_service.py`

## Troubleshooting

### Tests fail with "Module not found"
```bash
# Ensure you're in the virtual environment
source .venv/bin/activate  # or venv/bin/activate
```

### CHOPCHOP validation tests are skipped
The tests are skipped only if fixtures are missing required sequences or expected guides.

### Guide not found in our results
If a CHOPCHOP guide isn't found by our tool:
1. Check if it's in a filtered region (outside 5' 20%)
2. Verify the PAM sequence is valid NGG
3. Check for sequence errors in the fixture

## References

- **CHOPCHOP**: Labun et al., 2019. https://chopchop.cbu.uib.no/
- **Rule Set 2**: Doench et al., 2016. Optimized sgRNA design.
- **CFD Score**: Doench et al., 2016. Off-target scoring.

## Maintenance

| Task | Frequency | Script |
|------|-----------|--------|
| Fetch new gene sequences | As needed | `fetch_crispr_test_fixtures.py` |
| Update expected guides | After CHOPCHOP submission | Manual edit of JSON |
| Validate fixture format | Before commit | `update_crispr_test_fixtures.py` |
