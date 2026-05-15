# CGD Dataset Import Pipeline

Scripts for importing RNA-seq expression datasets into CGD.

## Overview

This pipeline automates the process of:
1. Downloading FASTQ files from SRA/GEO
2. Aligning reads to reference genomes
3. Generating BigWig coverage files
4. Creating configuration entries for `expression_service.py`

## Requirements

### Software Dependencies
- **SRA Toolkit** (prefetch, fasterq-dump)
- **NCBI E-utilities** (esearch, efetch, elink) - for BioProject/GEO queries
- **HISAT2** - read alignment
- **samtools** - BAM processing
- **deeptools** (bamCoverage) - BigWig generation
- **Python 3.9+**

### Installation (Ubuntu/Debian)
```bash
# SRA Toolkit
wget https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-ubuntu64.tar.gz
tar -xzf sratoolkit.current-ubuntu64.tar.gz
export PATH=$PATH:$PWD/sratoolkit.*/bin

# NCBI E-utilities
sh -c "$(curl -fsSL https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect/install-edirect.sh)"

# HISAT2, samtools, deeptools
conda install -c bioconda hisat2 samtools deeptools
# or
apt-get install hisat2 samtools
pip install deeptools
```

### HISAT2 Genome Indexes
Before running the pipeline, ensure HISAT2 indexes exist for each species:
```bash
# Example for C. auris
hisat2-build /data/genomes/C_auris_B8441/C_auris_B8441.fa \
    /data/genomes/C_auris_B8441/hisat2_index/C_auris_B8441
```

## Quick Start

### Full Pipeline (Download → Align → BigWig)
```bash
./run_pipeline.sh C_auris_B8441 PRJNA1086003 Wang_2024 --pmid 39455573
```

### Individual Steps
```bash
# Step 1: Download FASTQ from SRA
./01_download_sra.sh PRJNA1086003 /data/tmp/fastq/Wang_2024

# Step 2: Align reads
./02_align_reads.sh C_auris_B8441 /data/tmp/fastq/Wang_2024 SRR12345678

# Step 3: Generate BigWig
./03_generate_bigwig.sh /data/aligned/SRR12345678.sorted.bam

# Step 4: Generate config
python 04_generate_config.py --species C_auris_B8441 --study Wang_2024 \
    --bigwig-dir /data/HTS/C_auris_B8441/bam/Wang_2024
```

## Scripts

| Script | Description |
|--------|-------------|
| `run_pipeline.sh` | Master script - runs complete pipeline |
| `01_download_sra.sh` | Download FASTQ from SRA/GEO |
| `02_align_reads.sh` | Align reads with HISAT2 |
| `03_generate_bigwig.sh` | Generate BigWig from BAM |
| `04_generate_config.py` | Generate expression_service.py config |
| `config.yaml` | Species genome paths and settings |

## Supported Species

| Species ID | Display Name |
|------------|--------------|
| `C_albicans_SC5314` | *Candida albicans* SC5314 |
| `C_auris_B8441` | *Candida auris* B8441 |
| `C_glabrata_CBS138` | *Candida glabrata* CBS138 |
| `C_dubliniensis_CD36` | *Candida dubliniensis* CD36 |
| `C_parapsilosis_CDC317` | *Candida parapsilosis* CDC317 |
| `C_tropicalis_MYA3404` | *Candida tropicalis* MYA-3404 |

## Output Structure

```
/data/HTS/{species}/bam/{study_name}/
├── SRR12345678.bw           # BigWig coverage file
├── SRR12345678.library_size.txt  # Total mapped reads
├── SRR12345679.bw
└── ...
```

## After Pipeline Completion

### 1. Add to expression_service.py
Copy the generated config to `cgd/api/services/expression_service.py`:
```python
EXPRESSION_STUDIES = {
    "C_auris_B8441": {
        # ... existing studies ...
        "Wang_2024": {
            "category": "Gene Expression",
            "pmid": "39455573",
            # ... generated config ...
        },
    },
}
```

### 2. Add Library Sizes
Add library sizes to `LIBRARY_SIZES` dict in expression_service.py:
```python
LIBRARY_SIZES = {
    "C_auris_B8441": {
        "Wang_2024": {
            "SRR12345678": 15.5,  # millions of reads
            "SRR12345679": 12.3,
        },
    },
}
```

### 3. Rebuild Expression Cache
```bash
python scripts/build_expression_cache.py --organism C_auris_B8441
```

### 4. Add JBrowse2 Tracks (Optional)
Add track configuration to JBrowse2 config for visualization.

## Troubleshooting

### Download Fails
- Check NCBI API rate limits
- Verify accession ID is correct
- Try using `ffq` as alternative: `pip install ffq`

### Low Alignment Rate (<50%)
- Check if correct species/genome is selected
- Verify FASTQ quality with FastQC
- Try different aligner parameters

### BigWig Generation Fails
- Ensure BAM is sorted and indexed
- Check available disk space
- Verify deeptools installation

## Batch Processing

For multiple datasets, create a batch file:
```bash
# datasets.txt
C_auris_B8441 PRJNA1086003 Wang_2024 --pmid 39455573
C_auris_B8441 PRJNA477447 Kean_2018 --pmid 29997121
C_albicans_SC5314 GSE245114 Henry_2024 --pmid 38380913

# Run all
while read line; do
    ./run_pipeline.sh $line
done < datasets.txt
```

## See Also

- [EXPRESSION_DATA_INVENTORY.md](../../docs/EXPRESSION_DATA_INVENTORY.md) - Dataset catalog
- [expression_service.py](../../cgd/api/services/expression_service.py) - Expression API
- [build_expression_cache.py](../build_expression_cache.py) - Cache builder
