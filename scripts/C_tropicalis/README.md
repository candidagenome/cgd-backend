# C. tropicalis Data Loading Scripts

Scripts for loading C. tropicalis MYA-3404 data into the CGD database.

## Prerequisites

- Database connection configured via environment variables
- Python environment with cgd-backend dependencies installed
- Data files prepared on the server

## Data Files Needed

```
/home/ec2-user/C_tropicalis/
├── Ctrop_liftover3_sorted.gff              # Gene annotations
├── C_tropicalis_MYA-3404_proteins.fasta    # Protein sequences (in orthologs/)
├── ctrop_ortholog_descriptions.tsv         # Generated descriptions
├── orthologs/
│   ├── reciprocal_best_hits.txt            # C. albicans orthologs
│   ├── reciprocal_best_hits_caur.txt       # C. auris orthologs
│   ├── reciprocal_best_hits_cdub.txt       # C. dubliniensis orthologs
│   ├── reciprocal_best_hits_cgla.txt       # C. glabrata orthologs
│   └── reciprocal_best_hits_cpar.txt       # C. parapsilosis orthologs
└── iprscan_results/
    └── all_results.tsv                     # InterProScan output
```

## Loading Order

Scripts must be run in this order due to dependencies:

### 1. Create Organism Entry

```bash
cd /opt/cgd_api
PYTHONPATH=/opt/cgd_api python3 scripts/C_tropicalis/load_organism.py
```

### 2. Load Genes and Sequences

```bash
PYTHONPATH=/opt/cgd_api python3 scripts/C_tropicalis/load_genes_and_sequences.py \
    --gff /home/ec2-user/C_tropicalis/Ctrop_liftover3_sorted.gff \
    --proteins /home/ec2-user/C_tropicalis/orthologs/C_tropicalis_MYA-3404_proteins.fasta
```

### 3. Load Gene Descriptions (from orthologs)

```bash
PYTHONPATH=/opt/cgd_api python3 scripts/C_tropicalis/load_descriptions.py \
    --descriptions /home/ec2-user/C_tropicalis/ctrop_ortholog_descriptions.tsv
```

### 4. Load Orthologs

```bash
PYTHONPATH=/opt/cgd_api python3 scripts/C_tropicalis/load_orthologs.py \
    --orthologs-dir /home/ec2-user/C_tropicalis/orthologs
```

### 5. Load Protein Domains (from InterProScan)

```bash
PYTHONPATH=/opt/cgd_api python3 scripts/C_tropicalis/load_protein_domains.py \
    --tsv /home/ec2-user/C_tropicalis/iprscan_results/all_results.tsv
```

### 6. Load GO Annotations (IEA from InterProScan)

```bash
PYTHONPATH=/opt/cgd_api python3 scripts/C_tropicalis/load_go_annotations.py \
    --tsv /home/ec2-user/C_tropicalis/iprscan_results/all_results.tsv
```

## Dry Run Mode

All scripts support `--dry-run` flag to preview changes without modifying the database:

```bash
PYTHONPATH=/opt/cgd_api python3 scripts/C_tropicalis/load_organism.py --dry-run
```

## Helper Scripts

- `iprscan_submit.py` - Submit proteins to EBI InterProScan REST API
- `generate_ortholog_descriptions.py` - Generate descriptions from C. albicans orthologs

## Notes

- Scripts are idempotent and can be re-run safely
- Each script checks for existing data before inserting
- Progress is logged and committed in batches of 500 records
- Use `--dry-run` to test before actual loading
