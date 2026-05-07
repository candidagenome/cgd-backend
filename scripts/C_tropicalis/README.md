# C. tropicalis MYA-3404 Data Loading Scripts

Scripts for loading *Candida tropicalis* MYA-3404 data into the CGD database.

## Prerequisites

- Python 3.9+
- Database connection configured via `DATABASE_URL` environment variable
- Python environment with cgd-backend dependencies installed
- Data files prepared on the server

## Data Files Needed

```
/home/ec2-user/
├── Ctrop_liftover3_sorted.gff                      # Gene annotations (GFF)
├── GCA_013177555.1_ASM1317755v1_genomic.fna        # Genomic FASTA (chromosomes)
├── C_tropicalis_MYA-3404_proteins.fasta            # Protein sequences
├── ctrop_ortholog_descriptions.tsv                 # Generated descriptions
├── orthologs/
│   ├── reciprocal_best_hits.txt                    # C. albicans orthologs
│   ├── reciprocal_best_hits_caur.txt               # C. auris orthologs
│   ├── reciprocal_best_hits_cdub.txt               # C. dubliniensis orthologs
│   ├── reciprocal_best_hits_cgla.txt               # C. glabrata orthologs
│   └── reciprocal_best_hits_cpar.txt               # C. parapsilosis orthologs
└── iprscan_results/
    └── all_results.tsv                             # InterProScan output (merged)
```

## Loading Order

Scripts must be run in this order due to database dependencies:

### 1. Create Organism Entry

```bash
python scripts/C_tropicalis/load_organism.py [--dry-run]
```

Creates organism and genome version for C. tropicalis MYA-3404.

### 2. Load Genes and Protein Sequences

```bash
python scripts/C_tropicalis/load_genes_and_sequences.py \
    --gff /home/ec2-user/Ctrop_liftover3_sorted.gff \
    --proteins /home/ec2-user/C_tropicalis_MYA-3404_proteins.fasta \
    [--dry-run]
```

Loads gene features from GFF and protein sequences from FASTA.

### 3. Load Feature Coordinates

```bash
python scripts/C_tropicalis/load_coordinates.py \
    --gff /home/ec2-user/Ctrop_liftover3_sorted.gff \
    --genomic /home/ec2-user/GCA_013177555.1_ASM1317755v1_genomic.fna \
    [--dry-run] [--skip-subfeatures]
```

Creates chromosome/scaffold features and loads chromosomal coordinates into `feat_location`.

### 4. Load Genomic DNA Sequences

```bash
python scripts/C_tropicalis/load_genomic_sequences.py \
    --gff /home/ec2-user/Ctrop_liftover3_sorted.gff \
    --genomic /home/ec2-user/GCA_013177555.1_ASM1317755v1_genomic.fna \
    [--dry-run]
```

Extracts and loads genomic DNA sequences (with introns) for each gene.

### 5. Load CDS and Intron Features

```bash
python scripts/C_tropicalis/load_cds_and_introns.py \
    --gff /home/ec2-user/Ctrop_liftover3_sorted.gff \
    [--dry-run]
```

Creates CDS and intron features with:
- Feature records (`feature_type='CDS'` or `'intron'`)
- `FEAT_RELATIONSHIP` linking to parent gene (`relationship_type='part of'`, `rank=2`)
- `FEAT_LOCATION` with chromosomal coordinates

### 6. Load Gene Descriptions

```bash
python scripts/C_tropicalis/load_descriptions.py \
    --descriptions /home/ec2-user/ctrop_ortholog_descriptions.tsv \
    [--dry-run]
```

Loads gene descriptions derived from C. albicans ortholog data into `feature.headline`.

### 7. Load Orthologs

```bash
python scripts/C_tropicalis/load_orthologs.py \
    --orthologs-dir /home/ec2-user/orthologs \
    [--dry-run]
```

Loads ortholog relationships (BLAST reciprocal best hits) to 5 Candida species via `homology_group` and `feat_homology` tables.

### 8. Load Protein Domains

```bash
python scripts/C_tropicalis/load_protein_domains.py \
    --tsv /home/ec2-user/iprscan_results/all_results.tsv \
    [--dry-run]
```

Loads protein domains from InterProScan TSV into `dbxref` and `dbxref_feat` tables.

### 9. Load GO Annotations

```bash
python scripts/C_tropicalis/load_go_annotations.py \
    --tsv /home/ec2-user/iprscan_results/all_results.tsv \
    [--dry-run]
```

Loads GO annotations (IEA evidence) from InterProScan into `go_annotation` table.

## Utility Scripts

### generate_ortholog_descriptions.py

Generates gene descriptions based on C. albicans ortholog data.

```bash
python scripts/C_tropicalis/generate_ortholog_descriptions.py \
    --orthologs /home/ec2-user/orthologs/reciprocal_best_hits.txt \
    --output /home/ec2-user/ctrop_ortholog_descriptions.tsv
```

### merge_iprscan_results.py

Merges individual InterProScan TSV results into a single file, correcting protein IDs from filenames.

```bash
python scripts/C_tropicalis/merge_iprscan_results.py \
    --input-dir /home/ec2-user/iprscan_results/ \
    --output /home/ec2-user/iprscan_results/all_results.tsv
```

### iprscan_submit.py

Submits protein sequences to EBI InterProScan REST API for analysis.

```bash
python scripts/C_tropicalis/iprscan_submit.py \
    --fasta /home/ec2-user/C_tropicalis_MYA-3404_proteins.fasta \
    --output-dir /home/ec2-user/iprscan_results/
```

### cleanup_features.py

Utility script to delete C. tropicalis features before reloading.

```bash
python scripts/C_tropicalis/cleanup_features.py [--dry-run]
```

## Data Summary

**Organism:** Candida tropicalis MYA-3404
**Abbreviation:** C_tropicalis
**Taxon ID:** 294747

After loading, the following data should be present:

| Data Type | Count | Data Source |
|-----------|------:|-------------|
| **Features** | | |
| ORF Features | 6,678 | GFF file (Ctrop_liftover3_sorted.gff) |
| Contig/Scaffold Features | 7 | [NCBI Assembly GCA_013177555.1](https://www.ncbi.nlm.nih.gov/assembly/GCA_013177555.1) |
| CDS Features | 6,655 | GFF file (CDS entries) |
| Intron Features | 163 | Calculated from CDS coordinates in GFF |
| **Sequences** | | |
| Protein Sequences | 6,218 | Protein FASTA (C_tropicalis_MYA-3404_proteins.fasta) |
| Genomic DNA Sequences (genes) | 6,678 | Extracted from Genomic FASTA using GFF coordinates |
| Chromosome Sequences | 7 | [NCBI Assembly GCA_013177555.1](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/013/177/555/GCA_013177555.1_ASM1317755v1/) |
| **Coordinates** | | |
| Feature Locations (ORFs) | 6,678 | GFF file coordinates |
| Feature Locations (CDS) | 6,655 | GFF file coordinates |
| Feature Locations (introns) | 163 | Calculated from CDS coordinates |
| Feature Relationships | 6,818 | Generated during CDS/intron loading |
| **Annotations** | | |
| Gene Descriptions | 5,246 | C. albicans ortholog data (reciprocal best hits) |
| Ortholog Entries | 23,167 | BLAST reciprocal best hits (5 species) |
| Homology Groups | 23,167 | BLAST reciprocal best hits |
| Domain Annotations | 57,607 | InterProScan (all_results.tsv) |
| GO Annotations | 20,308 | InterProScan (all_results.tsv) - IEA evidence |

### Download URLs

- **Assembly page:** https://www.ncbi.nlm.nih.gov/assembly/GCA_013177555.1
- **FTP directory:** https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/013/177/555/GCA_013177555.1_ASM1317755v1/
- **Genomic FASTA:** https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/013/177/555/GCA_013177555.1_ASM1317755v1/GCA_013177555.1_ASM1317755v1_genomic.fna.gz

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Database connection URL | Required |
| `DB_SCHEMA` | Database schema name | `MULTI` |
| `ADMIN_USER` | User for `created_by` field | `CGDADMIN` |

## JBrowse2 Protein Domain Visualization

After loading protein domain data into the database, set up JBrowse2 to display domain tracks on the Protein tab.

### Prerequisites

- InterProScan TSV results loaded into `protein_detail` table (Step 8 above)
- Protein FASTA file
- Access to the frontend server (`cgd-frontend-dev` or production)
- Tools: `samtools`, `bgzip`, `tabix`

### Data Files Needed

On the frontend server, ensure these directories exist:
```
/data/jbrowse2/protein_data/     # Protein FASTA and domain GFF files
/data/domain/C_tropicalis_MYA3404/  # Source InterProScan data
```

### Setup Steps

#### 1. Copy Data Files to Frontend Server

**Development (separate servers):** Copy from backend to frontend:
```bash
# On cgd-backend-dev
scp -i /path/to/cgd-dev.pem \
    /home/ec2-user/iprscan_results/all_results.tsv \
    /home/ec2-user/C_tropicalis/orthologs/C_tropicalis_MYA-3404_proteins.fasta \
    ec2-user@cgd-frontend-dev:/data/domain/C_tropicalis_MYA3404/
```

**Production (same server):** Simply copy/move files:
```bash
cp /home/ec2-user/iprscan_results/all_results.tsv /data/domain/C_tropicalis_MYA3404/
cp /home/ec2-user/C_tropicalis_MYA-3404_proteins.fasta /data/domain/C_tropicalis_MYA3404/
```

#### 2. Run JBrowse2 Setup Script

On the frontend server:
```bash
cd /data/jbrowse2/protein_data
bash /path/to/setup_jbrowse_proteins.sh
```

Or manually create the files:

```bash
ORGANISM="C_tropicalis"
DOMAIN_DIR="/data/domain/C_tropicalis_MYA3404"
DATA_DIR="/data/jbrowse2/protein_data"

# Create protein FASTA symlink and index
ln -sf ${DOMAIN_DIR}/C_tropicalis_MYA-3404_proteins.fasta ${DATA_DIR}/${ORGANISM}_proteins.fasta
samtools faidx ${DATA_DIR}/${ORGANISM}_proteins.fasta

# Convert InterProScan TSV to GFF for each database
python interproscan_to_gff.py \
    --tsv ${DOMAIN_DIR}/all_results.tsv \
    --output /tmp/${ORGANISM}_domains.gff

# Split by database, sort, compress, and index
for DB in Pfam PANTHER SUPERFAMILY CATH SMART ProSiteProfiles CDD PRINTS; do
    grep -P "\\t${DB}\\t" /tmp/${ORGANISM}_domains.gff | \
        sort -k1,1 -k4,4n > /tmp/${ORGANISM}_${DB}_sorted.gff
    bgzip -c /tmp/${ORGANISM}_${DB}_sorted.gff > ${DATA_DIR}/${ORGANISM}_${DB}.gff.gz
    tabix -p gff ${DATA_DIR}/${ORGANISM}_${DB}.gff.gz
done
```

#### 3. Update JBrowse2 Configuration

Edit `/data/jbrowse2/config.json` to add the protein assembly and tracks:

```json
{
  "assemblies": [
    {
      "name": "C_tropicalis_prot",
      "displayName": "C. tropicalis Proteins",
      "sequence": {
        "type": "ReferenceSequenceTrack",
        "trackId": "C_tropicalis_prot-ReferenceSequenceTrack",
        "adapter": {
          "type": "IndexedFastaAdapter",
          "fastaLocation": { "uri": "protein_data/C_tropicalis_proteins.fasta" },
          "faiLocation": { "uri": "protein_data/C_tropicalis_proteins.fasta.fai" }
        }
      }
    }
  ],
  "tracks": [
    {
      "type": "FeatureTrack",
      "trackId": "C_tropicalis_Pfam",
      "name": "Pfam",
      "assemblyNames": ["C_tropicalis_prot"],
      "adapter": {
        "type": "Gff3TabixAdapter",
        "gffGzLocation": { "uri": "protein_data/C_tropicalis_Pfam.gff.gz" },
        "index": { "location": { "uri": "protein_data/C_tropicalis_Pfam.gff.gz.tbi" } }
      }
    }
    // ... repeat for PANTHER, SUPERFAMILY, CATH, SMART, ProSiteProfiles, CDD, PRINTS
  ]
}
```

#### 4. Update Frontend Configuration

In `cgd-frontend/src/components/locus/ProteinDetails.jsx`, add the organism to `proteinAssemblyConfig`:

```javascript
const proteinAssemblyConfig = {
  // ... existing organisms ...
  'C_tropicalis': {
    assembly: 'C_tropicalis_prot',
  },
};
```

#### 5. Rebuild Frontend

```bash
cd cgd-frontend
npm run build
# Deploy to server
```

### Files Created

After setup, these files should exist in `/data/jbrowse2/protein_data/`:

```
C_tropicalis_proteins.fasta      # Symlink to protein FASTA
C_tropicalis_proteins.fasta.fai  # FASTA index
C_tropicalis_Pfam.gff.gz         # Domain tracks (bgzipped)
C_tropicalis_Pfam.gff.gz.tbi     # Tabix index
C_tropicalis_PANTHER.gff.gz
C_tropicalis_PANTHER.gff.gz.tbi
C_tropicalis_SUPERFAMILY.gff.gz
C_tropicalis_SUPERFAMILY.gff.gz.tbi
C_tropicalis_CATH.gff.gz
C_tropicalis_CATH.gff.gz.tbi
C_tropicalis_SMART.gff.gz
C_tropicalis_SMART.gff.gz.tbi
C_tropicalis_ProSiteProfiles.gff.gz
C_tropicalis_ProSiteProfiles.gff.gz.tbi
C_tropicalis_CDD.gff.gz
C_tropicalis_CDD.gff.gz.tbi
C_tropicalis_PRINTS.gff.gz
C_tropicalis_PRINTS.gff.gz.tbi
```

### Troubleshooting

- **"Could not resolve identifiers" error**: Track doesn't exist in JBrowse config. Check that the track ID in `config.json` matches the frontend's `domainTrackTypes` array.
- **No JBrowse viewer showing**: Ensure `protein_length` is available (calculated from sequence if not in `protein_info`).
- **Empty tracks**: Verify the GFF files have data: `zcat C_tropicalis_Pfam.gff.gz | head`

## Notes

- All scripts support `--dry-run` flag to preview changes without modifying the database
- Scripts are idempotent and can be re-run safely
- Each script checks for existing data before inserting
- Progress is logged and committed in batches of 500 records
