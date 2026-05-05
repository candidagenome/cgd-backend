# CGD Expression Data Inventory

## Overview

This document catalogs the expression datasets available at CGD, including RNA-seq, ChIP-seq, and other high-throughput sequencing data. The data is currently accessible through JBrowse as coverage tracks but is not yet tied to gene-level queries.

## Current State

- **Data Storage**: BigWig coverage files in JBrowse (`/data/jbrowse2/`) and `/data/HTS/{species}/bam/`
- **Gene-Level Queries**: Implemented via Expression Tab API endpoints
- **Fold Change Calculation**: Library-size normalized (see Fold Change Calculation section below)
- **Metadata**: Study configurations in `expression_service.py`

## Data Summary by Species

| Species | Datasets | Data Types | Primary Research Areas |
|---------|----------|------------|------------------------|
| *C. albicans* SC5314 | 60+ | RNA-seq, ChIP-seq, DNA-seq | Stress, morphogenesis, biofilm, drug response |
| *C. auris* | 30+ | RNA-seq, ChIP-seq | Drug resistance, virulence, biofilm |
| *C. glabrata* CBS138 | 25+ | RNA-seq, ChIP-seq | Drug resistance, stress, virulence |
| *C. dubliniensis* CD36 | 4 | RNA-seq | Hyphal induction, species comparison |

---

## C. albicans Datasets

### Completed/In JBrowse

| Author | Year | PMID | NCBI ID | Data Type | Experiment | Notes |
|--------|------|------|---------|-----------|------------|-------|
| Bruno et al. | 2010 | 20810668 | - | RNA-seq | Stress responses | Oxidative, nitrosative, pH, cell wall damage, serum |
| Butler et al. | 19465905 | - | DNA-seq | WO-1 v. SC5314 | |
| Desai et al. | 2013 | 23572557 | - | RNA-seq | Biofilms, RHR2 | SC5314, WO-1, biofilm vs planktonic |
| Lohse et al. | 2016 | 26772749 | - | ChIP-seq | Wor1p ChIP-Seq | Control opaque cells |
| Muzzey et al. | 2013 | 24025428 | - | DNA-seq | By chromosome and haplotype | |
| Niemiec et al. | 2017 | 28874114 | - | RNA-seq | Hyphae v yeast by haplotype | |
| Segal et al. | 2018 | 30377286 | - | DNA-seq | Transposon mutagenesis by haplotype | |
| Xie et al. | 2013 | 23555196 | - | RNA-seq | White v opaque by haplotype | |
| Shivarathri et al. | 2019 | 31263212 | PRJNA508646 | RNA-seq | CSP in gcn5 v wt | Histone Acetyltransferase Gcn5 - virulence through multiple pathways |
| Glazier et al. | 2023 | 37737633 | PRJNA997824 | RNA-seq | Rare dominant allele | Rob1 modulates filamentation, biofilm formation, commensalism |
| Rai (EBI) | 2024 | 38905306 | PRJEB50560 | RNA-seq | ZCF15 and ZCF26 OE | Biofilm gene circuitry (RNA-seq data) |
| Zhang et al. | 2024 | - | - | RNA-seq | BWP17, ctm1Δ/Δ, cyc1K79A | Yeast and hyphal-inducing conditions |
| Iracane | 2024 | 38625945 | PRJNA1074533 | RNA-seq | 2024 RNAi paper | Only importing SC5314 data runs; SC5314 is RNAi-inactive |

### Next Priority

| Author | Year | PMID | NCBI ID | Data Type | Experiment | Notes |
|--------|------|------|---------|-----------|------------|-------|
| Tirosh et al. | - | PRJNA79981 | - | RNA-seq | Stress responses | Divergence between induced and constitutive activation |
| Ganser C et al. | 2023 | 38091321 | PRJNA1003046 | RNA-seq | efg1 v brg1 v wt | Filamentation and biofilm formation by phase-separation |
| Brandt et al. | 2023 | 37097196 | PRJNA925798 | RNA-seq | (+/-) diff carbon | High-throughput profiling for C aur but same set of expts on albicans |
| Henry et al. | 2024 | 38380913 | GSE245114 | RNA-seq | (+/-) manganese | Manganese homeostasis modulates virulence and stress tolerance |
| Danhof et al. | 2016 | H27935835 | GSE87832 | RNA-seq | (+/-) carboxylic acids | Robust Extracellular pH Modulation during Growth in Carboxylic Acids |
| Yau KPS | - | 37075064 | - | RNA-seq | WT v rpn4 and (+/-) Fks | Analysis of putative sIS2 plus S3 datasets |
| Jin X | - | 37160123 | - | RNA-seq | (+/-) drug candidate | GEO: GSE209608 |
| Hu L | - | 37173214 | - | RNA-seq | (+/-) drug candidate 2 | GEO under accession code GSE226137 |
| Sharma | - | 37405402 | - | RNA-seq | WT v hgt1 mutants | NCBI-deposited, two different mutants with respective WT strains |

### Lower Priority / Pending

| Author | Year | PMID | Data Type | Experiment | Notes |
|--------|------|------|-----------|------------|-------|
| Shrivastava | - | 37791798 | RNA-seq | (+/-) activation | PMID: 37791798 |
| Zeng | - | 37980562 | RNA-seq | BWP17, ctm1Δ/Δ, cyc1K79A | Yeast and hyphal-inducing conditions |
| Guan | 2023 | 37933972 | RNA-seq | (+/-) rfg1 mutation | Function of Rfg1 in sensing acidic pH |
| van Wijlick | 2023 | 36004328 | RNA-seq | (+/-) irf1 mutation | Functional Portrait of Irf1 (Orf19.217) |
| van Wijlick | 2023 | 36004328 | ChIP-seq | (+/-) irf1 mutation | ChIP-Seq version |
| Ubom | 2021 | 32796481 | RNA-seq | (+/-) yck2 mutation | Carbon Metabolism and Morphogenesis |
| Koch | 2018 | 30463019 | RNA-seq | inhibitor + mutation | Nitric oxide signaling |
| Wartenberg | 2024 | 25474009 | RNA-seq | Microevolution in macrophages | Restores filamentation in nonfilamentous mutant |

### Skipped

| Author | PMID | Reason |
|--------|------|--------|
| Dumeaux et al. | PRJNA842701 | FLC, RAPA, CSP - heterogeneous and adaptive cytoprotective responses |
| Choudhary et al. | PRJNA945876 | RNA-seq + Ribo-seq - genome-wide translational response to fluconazole |

---

## C. auris Datasets

### Completed/In JBrowse

| Author | Year | PMID | NCBI ID | Data Type | Experiment | Notes |
|--------|------|------|---------|-----------|------------|-------|
| Shivarathri et al. | 2021 | 35652307 | PRJNA788930 | RNA-seq | AmpB sens v res | Comparative Transcriptomics - Amphotericin B Resistance |
| Jakab et al. | 2021 | - | PRJNA746543 | RNA-seq | Transcriptional profiling | Candida auris Response to Exogenous Farnesol Exposure |
| Gao J et al. | 2021 | - | PRJNA718866 | RNA-seq | lncRNA DINOR | Virulence factor and global regulator of stress responses |
| Jenull S et al. | 2021 | 33937102 | PRJNA697848 | RNA-seq | Transcriptional Signatures Predict Phenotypic Variations | |
| Simm C et al. | 2022 | 35412372 | PRJNA735943 | RNA-seq | DGE in C. auris | Disruption of Iron Homeostasis and Mitochondrial Metabolism |
| Balla et al. | 2022 | 37532970 | GSE223953 | RNA-seq | (+/-) tyrosol as planktonic | Total transcriptome analysis |
| Biermann et al. | 2022 | 35473297 | PRJNA801628 | RNA-seq | Mrt1 Induction | Transcriptional Response to Mrt1 |
| Munhoz da Rocha et al. | 2021 | 34908466 | - | RNA-seq | Small RNA and EVs | Cellular and Extracellular Vesicle RNA Analysis |
| Chow | 2023 | 38014938 | PRJNA902399 | RNA-seq | Ubr2 and MThe | Transcription factor Rpn4 - drug efflux pumps expression |

### Next Priority

| Author | Year | PMID | NCBI ID | Data Type | Experiment | Notes |
|--------|------|------|---------|-----------|------------|-------|
| Wang | 2024 | 39455573 | PRJNA1086003 | RNA-seq | WGS of mRNA-seq | AR0382 and AR0387 grown in vitro and in vivo conditions |
| Chauhan | 2025 | 40064990 | PRJNA1232830 | RNA-seq | Gcn5 | Lysine acetyltransferase mediates cell wall remodeling, antifungal drug resistance |
| Zhang Y et al. | 2025 | 40394068 | GSE293594 | RNA-seq | GCN5 (+/-) TEC | Targeting epigenetic regulators to overcome drug resistance |
| Phan-Canh et al. | 2025 | 40638387 | GSE278878 | RNA-seq | White v brown switching | Controls phenotypic plasticity and virulence |
| Kean | 2018 | 29997121 | PRJNA477447 | RNA-seq | Biofilm formation | Transcriptome Assembly and Profiling - Novel Insights into Biofilm-Mediated Resistance |
| Kovacs | 2026 | 41817193 | GSE302377 | RNA-seq | Caspo and poco | Synergistic activity of caspofungin and posaconazole |

### Lower Priority

| Author | PMID | NCBI ID | Notes |
|--------|------|---------|-------|
| Pelletier | PRJNA983706 | 38466738 isolate | Two strains with differential gene expression during media induced aggregation |
| - | PRJNA1001264 | can't find paper | 2023 two strains, understanding environmental and biocide survival |

---

## C. glabrata Datasets

### Completed/In JBrowse

| Author | Year | PMID | NCBI ID | Data Type | Experiment | Notes |
|--------|------|------|---------|-----------|------------|-------|
| Ni | 2023 | 37891489 | PRJNA97198 | RNA-seq | CK2 complex | DNA damage response and virulence |
| Bhakt | 2022 | 36108742 | PRJNA83688 | RNA-seq | CgSet4 | Ergosterol biosynthesis transcriptional regulator CgUpc2a |
| - | 2021 | 34935446 | PRJNA78987 | RNA-seq | ROX1 mutations | Fluconazole susceptibility of upc2AΔ mutation |
| - | 2021 | 34591857 | PRJNA75636 | RNA-seq | Upc2A | Global regulator of antifungal drug resistance pathways |
| - | 2021 | 34591857 | PRJNA75636 | RNA-seq | Fluconazole analysis | Same study, 12 samples |
| - | 2022 | 35050001 | PRJNA68512 | RNA-seq | CgMar1 | Role in Azole Susceptibility |
| - | 2022 | 35774458 | PRJNA68282 | RNA-seq | Copper + fluconazole | Synergistically affects drug efflux, zinc homeostasis |
| - | 2020 | 33323516 | PRJNA65524 | RNA-seq | DNA damage checkpoint | Response in a Major Fungal Pathogen |
| - | 2020 | 33135521 | PRJNA62375 | RNA-seq | Oxidative stress crossroads | Transcription factor CgTog1 |
| - | 2020 | 32134928 | PRJNA59816 | RNA-seq | Histone H4 dosage | DNA damage response via homologous recombination |
| Kumar | 2024 | 38632999 | PRJNA98273 | RNA-seq | 2h macrophage-ingested | WT and Cgsnf2Δ, compared to RPMI-grown cells |
| Dottor | 2024 | 38861404 | PRJNA10607 | RNA-seq | Txn Reg under thiamine starvation | |

### Next Priority

| Author | Year | PMID | NCBI ID | Experiment | Notes |
|--------|------|------|---------|------------|-------|
| Raj | 2024 | 38641593 | PRJNA64040 | Global transcplanktonic v biofilm | 6 samples |
| Rana | 2025 | 40677213 | - | (+/-) 3AT (amino acid starvation) and H2O2 | Oxidative stress |
| Rana | 2025 | 40677213 | - | (+/-) GCN4 | |

### With MS/JB Notes

| Author | Year | PMID | NCBI ID | Notes |
|--------|------|------|---------|-------|
| - | 2015 | 25586221 | PRJNA26167 | Defining the transcriptome landscape during pH change and nitrosative stress |

---

## C. dubliniensis Datasets

| Status | Author | Year | PMID | NCBI ID | Strain | Experiment | Notes |
|--------|--------|------|------|---------|--------|------------|-------|
| DONE | Grumaz | 2013 | 23547856 | PRJNA178117 | CD36 | (+/-) hyphal induction | Ignored albicans-dubliniensis hybrids data |
| DONE | Singh-Babakh | 2021 | 33723044 | PRJNA706031 | CD36 | (+/-) OE of TYE7, GAL4, GLN3 | Ignored albicans-dubliniensis hybrids data |
| TO DO | Meza-Devalos | 2025 | 40673384 | - | - | (+/-) BCR1 | |

### Additional C. dubliniensis (CLIB 214 strain)

| Status | Folder | NCBI ID | PMID | Year | Project | Samples |
|--------|--------|---------|------|------|---------|---------|
| 72% alignment | Jakab_2019 | PRJNA531086 | 31399405 | 2019 | Physiological and transcript (+/-) tyrosol | 6 |
| done | Connolly_2013 | PRJNA175662 | 23895281 | 2013 | APSES Transcription factor Efg1 | 8 |
| done | Guida_2011 | PRJNA154483 | 22192698 | 2011 | Transcriptional landscape (next-gen seq) | 26 |
| 49% alignment | Bliss_2021 | PRJNA668176 | 33568454 | 2021 | Transcription Profiles - Inducible Adhesins | 27 |

---

## ChIP-seq Datasets

| Status | Author | Year | PMID | NCBI ID | Species | Experiment | Notes |
|--------|--------|------|------|---------|---------|------------|-------|
| NEXT | Dottor | 2024 | 38861404 | 15 ChIP-seq | C. albicans | ChIP-Seq | |
| data? | Hernandez-H | 2024 | 39360930 | have to look | C. albicans | ChIP-Seq | Telomeric silencing of adhesion genes |
| NEXT | Rai | 2023 | 38905306 | PRJEB50618 | C. albicans | OE genes | ZCF15 and ZCF26 modulating biofilm gene circuitry |
| NEXT (after C) | van Wijlick | - | 36004328 | PRJNA853561 | C. albicans | (+/-) irf1 mutation | Functional Portrait of Irf1 |
| - | Tebbji | 2017 | 29152582 | PRJNA416122 | C. albicans | Snf6 occupancy | Genomic Landscape of the Fungus - ChIP-chip with microarrays |
| - | She | 2024 | 39370643 | PRJNA105759 | C. albicans | Hfl1p | Histone-like transcription factor harmonizes nuclear and mitochondrial genomic network |

---

## Data Not in CGD (External Only)

| PMID | Author | Species | Data Type | Experiment | Notes |
|------|--------|---------|-----------|------------|-------|
| 37082713 | Luther | albicans | RNA-seq | WT v sky1 null | Data at SRA under PRJNA941841 |
| 37082713 | Luther | albicans | RNA-seq | WT v sky2 null | Data at SRA under PRJNA941841 |
| 37072087 | Wu Y | albicans | RNA-seq | (+/-) miltefosine | Gene is actually C2_08100W_A (AIF1 alias) |
| 37072087 | Wu Y | albicans | Proteomics | (+/-) miltefosine | Raw quantitative proteomics data |

---

## Proposed Tool Categories

Based on the curator's suggestion, expression tools could be divided into:

### 1. Drug Response Datasets
- Antifungal treatments (fluconazole, caspofungin, amphotericin B, etc.)
- Drug resistance mechanisms
- Clinical isolate comparisons

### 2. Basic Cell Biology Datasets
- Morphogenesis (yeast-hyphal transition)
- Biofilm formation
- Stress responses (oxidative, nitrosative, pH, cell wall)
- White-opaque switching
- Cell cycle and growth conditions

---

## Technical Notes

### Current Data Location
```
/data/jbrowse2/           # Symlinks to BigWig files
/data/HTS/{species}/bam/  # Source BAM and BigWig files
```

### File Naming Convention
```
{Author}{Year}_Hap{A|B}_{condition}_coverage.bigwig
```

### NCBI Search Queries for Finding More Data
```
# C. albicans with publications + multi-isolate
txid5476[Organism:exp] AND "bioproject pubmed"[Filter] AND "transcriptome gene expression"[Filter] AND "scope multiisolate"[Filter]

# C. albicans with publications + mono-isolate
txid5476[Organism:exp] AND "bioproject pubmed"[Filter] AND "transcriptome gene expression"[Filter] AND "scope monoisolate"[Filter]

# C. auris with publications + multi-isolate
find txid
```

---

## Fold Change Calculation

The Expression Tab displays fold change values comparing each experimental condition to a designated control condition within each study.

### Formula

**Normalized fold change** (corrects for sequencing depth differences):

```
fold_change = (condition_value × control_library_size) / (control_value × condition_library_size)
```

This is mathematically equivalent to:

```
fold_change = (condition_value / condition_library_size) / (control_value / control_library_size)
            = condition_CPM / control_CPM
```

### Step-by-Step Process

1. **Extract raw mean coverage** from BigWig files using pyBigWig:
   ```python
   condition_value = pyBigWig.stats(chromosome, start, end, type="mean")
   control_value = pyBigWig.stats(chromosome, start, end, type="mean")
   ```

2. **Get library sizes** (total mapped reads in millions) from BAM files:
   ```python
   condition_library_size = LIBRARY_SIZES[organism][study][condition_id]  # e.g., 6.0M reads
   control_library_size = LIBRARY_SIZES[organism][study][control_id]      # e.g., 10.44M reads
   ```

3. **Calculate normalized fold change**:
   ```python
   fold_change = (condition_value * control_library_size) / (control_value * condition_library_size)
   ```

### Example (Bruno_2010: High H2O2 vs Control)

| Sample | Raw Coverage | Library Size |
|--------|-------------|--------------|
| Control (nOxi) | 50 | 10.44M reads |
| High H2O2 (hOxi) | 30 | 6.0M reads |

**Without normalization:**
```
fold_change = 30 / 50 = 0.6  (appears downregulated)
```

**With normalization:**
```
fold_change = (30 × 10.44) / (50 × 6.0) = 313.2 / 300 = 1.044  (actually ~unchanged)
```

The gene appeared downregulated only because the H2O2 sample had fewer sequencing reads (6M vs 10.44M).

### Why Not RPKM/TPM?

- **Gene length normalization is not needed** because the same gene region is used for both condition and control - gene length cancels out in the ratio
- **Library size normalization IS needed** to correct for differences in sequencing depth between samples
- The result is equivalent to comparing CPM (Counts Per Million) values

### Code Reference

See `_calculate_fold_change()` function in `cgd/api/services/expression_service.py`

---

## Next Steps for Expression Tool Development

1. **Extract gene-level expression values** from BigWig files
2. **Create metadata database table** with standardized condition ontology
3. **Build search interface** allowing queries by:
   - Gene name
   - Condition/treatment category
   - Species
   - Data type (RNA-seq, ChIP-seq)
4. **Display options**:
   - Heatmap ribbons (like FlyBase)
   - Tabular FPKM/TPM values
   - Links to JBrowse for genomic context

---

*Last updated: 2026-05-05*
*Source: CGD Curator Spreadsheets*
