# CGD GAF Syntax Check Issues - Complete Summary

**Date:** 2026-04-20
**Source:** https://ftp.ebi.ac.uk/pub/contrib/goa/reports/CGD_syntax_check.log.gz
**GAF File:** https://www.candidagenome.org/download/go/cgd_C_albicans_SC5314.gaf.gz

## Overview

| Error Type | Count | Severity | Fix Complexity |
|------------|------:|----------|----------------|
| Unsupported / missing reference | 199,336 | Low | Filter IEA from submission |
| Unsupported evidence code (IEA) | 199,329 | Low | Filter IEA from submission |
| Unsupported / unmapped identifier | 151,034 | Medium | Upload gp2protein to EBI |
| Restricted GO terms | 11,354 | High | Replace with specific terms |
| Invalid with/from components | 5,391 | Medium | Fix identifier formats |
| Missing with/from | 1,583 | High | Add required references |
| Obsolete GO IDs | 896 | Medium | Update to current terms |

---

## Issue 1: IEA Annotations Rejected (~199k errors)

**Problem:** GOA doesn't accept IEA annotations from external MODs - they generate their own.

**Fix:**
- Filter out all IEA evidence code annotations before submitting to GOA
- Keep IEA annotations in CGD's own GAF file, just exclude from GOA submission
- This is **expected behavior** - not a data quality issue

---

## Issue 2: Unmapped Identifiers (~151k errors)

**Problem:** CGD identifiers aren't being resolved by GOA's validator.

**Root Causes:**
1. CGD's gp2protein file exists but **isn't uploaded to EBI's FTP**
2. ~163 are non-coding RNA genes (tRNA, snoRNA) that can't have UniProt entries
3. ~155 protein-coding genes missing UniProt mappings

### Breakdown by species:

| Species | Prefix | Errors |
|---------|--------|-------:|
| C. albicans WO-1 | CAWG | 19,149 |
| C. tropicalis | CTRG | 19,098 |
| M. guilliermondii | PGUG | 18,919 |
| C. orthopsilosis | CORT | 18,625 |
| D. hansenii | DEHA | 18,175 |
| L. elongisporus | LELG | 18,057 |
| C. lusitaniae | CLUG | 17,883 |
| C. glabrata | CAL (taxon:284593) | 1,787 |
| C. albicans SC5314 | CAL (taxon:237561) | 1,112 |
| C. parapsilosis | CAL (taxon:578454) | 390 |
| C. dubliniensis | CAL (taxon:573826) | 305 |

### For C. albicans SC5314 specifically:

| Category | Unique Genes | Notes |
|----------|-------------:|-------|
| Total unmapped | 318 | |
| tRNA genes | 156 | Cannot have UniProt entries |
| Other ncRNA | 7 | SNR6, RDN, RPR1 - cannot have UniProt entries |
| Protein-coding | ~155 | Need UniProt mappings |

**Fix:**
1. **Upload gp2protein files to EBI** at `ftp.ebi.ac.uk/pub/contrib/goa/gp2protein/`
2. **For ncRNA genes**: Either exclude from GOA submission or map to RNAcentral
3. **For missing proteins**: Submit sequences to UniProt, then add to gp2protein
4. **Coordinate with GOA** (`goa@ebi.ac.uk`) to set up automated sync

### CGD's existing gp2protein files:
- https://www.candidagenome.org/download/External_id_mappings/gp2protein.cgd.gz
- https://www.candidagenome.org/download/External_id_mappings/gp2protein_C_albicans_SC5314.gz
- https://www.candidagenome.org/download/External_id_mappings/gp2protein_C_dubliniensis_CD36.gz
- https://www.candidagenome.org/download/External_id_mappings/gp2protein_C_glabrata_CBS138.gz
- https://www.candidagenome.org/download/External_id_mappings/gp2protein_C_parapsilosis_CDC317.gz

These files are correctly formatted but need to be uploaded to EBI's FTP server.

---

## Issue 3: Restricted GO Terms (~11k errors)

**Problem:** Using high-level GO terms marked "do not annotate directly."

**Top offenders:**

| Count | GO Term | Name | Action |
|------:|---------|------|--------|
| 2,155 | GO:0044419 | biological process involved in interspecies interaction | Use specific child term |
| 2,128 | GO:0005622 | intracellular anatomical structure | Use specific compartment |
| 1,866 | GO:0008152 | metabolic process | Use specific pathway |
| 1,798 | GO:0071216 | cellular response to biotic stimulus | Use specific response |
| 980 | GO:0006810 | transport | Use specific transport type |
| 498 | GO:0009058 | biosynthetic process | Use specific biosynthesis |
| 412 | GO:0005215 | transporter activity | Use specific transporter |
| 269 | GO:0042710 | biofilm formation | May need GO Consortium guidance |
| 213 | GO:0070887 | cellular response to chemical stimulus | Use specific response |

**Fix:**
- Replace each restricted term with a more specific child term
- Use QuickGO or AmiGO to find appropriate descendants
- Review annotations using these terms and re-annotate

---

## Issue 4: Invalid With/From Components (~5k errors)

**Problem:** With/from column contains unrecognized identifiers.

**Examples:**
```
CGD:CAL0002429     -> CGD ID not recognized
UniProtKB:M10123   -> Invalid UniProtKB format (should be like P12345)
```

**Fix:**
- Use recognized identifier formats:
  - UniProtKB: `UniProtKB:P12345` or `UniProtKB:A0A1D8PHS3`
  - SGD: `SGD:S000001234`
  - PomBase: `PomBase:SPAC123.01`
- Ensure CGD IDs in with/from are also in gp2protein file

---

## Issue 5: Missing With/From (~1.5k errors)

**Problem:** IGI and ISS evidence codes require the with/from column but it's empty.

**Evidence codes affected:**
- **IGI** (Inferred from Genetic Interaction) - needs interacting gene
- **ISS** (Inferred from Sequence Similarity) - needs reference sequence

**Example bad annotation:**
```
CGD  CAL0000196141  AAF1  GO:0005634  ...  ISS  [EMPTY]
                                            ^     ^
                                         evidence  missing!
```

**Fix:**
- Add the gene/protein ID that the inference was based on
- Format: `UniProtKB:P12345` or `SGD:S000001234`

---

## Issue 6: Obsolete GO IDs (~900 errors)

**Problem:** Using GO terms that have been made obsolete.

**Examples:**

| Obsolete Term | Action |
|---------------|--------|
| GO:0019427 | Check GO for replaced_by |
| GO:0042450 | Check GO for replaced_by |
| GO:0000949 | Check GO for replaced_by |
| GO:0051082 | Check GO for replaced_by |
| GO:0032075 | Check GO for replaced_by |
| GO:0044806 | Check GO for replaced_by |

**Fix:**
- Query GO for each obsolete term to find `replaced_by` or `consider` terms
- Update annotations to use current terms
- Set up automated checking against GO releases

---

## Priority Action Plan

### Immediate (High Impact)
1. **Filter IEA annotations** from GOA submission -> eliminates ~199k errors
2. **Upload gp2protein files to EBI** -> eliminates most of ~151k errors

### Short-term (Data Quality)
3. **Fix missing with/from** for IGI/ISS annotations (~1.5k)
4. **Replace restricted GO terms** with specific children (~11k)
5. **Update obsolete GO IDs** (~900)

### Medium-term (Infrastructure)
6. **Fix invalid with/from identifiers** (~5k)
7. **Submit missing proteins to UniProt**
8. **Set up automated validation** before submission
9. **Handle ncRNA annotations** (RNAcentral or exclude)

---

## Contact Information

- **GOA team:** goa@ebi.ac.uk
- **UniProt submissions:** https://www.uniprot.org/submissions

---

## Comparison with Other MODs

For reference, other MODs have far fewer errors:
- **SGD:** 186 unmapped identifier errors
- **PomBase:** 6 unmapped identifier errors

This is because they have their gp2protein files properly uploaded to EBI and coordinate regularly with GOA.
