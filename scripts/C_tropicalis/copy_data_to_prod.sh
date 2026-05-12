#!/bin/bash
# Copy C. tropicalis database data files from dev to prod backend server
#
# Usage: bash copy_data_to_prod.sh [--dry-run]
#
# Run this script from your laptop (files go: dev -> laptop -> prod)

set -e

DRY_RUN=false
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "=== DRY RUN MODE - No files will be copied ==="
    echo
fi

# Server hostnames
DEV_SERVER="cgd-backend-dev"
PROD_SERVER="cgd-prod"

# Remote directory
DATA_DIR="/home/ec2-user"

# Local temp directory
LOCAL_TEMP="/tmp/ctrop_data_transfer"

# Individual data files
DATA_FILES=(
    "Ctrop_liftover3_sorted.gff"
    "GCA_013177555.1_ASM1317755v1_genomic.fna"
    "C_tropicalis_MYA-3404_proteins.fasta"
    "ctrop_ortholog_descriptions.tsv"
)

# Ortholog files
ORTHOLOG_FILES=(
    "reciprocal_best_hits.txt"
    "reciprocal_best_hits_caur.txt"
    "reciprocal_best_hits_cdub.txt"
    "reciprocal_best_hits_cgla.txt"
    "reciprocal_best_hits_cpar.txt"
)

# InterProScan results
IPRSCAN_FILES=(
    "all_results.tsv"
)

echo "=== C. tropicalis Database Data Files Copy Script ==="
echo "Path: $DEV_SERVER -> laptop -> $PROD_SERVER"
echo "Local temp: $LOCAL_TEMP"
echo

# Create local temp directories
if ! $DRY_RUN; then
    mkdir -p "$LOCAL_TEMP"
    mkdir -p "$LOCAL_TEMP/orthologs"
    mkdir -p "$LOCAL_TEMP/iprscan_results"
fi

# Step 1: Download main data files from dev
echo "=== Step 1: Downloading main data files from dev ==="
for file in "${DATA_FILES[@]}"; do
    echo "  $file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${DEV_SERVER}:${DATA_DIR}/${file} ${LOCAL_TEMP}/"
    else
        scp "${DEV_SERVER}:${DATA_DIR}/${file}" "${LOCAL_TEMP}/" || echo "    Warning: Failed to copy $file"
    fi
done
echo

# Step 2: Download ortholog files from dev
echo "=== Step 2: Downloading ortholog files from dev ==="
for file in "${ORTHOLOG_FILES[@]}"; do
    echo "  orthologs/$file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${DEV_SERVER}:${DATA_DIR}/orthologs/${file} ${LOCAL_TEMP}/orthologs/"
    else
        scp "${DEV_SERVER}:${DATA_DIR}/orthologs/${file}" "${LOCAL_TEMP}/orthologs/" || echo "    Warning: Failed to copy $file"
    fi
done
echo

# Step 3: Download InterProScan results from dev
echo "=== Step 3: Downloading InterProScan results from dev ==="
for file in "${IPRSCAN_FILES[@]}"; do
    echo "  iprscan_results/$file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${DEV_SERVER}:${DATA_DIR}/iprscan_results/${file} ${LOCAL_TEMP}/iprscan_results/"
    else
        scp "${DEV_SERVER}:${DATA_DIR}/iprscan_results/${file}" "${LOCAL_TEMP}/iprscan_results/" || echo "    Warning: Failed to copy $file"
    fi
done
echo

# Step 4: Create directories on prod
echo "=== Step 4: Creating directories on prod ==="
if $DRY_RUN; then
    echo "  [DRY-RUN] ssh $PROD_SERVER mkdir -p ${DATA_DIR}/orthologs ${DATA_DIR}/iprscan_results"
else
    ssh "$PROD_SERVER" "mkdir -p ${DATA_DIR}/orthologs ${DATA_DIR}/iprscan_results"
    echo "  Directories created"
fi
echo

# Step 5: Upload main data files to prod
echo "=== Step 5: Uploading main data files to prod ==="
for file in "${DATA_FILES[@]}"; do
    echo "  $file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${LOCAL_TEMP}/${file} ${PROD_SERVER}:${DATA_DIR}/"
    else
        scp "${LOCAL_TEMP}/${file}" "${PROD_SERVER}:${DATA_DIR}/" || echo "    Warning: Failed to copy $file"
    fi
done
echo

# Step 6: Upload ortholog files to prod
echo "=== Step 6: Uploading ortholog files to prod ==="
for file in "${ORTHOLOG_FILES[@]}"; do
    echo "  orthologs/$file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${LOCAL_TEMP}/orthologs/${file} ${PROD_SERVER}:${DATA_DIR}/orthologs/"
    else
        scp "${LOCAL_TEMP}/orthologs/${file}" "${PROD_SERVER}:${DATA_DIR}/orthologs/" || echo "    Warning: Failed to copy $file"
    fi
done
echo

# Step 7: Upload InterProScan results to prod
echo "=== Step 7: Uploading InterProScan results to prod ==="
for file in "${IPRSCAN_FILES[@]}"; do
    echo "  iprscan_results/$file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${LOCAL_TEMP}/iprscan_results/${file} ${PROD_SERVER}:${DATA_DIR}/iprscan_results/"
    else
        scp "${LOCAL_TEMP}/iprscan_results/${file}" "${PROD_SERVER}:${DATA_DIR}/iprscan_results/" || echo "    Warning: Failed to copy $file"
    fi
done
echo

# Step 8: Cleanup
echo "=== Step 8: Cleanup ==="
if $DRY_RUN; then
    echo "  [DRY-RUN] Would remove ${LOCAL_TEMP}"
else
    read -p "Remove local temp files? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf "${LOCAL_TEMP}"
        echo "  Temp files removed"
    else
        echo "  Temp files kept at ${LOCAL_TEMP}"
    fi
fi
echo

if $DRY_RUN; then
    echo "=== DRY RUN COMPLETE ==="
    echo "Re-run without --dry-run to copy files"
else
    echo "=== COPY COMPLETE ==="
    echo
    echo "Next steps - run loading scripts on prod in order:"
    echo "  1. python scripts/C_tropicalis/load_organism.py"
    echo "  2. python scripts/C_tropicalis/load_genes_and_sequences.py --gff ${DATA_DIR}/Ctrop_liftover3_sorted.gff --proteins ${DATA_DIR}/C_tropicalis_MYA-3404_proteins.fasta"
    echo "  3. python scripts/C_tropicalis/add_feature_qualifiers.py"
    echo "  4. python scripts/C_tropicalis/load_coordinates.py --gff ${DATA_DIR}/Ctrop_liftover3_sorted.gff --genomic ${DATA_DIR}/GCA_013177555.1_ASM1317755v1_genomic.fna"
    echo "  5. python scripts/C_tropicalis/load_genomic_sequences.py --gff ${DATA_DIR}/Ctrop_liftover3_sorted.gff --genomic ${DATA_DIR}/GCA_013177555.1_ASM1317755v1_genomic.fna"
    echo "  6. python scripts/C_tropicalis/load_cds_and_introns.py --gff ${DATA_DIR}/Ctrop_liftover3_sorted.gff"
    echo "  7. python scripts/C_tropicalis/load_descriptions.py --descriptions ${DATA_DIR}/ctrop_ortholog_descriptions.tsv"
    echo "  8. python scripts/C_tropicalis/load_orthologs.py --orthologs-dir ${DATA_DIR}/orthologs"
    echo "  9. python scripts/C_tropicalis/load_protein_domains.py --tsv ${DATA_DIR}/iprscan_results/all_results.tsv"
    echo " 10. python scripts/C_tropicalis/load_go_annotations.py --tsv ${DATA_DIR}/iprscan_results/all_results.tsv"
fi
