#!/bin/bash
# Copy C. tropicalis JBrowse files from dev to prod server
#
# Usage: bash copy_jbrowse_to_prod.sh [--dry-run]
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
DEV_SERVER="cgd-frontend-dev"
PROD_SERVER="cgd-prod"

# Remote directories
GENOMES_DIR="/data/genomes"
JBROWSE_DIR="/data/jbrowse2"
PROTEIN_DATA_DIR="/data/jbrowse2/protein_data"
DOMAIN_DIR="/data/domain/C_tropicalis_MYA3404"

# Local temp directory
LOCAL_TEMP="/tmp/ctrop_jbrowse_transfer"

# Files to copy
GENOME_FILES=(
    "C_tropicalis_current_chromosomes.fasta"
    "C_tropicalis_current_chromosomes.fasta.fai"
    "C_tropicalis_features.sorted.gff.gz"
    "C_tropicalis_features.sorted.gff.gz.tbi"
)

PROTEIN_FILES=(
    "C_tropicalis_proteins.fasta"
    "C_tropicalis_proteins.fasta.fai"
    "C_tropicalis_Pfam.gff.gz"
    "C_tropicalis_Pfam.gff.gz.tbi"
    "C_tropicalis_PANTHER.gff.gz"
    "C_tropicalis_PANTHER.gff.gz.tbi"
    "C_tropicalis_SUPERFAMILY.gff.gz"
    "C_tropicalis_SUPERFAMILY.gff.gz.tbi"
    "C_tropicalis_CATH.gff.gz"
    "C_tropicalis_CATH.gff.gz.tbi"
    "C_tropicalis_SMART.gff.gz"
    "C_tropicalis_SMART.gff.gz.tbi"
    "C_tropicalis_CDD.gff.gz"
    "C_tropicalis_CDD.gff.gz.tbi"
    "C_tropicalis_PRINTS.gff.gz"
    "C_tropicalis_PRINTS.gff.gz.tbi"
    "C_tropicalis_ProSiteProfiles.gff.gz"
    "C_tropicalis_ProSiteProfiles.gff.gz.tbi"
    "C_tropicalis_Coils.gff.gz"
    "C_tropicalis_Coils.gff.gz.tbi"
    "C_tropicalis_MobiDBLite.gff.gz"
    "C_tropicalis_MobiDBLite.gff.gz.tbi"
)

echo "=== C. tropicalis JBrowse Files Copy Script ==="
echo "Path: $DEV_SERVER -> laptop -> $PROD_SERVER"
echo "Local temp: $LOCAL_TEMP"
echo

# Create local temp directories
if ! $DRY_RUN; then
    mkdir -p "$LOCAL_TEMP/genomes"
    mkdir -p "$LOCAL_TEMP/protein_data"
    mkdir -p "$LOCAL_TEMP/domain"
fi

# Step 1: Download genome files from dev
echo "=== Step 1: Downloading genome files from dev ==="
for file in "${GENOME_FILES[@]}"; do
    echo "  $file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${DEV_SERVER}:${GENOMES_DIR}/${file} ${LOCAL_TEMP}/genomes/"
    else
        scp "${DEV_SERVER}:${GENOMES_DIR}/${file}" "${LOCAL_TEMP}/genomes/"
    fi
done
echo

# Step 2: Download protein domain files from dev
echo "=== Step 2: Downloading protein domain files from dev ==="
for file in "${PROTEIN_FILES[@]}"; do
    echo "  $file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${DEV_SERVER}:${PROTEIN_DATA_DIR}/${file} ${LOCAL_TEMP}/protein_data/"
    else
        scp "${DEV_SERVER}:${PROTEIN_DATA_DIR}/${file}" "${LOCAL_TEMP}/protein_data/"
    fi
done
echo

# Step 3: Download domain source data from dev
echo "=== Step 3: Downloading domain source data from dev ==="
if $DRY_RUN; then
    echo "  [DRY-RUN] scp -r ${DEV_SERVER}:${DOMAIN_DIR} ${LOCAL_TEMP}/domain/"
else
    scp -r "${DEV_SERVER}:${DOMAIN_DIR}" "${LOCAL_TEMP}/domain/"
fi
echo

# Step 4: Download config.json from dev
echo "=== Step 4: Downloading config.json from dev ==="
if $DRY_RUN; then
    echo "  [DRY-RUN] scp ${DEV_SERVER}:${JBROWSE_DIR}/config.json ${LOCAL_TEMP}/"
else
    scp "${DEV_SERVER}:${JBROWSE_DIR}/config.json" "${LOCAL_TEMP}/"
fi
echo

# Step 5: Upload genome files to prod
echo "=== Step 5: Uploading genome files to prod ==="
for file in "${GENOME_FILES[@]}"; do
    echo "  $file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${LOCAL_TEMP}/genomes/${file} ${PROD_SERVER}:${GENOMES_DIR}/"
    else
        scp "${LOCAL_TEMP}/genomes/${file}" "${PROD_SERVER}:${GENOMES_DIR}/"
    fi
done
echo

# Step 6: Upload protein domain files to prod
echo "=== Step 6: Uploading protein domain files to prod ==="
for file in "${PROTEIN_FILES[@]}"; do
    echo "  $file"
    if $DRY_RUN; then
        echo "    [DRY-RUN] scp ${LOCAL_TEMP}/protein_data/${file} ${PROD_SERVER}:${PROTEIN_DATA_DIR}/"
    else
        scp "${LOCAL_TEMP}/protein_data/${file}" "${PROD_SERVER}:${PROTEIN_DATA_DIR}/"
    fi
done
echo

# Step 7: Upload domain source data to prod
echo "=== Step 7: Uploading domain source data to prod ==="
if $DRY_RUN; then
    echo "  [DRY-RUN] scp -r ${LOCAL_TEMP}/domain/C_tropicalis_MYA3404 ${PROD_SERVER}:/data/domain/"
else
    scp -r "${LOCAL_TEMP}/domain/C_tropicalis_MYA3404" "${PROD_SERVER}:/data/domain/"
fi
echo

# Step 8: Create symlinks on prod
echo "=== Step 8: Creating symlinks on prod ==="
SYMLINK_CMDS="
cd ${JBROWSE_DIR} && \
ln -sf ${GENOMES_DIR}/C_tropicalis_current_chromosomes.fasta C_tropicalis_current_chromosomes.fasta && \
ln -sf ${GENOMES_DIR}/C_tropicalis_current_chromosomes.fasta.fai C_tropicalis_current_chromosomes.fasta.fai && \
ln -sf ${GENOMES_DIR}/C_tropicalis_features.sorted.gff.gz C_tropicalis_features.sorted.gff.gz && \
ln -sf ${GENOMES_DIR}/C_tropicalis_features.sorted.gff.gz.tbi C_tropicalis_features.sorted.gff.gz.tbi && \
echo 'Symlinks created'
"

if $DRY_RUN; then
    echo "  [DRY-RUN] Would create symlinks on $PROD_SERVER"
else
    ssh "$PROD_SERVER" "$SYMLINK_CMDS"
fi
echo

# Step 9: Upload config.json to prod
echo "=== Step 9: Upload config.json to prod ==="
if $DRY_RUN; then
    echo "  [DRY-RUN] scp ${LOCAL_TEMP}/config.json ${PROD_SERVER}:${JBROWSE_DIR}/config.json"
else
    read -p "Copy config.json from dev to prod? This will OVERWRITE prod config. (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        scp "${LOCAL_TEMP}/config.json" "${PROD_SERVER}:${JBROWSE_DIR}/config.json"
        echo "  config.json copied"
    else
        echo "  Skipped. Remember to manually update config.json on $PROD_SERVER"
    fi
fi
echo

# Step 10: Cleanup
echo "=== Step 10: Cleanup ==="
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
fi
