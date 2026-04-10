#!/usr/bin/env bash
#
# Update JBrowse2 data files after GFF and sequence dumps.
#
# This script:
#   1. Sorts GFF files by chromosome and position
#   2. Compresses with bgzip and creates tabix index
#   3. Decompresses chromosome FASTA and creates samtools faidx index
#   4. Creates symlinks in JBROWSE_DIR pointing to the processed files
#
# Prerequisites:
#   - samtools (for faidx)
#   - htslib (for bgzip and tabix)
#
# Usage:
#   ./slack-cron.sh ./cgd_update_jbrowse.sh
#
# Environment Variables:
#   DOWNLOAD_DIR: Base directory for dump files (default: /opt/cgd_api/data)
#   JBROWSE_DIR: JBrowse2 data directory (default: /data/jbrowse2)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Configuration
DOWNLOAD_DIR="${DOWNLOAD_DIR:-$PROJECT_ROOT/data}"
JBROWSE_DIR="${JBROWSE_DIR:-/data/jbrowse2}"
GFF_DIR="$DOWNLOAD_DIR/gff"
SEQ_DIR="$DOWNLOAD_DIR/sequence"

echo "CGD JBrowse2 Update"
echo "Generated: $(date)"
echo "GFF source: $GFF_DIR"
echo "Sequence source: $SEQ_DIR"
echo "JBrowse target: $JBROWSE_DIR"
echo "========================================"

# Check required tools
for cmd in bgzip tabix samtools; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "ERROR: Required tool '$cmd' not found"
        exit 1
    fi
done

# Create JBrowse directory if needed
mkdir -p "$JBROWSE_DIR"

# Track errors
errors=0

# Species configuration: strain_abbrev -> assembly_suffix (empty for single-assembly species)
# C. albicans has multiple assemblies, others have single current assembly
declare -A SPECIES_ASSEMBLIES=(
    ["C_albicans_SC5314"]="_A22"
    ["C_dubliniensis_CD36"]=""
    ["C_glabrata_CBS138"]=""
    ["C_parapsilosis_CDC317"]=""
    ["C_auris_B8441"]=""
)

process_gff() {
    local strain=$1
    local assembly_suffix=$2
    local gff_source="$GFF_DIR/${strain}.gff"
    local output_name="${strain}${assembly_suffix}_features"
    local sorted_gff="$JBROWSE_DIR/${output_name}.sorted.gff"
    local bgzipped_gff="${sorted_gff}.gz"

    echo ""
    echo "Processing GFF: $strain$assembly_suffix"

    if [ ! -f "$gff_source" ]; then
        echo "  WARNING: Source GFF not found: $gff_source"
        return 1
    fi

    # Sort GFF by chromosome (col 1) and start position (col 4)
    # Skip comment lines, sort, then prepend comments back
    echo "  Sorting GFF..."
    (grep "^#" "$gff_source" || true; grep -v "^#" "$gff_source" | sort -k1,1 -k4,4n) > "$sorted_gff"

    # Bgzip compress (removes original)
    echo "  Compressing with bgzip..."
    bgzip -f "$sorted_gff"

    # Create tabix index
    echo "  Creating tabix index..."
    tabix -p gff "$bgzipped_gff"

    echo "  Done: $bgzipped_gff"
    return 0
}

process_fasta() {
    local strain=$1
    local assembly_suffix=$2
    local seq_subdir="${strain}${assembly_suffix}"
    local fasta_source="$SEQ_DIR/$seq_subdir/${strain}_chromosomes.fasta.gz"
    local output_name="${strain}${assembly_suffix}_current_chromosomes.fasta"
    local output_fasta="$JBROWSE_DIR/$output_name"

    echo ""
    echo "Processing FASTA: $strain$assembly_suffix"

    if [ ! -f "$fasta_source" ]; then
        echo "  WARNING: Source FASTA not found: $fasta_source"
        return 1
    fi

    # Decompress FASTA (JBrowse2 needs uncompressed FASTA with .fai)
    echo "  Decompressing FASTA..."
    gunzip -c "$fasta_source" > "$output_fasta"

    # Create samtools faidx index
    echo "  Creating FASTA index..."
    samtools faidx "$output_fasta"

    echo "  Done: $output_fasta"
    return 0
}

# Process each species
for strain in "${!SPECIES_ASSEMBLIES[@]}"; do
    assembly_suffix="${SPECIES_ASSEMBLIES[$strain]}"

    if ! process_gff "$strain" "$assembly_suffix"; then
        errors=$((errors + 1))
    fi

    if ! process_fasta "$strain" "$assembly_suffix"; then
        errors=$((errors + 1))
    fi
done

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "WARNING: $errors files could not be processed"
    echo "This may be expected if some dumps have not run yet."
    exit 0  # Don't fail the cron job for missing source files
fi

echo "JBrowse2 update completed successfully."
exit 0
