#!/usr/bin/env bash
#
# Update JBrowse2 data files after GFF and sequence dumps.
#
# This script:
#   1. Sorts GFF files by chromosome and position
#   2. Compresses with bgzip and creates tabix index
#   3. Decompresses chromosome FASTA and creates samtools faidx index
#   4. Files are written to GENOMES_DIR (/data/genomes) where JBrowse2 symlinks point
#
# Prerequisites:
#   - samtools (for faidx)
#   - htslib (for bgzip and tabix)
#
# Usage:
#   ./slack-cron.sh ./cgd_update_jbrowse.sh
#
# Environment Variables:
#   DOWNLOAD_DIR: Base directory for dump files (default: /data/downloads)
#   GENOMES_DIR: Output directory for processed files (default: /data/genomes)
#   JBROWSE_DIR: JBrowse2 data directory with symlinks (default: /data/jbrowse2)
#

set -e

# Configuration
DOWNLOAD_DIR="${DOWNLOAD_DIR:-/data/downloads}"
GENOMES_DIR="${GENOMES_DIR:-/data/genomes}"
JBROWSE_DIR="${JBROWSE_DIR:-/data/jbrowse2}"
GFF_DIR="$DOWNLOAD_DIR/gff"
SEQ_DIR="$DOWNLOAD_DIR/sequence"

echo "CGD JBrowse2 Update"
echo "Generated: $(date)"
echo "GFF source: $GFF_DIR"
echo "Sequence source: $SEQ_DIR"
echo "Output directory: $GENOMES_DIR"
echo "JBrowse directory: $JBROWSE_DIR"
echo "========================================"

# Check required tools
for cmd in bgzip tabix samtools; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "ERROR: Required tool '$cmd' not found"
        exit 1
    fi
done

# Create output directories if needed
mkdir -p "$GENOMES_DIR"
mkdir -p "$JBROWSE_DIR"

# Track errors and updates
errors=0
updated=0

# Species configuration: strain_abbrev -> assembly_suffix|gff_subdir
# C. albicans has multiple assemblies with subdirectory, others have single assembly
# Format: "assembly_suffix|gff_subdir" where gff_subdir is optional subdirectory for GFF files
declare -A SPECIES_CONFIG=(
    ["C_albicans_SC5314"]="_A22|Assembly22"
    ["C_auris_B8441"]="|"
    ["C_dubliniensis_CD36"]="|"
    ["C_glabrata_CBS138"]="|"
    ["C_parapsilosis_CDC317"]="|"
    ["C_tropicalis_MYA-3404"]="|"
)

# Find source GFF file using glob pattern
find_gff_source() {
    local strain=$1
    local gff_subdir=$2
    local search_dir="$GFF_DIR/$strain"

    if [ -n "$gff_subdir" ]; then
        search_dir="$search_dir/$gff_subdir"
    fi

    # Find *_features.gff (not intergenic, not with_chromosome_sequences)
    local found
    found=$(find "$search_dir" -maxdepth 1 -name "${strain}_version_*_features.gff" -type f 2>/dev/null | head -1)

    if [ -z "$found" ]; then
        # Try alternate pattern without version
        found=$(find "$search_dir" -maxdepth 1 -name "${strain}*_features.gff" -type f ! -name "*intergenic*" ! -name "*with_chromosome*" 2>/dev/null | head -1)
    fi

    echo "$found"
}

# Find source FASTA file using glob pattern
find_fasta_source() {
    local strain=$1
    local search_dir="$SEQ_DIR/$strain"

    # For C_albicans, check Assembly22/current; for others, check current
    local assembly_subdir
    assembly_subdir=$(ls -d "$search_dir"/Assembly[0-9]* 2>/dev/null | sort -V | tail -1)

    if [ -n "$assembly_subdir" ]; then
        search_dir="$assembly_subdir/current"
    else
        search_dir="$search_dir/current"
    fi

    # Find *_chromosomes.fasta.gz
    local found
    found=$(find "$search_dir" -maxdepth 1 -name "*_chromosomes.fasta.gz" -type f 2>/dev/null | head -1)

    echo "$found"
}

# Check if source is newer than target
is_source_newer() {
    local source=$1
    local target=$2

    if [ ! -f "$target" ]; then
        return 0  # Target doesn't exist, needs update
    fi

    if [ "$source" -nt "$target" ]; then
        return 0  # Source is newer
    fi

    return 1  # Target is up to date
}

process_gff() {
    local strain=$1
    local assembly_suffix=$2
    local gff_subdir=$3
    local output_name="${strain}${assembly_suffix}_features"
    local sorted_gff="$GENOMES_DIR/${output_name}.sorted.gff"
    local bgzipped_gff="${sorted_gff}.gz"

    echo ""
    echo "Processing GFF: $strain$assembly_suffix"

    # Find the source GFF file
    local gff_source
    gff_source=$(find_gff_source "$strain" "$gff_subdir")

    if [ -z "$gff_source" ] || [ ! -f "$gff_source" ]; then
        echo "  WARNING: Source GFF not found for $strain"
        echo "  Searched in: $GFF_DIR/$strain${gff_subdir:+/$gff_subdir}"
        return 1
    fi

    echo "  Source: $gff_source"

    # Check if update is needed
    if ! is_source_newer "$gff_source" "$bgzipped_gff"; then
        echo "  SKIPPED: Target is up to date"
        return 0
    fi

    # Sort GFF by chromosome (col 1) and start position (col 4)
    # Skip comment lines, sort, then prepend comments back
    echo "  Sorting GFF..."
    (grep "^#" "$gff_source" || true; grep -v "^#" "$gff_source" | sort -k1,1 -k4,4n) > "$sorted_gff"

    # Bgzip compress (removes original sorted file)
    echo "  Compressing with bgzip..."
    bgzip -f "$sorted_gff"

    # Create tabix index
    echo "  Creating tabix index..."
    tabix -p gff "$bgzipped_gff"

    echo "  Done: $bgzipped_gff"
    ((updated++)) || true
    return 0
}

process_fasta() {
    local strain=$1
    local assembly_suffix=$2
    local output_name="${strain}${assembly_suffix}_current_chromosomes.fasta"
    local output_fasta="$GENOMES_DIR/$output_name"
    local output_fai="${output_fasta}.fai"

    echo ""
    echo "Processing FASTA: $strain$assembly_suffix"

    # Find the source FASTA file
    local fasta_source
    fasta_source=$(find_fasta_source "$strain")

    if [ -z "$fasta_source" ] || [ ! -f "$fasta_source" ]; then
        echo "  WARNING: Source FASTA not found for $strain"
        echo "  Searched in: $SEQ_DIR/$strain/*/current/"
        return 1
    fi

    echo "  Source: $fasta_source"

    # Check if update is needed
    if ! is_source_newer "$fasta_source" "$output_fasta"; then
        echo "  SKIPPED: Target is up to date"
        return 0
    fi

    # Decompress FASTA (JBrowse2 needs uncompressed FASTA with .fai)
    echo "  Decompressing FASTA..."
    gunzip -c "$fasta_source" > "$output_fasta"

    # Create samtools faidx index
    echo "  Creating FASTA index..."
    samtools faidx "$output_fasta"

    echo "  Done: $output_fasta"
    ((updated++)) || true
    return 0
}

# Process each species
for strain in "${!SPECIES_CONFIG[@]}"; do
    config="${SPECIES_CONFIG[$strain]}"
    assembly_suffix="${config%%|*}"
    gff_subdir="${config##*|}"

    if ! process_gff "$strain" "$assembly_suffix" "$gff_subdir"; then
        errors=$((errors + 1))
    fi

    if ! process_fasta "$strain" "$assembly_suffix"; then
        errors=$((errors + 1))
    fi
done

echo ""
echo "========================================"
echo "Summary: $updated files updated, $errors errors"

if [ $errors -gt 0 ]; then
    echo "WARNING: $errors files could not be processed"
    echo "This may be expected if some dumps have not run yet."
    exit 0  # Don't fail the cron job for missing source files
fi

echo "JBrowse2 update completed successfully."
exit 0
