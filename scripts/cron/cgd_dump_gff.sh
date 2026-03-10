#!/usr/bin/env bash
#
# Dump GFF files for all CGD organisms.
#
# Usage:
#   ./cgd_dump_gff.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Change to project root for relative paths to work
cd "$PROJECT_ROOT"

# Output directory
OUTPUT_DIR="${DATA_DIR:-$PROJECT_ROOT/data}/gff"
mkdir -p "$OUTPUT_DIR"

echo "Starting GFF dump at $(date)"
echo "Output directory: $OUTPUT_DIR"
echo "========================================"

python3 "$SCRIPT_DIR/dump_gff.py" C_albicans_SC5314 --output "$OUTPUT_DIR/C_albicans_SC5314.gff"
python3 "$SCRIPT_DIR/dump_gff.py" C_dubliniensis_CD36 --output "$OUTPUT_DIR/C_dubliniensis_CD36.gff"
python3 "$SCRIPT_DIR/dump_gff.py" C_glabrata_CBS138 --output "$OUTPUT_DIR/C_glabrata_CBS138.gff"
python3 "$SCRIPT_DIR/dump_gff.py" C_parapsilosis_CDC317 --output "$OUTPUT_DIR/C_parapsilosis_CDC317.gff"
python3 "$SCRIPT_DIR/dump_gff.py" C_auris_B8441 --output "$OUTPUT_DIR/C_auris_B8441.gff"

echo "========================================"
echo "Finished GFF dump at $(date)"
