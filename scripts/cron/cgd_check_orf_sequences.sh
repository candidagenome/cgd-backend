#!/usr/bin/env bash
#
# Run ORF sequence checks for all CGD organisms.
#
# Usage:
#   ./cgd_check_orf_sequences.sh
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

echo "Starting ORF sequence checks at $(date)"
echo "========================================"

python3 "$SCRIPT_DIR/check_orf_sequences.py" C_albicans_SC5314
python3 "$SCRIPT_DIR/check_orf_sequences.py" C_dubliniensis_CD36
python3 "$SCRIPT_DIR/check_orf_sequences.py" C_glabrata_CBS138
python3 "$SCRIPT_DIR/check_orf_sequences.py" C_parapsilosis_CDC317
python3 "$SCRIPT_DIR/check_orf_sequences.py" C_auris_B8441

echo "========================================"
echo "Finished ORF sequence checks at $(date)"
