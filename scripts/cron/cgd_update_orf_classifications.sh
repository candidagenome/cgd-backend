#!/usr/bin/env bash
#
# Update ORF classifications for all CGD organisms.
# Python equivalent of cgd-updateORFclassifications (Perl version).
#
# Usage:
#   ./cgd_update_orf_classifications.sh
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

echo "Starting ORF classification update at $(date)"
echo "========================================"

python3 "$SCRIPT_DIR/update_orf_classifications.py" C_albicans_SC5314
python3 "$SCRIPT_DIR/update_orf_classifications.py" C_dubliniensis_CD36
python3 "$SCRIPT_DIR/update_orf_classifications.py" C_glabrata_CBS138
python3 "$SCRIPT_DIR/update_orf_classifications.py" C_parapsilosis_CDC317
python3 "$SCRIPT_DIR/update_orf_classifications.py" C_auris_B8441

echo "========================================"
echo "Finished ORF classification update at $(date)"
