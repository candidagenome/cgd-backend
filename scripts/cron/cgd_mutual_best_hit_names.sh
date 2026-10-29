#!/usr/bin/env bash
#
# Mutual-BLAST-best-hit name-transfer candidates (2nd of each month) for curator review.
# Read-only: writes a dated TSV to NAME_TRANSFER_DIR and emails it to
# NAME_TRANSFER_EMAIL; see name_transfer_candidates.py.
#
# Usage:
#   ./slack-cron.sh ./cgd_mutual_best_hit_names.sh
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

cd "$PROJECT_ROOT"

python3 "$SCRIPT_DIR/name_transfer_candidates.py" mbh "$@"
