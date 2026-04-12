#!/usr/bin/env bash
#
# Dump GTF files for all CGD organisms.
#
# Usage:
#   ./slack-cron.sh ./cgd_dump_gtf.sh
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Change to project root for relative paths to work
cd "$PROJECT_ROOT"

echo "CGD GTF Dump"
echo "Generated: $(date)"
echo "========================================"

# Run the GTF dump script for all strains
python3 "$SCRIPT_DIR/dump_gtf.py" --all

exit $?
