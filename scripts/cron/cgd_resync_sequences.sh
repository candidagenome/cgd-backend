#!/usr/bin/env bash
#
# Weekly Oracle sequence sync guard.
#
# Runs scripts/resync_all_sequences.py --apply: any sequence that has fallen
# behind its table's MAX(pk) is advanced, so trigger-assigned inserts (e.g.
# the weekly PubMed reference load, Sat 21:00) cannot collide with ORA-00001.
#
# A resync is repair, not routine: since 2026-09-13 all committed loaders
# allocate through MAKEDBID / <TABLE>_SEQ.NEXTVAL, so a behind sequence means
# some load bypassed sequence allocation (MAX+1). The job therefore exits 1
# whenever it had to fix something, so slack-cron posts an alert and the
# offending load can be tracked down. Nothing behind -> quiet success.
#
# Scheduled Saturday 20:30 UTC, ahead of the 21:00 PubMed load.
#
# Usage:
#   ./slack-cron.sh ./cgd_resync_sequences.sh
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

echo "CGD Oracle Sequence Sync Guard"
echo "Generated: $(date)"
echo "========================================"

OUTPUT=$(python3 scripts/resync_all_sequences.py --apply 2>&1)
EXIT_CODE=$?
echo "$OUTPUT"

RESYNCED=$(echo "$OUTPUT" | sed -n 's/^Resynced *: *\([0-9]*\).*/\1/p')
FAILED=$(echo "$OUTPUT" | sed -n 's/^Failed *: *\([0-9]*\).*/\1/p')

echo ""
echo "========================================"

if [ $EXIT_CODE -ne 0 ] || [ "${FAILED:-0}" -gt 0 ]; then
    echo "ALERT: sequence resync FAILED for ${FAILED:-?} sequence(s) —"
    echo "trigger-assigned inserts (incl. the PubMed load) may hit ORA-00001."
    echo "Investigate and fix before Saturday 21:00 UTC."
    exit 1
fi

if [ "${RESYNCED:-0}" -gt 0 ]; then
    echo "ALERT: $RESYNCED sequence(s) were BEHIND and have been resynced."
    echo "The database is healthy again, but a recent load bypassed sequence"
    echo "allocation (MAX+1 instead of MAKEDBID / <TABLE>_SEQ.NEXTVAL)."
    echo "Find and fix the offending loader — see docs in"
    echo "scripts/resync_all_sequences.py."
    exit 1
fi

echo "All sequences in sync."
exit 0
