#!/usr/bin/env bash
#
# Run CGD backend test suite and send Slack report.
#
# This script:
#   1. Runs pytest with JUnit XML output
#   2. Parses results for passed/failed/skipped counts
#   3. Lists failed tests with details
#   4. Sends a formatted Slack report
#
# Usage:
#   ./cgd_run_tests.sh [pytest-args...]
#   ./cgd_run_tests.sh                    # Run all tests
#   ./cgd_run_tests.sh tests/api/         # Run only API tests
#   ./cgd_run_tests.sh -k "locus"         # Run tests matching "locus"
#
# Environment Variables:
#   SLACK_WEBHOOK_URL: Slack webhook for notifications (optional)
#   ENV_STATE: Environment label (prod/dev)
#
# Prerequisites:
#   - Python 3.9+ with pytest installed
#   - jq (for JSON formatting)
#   - curl (for Slack notifications)
#

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Activate virtual environment
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    source "$PROJECT_ROOT/venv/bin/activate"
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Configuration
LOG_DIR="${LOG_DIR:-$PROJECT_ROOT/logs}"
RESULTS_DIR="$LOG_DIR/test-results"
TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
JUNIT_XML="$RESULTS_DIR/junit-$TIMESTAMP.xml"
LOG_FILE="$RESULTS_DIR/test-$TIMESTAMP.log"

# Create directories
mkdir -p "$RESULTS_DIR"

echo "========================================"
echo "CGD Backend Test Suite"
echo "========================================"
echo "Started: $(date)"
echo "Project: $PROJECT_ROOT"
echo "Results: $RESULTS_DIR"
echo "========================================"

# Change to project root
cd "$PROJECT_ROOT"

# Run pytest with JUnit XML output
# Capture both stdout and the exit code
echo ""
echo "Running tests..."
echo ""

pytest_args="${@:--v}"
python -m pytest $pytest_args \
    --tb=short \
    --junit-xml="$JUNIT_XML" \
    2>&1 | tee "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "========================================"
echo "Parsing Results..."
echo "========================================"

# Parse JUnit XML for summary
if [ -f "$JUNIT_XML" ]; then
    # Extract counts from JUnit XML
    TESTS=$(grep -oP 'tests="\K[0-9]+' "$JUNIT_XML" | head -1)
    ERRORS=$(grep -oP 'errors="\K[0-9]+' "$JUNIT_XML" | head -1)
    FAILURES=$(grep -oP 'failures="\K[0-9]+' "$JUNIT_XML" | head -1)
    SKIPPED=$(grep -oP 'skipped="\K[0-9]+' "$JUNIT_XML" | head -1)
    TIME=$(grep -oP 'time="\K[0-9.]+' "$JUNIT_XML" | head -1)

    # Default to 0 if not found
    TESTS=${TESTS:-0}
    ERRORS=${ERRORS:-0}
    FAILURES=${FAILURES:-0}
    SKIPPED=${SKIPPED:-0}
    TIME=${TIME:-0}

    # Calculate passed
    FAILED=$((ERRORS + FAILURES))
    PASSED=$((TESTS - FAILED - SKIPPED))

    # Extract failed test names
    FAILED_TESTS=""
    if [ $FAILED -gt 0 ]; then
        # Extract testcase elements that have failure or error children
        FAILED_TESTS=$(grep -B1 -E '<(failure|error)' "$JUNIT_XML" | \
            grep '<testcase' | \
            sed -E 's/.*classname="([^"]+)".*name="([^"]+)".*/\1::\2/' | \
            head -20)
    fi
else
    TESTS=0
    PASSED=0
    FAILED=0
    SKIPPED=0
    TIME=0
    FAILED_TESTS=""
fi

# Build summary report
echo ""
echo "========================================"
echo "TEST RESULTS SUMMARY"
echo "========================================"
echo ""
printf "  %-20s %s\n" "Total Tests:" "$TESTS"
printf "  %-20s %s\n" "Passed:" "$PASSED"
printf "  %-20s %s\n" "Failed:" "$FAILED"
printf "  %-20s %s\n" "Skipped:" "$SKIPPED"
printf "  %-20s %s seconds\n" "Duration:" "$TIME"
echo ""

if [ $FAILED -gt 0 ]; then
    echo "FAILED TESTS:"
    echo "----------------------------------------"
    echo "$FAILED_TESTS" | while read -r test; do
        echo "  ✗ $test"
    done
    echo ""
fi

echo "========================================"
echo "Completed: $(date)"
echo "Exit Code: $EXIT_CODE"
echo "========================================"

# Send Slack notification if webhook is configured
if [ -n "$SLACK_WEBHOOK_URL" ]; then
    echo ""
    echo "Sending Slack notification..."

    # Determine status emoji and color
    if [ $EXIT_CODE -eq 0 ]; then
        EMOJI=":white_check_mark:"
        STATUS="All Tests Passed"
        COLOR="good"
    elif [ $FAILED -gt 0 ]; then
        EMOJI=":x:"
        STATUS="$FAILED Test(s) Failed"
        COLOR="danger"
    else
        EMOJI=":warning:"
        STATUS="Test Run Error"
        COLOR="warning"
    fi

    # Environment label
    if [ "$ENV_STATE" = "prod" ] || [ "$ENV_STATE" = "production" ]; then
        ENV_LABEL="PROD"
    else
        ENV_LABEL="DEV"
    fi

    # Build failed tests list (truncate if too many)
    FAILED_LIST=""
    if [ -n "$FAILED_TESTS" ]; then
        FAILED_COUNT=$(echo "$FAILED_TESTS" | wc -l | tr -d ' ')
        if [ "$FAILED_COUNT" -gt 10 ]; then
            FAILED_LIST=$(echo "$FAILED_TESTS" | head -10)
            FAILED_LIST="$FAILED_LIST
... and $((FAILED_COUNT - 10)) more"
        else
            FAILED_LIST="$FAILED_TESTS"
        fi
    fi

    # Build Slack message
    MESSAGE="$EMOJI *CGD Test Suite ($ENV_LABEL)*

*Status:* $STATUS

\`\`\`
┌─────────────────────────────────┐
│  TEST RESULTS SUMMARY           │
├─────────────────────────────────┤
│  Total:    $TESTS
│  Passed:   $PASSED ✓
│  Failed:   $FAILED ✗
│  Skipped:  $SKIPPED ○
│  Duration: ${TIME}s
└─────────────────────────────────┘
\`\`\`"

    if [ -n "$FAILED_LIST" ]; then
        MESSAGE="$MESSAGE

*Failed Tests:*
\`\`\`
$FAILED_LIST
\`\`\`"
    fi

    MESSAGE="$MESSAGE

_Log: $LOG_FILE_"

    # Send to Slack using jq for proper JSON escaping
    if command -v jq &> /dev/null; then
        JSON=$(jq -n --arg text "$MESSAGE" '{text: $text}')
        curl -s -X POST -H "Content-type: application/json" --data "$JSON" "$SLACK_WEBHOOK_URL" > /dev/null
    else
        # Fallback without jq (less safe)
        curl -s -X POST -H "Content-type: application/json" \
            --data "{\"text\": $(echo "$MESSAGE" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}" \
            "$SLACK_WEBHOOK_URL" > /dev/null
    fi

    echo "Slack notification sent."
fi

# Cleanup old test results (keep last 30 days)
find "$RESULTS_DIR" -name "*.xml" -mtime +30 -delete 2>/dev/null
find "$RESULTS_DIR" -name "*.log" -mtime +30 -delete 2>/dev/null

exit $EXIT_CODE
