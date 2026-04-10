#!/usr/bin/env bash
#
# CGD Smoke Tests - Check API endpoints and frontend pages
#
# This script verifies that key pages and API endpoints are working correctly
# by checking for expected content in responses.
#
# Usage:
#   ./cgd_smoke_tests.sh              # Run all checks
#   ./cgd_smoke_tests.sh --api-only   # Run API checks only
#   ./cgd_smoke_tests.sh --web-only   # Run frontend checks only
#
# Environment Variables:
#   CGD_API_URL: API base URL (default: http://localhost:8000/api)
#   CGD_WEB_URL: Frontend base URL (default: http://localhost:3000)
#   SLACK_WEBHOOK_URL: Slack webhook for failure notifications (optional)
#   ENV_STATE: Environment label (prod/dev)
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

# Configuration - adjust these URLs for your environment
API_URL="${CGD_API_URL:-http://localhost:8000/api}"
WEB_URL="${CGD_WEB_URL:-http://localhost:3000}"
TIMEOUT=30

# Test results
TOTAL=0
PASSED=0
FAILED=0
FAILED_TESTS=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "CGD Smoke Tests"
echo "========================================"
echo "Started: $(date)"
echo "API URL: $API_URL"
echo "Web URL: $WEB_URL"
echo "========================================"
echo ""

# Function to check API endpoint
check_api() {
    local name="$1"
    local endpoint="$2"
    local expected="$3"

    TOTAL=$((TOTAL + 1))
    local url="${API_URL}${endpoint}"

    printf "  API: %-40s " "$name"

    local response
    local http_code

    # Get response and HTTP code
    response=$(curl -s -w "\n%{http_code}" --max-time $TIMEOUT "$url" 2>/dev/null)
    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | sed '$d')

    if [ "$http_code" != "200" ]; then
        echo -e "${RED}FAIL${NC} (HTTP $http_code)"
        FAILED=$((FAILED + 1))
        FAILED_TESTS="$FAILED_TESTS\n  ✗ API: $name - HTTP $http_code"
        return 1
    fi

    if ! echo "$body" | grep -q "$expected"; then
        echo -e "${RED}FAIL${NC} (missing: $expected)"
        FAILED=$((FAILED + 1))
        FAILED_TESTS="$FAILED_TESTS\n  ✗ API: $name - missing expected content"
        return 1
    fi

    echo -e "${GREEN}OK${NC}"
    PASSED=$((PASSED + 1))
    return 0
}

# Function to check frontend page
check_page() {
    local name="$1"
    local path="$2"
    local expected="$3"

    TOTAL=$((TOTAL + 1))
    local url="${WEB_URL}${path}"

    printf "  Web: %-40s " "$name"

    local response
    local http_code

    # Get response and HTTP code
    response=$(curl -s -w "\n%{http_code}" --max-time $TIMEOUT "$url" 2>/dev/null)
    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | sed '$d')

    if [ "$http_code" != "200" ]; then
        echo -e "${RED}FAIL${NC} (HTTP $http_code)"
        FAILED=$((FAILED + 1))
        FAILED_TESTS="$FAILED_TESTS\n  ✗ Web: $name - HTTP $http_code"
        return 1
    fi

    if ! echo "$body" | grep -q "$expected"; then
        echo -e "${RED}FAIL${NC} (missing: $expected)"
        FAILED=$((FAILED + 1))
        FAILED_TESTS="$FAILED_TESTS\n  ✗ Web: $name - missing expected content"
        return 1
    fi

    echo -e "${GREEN}OK${NC}"
    PASSED=$((PASSED + 1))
    return 0
}

# Parse arguments
RUN_API=true
RUN_WEB=true

if [ "$1" = "--api-only" ]; then
    RUN_WEB=false
elif [ "$1" = "--web-only" ]; then
    RUN_API=false
fi

# ============================================
# API Endpoint Checks
# ============================================
if [ "$RUN_API" = true ]; then
    echo "API Endpoint Checks:"
    echo "----------------------------------------"

    # Health check
    check_api "Health check" "/health" "ok"

    # Locus endpoints
    check_api "Locus ACT1" "/locus/ACT1" "gene_name"
    check_api "Locus GO details" "/locus/ACT1/go_details" "annotations"
    check_api "Locus phenotype details" "/locus/ACT1/phenotype_details" "results"
    check_api "Locus sequence details" "/locus/ACT1/sequence_details" "results"
    check_api "Locus protein details" "/locus/ACT1/protein_details" "results"

    # GO endpoints
    check_api "GO term" "/go/GO:0008150" "go_term"

    # Search endpoints
    check_api "Search autocomplete" "/search/autocomplete?query=act&limit=5" "results"

    # Reference endpoint
    check_api "Reference" "/reference/8349105" "reference_no"

    echo ""
fi

# ============================================
# Frontend Page Checks
# ============================================
if [ "$RUN_WEB" = true ]; then
    echo "Frontend Page Checks:"
    echo "----------------------------------------"

    # Home page
    check_page "Home page" "/" "Candida Genome Database"

    # Locus page
    check_page "Locus page (ACT1)" "/locus/ACT1" "ACT1"

    # GO page
    check_page "GO term page" "/go/GO:0008150" "GO:0008150"

    # Search page
    check_page "Feature search page" "/feature-search" "Search"

    # BLAST page
    check_page "BLAST page" "/blast" "BLAST"

    # Phenotype search page
    check_page "Phenotype search" "/phenotype-search" "Phenotype"

    echo ""
fi

# ============================================
# Summary
# ============================================
echo "========================================"
echo "SMOKE TEST RESULTS"
echo "========================================"
echo ""
printf "  %-20s %s\n" "Total:" "$TOTAL"
printf "  %-20s %s\n" "Passed:" "$PASSED"
printf "  %-20s %s\n" "Failed:" "$FAILED"
echo ""

if [ $FAILED -gt 0 ]; then
    echo "FAILED CHECKS:"
    echo "----------------------------------------"
    echo -e "$FAILED_TESTS"
    echo ""
fi

echo "========================================"
echo "Completed: $(date)"
echo "========================================"

# Send Slack notification on failure
if [ $FAILED -gt 0 ] && [ -n "$SLACK_WEBHOOK_URL" ]; then
    echo ""
    echo "Sending Slack failure notification..."

    # Environment label
    if [ "$ENV_STATE" = "prod" ] || [ "$ENV_STATE" = "production" ]; then
        ENV_LABEL="PROD"
    else
        ENV_LABEL="DEV"
    fi

    MESSAGE=":rotating_light: *CGD Smoke Test Failed ($ENV_LABEL)*

*$FAILED of $TOTAL checks failed*

\`\`\`$(echo -e "$FAILED_TESTS")\`\`\`

API: $API_URL
Web: $WEB_URL"

    if command -v jq &> /dev/null; then
        JSON=$(jq -n --arg text "$MESSAGE" '{text: $text}')
        curl -s -X POST -H "Content-type: application/json" --data "$JSON" "$SLACK_WEBHOOK_URL" > /dev/null
    else
        curl -s -X POST -H "Content-type: application/json" \
            --data "{\"text\": $(echo "$MESSAGE" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}" \
            "$SLACK_WEBHOOK_URL" > /dev/null
    fi

    echo "Slack notification sent."
fi

# Exit with failure code if any tests failed
if [ $FAILED -gt 0 ]; then
    exit 1
fi

exit 0
