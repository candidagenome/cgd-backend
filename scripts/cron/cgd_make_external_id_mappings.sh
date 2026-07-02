#!/usr/bin/env bash
#
# Regenerate the External_id_mappings download files for all CGD species:
#   - gp2protein_<species>.gz (6 species) + gp2protein.cgd.gz (master)
#   - CGDID_2_GeneID.tab.gz
#   - CGDID_2_RefSeqID.tab.gz
#
# Files are written to $DOWNLOAD_DIR/External_id_mappings (each generator
# archives the previous version to archive/ before replacing it).
#
# Note: CGDID_2_RefSeqID streams NCBI gene2refseq (~2.3 GB) on each run.
# This job regenerates the files from the current database; it does NOT refresh
# the C. albicans Entrez Gene ID cross-references in the database -- run
# scripts/refresh_albicans_entrez_geneids.py for that (heavier, less frequent).
#
# Usage:
#   ./slack-cron.sh ./cgd_make_external_id_mappings.sh

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

echo "CGD External_id_mappings Generation"
echo "Generated: $(date)"
echo "========================================"

errors=0

run_step() {
    local label=$1
    shift
    echo ""
    echo "Running $label..."
    if ! python3 "$@" 2>&1 | grep -v "^[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}.*-" | grep -v "^$"; then
        echo "ERROR: $label failed"
        errors=$((errors + 1))
    fi
}

run_step "gp2protein (6 species + master)" "$SCRIPT_DIR/make_gp2protein.py" --all
run_step "CGDID_2_GeneID"                  "$SCRIPT_DIR/make_cgdid_2_geneid.py"
run_step "CGDID_2_RefSeqID"                "$SCRIPT_DIR/make_cgdid_2_refseqid.py"

echo ""
echo "========================================"

if [ $errors -gt 0 ]; then
    echo "ERROR: $errors step(s) failed"
    exit 1
fi

echo "All External_id_mappings files generated successfully."
exit 0
