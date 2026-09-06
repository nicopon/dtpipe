#!/bin/bash
set -e

# validate_single_engine.sh
# One engine, not two.
#
# A dry-run is the real execution on a sample with the writer neutralised. It used to be a
# second engine: DryRunAnalyzer walked rows through IDataTransformer.Transform while
# PipelineExecutor used TransformMany and Flush, and the two disagreed — a --window pipeline
# reported every row as dropped while the run wrote aggregates. That engine is deleted; this
# guard is about it not growing back.
#
# --------------------------------------------------------------------------
# Scope, stated honestly — this is a grep
# --------------------------------------------------------------------------
# It catches the plausible regression: a second execution loop written in plain sight, or the
# rendering side reaching back to the source to fetch its own rows. It does NOT catch a call
# made by reflection, one routed through a helper, or an interface renamed around it.
#
# The behavioural guard is SampleModeEquivalenceTests (CI), which asserts that what a sample
# run reports is what a real run writes. That is the test that would fail if the two paths
# diverged again. This script only makes the cheap regression cheap to detect.
#
# Deliberately NOT checked: IDataTransformer.Flush(). A grep cannot tell it from Stream.Flush()
# or TextWriter.Flush(), of which the adapters have several legitimate calls. A check with
# known false positives trains people to ignore the script, which is worse than no check.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Single Engine Validation"
echo "========================================"

# 1. The deleted analyser stays deleted.
if [ -f "$PROJECT_ROOT/src/DtPipe/DryRun/DryRunAnalyzer.cs" ]; then
    fail "src/DtPipe/DryRun/DryRunAnalyzer.cs is back — the second engine was deleted, not disabled"
fi
pass "the second engine's file is absent"

# 2. Only the executor drives transformers.
#    TransformMany has exactly one legitimate caller: the row segment runner. A second one is
#    a second engine, whatever it is called.
TM_HITS=$(grep -rn "\.TransformMany(" "$PROJECT_ROOT/src/" --include="*.cs" 2>/dev/null \
    | grep -v "/obj/" \
    | grep -v "/bin/" \
    | grep -v "src/DtPipe.Transformers/" \
    | grep -v "src/DtPipe/Services/PipelineExecutor.cs" || true)
if [ -n "$TM_HITS" ]; then
    echo "$TM_HITS"
    fail "IMultiRowTransformer.TransformMany called outside PipelineExecutor — a second row engine"
fi
pass "transformer rows are driven from one place"

# 3. The reporting side never reads the source.
#    A renderer that fetches its own rows is how the two paths drifted apart the first time:
#    it read with different batching, no limit and no sampling, then reported the result as if
#    it were the run. Everything under DryRun/ must be handed a capture, never take one.
READ_HITS=$(grep -rn "ReadBatchesAsync(\|ReadRecordBatchesAsync(" \
        "$PROJECT_ROOT/src/DtPipe/DryRun/" "$PROJECT_ROOT/src/DtPipe/Cli/DryRun/" --include="*.cs" 2>/dev/null \
    | grep -v "/obj/" | grep -v "/bin/" || true)
if [ -n "$READ_HITS" ]; then
    echo "$READ_HITS"
    fail "the sample-report side reads the source directly — it must be handed the run's capture"
fi
pass "the reporting side consumes a capture, it does not produce one"

# 4. The observer contract says render, not run.
#    The old name (RunDryRunAsync) is what let an engine hide behind a display interface.
if grep -rn "RunDryRunAsync" "$PROJECT_ROOT/src/" --include="*.cs" 2>/dev/null \
    | grep -v "/obj/" | grep -v "/bin/" > /dev/null; then
    fail "RunDryRunAsync is back on the observer — that interface renders a report, it does not run one"
fi
pass "the observer contract renders rather than runs"

echo ""
echo -e "${GREEN}Single engine checks passed.${NC}"
