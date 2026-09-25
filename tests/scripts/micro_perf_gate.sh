#!/usr/bin/env bash
# =============================================================================
# micro_perf_gate.sh
# The micro stage of the three-tier performance gate: BenchmarkDotNet in-process
# on the hot conversion paths, no infrastructure. Local only, on the reference
# machine, run deliberately before an engine change. Not wired into CI: shared
# runners give no stable measure, so no fixed threshold separates their noise
# from a real regression.
#
# The two macro stages (complete: Oracle + SQL Server; light: file<->file +
# PostgreSQL) live in the dtpipe-sandbox repository, local only.
#
# --------------------------------------------------------------------------
# The machine-fingerprint rule
# --------------------------------------------------------------------------
# A baseline records the machine it was measured on. Against a baseline from
# other hardware the difference is mostly hardware, so a verdict would mislead:
#
#   - Same fingerprint  -> a verdict is rendered at whatever threshold is asked.
#   - Different one     -> the gate REFUSES (exit 2) and renders no verdict,
#                          unless --allow-foreign-host is passed explicitly.
#   - --allow-foreign-host clamps the threshold to no tighter than
#     $FOREIGN_HOST_MIN_THRESHOLD %. That mode detects a x2, not a +15 %.
#
# --------------------------------------------------------------------------
# Usage
# --------------------------------------------------------------------------
#   ./tests/scripts/micro_perf_gate.sh --update
#       Run the benchmarks and write tests/DtPipe.Benchmarks/baselines/micro_perf.json.
#
#   ./tests/scripts/micro_perf_gate.sh
#       Run and compare against that baseline. Strict: refuses a foreign host.
#
#   ./tests/scripts/micro_perf_gate.sh --allow-foreign-host --threshold 100
#       A deliberate cross-machine check, wide threshold, run by hand.
#
#   ./tests/scripts/micro_perf_gate.sh --report-only
#       Run and print the numbers, compare nothing, never fail.
#
# Options:
#   --threshold PCT        Regression tolerance in percent   (default: 100)
#   --filter GLOB          BenchmarkDotNet filter            (default: *)
#   --allow-foreign-host   Compare across machines, clamped threshold
#   --update               Record the current run as the baseline
#   --report-only          Measure and print, no comparison
#   --baseline FILE        Baseline path (default: <bench project>/baselines/micro_perf.json)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BENCH_PROJECT="$ROOT_DIR/tests/DtPipe.Benchmarks"
# BenchmarkDotNet writes artifacts relative to the *current working directory*, not
# to the project — so the path is pinned with --artifacts rather than guessed, and
# the gate reads back from the same place whatever directory it was invoked from.
ARTIFACTS_ROOT="$BENCH_PROJECT/BenchmarkDotNet.Artifacts"
ARTIFACTS_DIR="$ARTIFACTS_ROOT/results"

# Lives with the benchmark project, not in tests/scripts/baselines/ — that
# directory holds golden *data* fixtures, which this is not.
BASELINE_FILE="$BENCH_PROJECT/baselines/micro_perf.json"
THRESHOLD=100
FILTER="*"
ALLOW_FOREIGN_HOST=false
UPDATE=false
REPORT_ONLY=false

# Below this, a cross-machine verdict says more about the hardware than the code.
FOREIGN_HOST_MIN_THRESHOLD=50

# Exit codes: 0 pass · 1 regression · 2 refused to render a verdict · 3 setup error
EXIT_PASS=0
EXIT_REGRESSION=1
EXIT_REFUSED=2
EXIT_ERROR=3

if [ -t 1 ] && [ "${NO_COLOR:-}" != "1" ]; then
    GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
else
    GREEN=''; YELLOW=''; RED=''; CYAN=''; NC=''
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --threshold)          THRESHOLD="$2"; shift 2 ;;
        --filter)             FILTER="$2"; shift 2 ;;
        --baseline)           BASELINE_FILE="$2"; shift 2 ;;
        --allow-foreign-host) ALLOW_FOREIGN_HOST=true; shift ;;
        --update)             UPDATE=true; shift ;;
        --report-only)        REPORT_ONLY=true; shift ;;
        -h|--help)            awk 'NR > 1 && !/^#/ { exit } NR > 1' "$0"; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit $EXIT_ERROR ;;
    esac
done

command -v python3 >/dev/null 2>&1 || {
    echo -e "${RED}python3 is required (JSON handling).${NC}" >&2
    exit $EXIT_ERROR
}

# =============================================================================
# Machine fingerprint — what makes two baselines comparable, or not.
# Deliberately coarse: OS, architecture, CPU model, core count. Anything finer
# (clock speed, kernel build) would refuse comparisons that are in fact valid.
# =============================================================================
machine_fingerprint() {
    local os arch cpu cores
    os="$(uname -s)"
    arch="$(uname -m)"
    cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo '?')"
    if [ "$os" = "Darwin" ]; then
        cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo '?')"
    else
        cpu="$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || echo '?')"
        [ -z "$cpu" ] && cpu="$(uname -p 2>/dev/null || echo '?')"
    fi
    echo "${os}/${arch}/${cpu}/${cores}c"
}

FINGERPRINT="$(machine_fingerprint)"

echo ""
echo -e "${CYAN}================================================${NC}"
echo -e "${CYAN}  Micro performance gate — BenchmarkDotNet${NC}"
echo -e "${CYAN}================================================${NC}"
echo -e "  Host:      ${FINGERPRINT}"
echo -e "  Filter:    ${FILTER}"
echo -e "  Baseline:  ${BASELINE_FILE}"
echo ""

# =============================================================================
# Run the benchmarks
# =============================================================================
rm -rf "$ARTIFACTS_DIR"

echo -e "${YELLOW}Running benchmarks (Release, in-process)...${NC}"
if ! dotnet run -c Release --project "$BENCH_PROJECT" -- \
        --filter "$FILTER" --artifacts "$ARTIFACTS_ROOT"; then
    echo -e "${RED}Benchmark run failed.${NC}" >&2
    exit $EXIT_ERROR
fi

shopt -s nullglob
REPORT_FILES=("$ARTIFACTS_DIR"/*-report-full.json)
shopt -u nullglob

if [ ${#REPORT_FILES[@]} -eq 0 ]; then
    echo -e "${RED}No BenchmarkDotNet JSON report found in $ARTIFACTS_DIR.${NC}" >&2
    echo -e "${RED}InProcessConfig must register JsonExporter.Full.${NC}" >&2
    exit $EXIT_ERROR
fi

# =============================================================================
# Collect { "Class.Method": { mean_ns, stddev_ns, allocated_bytes } }
# =============================================================================
MEASURED_JSON="$(mktemp)"
trap 'rm -f "$MEASURED_JSON"' EXIT

python3 - "$MEASURED_JSON" "$FINGERPRINT" "${REPORT_FILES[@]}" <<'PY'
import json, sys, datetime

out_path, fingerprint = sys.argv[1], sys.argv[2]
results = {}

for path in sys.argv[3:]:
    with open(path) as fh:
        doc = json.load(fh)
    for bench in doc.get("Benchmarks", []):
        stats = bench.get("Statistics") or {}
        mean = stats.get("Mean")
        if mean is None:
            continue
        key = f"{bench.get('Type', '?')}.{bench.get('Method', '?')}"
        memory = bench.get("Memory") or {}
        results[key] = {
            "mean_ns": round(mean, 3),
            "min_ns": round(stats.get("Min", mean), 3),
            "stddev_ns": round(stats.get("StandardDeviation", 0.0), 3),
            "allocated_bytes": memory.get("BytesAllocatedPerOperation"),
        }

json.dump(
    {
        "recorded": datetime.datetime.now(datetime.timezone.utc)
                    .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "machine_fingerprint": fingerprint,
        "benchmarks": dict(sorted(results.items())),
    },
    open(out_path, "w"),
    indent=2,
)
print(f"Collected {len(results)} benchmark result(s).")
PY

# =============================================================================
# --update: record and stop
# =============================================================================
if [ "$UPDATE" = true ]; then
    mkdir -p "$(dirname "$BASELINE_FILE")"
    cp "$MEASURED_JSON" "$BASELINE_FILE"
    echo ""
    echo -e "${GREEN}Baseline written: $BASELINE_FILE${NC}"
    echo -e "  Fingerprint: ${FINGERPRINT}"
    echo -e "  ${YELLOW}This baseline is only strictly comparable on this machine.${NC}"
    exit $EXIT_PASS
fi

# =============================================================================
# --report-only: print and stop
# =============================================================================
if [ "$REPORT_ONLY" = true ]; then
    python3 - "$MEASURED_JSON" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
print()
print(f"{'Benchmark':<62} {'Mean':>14} {'Alloc':>12}")
print("-" * 90)
for name, v in doc["benchmarks"].items():
    alloc = v["allocated_bytes"]
    alloc_s = f"{alloc:,} B" if alloc is not None else "-"
    print(f"{name:<62} {v['mean_ns']:>11,.0f} ns {alloc_s:>12}")
PY
    exit $EXIT_PASS
fi

# =============================================================================
# Comparison
# =============================================================================
if [ ! -f "$BASELINE_FILE" ]; then
    echo ""
    echo -e "${YELLOW}No baseline at $BASELINE_FILE.${NC}"
    echo -e "${YELLOW}Record one with: $0 --update${NC}"
    exit $EXIT_REFUSED
fi

BASELINE_FINGERPRINT="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("machine_fingerprint","?"))' "$BASELINE_FILE")"

EFFECTIVE_THRESHOLD="$THRESHOLD"
CROSS_MACHINE=false

if [ "$FINGERPRINT" != "$BASELINE_FINGERPRINT" ]; then
    CROSS_MACHINE=true
    echo ""
    echo -e "${YELLOW}Machine fingerprint differs from the baseline:${NC}"
    echo -e "  baseline: ${BASELINE_FINGERPRINT}"
    echo -e "  current:  ${FINGERPRINT}"

    if [ "$ALLOW_FOREIGN_HOST" != true ]; then
        echo ""
        echo -e "${RED}REFUSED — no verdict rendered.${NC}"
        echo -e "${RED}Comparing durations measured on different hardware does not give a${NC}"
        echo -e "${RED}weaker verdict, it gives a misleading one: most of the difference${NC}"
        echo -e "${RED}between the two numbers would be the machine, not the code.${NC}"
        echo ""
        echo -e "  Either record a baseline here    : $0 --update"
        echo -e "  or accept a x2-only comparison   : $0 --allow-foreign-host"
        exit $EXIT_REFUSED
    fi

    if [ "$EFFECTIVE_THRESHOLD" -lt "$FOREIGN_HOST_MIN_THRESHOLD" ]; then
        echo -e "${YELLOW}Threshold ${EFFECTIVE_THRESHOLD}% clamped to ${FOREIGN_HOST_MIN_THRESHOLD}%: below that,${NC}"
        echo -e "${YELLOW}a cross-machine verdict describes the hardware, not the change.${NC}"
        EFFECTIVE_THRESHOLD="$FOREIGN_HOST_MIN_THRESHOLD"
    fi
    echo -e "${YELLOW}Proceeding cross-machine at ${EFFECTIVE_THRESHOLD}% — detects a x2, not a +15%.${NC}"
fi

echo ""
set +e
python3 - "$BASELINE_FILE" "$MEASURED_JSON" "$EFFECTIVE_THRESHOLD" "$CROSS_MACHINE" <<'PY'
import json, sys

baseline = json.load(open(sys.argv[1]))["benchmarks"]
current = json.load(open(sys.argv[2]))["benchmarks"]
threshold = float(sys.argv[3])
cross_machine = sys.argv[4] == "true"

rows = []
for name, base in baseline.items():
    if name not in current or base["mean_ns"] <= 0:
        continue
    b = base["mean_ns"]
    c = current[name]["mean_ns"]
    rows.append((name, b, c, (c - b) / b * 100.0))

missing = [n for n in baseline if n not in current]
added = [n for n in current if n not in baseline]
regressions = [r for r in rows if r[3] > threshold]

print(f"{'Benchmark':<58} {'baseline':>12} {'current':>12} {'delta':>9}")
print("-" * 94)
for name, b, c, d in sorted(rows, key=lambda r: -r[3]):
    flag = "  REGRESSION" if d > threshold else ("  faster" if d < -threshold else "")
    print(f"{name:<58} {b:>9,.0f} ns {c:>9,.0f} ns {d:>+8.1f}%{flag}")

label = "cross-machine" if cross_machine else "same machine"
print()
print(f"Threshold: {threshold:.0f}% ({label})")
if missing:
    print(f"In baseline but not measured: {', '.join(missing)}")
if added:
    print(f"New, not yet in baseline:     {', '.join(added)}")

print()
if regressions:
    print(f"FAIL - {len(regressions)} benchmark(s) slower than the baseline by more than {threshold:.0f}%.")
    sys.exit(1)
print("PASS - no benchmark exceeds the threshold.")
sys.exit(0)
PY
COMPARE_STATUS=$?
set -e

if [ $COMPARE_STATUS -ne 0 ]; then
    echo -e "${RED}Micro performance gate: FAIL${NC}"
    exit $EXIT_REGRESSION
fi

echo -e "${GREEN}Micro performance gate: PASS${NC}"
exit $EXIT_PASS
