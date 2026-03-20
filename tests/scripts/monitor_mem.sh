#!/bin/bash
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DTPIPE_CMD="$ROOT_DIR/dist/release/dtpipe"

# Start the benchmark
bash bench.sh > bench_run.log 2>&1 &
SHELL_PID=$!

echo "Shell PID: $SHELL_PID"
DTPIPE_PID=""

# Wait for dtpipe to start
while [ -z "$DTPIPE_PID" ]; do
  DTPIPE_PID=$(pgrep -P $SHELL_PID dtpipe)
  sleep 0.5
done

echo "Monitoring dtpipe PID: $DTPIPE_PID"
while kill -0 $DTPIPE_PID 2>/dev/null; do
  ps -o rss= -p $DTPIPE_PID | awk '{print $1 / 1024 " MB"}'
  sleep 2
done

wait $SHELL_PID
echo "Benchmark finished."
