#!/bin/bash
# Build DtPipe with experimental features enabled (DataFusion native bridge).
# Equivalent to: DTPIPE_EXPERIMENTAL=1 ./build.sh
DTPIPE_EXPERIMENTAL=1 exec "$(dirname "$0")/build.sh" "$@"
