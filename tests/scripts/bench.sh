DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$DIR/../.." && pwd)"
ARTIFACTS_DIR="$DIR/../artifacts"
DTPIPE_CMD="$ROOT_DIR/dist/release/dtpipe"

MAIN_PARQUET="$ARTIFACTS_DIR/main.parquet"
REF_CSV="$ARTIFACTS_DIR/ref1_10k.csv"
REF2_CSV="$ARTIFACTS_DIR/ref2_10k.csv"

QUERY_FUSION='SELECT m.*, r.Id as ref1_id, r2.Id as ref2_id FROM main m LEFT JOIN ref r ON m.GenerateIndex = CAST(r.Id AS BIGINT) LEFT JOIN ref2 r2 ON m.GenerateIndex = CAST(r2.Id AS BIGINT)'
#QUERY_FUSION='SELECT count(*) FROM main m LEFT JOIN ref r ON m.GenerateIndex = CAST(r.Id AS BIGINT) LEFT JOIN ref2 r2 ON m.GenerateIndex = CAST(r2.Id AS BIGINT) LIMIT 10000000'

"$DTPIPE_CMD" \
  --input "parquet:$MAIN_PARQUET" --alias main \
  --input "csv:$REF_CSV"          --alias ref \
  --input "csv:$REF2_CSV"         --alias ref2 \
  --xstreamer fusion-engine \
  --main main --ref ref --ref ref2 \
  --query "$QUERY_FUSION" \
  --output "arrow:$ARTIFACTS_DIR/bench_output.arrow"
#  --output null
