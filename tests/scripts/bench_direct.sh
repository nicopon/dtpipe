ROOT_DIR="/Users/REDACTED/Source/dtpipe"
DIR="$ROOT_DIR/tests/scripts"
ARTIFACTS_DIR="$DIR/artifacts"
DTPIPE_CMD="$ROOT_DIR/dist/release/dtpipe"

MAIN_PARQUET="$ARTIFACTS_DIR/main.parquet"
REF_CSV="$ARTIFACTS_DIR/ref1_10k.csv"
REF2_CSV="$ARTIFACTS_DIR/ref2_10k.csv"

QUERY_FUSION='SELECT COUNT(*) FROM main m JOIN ref r ON m.GenerateIndex = CAST(r.Id AS BIGINT) JOIN ref2 r2 ON m.GenerateIndex = CAST(r2.Id AS BIGINT)'

# Direct Mode: DataFusion reads files directly via --src-main/--src-ref
# We MUST use --main and --ref to define the aliases for the SQL query
"$DTPIPE_CMD" \
  --xstreamer fusion-engine \
  --src-main "parquet:$MAIN_PARQUET" --main main \
  --src-ref "csv:$REF_CSV"          --ref ref \
  --src-ref "csv:$REF2_CSV"         --ref ref2 \
  --query "$QUERY_FUSION" \
  --output null
