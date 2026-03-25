#!/bin/bash

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
DTPIPE="$SCRIPT_DIR/../../dist/release/dtpipe"
LOG_FILE="$ARTIFACTS_DIR/run_catalog_tests.log"

# Cleanup previous logs
rm -f "$LOG_FILE"
touch "$LOG_FILE"

echo "===================================================="
echo "   dtpipe 135-Command Test Runner"
echo "===================================================="
echo "Starting at $(date)"
echo ""

# Helper to run a test.
# Tests listed in the EXPECTED_ERROR regex should return a non-zero exit code.
# Any test NOT in the regex is expected to succeed (exit 0).
run_test() {
    local id=$1
    local cmd=$2

    echo -n "Test $id: "
    echo "----------------------------------------------------" >> "$LOG_FILE"
    echo "TEST $id: $cmd" >> "$LOG_FILE"
    echo "----------------------------------------------------" >> "$LOG_FILE"

    cd "$SCRIPT_DIR"
    eval "$cmd" >> "$LOG_FILE" 2>&1
    local status=$?

    # Tests in this list are EXPECTED to fail (non-zero exit code = PASS)
    if [[ "$id" =~ ^T(76|77|78|79|80|81|82|83|84|85|86|87|88|89|90|129|130|131|132|133|134|135)$ ]]; then
        if [ $status -eq 0 ]; then
            echo -e "\e[31mFAILED (Expected error but got success)\e[0m"
            return 1
        else
            echo -e "\e[32mPASSED (Expected error encountered)\e[0m"
            return 0
        fi
    else
        if [ $status -eq 0 ]; then
            echo -e "\e[32mPASSED\e[0m"
            
            # --- YAML Round-trip Verification ---
            if [ "$VERIFY_YAML" == "1" ]; then
                # Only verify if it's a success case and not a complex command already using --job or --alias/--from/--sql
                if [[ ! "$cmd" =~ "--job" ]] && [[ ! "$cmd" =~ "--alias" ]] && [[ ! "$cmd" =~ "--from" ]] && [[ ! "$cmd" =~ "--sql" ]] && [[ ! "$cmd" =~ "--export-job" ]]; then
                    local yaml_file="$ARTIFACTS_DIR/verify_$id.yaml"
                    echo "  -> YAML Export/Import Check..."
                    
                    # 1. Export to YAML
                    eval "$cmd --export-job $yaml_file" > /dev/null 2>&1
                    if [ $? -eq 0 ]; then
                        # 2. Run from YAML
                        echo "  -> YAML Verification execution..." >> "$LOG_FILE"
                        eval "$DTPIPE --job $yaml_file" >> "$LOG_FILE" 2>&1
                        if [ $? -eq 0 ]; then
                            echo -e "     \e[32mYAML OK\e[0m"
                        else
                            echo -e "     \e[31mYAML EXECUTION FAILED\e[0m"
                            return 1
                        fi
                    else
                        echo -e "     \e[31mYAML EXPORT FAILED\e[0m"
                        return 1
                    fi
                fi
            fi
            
            return 0
        else
            echo -e "\e[31mFAILED (Exit code $status)\e[0m"
            return 1
        fi
    fi
}

# Connection string shortcuts (DRY)
PG="pg:Host=localhost;Port=5440;Database=integration;Username=postgres;Password=password"
MSSQL="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;Encrypt=False"
ORA="ora:Data Source=localhost:1522/FREEPDB1;User Id=testuser;Password=password"

# 0. Setup
mkdir -p artifacts/split

# 1. Fundamental Functionality
# T1: Basic conversion: CSV source → Parquet target (single-hop format change)
run_test "T1" "$DTPIPE -i artifacts/test_data.csv -o artifacts/output_t1.parquet"

# T2: PostgreSQL write: Parquet → PG with Recreate strategy
run_test "T2" "$DTPIPE -i artifacts/test_data.parquet -o \"$PG\" --table \"output_t2\" --strategy Recreate"
# T3: PostgreSQL read + JS filter on string property (tests null-safe evaluation)
run_test "T3" "$DTPIPE -i \"$PG\" -q \"SELECT * FROM users_test\" --filter \"row.username.length > 5\" -o artifacts/output_t3.csv"
# T4: SQL Server read + mask transformer on credit card column
run_test "T4" "$DTPIPE -i \"$MSSQL\" -q \"SELECT * FROM users_test\" --mask \"credit_card:####-####-####-1234\" -o artifacts/output_t4.parquet"
# T5: Oracle read via --table + fake anonymization of FULL_NAME column
run_test "T5" "$DTPIPE -i \"$ORA\" --table \"USERS_TEST_DATA\" --fake \"FULL_NAME:name.fullName\" -o artifacts/output_t5.csv"
# T6: CSV → DuckDB with compute: derives a new FullName column
run_test "T6" "$DTPIPE -i artifacts/test_data.csv --compute \"FullName:row.FirstName + ' ' + row.LastName\" -o artifacts/output_t6.duckdb --table \"output_t6\" --strategy Recreate"
# T7: Force null on Category, hardcode Price to 0.0 with --overwrite
run_test "T7" "$DTPIPE -i artifacts/test_data.parquet --null \"Category\" --overwrite \"Price:0.0\" -o artifacts/output_t7.csv"
# T8: Reproducible row sampling with a fixed seed
run_test "T8" "$DTPIPE -i artifacts/test_data.csv --sampling-rate 0.1 --sampling-seed 42 -o artifacts/output_t8.csv"
# T9: Limit rows from Arrow source
run_test "T9" "$DTPIPE -i artifacts/test_data.arrow --limit 50 -o artifacts/output_t9.csv"
# T10: Drop multiple columns by name
run_test "T10" "$DTPIPE -i artifacts/test_data.csv --drop \"Email\" --drop \"Company\" -o artifacts/output_t10.parquet"
# T11: Rename two columns in one pass
run_test "T11" "$DTPIPE -i artifacts/test_data.csv --rename \"FirstName:Prenom\" --rename \"LastName:Nom\" -o artifacts/output_t11.csv"
# T12: Format transformer: inject Id into a string template
run_test "T12" "$DTPIPE -i artifacts/test_data.parquet --format \"Id:USR-{Id}\" -o artifacts/output_t12.csv"
# T13: Strict schema validation: PG → Parquet (must pass compatibility check)
run_test "T13" "$DTPIPE -i \"$PG\" -q \"SELECT * FROM users_test\" --strict-schema -o artifacts/output_t13.parquet"
# T14: Small batch size (10) to exercise the batching infrastructure
run_test "T14" "$DTPIPE -i artifacts/test_data.csv --batch-size 10 -o artifacts/output_t14.parquet"
# T15: Compute with parseInt for explicit numeric type coercion
run_test "T15" "$DTPIPE -i artifacts/test_data.csv --compute \"Score:parseInt(row.Score) || 0\" -o artifacts/output_t15.arrow"
# T16: Filter using JS String.includes() method
run_test "T16" "$DTPIPE -i artifacts/test_data.csv --filter \"row.Email.includes('@gmail.com')\" -o artifacts/output_t16.csv"
# T17: Fake regenerates an existing column with a Bogus commerce provider
run_test "T17" "$DTPIPE -i artifacts/test_data.parquet --fake \"Category:commerce.productName\" -o artifacts/output_t17.parquet"
# T18: Compute extracts year from BirthDate using JS Date object
run_test "T18" "$DTPIPE -i artifacts/test_data.csv --compute \"Year:new Date(row.BirthDate).getFullYear()\" -o artifacts/output_t18.csv"
# T19: Arrow source → PostgreSQL write with Truncate strategy
run_test "T19" "$DTPIPE -i artifacts/test_data.arrow -o \"$PG\" --table \"output_t19\" --strategy Truncate"
# T20: Limit 0 produces an empty output file (boundary edge case)
run_test "T20" "$DTPIPE -i artifacts/test_data.csv --limit 0 -o artifacts/output_t20.csv"

# 2. Advanced Pipelines & SQL Processors
# T21: DataFusion JOIN between Parquet and CSV on shared Id column
run_test "T21" "$DTPIPE -i artifacts/test_data.parquet --alias p -i artifacts/test_data.csv --alias c --from p --ref c --sql \"SELECT p.*, c.email FROM p JOIN c ON p.id = c.id\" -o artifacts/output_t21.parquet"
# T22: DataFusion aggregation: count(*) and avg() from PostgreSQL
run_test "T22" "$DTPIPE -i \"$PG\" -q \"SELECT * FROM users_test\" --alias db --from db --sql \"SELECT count(*) as total, avg(length(username)) FROM db\" -o artifacts/output_t22.csv"
# T23: DataFusion SQL filter on big Parquet (server-side pushdown)
run_test "T23" "$DTPIPE -i artifacts/test_data_big.parquet --alias b --from b --sql \"SELECT * FROM b WHERE value > 50000 LIMIT 10\" -o artifacts/output_t23.arrow"
# T24: Chained transformers: fake regenerates Email, mask immediately masks it
run_test "T24" "$DTPIPE -i artifacts/test_data.csv --fake \"Email:internet.email\" --mask \"Email:####@####.##\" -o artifacts/output_t24.csv"
# T25: Window transformer: compute a per-window running average of Score
run_test "T25" "$DTPIPE -i artifacts/test_data.csv --window-count 100 --window-script \"rows.map(r => ({ ...r, AvgScore: rows.reduce((acc, curr) => acc + curr.Score, 0)/100 }))\" -o artifacts/output_t25.csv"
# T26: DataFusion CROSS JOIN between two generated sources
run_test "T26" "$DTPIPE -i \"generate:100\" --alias g1 -i \"generate:50\" --alias g2 --from g1 --ref g2 --sql \"SELECT g1.* FROM g1 CROSS JOIN g2\" -o artifacts/output_t26.parquet"
# T27: Compute ternary with explicit output type annotation via --compute-types
run_test "T27" "$DTPIPE -i artifacts/test_data.csv --compute \"Type:row.Score > 500 ? 'H' : 'L'\" --compute-types \"Type:string\" -o artifacts/output_t27.parquet"
# T28: Fake generation seeded by a stable column to ensure reproducibility
run_test "T28" "$DTPIPE -i artifacts/test_data.parquet --fake \"Name:name.fullName\" --fake-seed-column \"Id\" -o artifacts/output_t28.csv"
# T29: Expand transformer: explodes a string into one row per character
run_test "T29" "$DTPIPE -i artifacts/test_data.csv --expand \"row.FirstName.split('').map(c => ({...row, Char: c}))\" -o artifacts/output_t29.arrow"

echo -e "\n### DAG & Fan-out Tests ###"
# T30: DAG fan-out: one source routed to two different file targets simultaneously
run_test "T30" "$DTPIPE -i artifacts/test_data.csv --alias src --from src -o artifacts/output_t30_a.parquet --from src -o artifacts/output_t30_b.csv"
# T31: Compute serializes the entire row as a JSON object into a new column
run_test "T31" "$DTPIPE -i artifacts/test_data.csv --compute \"Data:row\" -o artifacts/output_t31.csv"
# T32: DataFusion heterogeneous JOIN: PostgreSQL × SQL Server 
run_test "T32" "$DTPIPE -i \"$PG\" -q \"SELECT * FROM users_test\" --alias p -i \"$MSSQL\" -q \"SELECT * FROM users_test\" --alias m --from p --ref m --sql \"SELECT p.*, m.credit_card FROM p JOIN m ON p.id = m.id\" -o artifacts/output_t32.csv"
# T33: DataFusion SQL predicate filter on Parquet with string equality
run_test "T33" "$DTPIPE -i artifacts/test_data.parquet --alias main --from main --sql \"SELECT * FROM main WHERE category = 'Electronics'\" -o artifacts/output_t33.parquet"
# T34: DataFusion with upstream sampling applied before SQL execution
run_test "T34" "$DTPIPE -i artifacts/test_data_big.parquet --alias main --sampling-rate 0.01 --from main --sql \"SELECT * FROM main ORDER BY value DESC\" -o artifacts/output_t34.csv"
# T35: Fake clone: source one column value into a new column via fake literal
run_test "T35" "$DTPIPE -i artifacts/test_data.csv --fake \"Id:random.guid\" --fake \"IdClone:Id\" -o artifacts/output_t35.csv"
# T36: Compute with string functions on Arrow data (force string conversion)
run_test "T36" "$DTPIPE -i artifacts/test_data.arrow --compute \"ShortId:String(row.Id).substring(0,8).toUpperCase()\" -o artifacts/output_t36.csv"
# T37: Filter with modulo: keep only rows where Score is even
run_test "T37" "$DTPIPE -i artifacts/test_data.csv --filter \"parseInt(row.Score) % 2 == 0\" -o artifacts/output_t37.parquet"
# T38: Compute toUpperCase string transformation on a column
run_test "T38" "$DTPIPE -i artifacts/test_data.parquet --compute \"Label:row.Category.toUpperCase()\" -o artifacts/output_t38.csv"
# T39: Export pipeline config to YAML job file for later reuse
run_test "T39" "$DTPIPE -i artifacts/test_data.csv --limit 10 --export-job artifacts/output_t39.yaml"
# T40: Reload and execute the exported job file produced by T39
run_test "T40" "$DTPIPE --job artifacts/output_t39.yaml"
# T41: DataFusion JOIN between CSV and DuckDB table on shared Id
run_test "T41" "$DTPIPE -i artifacts/test_data.csv --alias c -i artifacts/test_data.duckdb --table \"geography\" --alias g --from c --ref g --sql \"SELECT c.*, g.city FROM c JOIN g ON c.id = g.id\" -o artifacts/output_t41.parquet"
# T42: Compute with Math.min to clamp Score at 500
run_test "T42" "$DTPIPE -i artifacts/test_data.csv --compute \"Score:Math.min(row.Score, 500)\" -o artifacts/output_t42.csv"
# T43: DataFusion window function: count(*) over() applied to a PG result
run_test "T43" "$DTPIPE -i \"$PG\" -q \"SELECT * FROM users_test\" --alias p --from p --sql \"SELECT username, count(*) over() as total FROM p\" -o artifacts/output_t43.csv"
# T44: Fake with JS Date.now() to inject a synthetic metadata column
run_test "T44" "$DTPIPE -i artifacts/test_data.parquet --fake \"Meta:{\\\"source\\\": \\\"parquet\\\", \\\"time\\\": Date.now()}\" -o artifacts/output_t44.csv"
# T45: Write to Postgres with --ignore-nulls: null cells are skipped on insert
run_test "T45" "$DTPIPE -i artifacts/test_data.csv --null \"Id\" --ignore-nulls --no-schema-validation -o \"$PG\" --table \"users_test\" --strategy Append"
# T46: DataFusion passthrough on big dataset (with upstream limit)
run_test "T46" "$DTPIPE -i artifacts/test_data_big.parquet --limit 1000 --alias b --from b --sql \"SELECT * FROM b\" -o artifacts/output_t46.arrow"
# T47: Compute boolean column by comparing BirthDate to a threshold date
run_test "T47" "$DTPIPE -i artifacts/test_data.csv --compute \"IsOld:new Date(row.BirthDate) < new Date('2000-01-01')\" -o artifacts/output_t47.parquet"
# T48: Overwrite with JS expression: inflate Price column by 20%
run_test "T48" "$DTPIPE -i artifacts/test_data.parquet --overwrite \"Price:row.Price * 1.2\" -o artifacts/output_t48.csv"
# T49: DAG split by predicate: high-score and low-score rows to separate files
run_test "T49" "$DTPIPE -i artifacts/test_data.csv --alias s --from s --filter 'row.Score>500' -o artifacts/output_t49_high.csv --from s --filter 'row.Score<=500' -o artifacts/output_t49_low.csv"
# T50: Compute from an external JS script file (tests @ file-path injection)
run_test "T50" "$DTPIPE -i artifacts/test_data.csv --compute \"@artifacts/my_script.js\" -o artifacts/T50_ext.parquet"

# 3. Performance & Volumetric
# T51: Full CSV dump of big Parquet (large sequential file I/O)
run_test "T51" "$DTPIPE -i artifacts/test_data_big.parquet -o artifacts/output_t51.csv"
# T52: Big Parquet → PostgreSQL with Recreate (large SQL bulk insert)
run_test "T52" "$DTPIPE -i artifacts/test_data_big.parquet -o \"$PG\" --table \"output_t52\" --strategy Recreate"
# T53: Big Parquet → DuckDB (native columnar format perf)
run_test "T53" "$DTPIPE -i artifacts/test_data_big.parquet -o artifacts/output_t53.duckdb --table \"output_t53\" --strategy Recreate"
# T54: PostgreSQL big table export → Parquet
run_test "T54" "$DTPIPE -i \"$PG\" --table \"output_t52\" -o artifacts/output_t54.parquet"
# T55: JS compute on big dataset to null (measures row-engine throughput)
run_test "T55" "$DTPIPE -i artifacts/test_data_big.parquet --compute \"V:row.Value * 1.5\" -o null"
# T56: DataFusion aggregation on big Parquet: count(*) + max(Value)
run_test "T56" "$DTPIPE -i artifacts/test_data_big.parquet --alias main --from main --sql \"SELECT count(*), max(value) FROM main\" -o artifacts/output_t56.csv"
# T57: Big Parquet → SQL Server with large batch size (50k rows/batch)
run_test "T57" "$DTPIPE -i artifacts/test_data_big.parquet -o \"$MSSQL\" --table \"output_t57\" --strategy Recreate --batch-size 50000"
# T58: Random 10% sampling on big dataset → Parquet
run_test "T58" "$DTPIPE -i artifacts/test_data_big.parquet --sampling-rate 0.1 -o artifacts/output_t58.parquet"
# T59: Big Parquet → Oracle with Recreate (large Oracle bulk write)
run_test "T59" "$DTPIPE -i artifacts/test_data_big.parquet -o \"$ORA\" --table \"OUTPUT_T59\" --strategy Recreate --insert-mode Bulk"
# T60: Generate 5M rows in-memory → null sink (measures generator throughput)
run_test "T60" "$DTPIPE -i \"generate:5000000\" -o null --batch-size 100000"
# T61: Mask transformer on big dataset (measures masking throughput)
run_test "T61" "$DTPIPE -i artifacts/test_data_big.parquet --mask \"Id:####-####\" -o null"
# T62: Format transformer on big dataset (measures string formatting throughput)
run_test "T62" "$DTPIPE -i artifacts/test_data_big.parquet --format \"Log:VALUE={Value} TIME={Timestamp}\" -o null"
# T63: Big Parquet → Arrow IPC (columnar passthrough, measures encoding perf)
run_test "T63" "$DTPIPE -i artifacts/test_data_big.parquet -o artifacts/output_t63.arrow"
# T64: Big Arrow IPC → Parquet (Arrow as source, measures IPC decoding)
run_test "T64" "$DTPIPE -i artifacts/output_t63.arrow -o artifacts/output_t64.parquet"
# T65: Compute + filter chain on big dataset (measures segmented pipeline throughput)
run_test "T65" "$DTPIPE -i artifacts/test_data_big.parquet --compute \"Val:parseInt(row.Value)\" --filter \"Val > 500\" -o null"
# T66: Output split across multiple files using --prefix pattern
run_test "T66" "$DTPIPE -i artifacts/test_data_big.parquet -o artifacts/split/ -p \"prefix_{batch}.parquet\""
# T67: Generate 1M rows with UUID fake column (generator + fake throughput)
run_test "T67" "$DTPIPE -i \"generate:1M\" --fake \"uuid:random.guid\" -o artifacts/big_uuids.csv"
# T68: DataFusion passthrough on big Parquet to null (SQL processor overhead baseline)
run_test "T68" "$DTPIPE -i artifacts/test_data_big.parquet --alias main --from main --sql \"SELECT * FROM main\" -o null"
# T69: PostgreSQL big table → CSV (measures PG read + CSV write throughput)
run_test "T69" "$DTPIPE -i \"$PG\" -q \"SELECT * FROM output_t52\" -o artifacts/output_t69.csv"
# T70: Big CSV → PostgreSQL (measures CSV read + PG bulk insert throughput)
run_test "T70" "$DTPIPE -i artifacts/output_t69.csv -o \"$PG\" --table \"output_t70\" --strategy Recreate"
# T71: Generate 100k rows then sample to ~100 (combined generator + sampler)
run_test "T71" "$DTPIPE -i \"generate:100k\" --sampling-rate 0.001 -o null"
# T72: Limit 10 on big Parquet (fast HEAD operation, measures startup cost)
run_test "T72" "$DTPIPE -i artifacts/test_data_big.parquet --limit 10 -o artifacts/output_t72.csv"
# T73: Throttle to 10k rows/sec (tests flow control mechanism)
run_test "T73" "$DTPIPE -i artifacts/test_data_big.parquet --throttle 10000 -o null"
# T74: PostgreSQL generate_series(1,1M) → null (DB-side generator throughput)
run_test "T74" "$DTPIPE -i \"$PG\" -q \"SELECT generate_series(1,1000000)\" -o null"
# T75: Batch size of 1 row (extreme stress test for batching machinery)
run_test "T75" "$DTPIPE -i artifacts/test_data_big.parquet --batch-size 1 -o null"

# 4. Corner Cases & Errors (T76-T90: expected to fail — non-zero exit = PASS)
# T76: Non-existent source file: must fail with file-not-found error
run_test "T76" "$DTPIPE -i non_existent.csv -o out.csv"
# T77: Write to directory with no write permissions: must fail with access error
run_test "T77" "$DTPIPE -i artifacts/test_data.csv -o artifacts/restricted/test.csv"
# T78: Unreachable PostgreSQL host: must fail with connection error
run_test "T78" "$DTPIPE -i \"pg:Host=badhost\" -o null"
# T79: Compute accesses a property on undefined/null: must fail with JS error
run_test "T79" "$DTPIPE -i artifacts/test_data.csv --compute \"Err:row.Missing.Prop\" -o null"
# T80: Strict schema rejects incompatible column types on existing PG table
run_test "T80" "$DTPIPE -i artifacts/test_data.parquet --strict-schema -o \"$PG\" --table \"wrong_schema\""
# T81: Same file used as both source and target: must fail (conflict)
run_test "T81" "$DTPIPE -i artifacts/test_data.csv -o artifacts/test_data.csv"
# T82: Negative --limit value: must fail with validation error
run_test "T82" "$DTPIPE -i artifacts/test_data.csv --limit -5 -o null"
# T83: Empty CSV source: produces empty output (not an error — 0 rows)
run_test "T83" "touch artifacts/empty.csv && $DTPIPE -i artifacts/empty.csv -o artifacts/output_t83.csv"
# T84: Corrupted Parquet binary: must fail with parse error
run_test "T84" "echo 'garbage' > artifacts/broken.parquet && $DTPIPE -i artifacts/broken.parquet -o null"
# T85: Division by zero in compute: must fail (Infinity/NaN guard)
run_test "T85" "$DTPIPE -i artifacts/test_data.csv --compute \"X:1/0\" -o null"
# T86: Unknown method on a known Bogus dataset: must fail with provider error
run_test "T86" "$DTPIPE -i artifacts/test_data.csv --fake \"Bad:name.nonexistent\" -o null"
# T87: Query against non-existent Postgres table: must fail with SQL error
run_test "T87" "$DTPIPE -i \"$PG\" -q \"SELECT * FROM non_existent_table\" -o null"
# T88: Rename references column absent from source schema: must fail
run_test "T88" "$DTPIPE -i artifacts/test_data.csv --rename \"NonExistent:New\" -o null"
# T89: Write to DuckDB with --no-schema-validation: bypasses compatibility check
run_test "T89" "$DTPIPE -i artifacts/test_data.csv -o artifacts/output_t89.duckdb --no-schema-validation"
# T90: Invalid Compute Script (Syntax Error)
run_test "T90" "$DTPIPE -i artifacts/test_data.csv --compute \"Score:{ row.Score \" -o null"
# T105: [ERROR] filter references a column dropped upstream: must fail (schema mismatch)
run_test "T105" "$DTPIPE -i artifacts/test_data.csv --drop \"Email\" --filter \"row.Email.includes('@')\" -o artifacts/output_t105.csv"

# 5. Real-world Scenarios
# T91: Filter + fake anonymize + mask: GDPR-style 3-stage hardening pipeline
run_test "T91" "$DTPIPE -i artifacts/test_data.csv --filter \"row.Score > 700\" --fake \"LastName:name.lastName\" --mask \"Email:####@####.com\" -o artifacts/output_t91.parquet"
# T92: LEFT JOIN anti-join: find Parquet rows absent from Postgres
run_test "T92" "$DTPIPE -i artifacts/test_data.parquet --alias p -i \"$PG\" -q \"SELECT * FROM users_test\" --alias db --from p --ref db --sql \"SELECT p.* FROM p LEFT JOIN db ON p.id = db.id WHERE db.id IS NULL\" -o artifacts/output_t92.csv"
# T93: DAG split by filtering: high and low scores to separate files
run_test "T93" "$DTPIPE -i artifacts/test_data.csv --alias s --from s --filter 'row.Score > 500' -o artifacts/output_t93_a.arrow --from s --filter 'row.Score <= 500' -o artifacts/output_t93_b.arrow"
# T94: Cross-DB sync: Postgres → rename → fake → SQL Server (multi-step real ETL)
run_test "T94" "$DTPIPE -i \"$PG\" -q \"SELECT * FROM users_test\" --rename \"username:Login\" --fake \"Login:internet.userName\" -o \"$MSSQL\" --table \"output_t94\" --strategy Recreate"
# T95: Compute string concatenation: derives FullName from FirstName + LastName
run_test "T95" "$DTPIPE -i artifacts/test_data.csv --compute \"FullName:row.FirstName + ' ' + row.LastName\" -o artifacts/output_t95.csv"
# T96: Compute + filter + limit chain on big dataset → SQL Server (realistic ETL)
run_test "T96" "$DTPIPE -i artifacts/test_data_big.parquet --compute \"X:row.Value * 1.5\" --filter \"row.Value > 100\" --limit 1000 -o \"$MSSQL\" --table \"output_t96\" --strategy Recreate"
# T97: DataFusion GROUP BY aggregation: first-letter distribution of FirstName
run_test "T97" "$DTPIPE -i artifacts/test_data.csv --alias c --from c --sql \"SELECT upper(firstname), count(*) FROM c GROUP BY 1\" -o artifacts/output_t97.csv"
# T98: DuckDB read with --query on geography table → Arrow export
run_test "T98" "$DTPIPE -i artifacts/test_data.duckdb --table \"geography\" -q \"SELECT * FROM geography\" -o artifacts/output_t98.arrow"
# T100: Sampling rate 1.0 keeps all rows (boundary condition: no data loss)
run_test "T100" "$DTPIPE -i artifacts/test_data.csv -o null --sampling-rate 1.0"

# 6. Transformer Chaining
# T101: 3-transformer chain: compute → filter → rename (each stage depends on previous)
run_test "T101" "$DTPIPE -i artifacts/test_data.csv --compute \"Initials:row.FirstName.charAt(0) + row.LastName.charAt(0)\" --filter \"row.Score > 500\" --rename \"Email:Mail\" -o artifacts/output_t101.parquet"
# T102: 4-transformer chain: fake → compute on faked column → filter → mask
run_test "T102" "$DTPIPE -i artifacts/test_data.csv --fake \"Email:internet.email\" --compute \"Domain:row.Email.split('@')[1]\" --filter \"row.Domain.includes('.com')\" --mask \"Email:####@####.com\" -o artifacts/output_t102.csv"
# T103: Columnar-to-row bridge: project/drop (columnar) → compute (row) → filter (row)
run_test "T103" "$DTPIPE -i artifacts/test_data.csv --drop \"Company\" --drop \"BirthDate\" --compute \"Label:row.Score > 500 ? 'High' : 'Low'\" --filter \"row.Score > 200\" -o artifacts/output_t103.parquet"
# T104: 5-transformer chain: fake → overwrite → compute on both → filter → rename
run_test "T104" "$DTPIPE -i artifacts/test_data.csv --fake \"FirstName:name.firstName\" --overwrite \"Score:500\" --compute \"Tag:row.FirstName + '-' + row.Score\" --filter \"row.Score >= 500\" --rename \"Tag:Label\" -o artifacts/output_t104.csv"
# T106: null → compute on nulled column: compute must handle null input gracefully
run_test "T106" "$DTPIPE -i artifacts/test_data.csv --null \"Score\" --compute \"Category:row.Score === null ? 'Unknown' : 'Known'\" -o artifacts/output_t106.csv"
# T107: Order matters: overwrite sets Score to 999, filter expects exactly 999
run_test "T107" "$DTPIPE -i artifacts/test_data.csv --overwrite \"Score:999\" --filter \"row.Score == 999\" -o artifacts/output_t107.csv"

# 7. Missing Adapters — SQLite
# T108: Write CSV to SQLite database (bootstrap SQLite target)
run_test "T108" "$DTPIPE -i artifacts/test_data.csv -o artifacts/output_t108.sqlite --table \"output_t108\" --strategy Recreate --no-schema-validation"
# T109: Read SQLite via --table (full table scan from previously created SQLite)
run_test "T109" "$DTPIPE -i artifacts/output_t108.sqlite --table \"output_t108\" -o artifacts/output_t109.csv"
# T110: Read SQLite via explicit --query with a WHERE clause
run_test "T110" "$DTPIPE -i artifacts/output_t108.sqlite --query \"SELECT * FROM output_t108 WHERE Score > 500\" -o artifacts/output_t110.parquet"

# 8. Missing Adapters — JSONL
# T111: Write CSV to JSONL format (newline-delimited JSON)
run_test "T111" "$DTPIPE -i artifacts/test_data.csv -o artifacts/output_t111.jsonl"
# T112: Read JSONL → CSV round-trip (validates JSONL reader after T111)
run_test "T112" "$DTPIPE -i artifacts/output_t111.jsonl -o artifacts/output_t112.csv"
# T113: JSONL source with compute transformer (derives FullName column)
run_test "T113" "$DTPIPE -i artifacts/output_t111.jsonl --compute \"FullName:row.FirstName + ' ' + row.LastName\" -o artifacts/output_t113.parquet"

# 9. Missing Adapters — Oracle explicit query & DuckDB explicit query
# T114: Oracle read with explicit SQL --query and WHERE clause (not just --table)
run_test "T114" "$DTPIPE -i \"$ORA\" --query \"SELECT ID, FULL_NAME, JOB_TITLE FROM USERS_TEST_DATA WHERE ROWNUM <= 100\" -o artifacts/output_t114.csv"
# T115: DuckDB read with explicit --query (SQL, not just --table passthrough)
run_test "T115" "$DTPIPE -i artifacts/test_data.duckdb --query \"SELECT City, count(*) as cnt FROM geography GROUP BY City ORDER BY cnt DESC LIMIT 10\" -o artifacts/output_t115.csv"

# 10. Observability & Operations
# T116: --dry-run: inspect schema and sample 5 rows (requires -o null)
run_test "T116" "$DTPIPE -i artifacts/test_data.csv --compute \"FullName:row.FirstName + ' ' + row.LastName\" -o null --dry-run 5"
# T117: --log: verify pipeline execution log is written to file
run_test "T117" "$DTPIPE -i artifacts/test_data.csv -o null --log artifacts/output_t117.log"
# T118: --metrics-path: verify metrics JSON is emitted after pipeline completes
run_test "T118" "$DTPIPE -i artifacts/test_data.csv --compute \"X:row.Score * 2\" -o null --metrics-path artifacts/output_t118.json"
# T119: --post-exec SQL: verify hook runs a SQL statement against the Postgres target
run_test "T119" "$DTPIPE -i artifacts/test_data.csv -o \"$PG\" --table \"output_t119\" --strategy Recreate --post-exec \"ANALYZE output_t119\""
# T120: --auto-migrate: add a new column to a Postgres table that already exists
run_test "T120" "$DTPIPE -i \"generate:10\" --fake \"Id:random.guid\" -o \"$PG\" --table \"output_t120\" --strategy Recreate --no-schema-validation && $DTPIPE -i \"generate:5\" --fake \"Id:random.guid\" --fake \"NewCol:name.firstName\" -o \"$PG\" --table \"output_t120\" --auto-migrate --no-schema-validation"

# 11. CLI Subcommands
# T121: 'inspect' subcommand: inspect schema and stats of a Parquet file
run_test "T121" "$DTPIPE inspect -i artifacts/test_data.parquet"
# T122: 'providers' subcommand: list all registered reader/writer/transformer providers
run_test "T122" "$DTPIPE providers"

# T130: Rename to an already existing column: must fail with collision error
run_test "T130" "$DTPIPE -i artifacts/test_data.csv --rename \"FirstName:LastName\" -o artifacts/output_t130.csv"

# 13. [DELETED T125/T126/T129 - Obsolete or Skip based]

# 14. New Error Cases
# T131: [ERROR] --project non-existent column
run_test "T131" "$DTPIPE -i artifacts/test_data.csv --project \"GhostColumn\" -o null"
# T132: [ERROR] rename to a column name that already exists in schema (Collision)
run_test "T132" "$DTPIPE -i artifacts/test_data.csv --rename \"FirstName:LastName\" -o null"
# T133: [ERROR] Oracle DDL statement rejected by the safety guard
run_test "T133" "$DTPIPE -i \"$ORA\" --query \"DROP TABLE USERS_TEST_DATA\" -o null"
# T134: [ERROR] JSONL source from a file containing invalid JSON
run_test "T134" "echo 'not json at all' > artifacts/broken.jsonl && $DTPIPE -i artifacts/broken.jsonl -o null"
# T135: [ERROR] DuckDB --query referencing a table that does not exist
run_test "T135" "$DTPIPE -i artifacts/test_data.duckdb --query \"SELECT * FROM ghost_table\" -o null"

echo -e "\n### Advanced Fan-out and Routing Tests ###"

# T136: Diamond pattern (fan-out -> filter -> join )
run_test "T136" "$DTPIPE -i artifacts/test_data.csv --alias src \
  --from src --filter 'row.Score > 500' --alias high \
  --from src --filter 'row.Score <= 500' --alias low \
  --from high --ref low --sql \"SELECT 'high' as segment, count(*) as n FROM high UNION ALL SELECT 'low', count(*) FROM low\" \
  -o artifacts/output_t136.csv"

# T137: Triple fan-out (3 consumers of same source)
run_test "T137" "$DTPIPE -i artifacts/test_data.csv --alias src \
  --from src -o artifacts/output_t137_a.csv \
  --from src -o artifacts/output_t137_b.parquet \
  --from src --limit 10 -o artifacts/output_t137_c.arrow"

# T138: Fan-out after SQL processor (join -> tee)
run_test "T138" "$DTPIPE \
  -i artifacts/test_data.parquet --alias p \
  -i artifacts/test_data.csv --alias c \
  --from p --ref c --sql \"SELECT p.*, c.email FROM p JOIN c ON p.id = c.id\" --alias joined \
  --from joined -o artifacts/output_t138_a.csv \
  --from joined -o artifacts/output_t138_b.parquet"

echo "----------------------------------------"

echo ""
echo "===================================================="
echo "   Tests Completed at $(date)"
echo "   Check $LOG_FILE for details."
echo "===================================================="
