#!/bin/bash
set -e

# validate_doc_examples.sh
# The examples printed in the docs/ site are run, and their documented output is compared.
#
# The site claims "every output shown was produced by running it". Before this script the claim
# rested on whoever last edited a page: the v0 draft shipped a deduplication result whose rows were
# in the wrong order, and a YAML sample that --export-job does not produce -- neither visible to a
# reader who does not retype the command.
#
# Only file-based examples live here, so no database and no container is needed and the check runs
# anywhere the binary does. Examples against PostgreSQL, Oracle or SQL Server are out of scope by
# construction, not by omission.
#
# Two kinds of assertion, and the difference matters:
#   - exact output, for anything seeded or deterministic;
#   - shape only (exit code, header, row count) for an unseeded --fake, whose values are random by
#     design and whose stability is NOT what the doc promises.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
WORK="$SCRIPT_DIR/artifacts/doc_examples"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

FAILED=0
pass() { echo -e "  ${GREEN}OK${NC}   $1"; }
bad()  { echo -e "  ${RED}FAIL${NC} $1"; FAILED=1; }

echo "========================================"
echo "    DtPipe Documentation Example Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

rm -rf "$WORK"; mkdir -p "$WORK/scripts"; cd "$WORK"

# Compares a produced file against the block the documentation prints. CSV is written with CRLF
# line endings, per RFC 4180, so the comparison strips carriage returns rather than asking the
# pages to print an invisible character a reader would have to reproduce.
expect_file() {
    local title=$1 actual=$2
    local want; want="$(cat)"
    local got;  got="$(tr -d '\r' < "$actual")"
    if [ "$got" = "$want" ]; then pass "$title"; else
        bad "$title"
        diff <(echo "$want") <(echo "$got") | sed 's/^/       /' || true
    fi
}

# Same, compared as a set. For an example whose engine guarantees which rows come out but not in
# which order.
expect_rows() {
    local title=$1 actual=$2
    local want; want="$(sort)"
    local got;  got="$(tr -d '\r' < "$actual" | sort)"
    if [ "$got" = "$want" ]; then pass "$title"; else
        bad "$title"
        diff <(echo "$want") <(echo "$got") | sed 's/^/       /' || true
    fi
}

# ----------------------------------------
# Fixtures, as the pages print them
# ----------------------------------------
cat > orders.csv <<'EOF'
order_id,customer_email,amount
10,alice@corp.com,120
11,bob@corp.com,80
12,alice@corp.com,45
EOF

cat > customers.csv <<'EOF'
id,email,name
1,alice@corp.com,Alice
2,bob@corp.com,Bob
EOF

cat > orders_2025.csv <<'EOF'
order_id,customer_email,amount
7,alice@corp.com,200
8,bob@corp.com,150
EOF

cat > dup.csv <<'EOF'
id,updated_at,status
1,2026-01-01,new
2,2026-01-01,new
1,2026-02-01,paid
3,2026-01-15,new
2,2026-03-01,cancelled
EOF

cat > emp.csv <<'EOF'
id,name,email,salary
1,Alice Martin,alice@corp.com,52000
2,Bob Durand,bob@corp.com,61000
3,Carla Neri,carla@corp.com,74000
4,Dan Petit,dan@corp.com,
EOF

cat > scripts/band.js <<'EOF'
const s = row.salary;
if (s === null) return "unknown";
if (s >= 70000) return "C";
if (s >= 55000) return "B";
return "A";
EOF

cat > cust_key.csv <<'EOF'
customer_id,email,name
C1,alice@corp.com,Alice Martin
C2,bob@corp.com,Bob Durand
EOF

cat > ord_key.csv <<'EOF'
order_id,customer_id,customer_email,amount
10,C1,alice@corp.com,120
11,C2,bob@corp.com,80
12,C1,alice@corp.com,45
EOF

echo "--- guides/dag.md ---"

"$DTPIPE" -i orders.csv --auto-column-types --alias o \
          -i customers.csv --alias c \
          --from o --ref c \
          --sql "SELECT c.name, sum(o.amount) AS total
                 FROM o JOIN c ON o.customer_email = c.email
                 GROUP BY c.name ORDER BY total DESC" \
          -o revenue.csv --no-stats > /dev/null 2>&1
expect_file "join two sources" revenue.csv <<'EOF'
name,total
Alice,165
Bob,80
EOF

"$DTPIPE" -i orders.csv --auto-column-types --alias s \
          --from s -o orders.parquet \
          --from s --sql "SELECT customer_email, sum(amount) AS total
                          FROM s GROUP BY customer_email ORDER BY total DESC" \
          -o totals.csv --no-stats > /dev/null 2>&1
expect_file "fan one source out to several targets" totals.csv <<'EOF'
customer_email,total
alice@corp.com,165
bob@corp.com,80
EOF

"$DTPIPE" -i orders_2025.csv --auto-column-types --alias a \
          -i orders.csv --auto-column-types --alias b \
          --from a,b --merge -o orders_all.csv --no-stats > /dev/null 2>&1
# Compared as a set: --merge is a UNION ALL of two branches that run concurrently, so it guarantees
# which rows arrive and not in which order. Asserting the printed order would be asserting a
# scheduling accident -- the mistake the deduplication case above made once, failing one run in
# three.
expect_rows "merge several sources" orders_all.csv <<'EOF'
order_id,customer_email,amount
7,alice@corp.com,200
8,bob@corp.com,150
10,alice@corp.com,120
11,bob@corp.com,80
12,alice@corp.com,45
EOF

echo "--- guides/sql-and-javascript.md ---"

# The page's ORDER BY is load-bearing, not decoration: without it DuckDB returns these three rows
# in a different arrangement from one run to the next, and this assertion fails once in three.
"$DTPIPE" -i dup.csv --alias src \
          --from src --sql "SELECT * FROM src
                            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
                            ORDER BY id" \
          -o dedup.csv --no-stats > /dev/null 2>&1
expect_file "deduplication with QUALIFY" dedup.csv <<'EOF'
id,updated_at,status
1,2026-02-01,paid
2,2026-03-01,cancelled
3,2026-01-15,new
EOF

"$DTPIPE" -i emp.csv --auto-column-types \
          --compute "domain:row.email.split('@')[1]" \
          --filter "row.salary > 55000" \
          -o js.csv --no-stats > /dev/null 2>&1
expect_file "compute and filter, single expression" js.csv <<'EOF'
id,name,email,salary,domain
2,Bob Durand,bob@corp.com,61000,corp.com
3,Carla Neri,carla@corp.com,74000,corp.com
EOF

"$DTPIPE" -i emp.csv --auto-column-types \
          --compute "band:@scripts/band.js" -o banded.csv --no-stats > /dev/null 2>&1
expect_file "compute from a script file, with a null guard" banded.csv <<'EOF'
id,name,email,salary,band
1,Alice Martin,alice@corp.com,52000,A
2,Bob Durand,bob@corp.com,61000,B
3,Carla Neri,carla@corp.com,74000,C
4,Dan Petit,dan@corp.com,,unknown
EOF

echo "--- guides/anonymization.md ---"

# The page's central claim, and the one that is fragile: anonymize both sides of a foreign key in
# two separate runs, and the join still holds. Asserted on the join itself rather than on the
# generated values, so it tests the property the page promises instead of a Bogus version.
"$DTPIPE" -i cust_key.csv --fake "email:internet.email" \
          --fake-seed-column customer_id -o cust_anon.csv --no-stats > /dev/null 2>&1
"$DTPIPE" -i ord_key.csv --fake "customer_email:internet.email" \
          --fake-seed-column customer_id -o ord_anon.csv --no-stats > /dev/null 2>&1

"$DTPIPE" -i ord_anon.csv --auto-column-types --alias o \
          -i cust_anon.csv --alias c \
          --from o --ref c \
          --sql "SELECT count(*) AS matched
                 FROM o JOIN c ON o.customer_email = c.email" \
          -o matched.csv --no-stats > /dev/null 2>&1
expect_file "seeded faking keeps a join across two runs" matched.csv <<'EOF'
matched
3
EOF

# An unseeded --fake is random by design: assert the shape, never the values.
"$DTPIPE" -i emp.csv --fake "name:name.fullName" --fake "email:internet.email" \
          --null salary -o emp_anon.csv --no-stats > /dev/null 2>&1
if [ "$(head -1 emp_anon.csv | tr -d '\r')" = "id,name,email,salary" ] \
   && [ "$(wc -l < emp_anon.csv | tr -d ' ')" = "5" ] \
   && ! grep -q "alice@corp.com" emp_anon.csv; then
    pass "unseeded faking replaces the values it is given"
else
    bad "unseeded faking replaces the values it is given"
fi

echo "--- quickstart.md ---"

"$DTPIPE" -i generate:1000 \
          --fake "name:name.fullName" --fake "email:internet.email" \
          --fake "city:address.city" --drop GenerateIndex \
          -o people.csv --no-stats > /dev/null 2>&1
if [ "$(head -1 people.csv | tr -d '\r')" = "name,email,city" ] \
   && [ "$(wc -l < people.csv | tr -d ' ')" = "1001" ]; then
    pass "generate a dataset from nothing"
else
    bad "generate a dataset from nothing"
fi

"$DTPIPE" -i people.csv -o people.parquet --no-stats > /dev/null 2>&1
[ -s people.parquet ] && pass "convert a file by extension" || bad "convert a file by extension"

# A dry run against a file target creates nothing at all; against DuckDB the driver creates the
# database file on connect, but the run must leave it without the table. The page says both.
"$DTPIPE" -i people.csv -o preview.csv --dry-run 3 > /dev/null 2>&1
[ ! -f preview.csv ] && pass "a dry run to a file target writes nothing" \
                     || bad "a dry run to a file target writes nothing"

"$DTPIPE" -i people.csv -o duck:analytics.duckdb --table people \
          --dry-run 3 > /dev/null 2>&1
if [ -z "$("$DTPIPE" -i duck:analytics.duckdb \
        --query "SELECT table_name FROM information_schema.tables" \
        -o jsonl:- --no-stats 2>/dev/null)" ]; then
    pass "a dry run creates no table"
else
    bad "a dry run creates no table"
fi

"$DTPIPE" -i people.csv -o duck:analytics.duckdb --table people --no-stats > /dev/null 2>&1
if "$DTPIPE" -i duck:analytics.duckdb \
      --query "SELECT count(*) AS people FROM people" \
      -o jsonl:- --no-stats 2>/dev/null | grep -q '{"people":1000}'; then
    pass "read back through jsonl:- on stdout"
else
    bad "read back through jsonl:- on stdout"
fi

"$DTPIPE" -i people.csv -o duck:analytics.duckdb --table people \
          --export-job load-people.yaml > /dev/null 2>&1
expect_file "export-job writes the documented YAML" load-people.yaml <<'EOF'
main:
  input: people.csv
  output: duck:analytics.duckdb
  batch-size: 32768
  sampling-rate: 1
  provider-options:
    duck-writer:
      table: people
EOF

# ----------------------------------------
# guides/secrets.md — what a routing failure prints
# ----------------------------------------

# The page shows this message verbatim to claim that a resolved credential does not reach the
# logs. Comparing the whole line is the point: an assertion on the exit code alone would pass
# just as well with the password printed.
SECRETS_MSG=$("$DTPIPE" -i "Host=db.internal;Database=app;Username=etl;Password=hunter2" \
    -o out.csv 2>&1 | grep -o "No reader factory resolved for input '[^']*'" | head -1)
if [ "$SECRETS_MSG" = "No reader factory resolved for input 'Host=db.internal;Database=app;Username=etl;Password=***'" ]; then
    pass "a routing failure prints the redacted connection"
else
    bad "a routing failure prints the redacted connection"
    echo "       got: $SECRETS_MSG"
fi

echo ""
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}Documentation example validation complete!${NC}"
else
    echo -e "${RED}Documentation examples do not match the pages.${NC}"
    exit 1
fi
