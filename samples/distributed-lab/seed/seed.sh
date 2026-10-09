#!/usr/bin/env bash
# Builds each node's database with dtpipe itself: generate: rows, faker columns, DuckDB SQL for the
# rest. Deterministic (row-seeded fakes, hashed values), so two seeds give the same data.
# Usage: seed.sh <dtpipe> <state-dir> [scale] [node...]   scale multiplies the row counts (default
# 1); the nodes named are seeded, every node when none is. A node's rows reference the others' counts,
# so every node of one lab is seeded at the same scale.
set -euo pipefail

DTPIPE="$1"
STATE="$2"
SCALE="${3:-1}"
NODES=("${@:4}")
[[ ${#NODES[@]} -gt 0 ]] || NODES=(node-1 node-2 node-3 node-4)
wants() { [[ " ${NODES[*]} " == *" $1 "* ]]; }

CUSTOMERS=$((20000 * SCALE))
PRODUCTS=500
ORDERS=$((300000 * SCALE))
LEGACY=$((100000 * SCALE))

for node in "${NODES[@]}"; do mkdir -p "$STATE/$node"; done
LOG="$(mktemp)"
trap 'rm -f "$LOG"' EXIT
run() { "$DTPIPE" "$@" --no-stats >"$LOG" 2>&1 || { cat "$LOG" >&2; exit 1; }; }

if wants node-1; then
    echo "node-1  crm.sqlite       customers      $CUSTOMERS rows"
    run -i "generate:$CUSTOMERS" \
        --fake "first_name:name.firstName" --fake "last_name:name.lastName" \
        --fake "email:internet.email" --fake "city:address.city" \
        --fake-locale fr --fake-seed-row --alias g \
        --from g --sql "
          SELECT GenerateIndex + 1 AS customer_id, first_name, last_name, email, city,
                 ['FR','BE','CH','CA','LU'][1 + (hash(GenerateIndex) % 5)::INT] AS country,
                 DATE '2019-01-01' + (hash(GenerateIndex * 7) % 2000)::INT AS signup_date
          FROM g" \
        -o "sqlite:$STATE/node-1/crm.sqlite" --table customers --strategy Recreate
fi

if wants node-3; then
    echo "node-3  catalog.sqlite   products       $PRODUCTS rows"
    run -i "generate:$PRODUCTS" \
        --fake "name:commerce.productName" --fake "category:commerce.department" \
        --fake-seed-row --alias g \
        --from g --sql "
          SELECT GenerateIndex + 1 AS product_id, name, category,
                 ROUND(5 + (hash(GenerateIndex) % 49500) / 100.0, 2)::DECIMAL(10,2) AS unit_price
          FROM g" \
        -o "sqlite:$STATE/node-3/catalog.sqlite" --table products --strategy Recreate
fi

if wants node-3; then
    echo "node-3  catalog.sqlite   legacy_orders  $LEGACY rows"
    run -i "generate:$LEGACY" --alias g \
        --from g --sql "
          SELECT GenerateIndex + 1 AS order_id,
                 1 + (hash(GenerateIndex * 3) % $CUSTOMERS)::INT AS customer_id,
                 1 + (hash(GenerateIndex * 5) % $PRODUCTS)::INT AS product_id,
                 1 + (hash(GenerateIndex * 11) % 5)::INT AS quantity,
                 DATE '2019-01-01' + (hash(GenerateIndex * 13) % 1826)::INT AS order_date,
                 'archived' AS status
          FROM g" \
        -o "sqlite:$STATE/node-3/catalog.sqlite" --table legacy_orders --strategy Recreate
fi

if wants node-2; then
    echo "node-2  sales.duckdb     orders         $ORDERS rows"
    run -i "generate:$ORDERS" --alias g \
        --from g --sql "
          SELECT 10000000 + GenerateIndex + 1 AS order_id,
                 1 + (hash(GenerateIndex * 3) % $CUSTOMERS)::INT AS customer_id,
                 1 + (hash(GenerateIndex * 5) % $PRODUCTS)::INT AS product_id,
                 1 + (hash(GenerateIndex * 11) % 5)::INT AS quantity,
                 DATE '2024-01-01' + (hash(GenerateIndex * 13) % 366)::INT AS order_date,
                 ['paid','paid','paid','shipped','refunded'][1 + (hash(GenerateIndex * 17) % 5)::INT] AS status
          FROM g" \
        -o "duck:$STATE/node-2/sales.duckdb" --table orders --strategy Recreate
fi

if wants node-4; then
    echo "node-4  warehouse.duckdb (emptied: the pipelines write it)"
    rm -f "$STATE/node-4/warehouse.duckdb" "$STATE/node-4/warehouse.duckdb.wal"
fi
