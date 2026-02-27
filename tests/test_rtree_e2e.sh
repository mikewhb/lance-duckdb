#!/usr/bin/env bash
#
# End-to-end validation script for Lance RTree spatial index scan.
#
# This script:
#   1. Generates a test Lance dataset with random geo points + RTree index (Python)
#   2. Runs spatial queries via DuckDB's lance_rtree_index_scan table function
#   3. Cross-validates results against Python full-table-scan baseline
#
# Usage:
#   bash tests/test_rtree_e2e.sh [duckdb_binary_path]
#
# Requirements:
#   - Python 3 with `lance` and `pyarrow` packages installed
#   - DuckDB binary with lance extension (debug or release build)

set -euo pipefail

# ============================================================
# Configuration
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

DUCKDB_BIN="${1:-${PROJECT_DIR}/build/debug/duckdb}"
LANCE_PATH="/tmp/lance_rtree_e2e_test_$$.lance"
NUM_ROWS=2000
SEED=42
COORD_MIN=0.0
COORD_MAX=10.0

# Test bounding boxes: (min_x, min_y, max_x, max_y)
BBOX1="1.0 2.0 3.0 4.0"
BBOX2="0.0 0.0 0.5 0.5"
BBOX3="5.0 5.0 10.0 10.0"
BBOX4="0.0 0.0 10.0 10.0"

# Color output helpers
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0

pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

cleanup() {
    if [ -d "${LANCE_PATH}" ]; then
        rm -rf "${LANCE_PATH}"
        info "Cleaned up test dataset: ${LANCE_PATH}"
    fi
}

trap cleanup EXIT

# ============================================================
# Step 1: Check prerequisites
# ============================================================
echo "============================================================"
echo "  Lance RTree Spatial Index - End-to-End Validation"
echo "============================================================"
echo ""

info "DuckDB binary: ${DUCKDB_BIN}"
info "Test dataset:  ${LANCE_PATH}"
info "Num rows:      ${NUM_ROWS}"
info "Seed:          ${SEED}"
echo ""

if [ ! -x "${DUCKDB_BIN}" ]; then
    fail "DuckDB binary not found or not executable: ${DUCKDB_BIN}"
    echo "  Hint: run 'make debug' or 'make release' first, or pass the path as argument."
    exit 1
fi

if ! python3 -c "import lance, pyarrow" 2>/dev/null; then
    fail "Python 3 with 'lance' and 'pyarrow' packages is required."
    echo "  Hint: pip install pylance pyarrow"
    exit 1
fi

pass "Prerequisites check"

# ============================================================
# Step 2: Generate test dataset with Python
# ============================================================
info "Generating test Lance dataset with ${NUM_ROWS} random geo points..."

python3 << PYEOF
import lance
import pyarrow as pa
import random

random.seed(${SEED})
NUM_ROWS = ${NUM_ROWS}
COORD_MIN = ${COORD_MIN}
COORD_MAX = ${COORD_MAX}

# Generate random point data
ids = list(range(NUM_ROWS))
names = [f"point_{i}" for i in range(NUM_ROWS)]
xs = [random.uniform(COORD_MIN, COORD_MAX) for _ in range(NUM_ROWS)]
ys = [random.uniform(COORD_MIN, COORD_MAX) for _ in range(NUM_ROWS)]

# Build GeoArrow point column (struct<x: float64, y: float64> with geoarrow.point metadata)
geom_type = pa.struct([
    pa.field("x", pa.float64(), nullable=False),
    pa.field("y", pa.float64(), nullable=False),
])
point_field = pa.field("geom", geom_type, metadata={
    b"ARROW:extension:name": b"geoarrow.point",
    b"ARROW:extension:metadata": b"{}",
})

geom_array = pa.StructArray.from_arrays(
    [pa.array(xs, type=pa.float64()), pa.array(ys, type=pa.float64())],
    names=["x", "y"],
)

schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    point_field,
])

table = pa.table({
    "id": pa.array(ids, type=pa.int64()),
    "name": pa.array(names, type=pa.string()),
    "geom": geom_array,
}, schema=schema)

# Write to Lance format
ds = lance.write_dataset(table, "${LANCE_PATH}", mode="overwrite")

# Create RTree spatial index
ds.create_scalar_index("geom", index_type="RTREE", replace=True)

# Verify
ds2 = lance.dataset("${LANCE_PATH}")
indices = ds2.list_indices()
assert len(indices) == 1, f"Expected 1 index, got {len(indices)}"
assert indices[0]["type"] == "RTree", f"Expected RTree, got {indices[0]['type']}"
print(f"Dataset created: {NUM_ROWS} rows, schema: {ds2.schema}")
print(f"Index: {indices[0]}")
PYEOF

if [ $? -eq 0 ]; then
    pass "Test dataset generated with RTree index"
else
    fail "Failed to generate test dataset"
    exit 1
fi

# ============================================================
# Step 3: Compute expected counts with Python (ground truth)
# ============================================================
info "Computing ground truth with Python full-table-scan..."

EXPECTED=$(python3 << PYEOF
import lance
import pyarrow.compute as pc

ds = lance.dataset("${LANCE_PATH}")
t = ds.to_table()
x = pc.struct_field(t.column("geom"), "x")
y = pc.struct_field(t.column("geom"), "y")

# Test bboxes: (min_x, min_y, max_x, max_y)
bboxes = [
    (1.0, 2.0, 3.0, 4.0),
    (0.0, 0.0, 0.5, 0.5),
    (5.0, 5.0, 10.0, 10.0),
    (0.0, 0.0, 10.0, 10.0),
]

for (min_x, min_y, max_x, max_y) in bboxes:
    mask = pc.and_(
        pc.and_(pc.greater_equal(x, min_x), pc.less_equal(x, max_x)),
        pc.and_(pc.greater_equal(y, min_y), pc.less_equal(y, max_y)),
    )
    count = pc.sum(mask).as_py()
    print(f"{min_x},{min_y},{max_x},{max_y}:{count}")
PYEOF
)

if [ $? -ne 0 ]; then
    fail "Failed to compute ground truth"
    exit 1
fi

pass "Ground truth computed"

# Parse expected counts into an associative array
declare -A EXPECTED_COUNTS
while IFS=: read -r bbox count; do
    EXPECTED_COUNTS["${bbox}"]="${count}"
done <<< "${EXPECTED}"

info "Expected counts:"
for bbox in "${!EXPECTED_COUNTS[@]}"; do
    info "  bbox(${bbox}) => ${EXPECTED_COUNTS[${bbox}]}"
done
echo ""

# ============================================================
# Step 4: Run DuckDB queries and validate
# ============================================================
info "Running DuckDB lance_rtree_index_scan queries..."
echo ""

run_duckdb_query() {
    local sql="$1"
    ASAN_OPTIONS=detect_container_overflow=0 "${DUCKDB_BIN}" -csv -noheader -c "${sql}" 2>/dev/null
}

# --- Test 4a: Count queries for each bbox ---
for bbox_str in "1.0,2.0,3.0,4.0" "0.0,0.0,0.5,0.5" "5.0,5.0,10.0,10.0" "0.0,0.0,10.0,10.0"; do
    IFS=',' read -r min_x min_y max_x max_y <<< "${bbox_str}"
    expected="${EXPECTED_COUNTS[${bbox_str}]}"

    sql="SELECT count(*) FROM lance_rtree_index_scan('${LANCE_PATH}', 'geom', ${min_x}, ${min_y}, ${max_x}, ${max_y});"
    actual=$(run_duckdb_query "${sql}" | tr -d '[:space:]')

    if [ "${actual}" = "${expected}" ]; then
        pass "Count bbox(${bbox_str}): got ${actual} (expected ${expected})"
    else
        fail "Count bbox(${bbox_str}): got ${actual}, expected ${expected}"
    fi
done

# --- Test 4b: Verify all returned points are within the bbox ---
info "Verifying spatial correctness of returned points..."

for bbox_str in "1.0,2.0,3.0,4.0" "0.0,0.0,0.5,0.5" "5.0,5.0,10.0,10.0"; do
    IFS=',' read -r min_x min_y max_x max_y <<< "${bbox_str}"

    sql="SELECT min(geom.x), max(geom.x), min(geom.y), max(geom.y), count(*) \
FROM lance_rtree_index_scan('${LANCE_PATH}', 'geom', ${min_x}, ${min_y}, ${max_x}, ${max_y});"
    result=$(run_duckdb_query "${sql}")

    if [ -z "${result}" ]; then
        fail "Bounds check bbox(${bbox_str}): empty result"
        continue
    fi

    IFS=',' read -r actual_min_x actual_max_x actual_min_y actual_max_y cnt <<< "${result}"

    # If count is 0, skip bounds checking
    if [ "${cnt}" = "0" ]; then
        pass "Bounds check bbox(${bbox_str}): 0 rows (nothing to check)"
        continue
    fi

    bounds_ok=true
    if python3 -c "
import sys
min_x, min_y, max_x, max_y = ${min_x}, ${min_y}, ${max_x}, ${max_y}
actual_min_x, actual_max_x = ${actual_min_x}, ${actual_max_x}
actual_min_y, actual_max_y = ${actual_min_y}, ${actual_max_y}

ok = True
if actual_min_x < min_x - 1e-9:
    print(f'  min_x out of range: {actual_min_x} < {min_x}')
    ok = False
if actual_max_x > max_x + 1e-9:
    print(f'  max_x out of range: {actual_max_x} > {max_x}')
    ok = False
if actual_min_y < min_y - 1e-9:
    print(f'  min_y out of range: {actual_min_y} < {min_y}')
    ok = False
if actual_max_y > max_y + 1e-9:
    print(f'  max_y out of range: {actual_max_y} > {max_y}')
    ok = False
sys.exit(0 if ok else 1)
" 2>&1; then
        pass "Bounds check bbox(${bbox_str}): all ${cnt} points within bounds"
    else
        fail "Bounds check bbox(${bbox_str}): some points out of bounds"
    fi
done

# --- Test 4c: DuckDB-level column projection ---
info "Testing DuckDB-level column projection..."

sql="SELECT id, name FROM lance_rtree_index_scan('${LANCE_PATH}', 'geom', 1.0, 2.0, 3.0, 4.0) LIMIT 3;"
proj_result=$(run_duckdb_query "${sql}")
if [ -n "${proj_result}" ]; then
    pass "Column projection (SELECT id, name) returned data"
else
    fail "Column projection (SELECT id, name) returned empty"
fi

# Verify projected result has correct number of columns (should be 2: id, name)
col_count=$(echo "${proj_result}" | head -1 | awk -F',' '{print NF}')
if [ "${col_count}" = "2" ]; then
    pass "Column projection returns exactly 2 columns"
else
    fail "Column projection returns ${col_count} columns (expected 2)"
fi

# --- Test 4d: Empty result (bbox outside data range) ---
info "Testing empty result (bbox outside data range)..."

sql="SELECT count(*) FROM lance_rtree_index_scan('${LANCE_PATH}', 'geom', 100.0, 100.0, 200.0, 200.0);"
empty_count=$(run_duckdb_query "${sql}" | tr -d '[:space:]')
if [ "${empty_count}" = "0" ]; then
    pass "Empty result: bbox(100,100,200,200) returned 0 rows"
else
    fail "Empty result: bbox(100,100,200,200) returned ${empty_count} rows (expected 0)"
fi

# --- Test 4e: Full-range bbox (should return all rows) ---
info "Testing full-range bbox..."

sql="SELECT count(*) FROM lance_rtree_index_scan('${LANCE_PATH}', 'geom', ${COORD_MIN}, ${COORD_MIN}, ${COORD_MAX}, ${COORD_MAX});"
full_count=$(run_duckdb_query "${sql}" | tr -d '[:space:]')
if [ "${full_count}" = "${NUM_ROWS}" ]; then
    pass "Full-range bbox: returned all ${NUM_ROWS} rows"
else
    fail "Full-range bbox: returned ${full_count} rows (expected ${NUM_ROWS})"
fi

# --- Test 4f: SQL aggregation on spatial results ---
info "Testing SQL aggregation on spatial results..."

sql="SELECT count(*), avg(id) FROM lance_rtree_index_scan('${LANCE_PATH}', 'geom', 1.0, 2.0, 3.0, 4.0);"
agg_result=$(run_duckdb_query "${sql}")
if [ -n "${agg_result}" ]; then
    pass "SQL aggregation (count, avg) on spatial results works"
else
    fail "SQL aggregation returned empty"
fi

# ============================================================
# Summary
# ============================================================
echo ""
echo "============================================================"
TOTAL=$((PASS_COUNT + FAIL_COUNT))
echo -e "  Results: ${GREEN}${PASS_COUNT} passed${NC}, ${RED}${FAIL_COUNT} failed${NC}, ${TOTAL} total"
echo "============================================================"

if [ "${FAIL_COUNT}" -gt 0 ]; then
    exit 1
else
    exit 0
fi
