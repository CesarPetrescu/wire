#!/bin/bash
# Run valgrind memcheck + cachegrind for all 3 variants.
# Produces ../memory/*.memory and ../cache/*.cache used for submission.
# Usage: apptainer exec ~/optimizari.sif bash run_valgrind.sh

set -e

cd "$(dirname "$0")"

make all >/dev/null

mkdir -p ../memory ../cache

rc=0

echo "=== Valgrind memcheck ==="
for variant in neopt blas opt_m; do
    echo "--- $variant ---"
    out="../memory/${variant}.memory"
    if valgrind --tool=memcheck --leak-check=full \
            ./tema1_${variant} ../input/input_valgrind \
            2> "$out"; then
        # Sanity-check the output really looks like a valgrind report.
        if ! grep -q "ERROR SUMMARY" "$out"; then
            echo "WARNING: $out does not contain ERROR SUMMARY" >&2
            rc=1
        fi
        echo "Saved to $out"
    else
        echo "ERROR: memcheck failed for $variant; see $out" >&2
        rc=1
    fi
done

echo ""
echo "=== Cachegrind ==="
for variant in neopt blas opt_m; do
    echo "--- $variant ---"
    rm -f cachegrind.out.*
    out="../cache/${variant}.cache"
    if valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes \
            ./tema1_${variant} ../input/input_valgrind \
            2> "$out"; then
        if ! grep -q "I   refs" "$out"; then
            echo "WARNING: $out does not contain cachegrind summary" >&2
            rc=1
        fi
        echo "Saved to $out"
    else
        echo "ERROR: cachegrind failed for $variant; see $out" >&2
        rc=1
    fi
done
rm -f cachegrind.out.*

exit $rc
