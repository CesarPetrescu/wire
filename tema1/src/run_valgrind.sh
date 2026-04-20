#!/bin/bash
# Run valgrind memcheck + cachegrind for all 3 variants.
# Produces ../memory/*.memory and ../cache/*.cache used for submission.
# Usage: apptainer exec ~/optimizari.sif bash run_valgrind.sh

cd "$(dirname "$0")"

make all >/dev/null

mkdir -p ../memory ../cache

echo "=== Valgrind memcheck ==="
for variant in neopt blas opt_m; do
    echo "--- $variant ---"
    valgrind --tool=memcheck --leak-check=full \
        ./tema1_${variant} ../input/input_valgrind \
        2> ../memory/${variant}.memory
    echo "Saved to ../memory/${variant}.memory"
done

echo ""
echo "=== Cachegrind ==="
for variant in neopt blas opt_m; do
    echo "--- $variant ---"
    rm -f cachegrind.out.*
    valgrind --tool=cachegrind --branch-sim=yes --cache-sim=yes \
        ./tema1_${variant} ../input/input_valgrind \
        2> ../cache/${variant}.cache
    echo "Saved to ../cache/${variant}.cache"
done
rm -f cachegrind.out.*
