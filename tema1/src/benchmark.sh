#!/bin/bash
# Benchmark all 3 variants with extended input (5 N values).
# Run inside apptainer on haswell.
# Usage: apptainer exec ~/optimizari.sif bash benchmark.sh > ../grafice/times.csv

set -e

cd "$(dirname "$0")"

make all >/dev/null

# Run one variant, print timing on stdout only if the binary succeeded
# *and* we could parse a Time= value. Returns non-zero otherwise.
run_one() {
    local bin="$1" input="$2" out rc time
    out=$("$bin" "$input" 2>&1)
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "$bin failed (exit $rc) for input $input: $out" >&2
        return $rc
    fi
    time=$(printf '%s\n' "$out" | grep -oP 'Time=\K[0-9.]+' | head -n 1)
    if [ -z "$time" ]; then
        echo "$bin: could not parse Time= from output:" >&2
        printf '%s\n' "$out" >&2
        return 1
    fi
    printf '%s' "$time"
}

echo "N,blas,neopt,opt_m"
for N in 400 600 800 1000 1200; do
    echo "1"              >  /tmp/bench_input
    echo "$N 123 /tmp/bo" >> /tmp/bench_input

    t_blas=$(run_one  ./tema1_blas   /tmp/bench_input)
    t_neopt=$(run_one ./tema1_neopt  /tmp/bench_input)
    t_opt=$(run_one   ./tema1_opt_m  /tmp/bench_input)

    echo "$N,$t_blas,$t_neopt,$t_opt"
done
