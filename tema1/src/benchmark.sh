#!/bin/bash
# Benchmark all 3 variants with extended input (5 N values).
# Run inside apptainer on haswell.
# Usage: apptainer exec ~/optimizari.sif bash benchmark.sh > ../grafice/times.csv

cd "$(dirname "$0")"

make all >/dev/null

echo "N,blas,neopt,opt_m"
for N in 400 600 800 1000 1200; do
    echo "1"              >  /tmp/bench_input
    echo "$N 123 /tmp/bo" >> /tmp/bench_input

    t_blas=$(./tema1_blas  /tmp/bench_input 2>/dev/null | grep -oP 'Time=\K[0-9.]+')
    t_neopt=$(./tema1_neopt /tmp/bench_input 2>/dev/null | grep -oP 'Time=\K[0-9.]+')
    t_opt=$(./tema1_opt_m  /tmp/bench_input 2>/dev/null | grep -oP 'Time=\K[0-9.]+')

    echo "$N,$t_blas,$t_neopt,$t_opt"
done
