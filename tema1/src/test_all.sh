#!/bin/bash
# Run all tests inside apptainer container on haswell.
# Usage (from inside the haswell node, inside container):
#   apptainer exec ~/optimizari.sif bash test_all.sh
# Or via sbatch:
#   sbatch -p haswell --time 00:05:00 --exclude=haswell-wn[29-30] \
#       --wrap="apptainer exec ~/optimizari.sif bash $PWD/test_all.sh"

# Fail fast on any solver/rename failure so stale out* files from a
# previous run cannot be compared silently.
set -e

cd "$(dirname "$0")"

# Clean any stale output artifacts from previous runs before we start.
rm -f out1 out2 out3 \
      out1_blas  out2_blas  out3_blas \
      out1_neopt out2_neopt out3_neopt \
      out1_opt   out2_opt   out3_opt

echo "=== Building ==="
make clean all

echo ""
echo "=== BLAS ==="
./tema1_blas ../input/input
mv out1 out1_blas; mv out2 out2_blas; mv out3 out3_blas

echo ""
echo "=== NEOPT ==="
./tema1_neopt ../input/input
mv out1 out1_neopt; mv out2 out2_neopt; mv out3 out3_neopt

echo ""
echo "=== OPT_M ==="
./tema1_opt_m ../input/input
mv out1 out1_opt;  mv out2 out2_opt;  mv out3 out3_opt

# From here on we *want* to keep running even if a compare reports a
# mismatch, so that we see failures across all sizes.
set +e

echo ""
echo "=== Correctness checks (blas vs neopt vs opt_m) ==="
rc=0
for i in 1 2 3; do
    echo -n "N=$i blas vs neopt : "
    ./compare out${i}_blas  out${i}_neopt 0.000001 || rc=1
    echo -n "N=$i blas vs opt_m : "
    ./compare out${i}_blas  out${i}_opt   0.000001 || rc=1
done
exit $rc
