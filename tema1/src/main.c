/*
 * Tema 1 ASC - main driver (skeleton, overwritten at grading).
 *
 * Reads a test-description file, generates matrices A, B and vector x
 * using the provided seed, calls my_solver, writes the result vector y
 * to the output path and prints the elapsed time.
 */

#include "utils.h"

static void generate_data(int N, unsigned int seed,
                          double *A, double *B, double *x) {
        int i, total = N * N;
        srand(seed);
        for (i = 0; i < total; i++)
                A[i] = ((double)rand() / RAND_MAX) * 2.0 - 1.0;
        for (i = 0; i < total; i++)
                B[i] = ((double)rand() / RAND_MAX) * 2.0 - 1.0;
        for (i = 0; i < N; i++)
                x[i] = ((double)rand() / RAND_MAX) * 2.0 - 1.0;
}

static int write_result(const char *path, double *y, int N) {
        FILE *f = fopen(path, "w");
        int i;
        if (!f) {
                perror("fopen");
                return -1;
        }
        for (i = 0; i < N; i++) {
                if (fprintf(f, "%.6lf\n", y[i]) < 0) {
                        perror("fprintf");
                        fclose(f);
                        return -1;
                }
        }
        if (fclose(f) != 0) {
                perror("fclose");
                return -1;
        }
        return 0;
}

int main(int argc, char **argv) {
        FILE *f;
        int num_tests, t, N;
        unsigned int seed;
        char out_path[512];
        double *A, *B, *x, *y;
        struct timespec t0, t1;

        if (argc != 2) {
                fprintf(stderr, "Usage: %s <input_file>\n", argv[0]);
                return 1;
        }
        f = fopen(argv[1], "r");
        if (!f) {
                perror("fopen input");
                return 1;
        }
        if (fscanf(f, "%d", &num_tests) != 1) {
                fprintf(stderr, "Cannot read number of tests\n");
                fclose(f);
                return 1;
        }

        for (t = 0; t < num_tests; t++) {
                if (fscanf(f, "%d %u %511s", &N, &seed, out_path) != 3) {
                        fprintf(stderr, "Bad test line %d\n", t);
                        fclose(f);
                        return 1;
                }
                /*
                 * The assignment guarantees N is a multiple of 40 and <= 1200.
                 * opt_m relies on N % 8 == 0 for its unrolled kernels, so
                 * refuse bad inputs early (local-testing safety net).
                 */
                if (N <= 0 || N % 40 != 0 || N > 1200) {
                        fprintf(stderr,
                                "Invalid N=%d (must be a positive multiple "
                                "of 40, <= 1200)\n", N);
                        fclose(f);
                        return 1;
                }
                A = (double *)malloc(sizeof(double) * N * N);
                B = (double *)malloc(sizeof(double) * N * N);
                x = (double *)malloc(sizeof(double) * N);
                if (!A || !B || !x) {
                        fprintf(stderr, "alloc failed for N=%d\n", N);
                        free(A); free(B); free(x);
                        fclose(f);
                        return 1;
                }
                generate_data(N, seed, A, B, x);

                clock_gettime(CLOCK_MONOTONIC, &t0);
                y = my_solver(N, A, B, x);
                clock_gettime(CLOCK_MONOTONIC, &t1);

                if (!y) {
                        fprintf(stderr, "Solver failed for N=%d\n", N);
                        free(A); free(B); free(x);
                        fclose(f);
                        return 1;
                }
                {
                        double secs = (t1.tv_sec - t0.tv_sec) +
                                      (t1.tv_nsec - t0.tv_nsec) / 1e9;
                        printf("Test N=%d Time=%.6lf\n", N, secs);
                }
                if (write_result(out_path, y, N) != 0) {
                        fprintf(stderr,
                                "Could not write result for N=%d to '%s'\n",
                                N, out_path);
                        free(A); free(B); free(x); free(y);
                        fclose(f);
                        return 1;
                }

                free(A); free(B); free(x); free(y);
        }
        fclose(f);
        return 0;
}
