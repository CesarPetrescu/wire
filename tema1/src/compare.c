/*
 * Tema 1 ASC - compare utility (skeleton, overwritten at grading).
 * Usage: ./compare file1 file2 [tolerance]
 */
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

int main(int argc, char **argv) {
        FILE *f1, *f2;
        double a, b, tol = 1e-6, max_err = 0.0;
        int line = 0, diffs = 0;

        if (argc < 3) {
                fprintf(stderr, "Usage: %s file1 file2 [tolerance]\n", argv[0]);
                return 2;
        }
        if (argc >= 4)
                tol = atof(argv[3]);
        f1 = fopen(argv[1], "r");
        f2 = fopen(argv[2], "r");
        if (!f1 || !f2) {
                perror("fopen");
                return 2;
        }
        for (;;) {
                int r1 = fscanf(f1, "%lf", &a);
                int r2 = fscanf(f2, "%lf", &b);
                if (r1 != 1 || r2 != 1) {
                        if (r1 != r2) {
                                printf("DIFF length mismatch at line %d "
                                       "(file1 %s, file2 %s)\n",
                                       line + 1,
                                       r1 == 1 ? "continues" : "ends",
                                       r2 == 1 ? "continues" : "ends");
                                fclose(f1); fclose(f2);
                                return 1;
                        }
                        break;
                }
                double e = fabs(a - b);
                if (e > max_err) max_err = e;
                if (e > tol) diffs++;
                line++;
        }
        fclose(f1); fclose(f2);

        if (diffs == 0)
                printf("OK (%d values, max_err=%.2e)\n", line, max_err);
        else
                printf("DIFF %d/%d values, max_err=%.2e (tol=%.2e)\n",
                       diffs, line, max_err, tol);
        return diffs == 0 ? 0 : 1;
}
