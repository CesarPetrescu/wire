/*
 * Tema 1 ASC
 * 2026 Spring
 */
#include "utils.h"
#include <string.h>
#include <cblas.h>

/*
 * BLAS implementation:
 * C = A^T * B
 * D = C * C^T  (symmetric)
 * y = 0; for i=0..N-1: y += D * row(C,i); y += x
 */
double* my_solver(int N, double *A, double *B, double *x) {
	double *C, *D, *y;
	int i, j;

	C = (double *)calloc(N * N, sizeof(double));
	D = (double *)calloc(N * N, sizeof(double));
	y = (double *)calloc(N, sizeof(double));
	if (!C || !D || !y) {
		free(C); free(D); free(y);
		return NULL;
	}

	/* C = A^T * B */
	cblas_dgemm(CblasRowMajor, CblasTrans, CblasNoTrans,
		    N, N, N, 1.0, A, N, B, N, 0.0, C, N);

	/* D = C * C^T (symmetric, use dsyrk for upper triangle) */
	cblas_dsyrk(CblasRowMajor, CblasUpper, CblasNoTrans,
		    N, N, 1.0, C, N, 0.0, D, N);

	/* Fill lower triangle from upper */
	for (i = 0; i < N; i++) {
		for (j = i + 1; j < N; j++) {
			D[j * N + i] = D[i * N + j];
		}
	}

	/* y = sum_{i=0}^{N-1} D * row(C, i) + x */
	for (i = 0; i < N; i++) {
		cblas_dgemv(CblasRowMajor, CblasNoTrans,
			    N, N, 1.0, D, N, C + i * N, 1, 1.0, y, 1);
	}

	/* y = y + x */
	cblas_daxpy(N, 1.0, x, 1, y, 1);

	free(C);
	free(D);

	return y;
}
