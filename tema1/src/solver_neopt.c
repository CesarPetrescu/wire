/*
 * Tema 1 ASC
 * 2026 Spring
 */
#include "utils.h"
#include <string.h>

/*
 * Unoptimized implementation:
 * C = A^T * B
 * D = C * C^T  (symmetric)
 * y = 0; for i=0..N-1: y += D * row(C, i); y += x
 */
double* my_solver(int N, double *A, double *B, double *x) {
	double *C, *D, *y;
	int i, j, k;

	C = (double *)calloc(N * N, sizeof(double));
	D = (double *)calloc(N * N, sizeof(double));
	y = (double *)calloc(N, sizeof(double));
	if (!C || !D || !y) {
		free(C); free(D); free(y);
		return NULL;
	}

	/* C = A^T * B
	 * C[i][j] = sum_k A^T[i][k] * B[k][j] = sum_k A[k][i] * B[k][j]
	 */
	for (i = 0; i < N; i++) {
		for (j = 0; j < N; j++) {
			double sum = 0.0;
			for (k = 0; k < N; k++) {
				sum += A[k * N + i] * B[k * N + j];
			}
			C[i * N + j] = sum;
		}
	}

	/* D = C * C^T  (D is symmetric, compute upper triangle and mirror)
	 * D[i][j] = sum_k C[i][k] * C^T[k][j] = sum_k C[i][k] * C[j][k]
	 */
	for (i = 0; i < N; i++) {
		for (j = i; j < N; j++) {
			double sum = 0.0;
			for (k = 0; k < N; k++) {
				sum += C[i * N + k] * C[j * N + k];
			}
			D[i * N + j] = sum;
			D[j * N + i] = sum;
		}
	}

	/* y = sum_{i=0}^{N-1} D * row(C, i)
	 * For each i: y[p] += sum_j D[p][j] * C[i][j]
	 */
	for (i = 0; i < N; i++) {
		for (j = 0; j < N; j++) {
			double c_val = C[i * N + j];
			for (k = 0; k < N; k++) {
				y[k] += D[k * N + j] * c_val;
			}
		}
	}

	/* y = y + x */
	for (i = 0; i < N; i++) {
		y[i] += x[i];
	}

	free(C);
	free(D);

	return y;
}
