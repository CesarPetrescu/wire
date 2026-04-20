/*
 * Tema 1 ASC
 * 2026 Spring
 */
#include "utils.h"
#include <string.h>

/*
 * Optimized implementation (same O(N^3) complexity as neopt):
 * - Cache-friendly kij loop ordering for C=A^T*B
 * - Register blocking & pointer arithmetic
 * - 8x loop unrolling throughout
 * - 8-way j-broadcast in y-loop to minimize y[] store traffic
 * - D symmetry exploited (upper triangle computed, then mirrored)
 * - 4 independent accumulators in dot products to exploit ILP
 *
 * Precondition: N is a multiple of 40 (per assignment statement:
 * "puteti presupune ca N este multiplu de 40 si ca este mai mic sau
 * egal cu 1200"). That guarantees N % 8 == 0 and N % 4 == 0, so the
 * unrolls below have no remainder to handle.
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

	/*
	 * C = A^T * B
	 * C[i][j] += A[k][i] * B[k][j]
	 * k-i-j order: A[k][i] broadcast, B[k][j] and C[i][j] sequential.
	 */
	for (k = 0; k < N; k++) {
		double *a_row = A + k * N;
		double *b_row = B + k * N;
		for (i = 0; i < N; i++) {
			register double a_ki = a_row[i];
			double *c_row = C + i * N;
			for (j = 0; j < N; j += 8) {
				c_row[j]     += a_ki * b_row[j];
				c_row[j + 1] += a_ki * b_row[j + 1];
				c_row[j + 2] += a_ki * b_row[j + 2];
				c_row[j + 3] += a_ki * b_row[j + 3];
				c_row[j + 4] += a_ki * b_row[j + 4];
				c_row[j + 5] += a_ki * b_row[j + 5];
				c_row[j + 6] += a_ki * b_row[j + 6];
				c_row[j + 7] += a_ki * b_row[j + 7];
			}
		}
	}

	/*
	 * D = C * C^T (symmetric)
	 * D[i][j] = sum_k C[i][k] * C[j][k]
	 * Only upper triangle computed, then mirrored.
	 * 4 independent accumulators for ILP.
	 */
	for (i = 0; i < N; i++) {
		double *ci = C + i * N;
		for (j = i; j < N; j++) {
			double *cj = C + j * N;
			register double s0 = 0.0, s1 = 0.0, s2 = 0.0, s3 = 0.0;
			for (k = 0; k < N; k += 8) {
				s0 += ci[k]     * cj[k]     + ci[k + 4] * cj[k + 4];
				s1 += ci[k + 1] * cj[k + 1] + ci[k + 5] * cj[k + 5];
				s2 += ci[k + 2] * cj[k + 2] + ci[k + 6] * cj[k + 6];
				s3 += ci[k + 3] * cj[k + 3] + ci[k + 7] * cj[k + 7];
			}
			register double sum = s0 + s1 + s2 + s3;
			D[i * N + j] = sum;
			D[j * N + i] = sum;
		}
	}

	/*
	 * y = sum_{i=0}^{N-1} D * row(C, i) + x
	 * Using D symmetry: D[p][j] = D[j][p], so access D row-wise.
	 * y[k] += D[j][k] * C[i][j]
	 * 8 j-values broadcast simultaneously, 4x k-unroll.
	 */
	for (i = 0; i < N; i++) {
		double *c_row = C + i * N;
		for (j = 0; j < N; j += 8) {
			register double c0 = c_row[j];
			register double c1 = c_row[j + 1];
			register double c2 = c_row[j + 2];
			register double c3 = c_row[j + 3];
			register double c4 = c_row[j + 4];
			register double c5 = c_row[j + 5];
			register double c6 = c_row[j + 6];
			register double c7 = c_row[j + 7];
			double *d0 = D + j * N;
			double *d1 = D + (j + 1) * N;
			double *d2 = D + (j + 2) * N;
			double *d3 = D + (j + 3) * N;
			double *d4 = D + (j + 4) * N;
			double *d5 = D + (j + 5) * N;
			double *d6 = D + (j + 6) * N;
			double *d7 = D + (j + 7) * N;
			for (k = 0; k < N; k += 4) {
				y[k]     += d0[k]*c0 + d1[k]*c1 + d2[k]*c2 + d3[k]*c3
					  + d4[k]*c4 + d5[k]*c5 + d6[k]*c6 + d7[k]*c7;
				y[k + 1] += d0[k+1]*c0 + d1[k+1]*c1 + d2[k+1]*c2 + d3[k+1]*c3
					  + d4[k+1]*c4 + d5[k+1]*c5 + d6[k+1]*c6 + d7[k+1]*c7;
				y[k + 2] += d0[k+2]*c0 + d1[k+2]*c1 + d2[k+2]*c2 + d3[k+2]*c3
					  + d4[k+2]*c4 + d5[k+2]*c5 + d6[k+2]*c6 + d7[k+2]*c7;
				y[k + 3] += d0[k+3]*c0 + d1[k+3]*c1 + d2[k+3]*c2 + d3[k+3]*c3
					  + d4[k+3]*c4 + d5[k+3]*c5 + d6[k+3]*c6 + d7[k+3]*c7;
			}
		}
	}

	for (i = 0; i < N; i++) {
		y[i] += x[i];
	}

	free(C);
	free(D);

	return y;
}
