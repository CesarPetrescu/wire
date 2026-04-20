# Tema 1 ASC - Makefile (skeleton, overwritten at grading)

CC       = gcc
CFLAGS   = -O2 -Wall -Wextra -std=gnu99
LDFLAGS  =
BLAS_LIB = -lopenblas -lm

COMMON   = main.c utils.h

all: tema1_blas tema1_neopt tema1_opt_m compare

tema1_blas: solver_blas.c main.c utils.h
	$(CC) $(CFLAGS) -o $@ main.c solver_blas.c $(BLAS_LIB)

tema1_neopt: solver_neopt.c main.c utils.h
	$(CC) $(CFLAGS) -o $@ main.c solver_neopt.c -lm

tema1_opt_m: solver_opt.c main.c utils.h
	$(CC) $(CFLAGS) -o $@ main.c solver_opt.c -lm

compare: compare.c
	$(CC) $(CFLAGS) -o $@ compare.c -lm

clean:
	rm -f tema1_blas tema1_neopt tema1_opt_m compare *.o

.PHONY: all clean
