# Compiler
CC      := gcc

# Common flags
CFLAGS  := -O3 -Wall -Wextra -fPIC -D_GNU_SOURCE
LDFLAGS := -lpthread

# Targets
LIB     := libpinning.so
OBJ     := libpinning.o
SRC     := libpinning.c

# Default target
all: $(LIB) $(OBJ)

# Atomic CPU support for the native Mojo wCQ algorithm (Linux x86-64).
# Queue operations remain in MPMC_queue_wcq.mojo.
wcq: wcq_atomics.o

wcq_atomics.o: wcq_atomics.c
	$(CC) $(CFLAGS) -std=c11 -mcx16 -c -o $@ $<

# Build shared library (.so)
$(LIB): $(SRC)
	$(CC) $(CFLAGS) -shared -o $@ $< $(LDFLAGS)

# Build object file
$(OBJ): $(SRC)
	$(CC) $(CFLAGS) -c -o $@ $<

# Clean build artifacts
clean:
	rm -f $(LIB) $(OBJ) wcq_atomics.o

.PHONY: all clean wcq
