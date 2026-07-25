# Compiler
CC      := gcc

# Common flags
CFLAGS  := -O3 -Wall -Wextra -fPIC -D_GNU_SOURCE
LDFLAGS := -lpthread
WCQ_CFLAGS := $(CFLAGS) -Wno-unused-parameter -std=gnu11

# Targets
LIB     := libpinning.so
OBJ     := libpinning.o
SRC     := libpinning.c
WCQ_LIB := libwcq_native.so
WCQ_OBJ := wcq_native.o
WCQ_SRC := wcq_native.c
WCQ_HDR := wcq_native.h wcq_vendor/wfring_cas2.h $(wildcard wcq_vendor/lf/*.h)

# Default target
all: $(LIB) $(OBJ) $(WCQ_LIB) $(WCQ_OBJ)

# Build shared library (.so)
$(LIB): $(SRC)
	$(CC) $(CFLAGS) -shared -o $@ $< $(LDFLAGS)

# Build object file
$(OBJ): $(SRC)
	$(CC) $(CFLAGS) -c -o $@ $<

$(WCQ_LIB): $(WCQ_SRC) $(WCQ_HDR)
	$(CC) $(WCQ_CFLAGS) -shared -o $@ $(WCQ_SRC) $(LDFLAGS)

$(WCQ_OBJ): $(WCQ_SRC) $(WCQ_HDR)
	$(CC) $(WCQ_CFLAGS) -c -o $@ $(WCQ_SRC)

# Clean build artifacts
clean:
	rm -f $(LIB) $(OBJ) $(WCQ_LIB) $(WCQ_OBJ)

.PHONY: all clean
