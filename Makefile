CC      := cc
CFLAGS  := -Wall -Wextra -g
BIN     := mini-db

OBJS := main.o lexer.o parser.o pager.o wal.o btree.o executor.o

HEADERS := common.h lexer.h parser.h pager.h wal.h btree.h executor.h

.PHONY: all test clean

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS)

# Pattern rule: any X.c -> X.o, rebuilt if any header changes.
%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

# Build then run the Python test suite against the fresh binary.
test: $(BIN)
	python3 -m unittest tests.py

clean:
	rm -f $(OBJS) $(BIN)
