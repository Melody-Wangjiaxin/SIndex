CC=g++
CFLAGS=-std=c++14 -mcx16 -c -g -Wall -fmax-errors=5 -faligned-new -march=native -mtune=native -fcommon
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)
