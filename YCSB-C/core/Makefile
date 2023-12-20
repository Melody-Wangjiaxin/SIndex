CC=g++
CFLAGS=-std=c++11 -c -Wall -fmax-errors=5 -faligned-new -march=native -mtune=native -fcommon
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)
