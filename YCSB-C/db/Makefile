CC=g++
CFLAGS=-std=c++14 -c -g -Wall -fmax-errors=5 -faligned-new -march=native -mtune=native -fgnu-tm -fcommon
INCLUDES=-L/opt/intel/oneapi/mkl/2022.2.0/lib/intel64/ -L/opt/intel/oneapi/compiler/2022.2.0/linux/compiler/lib/intel64_lin/ -I../ -I/opt/intel/oneapi/mkl/2022.2.0/include/ -lmkl_intel_ilp64 -lmkl_intel_thread -lmkl_core -liomp5 -lpthread -lm -ldl
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

