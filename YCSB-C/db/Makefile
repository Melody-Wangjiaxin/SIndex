CC=g++
CFLAGS=-std=c++14 -mcx16 -c -g -Wall -fmax-errors=5 -faligned-new -march=native -mtune=native -fgnu-tm -fcommon
INCLUDES=-I../ -I/opt/intel/oneapi/mkl/2022.2.0/include/ 
LDFLAGS=-L/opt/intel/oneapi/mkl/2022.2.0/lib/intel64/ -L/opt/intel/oneapi/compiler/2022.2.0/linux/compiler/lib/intel64_lin/ -lmkl_intel_ilp64 -lmkl_intel_thread -lmkl_core -liomp5 -lpthread -lm -ldl
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $(LDFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)

