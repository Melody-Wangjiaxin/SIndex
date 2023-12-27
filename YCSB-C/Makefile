CC=g++
CFLAGS=-std=c++14 -mcx16 -g -Wall -pthread -fmax-errors=5 -faligned-new -march=native -mtune=native -fcommon  
INCLUDES=-I./ -I/opt/intel/oneapi/mkl/2022.2.0/include/
LDFLAGS=-L/opt/intel/oneapi/mkl/2022.2.0/lib/intel64/ -L/opt/intel/oneapi/compiler/2022.2.0/linux/compiler/lib/intel64_lin/ -lmkl_intel_ilp64 -lmkl_intel_thread -lmkl_core -liomp5 -lpthread -lm -ldl -ltbb -lhiredis
SUBDIRS=core db redis
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

