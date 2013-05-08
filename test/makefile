MPI_ROOT	= /usr/local
PBL_ROOT	= ../data_stores/pbl_1_04_04/src
MDHIM_ROOT	= ../src
TESTER_ROOT	= ../test

CC	= mpicc -g -O0
CLIBS	= -L${MPI_ROOT}/lib -lmpi -L${MDHIM_ROOT} -lmdhim -L${PBL_ROOT} -lpbl
CINC	= -I${MPI_ROOT}/include -I${PBL_ROOT} -I${TESTER_ROOT}
CFLAGS	= 

PBLOBJ	= pbl.o
MDHIMOBJ	= mdhim.o

all:	tester

tester:	tester.c 
	$(CC) $(CFLAGS) -o $@.x $@.c $(CINC) $(CLIBS) -I${MDHIM_ROOT}

clean:
	rm -rf *.o ${MDHIM_ROOT}/*.o tester.x
