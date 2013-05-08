MPI_ROOT	= /usr/local
PBL_ROOT	= ../data_stores/pbl_1_04_04/src

CC	= mpicc -g -O0
CLIBS	= -L${MPI_ROOT}/lib -lmpi -L${PBL_ROOT} -lpbl
CINC	= -I${MPI_ROOT}/include -I${PBL_ROOT}
CFLAGS	= 

PBLOBJ	= pbl.o
MDHIMOBJ	= mdhim.o

all:	mdhim.o mdhim_dswrap.o mdhim_hash.o mdhim_link.o range_server.o lib

mdhim.o: mdhim.c mdhim_dswrap.o
	$(CC) -c mdhim.c mdhim_dswrap.o $(CINC) $(CLIBS)

mdhim_dswrap.o: mdhim_dswrap.c
	$(CC) -c mdhim_dswrap.c $(CINC) $(CLIBS)

mdhim_hash.o: mdhim_hash.c
	$(CC) -c mdhim_hash.c $(CINC) $(CLIBS)

mdhim_link.o: mdhim_link.c
	$(CC) -c mdhim_link.c $(CINC) $(CLIBS)

range_server.o: range_server.c
	$(CC) -c range_server.c $(CINC) $(CLIBS)

lib:	
	ar rvs libmdhim.a mdhim.o mdhim_link.o mdhim_hash.o mdhim_dswrap.o range_server.o

clean:
	rm -rf *.o *.o libmdhim.a
