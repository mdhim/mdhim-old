.PHONY: mdhim tester

all: mdhim tester

mdhim:
	make -C src

tester:
	make -C test

clean:
	make -C src clean
	make -C test clean
