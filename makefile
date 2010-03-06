main : main.o mapReduce.o
	mpicc -g -o main main.o mapReduce.o
main.o : main.c
	mpicc -g -o main.o -c main.c
mapReduce.o : mapReduce.c
	mpicc -g -o mapReduce.o -c mapReduce.c
