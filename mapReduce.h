#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#ifndef INT_H
#define INT_H

#define KEYSIZE 100

typedef char* keyy_t;
typedef char* val_t;


typedef struct KV_t {
  keyy_t key;		/*key_t is used by some library function*/
  val_t value;
} KV_t;


struct HT_bucket {
  keyy_t key;
  size_t size;
  struct HT_bucket* next_key;
  char *values;
};

typedef struct HT_bucket* HTABLE;

struct reducer_t {
  keyy_t key;
  char **vals;
  size_t size;
  struct reducer_t* next;
};

void mapReduce(int files, char **fnames, int nprocesses, int rprocesses,
	void *mapfunc, void *reducefunc);

void MAP (char* input_split,void (*mapfunc)(char**, KV_t*), void (*reducefunc)(char*, char*),
			int R, char **bucketfnames, MPI_Status stat, int rank, int numtasks);

void reduce(MPI_File file, MPI_Status status, void (*reducefunc)(char*, char*));
int intoReduceType(MPI_File file, struct reducer_t** first, MPI_Status status);

void userdefReduce(char* inout, char* in);

void itoa(int n, char s[]);
void reverse(char s[]);

int HASH(KV_t *kv, HTABLE htable[], int R);


#endif
