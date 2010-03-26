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

typedef void* keyy_t;
typedef void* val_t;

/**
 */

typedef struct KV_pair {
  keyy_t key;		/*key_t is used by some library function*/
  val_t value;
} KV_pair;

/**
 * Hash Table bucket
 */

struct HT_bucket {
  keyy_t key ;
  size_t size ;
  int count ;	//number of key-val pairs stored.
  struct HT_bucket* next_key ;
  DYNARRAY values ;
};

typedef struct HT_bucket* HTABLE;

struct reducer_t {
  keyy_t key;
  val_t* vals;
  size_t size;
  struct reducer_t* next;
};

struct options {
	int num_map_tasks;
	int num_reducer_tasks;
	int total_tasks;
};

struct udef_functions {
	void* (*map) (void*);
	void* (*reduce) (void*);
	void* (*merge) (void*);
};

typedef struct key_list {
	keyy_t key;
	key_list* next;
};

/*******************************************************/

void start_MR (struct options OPTIONS, struct udef_functions udf);

//return list of unique keys?
DATAARRAY MAP (int rank, struct options OPTIONS, struct udef_functions udf);

int key_to_rank(keyy_t key);

void REDUCE (int rank, struct options OPTIONS, struct udef_functions udf);



void MAP (char* input_split,void (*mapfunc)(char**, KV_t*), void (*reducefunc)(char*, char*), int R, char **bucketfnames, MPI_Status stat, int rank, int numtasks);

void reduce(MPI_File file, MPI_Status status, void (*reducefunc)(char*, char*));

int intoReduceType(MPI_File file, struct reducer_t** first, MPI_Status status);

void userdefReduce(char* inout, char* in);


/***********************************************************/

void itoa(int n, char s[]);

void reverse(char s[]);

int HASH(KV_t *kv, HTABLE htable[], int R);

/*****************************************************/
#endif
