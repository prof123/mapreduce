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


/**
 * Hash Table bucket
 */

struct HT_bucket {
  map_key_t key ;
  size_t size ;
  int count ;	//number of key-val pairs stored.
  struct HT_bucket* next_key ;
  DYNARRAY values ;
};

typedef struct HT_bucket* HTABLE;

/**
 * The tag table entry. Might need more fields. 
 * Stores the tag number and the key it corresponds to. 
 * Might also need to hash the key ??
 */
struct tag_entry {
  int tag ;
  map_key_t key ;
}

struct reducer_t {
  keyy_t key;
  val_t* vals;
  size_t size;
  struct reducer_t* next;
};

/**
 * All the user options go here ONLY!
 */
struct options {
	int num_map_tasks;
	int num_reducer_tasks;
	int total_tasks;
};

/**
 * All the user defined functions.
 * There will be more if needed. Hash etc an be user-defined too if the user insists
 */
struct udef_functions {
	void* (*map) (void*);
	void* (*reduce) (void*);
	void* (*merge) (void*);
};

/*****************************************************************************/
void start_MR (struct options OPTIONS, struct udef_functions udf);

//return list of unique keys?
DATAARRAY MAP (int rank, struct options OPTIONS, struct udef_functions udf);

int key_to_rank(keyy_t key);

void REDUCE (int rank, struct options OPTIONS, struct udef_functions udf);

/***********************************************************/

void itoa(int n, char s[]);

void reverse(char s[]);

int HASH(KV_t *kv, HTABLE htable[], int R);

/*****************************************************/
#endif
