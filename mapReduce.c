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
#include "mapReduce.h"
#include "MAP.h"
#include "dyn_array.h"

int main(int argc, char** argv)
{
	/**
	 * TODO:Spawn all the mpi stuff here..
	 * TODO:fill the options for MAP.
	 */
	if( is_mapper(myrank)) {

		initialize_HASH_table() ;
		MAP(myrank,OPTIONS,udf) ;
	}

	if(is_reducer(myrank)) {

	}
}


void MAP (int rank, struct options OPTIONS, struct udef_functions udf)
{
	void* mapfun = udf.mapfunc ;
	
	struct MAP_out map_output ; 	//Store the output of udefmap
     	map_output = (*mapfunc)(rank) ;
	/* User does the input split based on rank. */
	/* And does dynamic array stuff.*/
	DYNARRAY output = map_output.data ;
	size_t elem_size = map_output.data.elem_size ;

	int i = 0 ;
	struct MAP_keyval keyval ;

/*Careful!We now assume that the type of the keyval etc is known. 
 * If we go the size way then modify dynamic array implementation slightly
 */
	while (i < output.count) {

		keyval = output[i] ;

		HASH (keyval) ;

		i++ ;
	}
  	
	populate_tag_tab() ;
}


HTABLE Hash_Table ;

void initialize_HASH_table() 
{
	Hash_Table.count = 0 ;
	Hash_Table.next_key = NULL ;
}

/**
 * Hash given keyval pair into the Hash_Table. Assumes we have the keycompare function
 * keycmp.Should the user gives that?
 */

/**
 * TODO: CHECK this function for pointer safety and logic.
 */
int HASH (struct MAP_keyval keyval) 
{
	map_key_t key ;
	map_val_t val ;
	key = keyval.key ;
	value = keyval.value ;
	HTABLE curr_ptr = Hash_Table ; 

	while ((curr_ptr->next)!= NULL) {
		if( (*keycmp)(curr_ptr->key , key)) { //equal
			insert_into (curr_ptr->values , keyval) ;
			curr_ptr->count = curr_ptr->count+1 ;

			return 1;
		}
		curr_ptr = curr_ptr->next ;
	}
	//reach here means key not found.Create new.
	HTABLE new_entry ;
	new_entry->key = key ;
	new_entry->values = NULL ;
	insert_into(new_entry->values , keyval) ;
	new_entry->count = 1 ;
	new_entry->next = NULL ;

	return 1 ;
}
#define TAG_MAX	10000 
//tag_max is max number of tags supported == INT_MAX
struct tag_entry[TAG_MAX] tag_table ;

/**
 * goes through hash table and populates tag table..
 */
/*
 * TODO: CHECK for logical loopholes please
 */
int populate_tag_tab() 
{
	HTABLE curr_key = Hash_Table ;
	map_key_t key ;
	struct tag_entry t;
	int i = 0 ;
	while (curr_key!=NULL) {
		key = curr_key->key ;
		tag_table[i].tag = i ;
		tag_table[i].key = key ;
		return 1 ;
	}
	return 0 ;
}

/* TODO */
int is_mapper(int rank) {
	return 1 ;
}

/* TODO */
int is_reducer(int rank) {
	return 1;
}

/* TODO */
int is_merger(int rank) {
	return 1;
}

/**
 * Not needed presently but a simple hash function is always useful!
 */
int hashfun(char* str,int R) 
{
	int h=0;
 	int g;
 	char* p;
 	for(p=str;*p!='\0';p++){
   		h=(h<<4)+*p;
   		if(g=h&0xf0000000) {
     		h=h^(g>>24);
     		h=h^g;
     	}
   	}
 	return h%R;
}
