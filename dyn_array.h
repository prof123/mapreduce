#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

/**
 * Dynamic array implementation
 */

#ifndef ELEMENT
typedef char ELEMENT ;
#endif

struct dynarry_struct {
	int count ;
	int max ;
	size_t elem_size ;
	ELEMENT* DATA ;
	
}; 

typedef struct dynarry_struct* DYNARRAY ;

/*****************************************************************************/
DYNARRAY new_array (size_t elem_size) ;

int insert_into (DYNARRAY array , ELEMENT e) ;




