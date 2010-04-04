#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

/**
 * Dynamic array implementation
 */
/**
 * READ THE GODDAMN CODE BEFORE GETTING STUCK HERE FOR 3 WEEKS! >.<
 */
#ifndef ELEMENT
typedef char ELEMENT ;
#endif

struct dynarry_struct {
	int count ;
	int max ;
	size_t elem_size ;  //SEE THIS? YES SIZE
	ELEMENT* DATA ;
	
}; 

typedef struct dynarry_struct* DYNARRAY ;

/*****************************************************************************/
DYNARRAY new_array (size_t elem_size) ;

int insert_into (DYNARRAY array , ELEMENT e) ;

/**THis is for the case if we dont want statically determined types. Array access cant be so straight forward. Also notice the return type, it is a void*, a pointer to some sequence of bytes.*/
void get_i(DYNARRAY array, int position) ;


