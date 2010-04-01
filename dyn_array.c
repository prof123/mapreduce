#include "dyn_array.h"
/**
 * This is the dynamic array implementation used
 */

DYNARRAY new_array (size_t elem_size) 
{
	DYNARRAY array ;
	array->count = 0 ;
	array->max = 0 ;
	array->elem_size = elem_size ;
	array->DATA = malloc(elem_size) ;
	return array ;
}


int insert_into (DYNARRAY array , ELEMENT e)  //size of the element
{
	DYNARRAY v ;
	v = array ;
	if (array==NULL) {  /* try creating new array */
		v = new_array(sizeof(e)) ;
	}
	size_t new_size ;
	int count,max ;
	count = v->count ;
	max = v->max ;
	if (count >= max) {  /* reached max limit allocated */
		v->max = ((v->max)*2) + 1 ;
		new_size = (v->max)*(v->elem_size) ; //
		v->DATA = (ELEMENT*) realloc(v->DATA , new_size);
	}
	v->DATA[count] = e ;
	v->count = v->count + 1 ;
	
	return (v->count);
}
	




