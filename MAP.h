/**
 * Header file for the MAP tasks??
 */

typedef void* map_key_t ;
typedef void* map_val_t ;

struct MAP_keyval {
	map_key_t key ;
	map_val_t value ;
};

struct MAP_out {
	int count ;  	//number of key-value pairs in the dynamic array
	DYNARRAY  data ; //actual mapped list  //K: add another *, before data.
};
	



