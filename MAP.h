/**
 * Header file for the MAP tasks??
 */

typedef void* map_key_t ;
typedef void* map_val_t ;

/**
 * The key, value pairs handled in MAP. MAP gets this from the user defined map functions, and deals with these types throughout - hashing etc
 */
struct MAP_keyval {
	map_key_t key ;
	map_val_t value ;
};

/**
 * This is what the user-defined map function spits out - a dynamic array of key-value pairs, and a count of the number of pairs */

struct MAP_out {
	int count ;  	//number of key-value pairs in the dynamic array
	DYNARRAY  data ; //actual mapped list  //K: add another *, before data.
};
	



