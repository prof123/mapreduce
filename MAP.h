/**
 * Header file for the MAP tasks??
 */

typedef char* map_key_t ;
typedef char* map_val_t ;

struct MAP_keyval {
	map_key_t key ;
	map_val_t value ;
};

struct MAP_out {
	int count ;  	//number of key-value pairs in the dynamic array
	DYNARRAY data ; //actual mapped list
};
	



