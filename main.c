/*Map Reduce parallel implementation using MPI. C code which can execute paralle *ly on cluster parallely.
 *First done as part of the parallel computing course (April 2008).
 */
/*Design notes:
 *1.File split to give to individual map tasks
 *2.Mappers execute on their own, and group by key locally. (This is hopefully all done in local memory, since max amt of data given to mapper task is ~ 64 Mb even in google's implementation
 *3.??
 *4. Reduce. (profit!!!!)
 */


#include "mapReduce.h"

void mapfunc(char** foo, KV_t *kv);
void reducefunc();

int main(int argc, char **arg) {
	char **fnames;
	typedef struct HT_bucket* HTABLE;
	int nfiles, rprocesses;
	//	nfiles=1;
	rprocesses = atoi(arg[1]);
	
	fnames = &arg[2];
	mapReduce(nfiles, fnames, 10, rprocesses, mapfunc, reducefunc);
	
	//void mapReduce(int files, char **fnames, int nprocesses, int rprocesses,
	//void *mapfunc, void *reducefunc);

	
	return 0;
}
/**
 * User defined map function. 
 */
void mapfunc(char** foo, KV_t *kv) {
	char* c=(char*)*foo;
	sprintf(kv->key, "%c", *c);
	sprintf(kv->value, "1");
	(*foo)++;
}
/**
 * User defined reduce function
 */
void reducefunc(char* inout, char* in) {
	int i=atoi(in);
	int j=atoi(inout);
	sprintf(inout, "%d", (i+j));
}
