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
//#define SIZE 4


/*Ranks for the global communicator clique
 * Assume mapping tasks numbered from 0 to M-1. 0 is master.
 *reduce tasks from M to M+R-1. 
 *Hence the number of processes given as input is M+R
 */

/*map tasks CAN communicate with all other reducer tasks too.*/
/*choice: let master handle M--R communication or it be independent*/

typedef struct HT_bucket* HTABLE;


/*
 *MAP: for each (key,val) pair produce (ikey,ival)
 *(File,Text)->(String,String)
 */

/*
 *KeyValue pairs, @KV@: (String,String)
 *Will work for all steps of the process. We leave it to the user-defined map an *d reduce functions to do the appropriate conversions.
 */


/*
struct InputKV_t {
  FILE* file;
  void* file_ptr;
};
*/



/* struct H_TABLE { */
/*   struct HT_bucket* bucket[R]; */
/* }; */




void mapReduce(int files, char **fnames, int nprocesses, int rprocesses, void *mapfunc, void *reducefunc)
{
	int numtasks, rank, source=0, dest, tag=1, i, amount;
	char buf[LINE_MAX], **bucketfnames, bucketfname[LINE_MAX];
	MPI_File file, *bucketfiles;
	MPI_Offset size, startpoint;
	
	bzero(buf, LINE_MAX);

	MPI_Status stat;

	MPI_Init(NULL,NULL);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	
	if(MPI_File_open(MPI_COMM_WORLD, fnames[0], MPI_MODE_RDONLY | MPI_MODE_UNIQUE_OPEN, MPI_INFO_NULL, &file) < 0) {
		perror("File open");
		exit(0);
	}
	
	bucketfnames = malloc(rprocesses * sizeof(char *));
	//bucketfiles = malloc(rprocesses * sizeof(MPI_File));
	for(i = 0; i < rprocesses; i++) {
		sprintf(bucketfname, "file%d", i);
		bucketfnames[i] = malloc(strlen(bucketfname)+1);
		sprintf(bucketfnames[i], "%s", bucketfname);
	}

  	MPI_File_get_size(file, &size);
  	startpoint = size / numtasks * rank;
  	
  	if(rank != numtasks - 1)
  		amount = size / numtasks * (rank + 1) - startpoint;
  	else
  		amount = size - startpoint;
  	
    MPI_File_read_at(file, startpoint, buf, amount, MPI_CHAR, &stat);
  	
  	MAP (buf, mapfunc, reducefunc, rprocesses, bucketfnames, stat, rank, numtasks);
  	
 /* 	printf("rank = %d, data = ", rank);
  	for(i = 0; i < amount; i++)
  		printf(" %c",buf[i]);
  	printf("\n");*/
  	//MPI_File filee;
  	printf("The End\n");
	MPI_Barrier( MPI_COMM_WORLD );
	printf("Closing\n");
	
	MPI_File_close(&file);
	MPI_Finalize();
}

void MAP (char* input_split,void (*mapfunc)(char**, KV_t*) , void (*reducefunc)(char*, char*),
			int R, char **bucketfnames, MPI_Status stat, int rank, int numtasks)	/*list of file pointers*/
{  
  	HTABLE htable[R];
  	//bzero(htable, R);
  	int i, r, a;
  	char buf[LINE_MAX];
  	for(i = 0; i < R; i++)
  		htable[i] = NULL;
  /*Each Mapper maintains its own hash table*/
  	//KV_t *ikv;
  	KV_t *retkeyvpair = malloc(sizeof(KV_t));
  	retkeyvpair->key = malloc(sizeof(char)*KEYSIZE);
  	retkeyvpair->value = malloc(sizeof(char)*KEYSIZE);
  	
  	//printf("input = %s", input_split);
  	
  	char* input=input_split;
  	//sprintf(input, "1121112121321324564648768545423123123154654684654321\n");
  	while(*input!='\0') {
  		//ikv = malloc(sizeof(KV_t));
  		//ikv->key = malloc(sizeof(char)*KEYSIZE);
  		//ikv->value = malloc(sizeof(char)*KEYSIZE);
  		
      		(*mapfunc)(&input, retkeyvpair); /*Also increments input..*/ 
     	
     	//strncpy(ikv->key, retkeyvpair->key, KEYSIZE);
     	//strncpy(ikv->value, retkeyvpair->value, KEYSIZE);
     	
     	//printf("key:%s, value:%s\n", retkeyvpair->key, retkeyvpair->value);
     	
      		HASH(retkeyvpair,htable, R); /*effectively groups by key*/

		//printf("ikv.key = %s, ikv.value = %s, input = %c", ikv->key, ikv->value, *input );
  	}	
  	
  	MPI_File files[R];
  	
  	for(i = 0; i < R; i++) {
  	
  		//MPI_File file;
  		MPI_Barrier( MPI_COMM_WORLD );
  		if(MPI_File_open(MPI_COMM_WORLD, bucketfnames[i], MPI_MODE_RDWR | 
				MPI_MODE_CREATE, MPI_INFO_NULL, &files[i]) < 0) { //MPI_MODE_DELETE_ON_CLOSE
			perror("File open");
			exit(0);
		}
		MPI_Barrier( MPI_COMM_WORLD );
		
		int totalsize = 0;
  		if(htable[i] != NULL) {
  			//MPI_File file = bucketfiles[i];
  			struct HT_bucket *temp = htable[i];
  			
  			char sbuf[LINE_MAX];
  			bzero(buf, LINE_MAX);

  			while(temp != NULL) {
  				
  				bzero(sbuf, LINE_MAX);
  				sprintf(sbuf, "%d", temp->size);
  				
  				printf("printing key:%s, into:&buf[%d]\n", temp->key, totalsize);
  				
  				sprintf(&buf[totalsize], "%s", temp->key);
  				//memcpy(&buf[totalsize], temp->key, strlen(temp->key)+1);
  				
  				printf("printing size:%d, into:&buf[%d]\n", temp->size, strlen(temp->key)+1+totalsize);
  				
  				sprintf(&buf[strlen(temp->key)+1+totalsize], "%d", temp->size);
  				//memcpy(&buf[strlen(temp->key)+1+totalsize], sbuf, strlen(sbuf)+1);
  				
  				printf("copying values:");
  				for(r = 0; r < temp->size; r++) {
  					if(temp->values[r] == '\0')
  						printf("_");
  					else
  						printf("%c", temp->values[r]);
  				}
  				printf(", into:&buf[%d], strlen(temp->key):%d, strlen(sbuf):%d\n", strlen(temp->key)+1+strlen(sbuf)+1+totalsize, strlen(temp->key), strlen(sbuf));
  				
  				
  				memcpy(&buf[strlen(temp->key)+1+strlen(sbuf)+1+totalsize], temp->values, temp->size);
  				
  				printf("totalsize was:%d, new totalsize:%d\n", totalsize, totalsize+strlen(temp->key)+1+strlen(sbuf)+1+temp->size);
  				
  				totalsize += strlen(temp->key)+1+strlen(sbuf)+1+temp->size;
  				//printf("key = %s value = %s size = %d\n", temp->key, temp->values, temp->size);
  				temp = temp->next_key;
  				
  				printf("rank = %d, printing unready buffer: ", rank);
  				for(r = 0; r < totalsize; r++) {
  					if(buf[r] == '\0')
  						printf("_");
  					else
  						printf("%c", buf[r]);
  				}
  				printf("buffer printed\n");
  			}
  		}
  		
  		printf("rank = %d, printing buffer: ", rank);
  		for(r = 0; r < totalsize; r++) {
  			if(buf[r] == '\0')
  				printf("_");
  			else
  				printf("%c", buf[r]);
  		}
  		printf("buffer printed\n");
  		
  		for(a = 0; a < numtasks; a++) {
  			if(htable[i] != NULL && rank == a) {
  					//MPI_File_set_atomicity(file, 1);
  				printf("bucket = %d, buf = %s\n",i, buf);
  				MPI_File_write_shared(files[i], buf, totalsize, 
                    MPI_CHAR, &stat);
            	//MPI_File_sync(file) ;
           	}
           	MPI_File_sync(files[i]);
           	MPI_Barrier( MPI_COMM_WORLD );
        }
        //MPI_Barrier( MPI_COMM_WORLD );
        //if(rank == 0)
        //MPI_File_set_view( file, 0, MPI_CHAR, MPI_CHAR, "native", MPI_INFO_NULL );
        	//MPI_File_close(&file);
		//}
		//else
			//for(a = 0; a < numtasks; a++) {
			//	MPI_Barrier( MPI_COMM_WORLD );
			//}
	}
	printf("I continued. Rank = %d\n", rank);
	//MPI_File_close(&file);
	//MPI_Barrier( MPI_COMM_WORLD );
	//for(i = 0; i < R; i++) {
		//MPI_File_close(&bucketfiles[i]);
		//
	
	/** Now spawn reducer jobs.*/	
	for(i = 0; i < R; i++)
		if(rank == i)
			reduce(files[i], stat, reducefunc);
		
}

void reduce(MPI_File file, MPI_Status status, void (*reducefunc)(char*, char*)) {
	int i;
	struct reducer_t* first, *temp;
	intoReduceType(file, &first, status);


	printf("All the keys in the end:\n");
	
	temp = first;
	while(temp != NULL) {
			
		printf("stored key:%s, size:%d\n", temp->key, temp->size);
		for(i = 0; i < temp->size; i++)
			printf("value[%d]:%s\n", i, temp->vals[i]);
				
		temp = temp->next;
	}

	/*BEGIN: prateek
 	*Now onto the actual reduce. We are finished with grouping by key now***/
	temp=first; //first is the first 'bucket'
	while(temp!=NULL)  {
		int i=0;
 		char* reduced_val = temp->vals[0];  //the first value. ?

		for(i=0; i<(temp->size-1); i++) {
			//userdefReduce(reduced_val , temp->vals[i+1]);
			(*reducefunc)(reduced_val, temp->vals[i+1]);
		}

		printf("KEY:%s , VALUE:%s\n",temp->key,reduced_val);

 		temp=temp->next;
 	}


}

/****REDUCED****/
	

/*user defined Reduce function*** - simple addition*/
/*void userdefReduce(char* inout, char* in)
{
	int i=atoi(in);
	int j=atoi(inout);
	sprintf(inout, "%d", (i+j));
}
*/
/* itoa:  convert n to characters in s. picked from K&R. */
/*void itoa(int n, char s[])
{ 
    int i, sign;

    if ((sign = n) < 0)  /* record sign */
 /*       n = -n;          /* make n positive */
 /*   i = 0;
    do {       /* generate digits in reverse order */
  /*      s[i++] = n % 10 + '0';   /* get next digit */
   /* } while ((n /= 10) > 0);     /* delete it */
/*    if (sign < 0)
        s[i++] = '-';
    s[i] = '\0';
    reverse(s);
} 

/* reverse:  reverse string s in place.Again K&R. */
/*void reverse(char s[])
{
    int c, i, j;

    for (i = 0, j = strlen(s)-1; i<j; i++, j--) {
        c = s[i];
        s[i] = s[j];
        s[j] = c;
    }
}*/
/*END: prateek */


int intoReduceType(MPI_File file, struct reducer_t** firstt, MPI_Status status) {
	char buf[LINE_MAX], *bufptr, keybuf[100], sizebuf[100], **vals, valsbuf[100][100], valbuf[100];
	int ret = 0, r, atnow = 0, valsize, values, readbytes, i, a;
	MPI_Offset startpoint = 0, fsize;
	struct reducer_t *temp = NULL, *first = NULL, *next;
	
	bzero(buf, LINE_MAX);
	//MPI_File_set_view( file, 0, MPI_CHAR, MPI_CHAR, "native", MPI_INFO_NULL );
	//MPI_File_read(file, buf, LINE_MAX, MPI_CHAR, &status);
	
	MPI_File_get_size(file, &fsize);
	MPI_File_read_at(file, startpoint, buf, fsize, MPI_CHAR, &status);
	
	printf("printing buffer, size = %d: ", fsize);
  	for(r = 0; r < fsize; r++) {
  		if(buf[r] == '\0')
  			printf("_");
  		else
  			printf("%c", buf[r]);
  	}
  	printf("buffer printed\n");
	//exit(0);
	if(strlen(buf) == 0)
		return 0;
	
	bufptr = buf;
	while(1) {
		if((atnow+1) >= fsize)
			break;
		
		//bufptr = &buf[atnow];
		
		bzero(keybuf, 100);
		memcpy(keybuf, bufptr, strlen(bufptr));
		atnow += strlen(keybuf) +1;
		bufptr = &buf[atnow];
		
		bzero(sizebuf, 100);
		memcpy(sizebuf, bufptr, strlen(bufptr));
		atnow += strlen(sizebuf) +1;
		bufptr = &buf[atnow];
		
		valsize = atoi(sizebuf);
		values = 0;
		readbytes = 0;
		while(1) {
			bzero(valbuf, 100);
			memcpy(valbuf, bufptr, strlen(bufptr));
			values++;
			strcpy(valsbuf[values-1], valbuf);
			readbytes += strlen(valbuf)+1;
			
			atnow += strlen(valbuf)+1;
			bufptr = &buf[atnow];
			
			if(readbytes == valsize)
				break;
		}/*
		printf("key:%s, size:%d\n", keybuf, values);
		for(i = 0; i < values; i++)
			printf("value[%d]:%s\n", i, valsbuf[i]);
			*/
	//	vals = malloc(values * sizeof(char *));
		//for(i = 0; i < values; i++) {
		//	vals[i] = malloc(strlen(valsbuf[i]) * sizeof(char));
		//	strcpy(vals[i], valsbuf[i]);	
		//}
		
		temp = first;
		
					// This is for the first key.
		if(temp == NULL) {
			temp = malloc(sizeof(struct reducer_t));
			temp->next = NULL;
			temp->size = values;
			temp->key = malloc((strlen(keybuf)+1) * sizeof(char));
			strcpy(temp->key, keybuf);
			
			temp->vals = malloc(values * sizeof(char *));
			for(i = 0; i < values; i++) {
				temp->vals[i] = malloc((strlen(valsbuf[i])+1) * sizeof(char));
				strcpy(temp->vals[i], valsbuf[i]);	
			}
			
			first = temp;
			/*
			printf("first stored key:%s, size:%d\n", temp->key, temp->size);
			for(i = 0; i < temp->size; i++)
				printf("value[%d]:%s\n", i, temp->vals[i]);
			
			printf("position now:%d\n",atnow);*/
			
			continue;
		}
		
		
		//printf("");
		
		
		
		while(1) {
			if(strcmp(temp->key, keybuf) == 0) {
				char **copy = temp->vals;
				temp->vals = malloc((temp->size+values) * sizeof(char *));
				for(i = 0; i < temp->size; i++) {
					temp->vals[i] = malloc((strlen(copy[i])+1) * sizeof(char));
					strcpy(temp->vals[i], copy[i]);
				}
				for(a = 0;i < (temp->size+values); a++, i++) {
					temp->vals[i] = malloc((strlen(valsbuf[a])+1) * sizeof(char));
					strcpy(temp->vals[i], valsbuf[a]);
				}
				for(i = 0; i < temp->size; i++)
					free(copy[i]);
				free(copy);
				
				temp->size += values;
				
				/*
				printf("appended key:%s, size:%d\n", temp->key, temp->size);
				for(i = 0; i < temp->size; i++)
					printf("value[%d]:%s\n", i, temp->vals[i]);
					
				printf("position now:%d\n",atnow);*/
				break;
			}
			else if(temp->next == NULL) {
				next = malloc(sizeof(struct reducer_t));
				next->next = NULL;
				next->size = values;
				next->key = malloc((strlen(keybuf)+1) * sizeof(char));
				strcpy(next->key, keybuf);
				
				next->vals = malloc(values * sizeof(char *));
				for(i = 0; i < values; i++) {
					next->vals[i] = malloc((strlen(valsbuf[i])+1) * sizeof(char));
					strcpy(next->vals[i], valsbuf[i]);	
				}
				
				temp->next = next;
				/*
				printf("new stored key:%s, size:%d\n", temp->next->key, temp->next->size);
				for(i = 0; i < temp->next->size; i++)
					printf("value[%d]:%s\n", i, temp->next->vals[i]);
					
				printf("position now:%d\n",atnow);*/
				break;
			}
			temp = temp->next;
		}
		
  	}
  	
  	
	
	
	*firstt = first;
	
	
}



  /*Now that all mapping is done, wait for other maps to finish, group-by-key an   *d call reduce
   */
  //Wait_for_all_Mappers();
//  	MPI_Barrier(MPI_COMM_GLOBAL);
  
  /*
  	HTABLE* temp=htable;
  	struct kv_list* kv;
  	for(r=0;r<R;r++) {
    	temp=htable[r];
    	while(temp!=NULL) {
    		kv=temp->vals;
    		REDUCE(kv,r);
    	}
	}*/
//}

//InterKV udefMAP(char* input) {
  /*User defined MAP function*/
//}


/*Reduce: specify the intermediate key for which the reduce is to be performed*/
/*
struct reducer_t REDUCER(struct kv_list* to_reduce,int r)
{
  struct reducer_t reduce=_malloc();
  reduce->next=NULL;
  MPI_Reduce(/*something goes here*//*);
  MPI_Reduce((void*)to_reduce ,(void*)reduce->reduced_val ,to_reduce->count,
            MPI_Datatype datatype, MPI_Op op,r,COMM_GLOBAL)
    if(rank==r) {
      /*print the reduced values*//*
      _PRINT(reduced->val);
    }
}
*/
/*returns a pointer to the list containing all the (key,value) pairs for a uniq  *key. effectively, traverse the hash table, by keeping some 'global' state in t
 *he MAP function
 */
/*struct kv_list* get_next_unique_ikey(HTABLE htable[])
{ 
  int r=0;
  HTABLE* temp=htable;
  struct kv_list* kv;
  for(r=0;r<R;r++) {
    temp=htable[r];
    while(temp!=NULL) {
    kv=temp->vals;
    REDUCE(kv,r);
    }
}*/


int HASH(KV_t *kv, HTABLE htable[], int R)
{
  	/*firstly, determine where key will hash to*/
  	int bucket_num = hashfun(kv->key, R);

  	char *to_add = kv->value;
  	/*to_add->next assigned to previous head of list*/

  	/*now figure out the 'top-level' bucket to add it to*/
 	struct HT_bucket *buck = htable[bucket_num];
  	if(buck==NULL) {
    	/*new->values = malloc(sizeof(char) * LINE_MAX);
   		new->key = malloc(sizeof(char) * (strlen(kv->key)+1));
     	*create HT_bucket node, and add the 
     	*key-val pair to the bucket */
   		struct HT_bucket* new = (struct HT_bucket*) malloc(sizeof(struct HT_bucket) );
   		new->values = malloc(sizeof(char) * LINE_MAX);
   		new->key = malloc(sizeof(char) * (strlen(kv->key)+1));
    	strcpy(new->key, kv->key);
    	sprintf(new->values, "%s", to_add);
    	new->size = strlen(to_add) +1;
    	
    	new->next_key=NULL;
    	htable[bucket_num] = new;
    	
    	return 1;
  	}

	struct HT_bucket *temp=buck;
	while(1) {
		
						// Found right key. Adding entry.
		if(strcmp(temp->key, kv->key) == 0) {
			
			sprintf(&(temp->values[temp->size]), "%s", to_add);
			temp->size += strlen(to_add)+1;
			break;
		}
						// To next bucket
		else if(temp->next_key != NULL)
			temp = temp->next_key;
			
		else {			// Adding a new bucket
			struct HT_bucket* new=(struct HT_bucket*) malloc(sizeof(struct HT_bucket) );
			new->values = malloc(sizeof(char) * LINE_MAX);
   			new->key = malloc(sizeof(char) * (strlen(kv->key)+1));
   			strcpy(new->key, kv->key);
    		sprintf(new->values, "%s", to_add);
   			new->size = strlen(to_add) +1;
   			
			new->next_key=NULL;
			temp->next_key=new;
			break;
		}
	}
	
	return 1;

} 

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
