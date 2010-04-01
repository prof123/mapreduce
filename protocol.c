/* implementation of the protocol for the comminucation between map process and reduce process */
#define ACK_TABLE -1
#define ACK_VALUE -2
#define SEND_TABLE -3
#define SEND_VALUE -4 

void mapprocess()
{
	int reduce_rank;int flag =0;
	char *temp; void *buff;
	if(is_mapper()){
		reduce_rank = key_to_rank(key);
		if (!flag){
		
			MPI_Send(0, 0,MPI_INT , reduce_rank, SEND_TABLE, MPI_COMM_WORLD);
			flag=1;
		}
		while(1){
				MPI_Recv(temp, 10, MPI_INT, reduce_rank, TAG, MPI_COMM_WORLD);
				if( TAG == ACK_TABLE && tagtable[i]!=NULL){
					//size
					MPI_Send( &tagtable[i] , /*key->elem_size*/, tag_entry , reduce_rank, SEND_TABLE, MPI_COMM_WORLD);
				}
				else if( TAG == ACK_VALUE && mapout[i]!=NULL){
					MPI_Send( /*MAP_OUT*/ , /*key->elem_size*/, tag_entry , reduce_rank, SEND_VALUE, MPI_COMM_WORLD);
				}				
				else{
					break;
				}
		}		
		
	}		
}

void reduceproceess()
{
	int map_rank;int flag =0; int source_rank;
	char *temp; void *buff;
	if(is_reducer()){
		
		
		while(1){
				MPI_Recv(temp, 10, MPI_INT, MPI_ANY_SOURCE, tag, MPI_COMM_WORLD);
				source_rank=status(MPI_SOURCE);
				if( Tag == SEND_TABLE ){
					//allocate memory for table values
					MPI_Send(  0, 0, MPI_INT , status(MPI_SOURCE), ACK_TABLE, MPI_COMM_WORLD);
				}
				else if( TAG == SEND_VALUE ){
					// allocate memory for values
					MPI_Send( 0, 0, MPI_INT, source_rank, ACK_VALUE, MPI_COMM_WORLD);
				}				
				else{
					break;
				}
		}		
		
	}		

}
