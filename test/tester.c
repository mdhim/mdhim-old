/*
Copyright (c) 2011, Los Alamos National Security, LLC. All rights reserved.
Copyright 2011. Los Alamos National Security, LLC. This software was produced under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National Laboratory (LANL), which is operated by Los Alamos National Security, LLC for the U.S. Department of Energy. The U.S. Government ha rights to use, reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative works, such modified software should be clearly marked, so as not to confuse it with the version available from LANL.
 
Additionally, redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
·         Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
·         Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
·         Neither the name of Los Alamos National Security, LLC, Los Alamos National Laboratory, LANL, the U.S. Government, nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
/*
  tester is a parallel (MPI) program to test the functionality and correctness 
  of the MDHIM routines. 

  The test program will read in MDHIM initalization parameters from file 
  or from the command line. To run tester, use the following
  mpirun -n #procs ./tester.x input_params_file.txt mdhim_commands_file.txt fill_type
  where
  fill_type conrols how the input MDHIM commands file is read;
   = 1 expects file to have proc number and operation as explained below
   = 2 skips the first number in the file, except wildcards (*), and distributes the MDHIM commands in a round robin fashion between all procs.
   mdhim_commands_file.txt is the file that contains MDHIM commands
   input_params_file.txt is the file containing the MDHIM setup parameters

  The test program will read in the MDHIM operations to send to each of the 
  processes in the job. 

  Each line of the input MDHIM commands file has the following format: 

  rank operation [operation data]

  where rank is the rank of the process that you want to carry out 
  the operation. "rank" must be greater than zero since process zero only 
  reads, parses and sends out the commands to all other processes, but 
  never calls an MDHIM routine. "operation" is one of the valid operations 
  below and "operation data" is optional data related to the operation.
  
  Valid MDHIM operations and optional operation data:

  close
     will close all open MDHIM files.
  delete key_indx key_value

  finalize flush_flag
     will shut down all threads, close any open files, flush data if 
     flush_flag = 1 and shut down MDHIM
  find key_indx operation key_value
  
  flush
  
  get key_indx operation
  
  insert key1_size key1 key2_size key2 ... keyN_size keyN data
  
  open
  
  quit
     This tells rank 0 of the test program to stop reading from the input 
     commands file and send the "quit" command to all processes to stop 
     listening for commands and shut down. 
  
  where
  flush_flag = 1 to flush, 0 otherwise
  key_indx is 1 for primary key, 2 for first secondary key, etc.
  close, flush, finalize and open must be collective calls meaning all procs barrier and wait for others in these routines.

  Make sure rank is in the range of processors in this job. Remember, 
  rank 0 doesn't interact with MDHIM, just reads the input file 
  and sends commads to other processes.
  
  Global operations are specified with a "*" in place of rank. All processes 
  in the job, except rank 0, will perform this operation.
  
  To compile use included makefile in this directory.
  To run, use: mpirun ./queue.x queue_in.txt fill_num
  where 
  queue_in.txt is the input file with the queue elements
  fill_num is the fill type, defaults to 1
  
  Date: December 13, 2011

*/

/*
  Functions to implement basic queue functionality. The queue will hold 
  QUEUESIZEPLUS string elements. 
*/
#include "tester.h"

double insert_time = 0.0;
double delete_time = 0.0;
double flush_time = 0.0;
unsigned int total_inserts = 0;

int QUEUE_FULL(wk_queue *Q){
  if( (Q->head == Q->tail) && Q->data_size == QUEUESIZEPLUS)
    return TRUE;

  return FALSE;
}

int QUEUE_EMPTY(wk_queue *Q){
  if( (Q->head == Q->tail) && Q->data_size == 0)
    return TRUE;
  
  return FALSE;
}

int ENQUEUE(wk_queue *Q, char *qx, int size_qx){
  
  if(QUEUE_FULL(Q)){
    PRINT_QUEUE_DEBUG ("Alert: overflow.\n");
    PRINT_QUEUE_DEBUG ("Overflow: head[Q] = %d tail[Q] = %d\n", Q->head, Q->tail);
    return FALSE;
  }
  
  if( ++(Q->tail) > QUEUESIZE)
    Q->tail = 0;
  
  memset(Q->buf[Q->tail], '\0', MAXCOMMANDLENGTH*sizeof(char));
  strncpy(Q->buf[Q->tail], qx, size_qx);
  Q->data_size++;
  
  return TRUE;
}

int DEQUEUE(wk_queue *Q, char *qx, int *qx_size){
  if(QUEUE_EMPTY(Q)){
    PRINT_QUEUE_DEBUG ("Alert: underflow.\n");
    PRINT_QUEUE_DEBUG ("Underflow: head[Q] = %d tail[Q] = %d\n", Q->head, Q->tail);
    return FALSE;
  }
  
  if( ++(Q->head) > QUEUESIZE)
    Q->head = 0;
  
  *qx_size = strlen(Q->buf[Q->head]);
  memset(qx, '\0', MAXCOMMANDLENGTH*sizeof(char));
  strncpy(qx, Q->buf[Q->head], *qx_size);

  Q->data_size--;
  return TRUE;
}

void PRINT_QUEUE(wk_queue *Q){
  int i = 0;

  PRINT_QUEUE_DEBUG ("head[Q] = %d tail[Q] = %d data_size = %d\n", Q->head, Q->tail, Q->data_size);
  for(i = 0; i < QUEUESIZEPLUS; i++){
    PRINT_QUEUE_DEBUG ("%s ", Q->buf[i]);
  }
  PRINT_QUEUE_DEBUG ("\n");
}

void INIT_QUEUE(wk_queue *Q){
  int i;

  Q->head = Q->tail = 0;
  Q->data_size = 0;

  Q->buf = (char **)malloc(QUEUESIZEPLUS * sizeof(char *));
  for(i = 0; i < QUEUESIZEPLUS; i++){
    Q->buf[i] = (char *)malloc( (MAXCOMMANDLENGTH + 1)* sizeof(char));
    memset(Q->buf[i], '\0', MAXCOMMANDLENGTH * sizeof(char)); 
  }
}

void FREE_QUEUE(wk_queue *Q){
  int i;

  for(i = 0; i < MAXCOMMANDS; i++){
    if(&(Q->buf[i]) != NULL)
      free(Q->buf[i]);
  }
  
  if(Q->buf != NULL)
    free(Q->buf);
}

/*
  num_queues is the number of queues to fill in the wk_queue structure
  fill_type is how to place elements in the array of queues; 
  1 = use queue number from file and input parameter last_queue, 
  2 = fill by round robin, ignore first value in file, but wildcards are still honored, 
  3 = fill by round robin, assumes there is no queue number in file 

  Return: TRUE if all goes well and there is nothing more to read from the file
          FALSE otherwise
*/
int FILL_QUEUE(FILE *fp, wk_queue *Q, char *last_queue, char *last_element, int num_queues, int fill_type){
  int qindx = 0, indx, jndx;
  int rc, size_element;
  char j[MAXCOMMANDLENGTH], i[MAXCOMMANDLENGTH], dequeue_element[MAXCOMMANDLENGTH];
  
  PRINT_QUEUE_DEBUG ("*** FILL_QUEUE Entered: last_queue = %s last element = %s ***\n", last_queue, last_element);

  /*
    A last_queue value of -1 indicates last_queue and last_element are not 
    valid values and must read from file to get valid queue and element values.
  */
  strcpy(i, last_queue);
  strcpy(j, last_element);

  PRINT_QUEUE_DEBUG ("First elements are: %s %s\n", i, j);
  if(!strncmp(i, "-1", 2)){
    if(fill_type == 3){
      strcpy(i, "0");
      fscanf(fp, "%[^\n]", j);
    }
    else
      fscanf(fp, "%s %[^\n]", i, j);
    //XXX add checks to see if proc (i) is in correct range
  }
  

  if(strncmp(i, "*", 1)) {
    qindx = atoi(i);
  }
  
  while(!feof(fp)){
    /*
      Wildcard '*' means place element in all queues
    */
    if(!strncmp(i, "*", 1)){ 
      PRINT_QUEUE_DEBUG ("Queue number is wildcard %s\n", i);
      
      for(indx = 0; indx < num_queues; indx++){
	PRINT_QUEUE_DEBUG ("ENQUEUEING indx %d\n", indx);
	rc = ENQUEUE(&Q[indx], j, strlen(j));
	PRINT_QUEUE(&Q[indx]);
	
	/* 
	   If something goes wrong, undo enqueue meaning point the queue 
	   tail to one before the last element.
	*/
	if(!rc){ 
	  for(jndx = (indx - 1); jndx >= 0; jndx--){
	    if(Q[jndx].tail == 0){
	      Q[jndx].tail = QUEUESIZE;
	    }
	    else{
	      Q[jndx].tail--;
	    }
	    Q[jndx].data_size--;

	  }	    
	  indx = num_queues;
	  strcpy(last_element, j);
	  sprintf(last_queue, "%c", '*');
	  PRINT_QUEUE_DEBUG ("Exiting Queue is full. last queue %s last element %s\n", last_queue, last_element);
	  return FALSE;
	}
      }
      
    } /* end wildcard */
    else{ 
      
      if(fill_type == 1){
	qindx = atoi(i);
      }
      PRINT_QUEUE_DEBUG ("Queue number is %d\n", qindx);

      /*
	Check if the queue index is valid
      */
      if(qindx < num_queues){
	rc = ENQUEUE(&Q[qindx], j, strlen(j));
	PRINT_QUEUE(&Q[qindx]);
	/*
	  Queue is full. Exit the routine
	*/
	if(!rc){ 
	  strcpy(last_element, j);
	  sprintf(last_queue, "%d", qindx);
	  PRINT_QUEUE_DEBUG ("Exiting Queue is full. last queue %s last element %s\n", last_queue, last_element);
	  return FALSE;
	}
	
	if(fill_type == 2){ 
	  qindx++;
	  qindx %= num_queues;
	}
	else if(fill_type == 3){ 
	  qindx++;
	  qindx %= num_queues;
	}
      }
      else{
	printf("ERROR: Queue index %d is greater than the number of queues %d. Element %s will not be enqueued.\n", qindx, num_queues, j);
	
      }
    }
    
    if(fill_type == 3){
      strcpy(i, last_queue);
      fscanf(fp, "%[^\n]", j);
    }
    else
      fscanf(fp, "%s %[^\n]", i, j);
    //XXX add checks to see if proc (i) is in correct range
  }
  
  PRINT_QUEUE_DEBUG ("There are no more elements to read in the file\n");
  
  return TRUE;
}

int EMPTY_QUEUE(FILE *fp, wk_queue *Q, int num_queues){
  int qindx = 0, indx, jndx;
  char j[MAXCOMMANDLENGTH];
  int rc = TRUE, size_element;
  
  printf("EMPTY_QUEUE Entered\n");

  /*
    Empty queue until one of the queues is empty.
  */
  while(rc){
    
    for(indx = 0; indx < num_queues; indx++){
      PRINT_QUEUE_DEBUG ("DEQUEUEING indx %d\n", indx);
      rc = DEQUEUE(&Q[indx], j, &size_element);
      PRINT_QUEUE(&Q[qindx]);

    }
  }
  
  return TRUE;
}

void checkReturn(int rc, int rc_success, int rank, int opChar, char *inMsg){

  if(rc != rc_success)
    switch(opChar){
    case 'b':
      printf("Rank %d: ERROR - MPI Broadcast failed - %s\n", rank, inMsg);
      MPI_Abort(MPI_COMM_WORLD, 100);
      break;
    case 'f':
      printf("Rank %d: ERROR - fscanf failed - %s\n", rank, inMsg);
      MPI_Abort(MPI_COMM_WORLD, 100);
      break;
    case 'm':
      printf("Rank %d: MDHIM ERROR - %s\n", rank, inMsg);
      MPI_Abort(MPI_COMM_WORLD, 100);
      break;
    case 's':
      printf("Rank %d: ERROR - MPI Communicator Split failed - %s\n", rank, inMsg);
      MPI_Abort(MPI_COMM_WORLD, 100);
      break;
    default:
      break;
    }
}

char * printMdhimError(int err){
  

  if( err == MDHIM_SUCCESS){
    return "MDHIM_SUCCESS";
  }
  else if( err == MDHIM_ERROR_BASE){
    return "MDHIM_ERROR_BASE";
  }
  else if( err == MDHIM_ERROR_IDX_PATH){
    return "MDHIM_ERROR_IDX_PATH";
  }
  else if( err == MDHIM_ERROR_CTRL_INFO_PATH){
    return "MDHIM_ERROR_CTRL_INFO_PATH";
  }
  else if( err == MDHIM_ERROR_CTRL_INFOR_WRITE){
    return "MDHIM_ERROR_CTRL_INFOR_WRITE";
  }
  else if( err == MDHIM_ERROR_COMM){
    return "MDHIM_ERROR_COMM";
  }
  else if( err == MDHIM_ERROR_REC_NUM){
    return "MDHIM_ERROR_REC_NUM";
  }
  else if( err == MDHIM_ERROR_KEY){
    return "MDHIM_ERROR_KEY";
  }
  else if( err == MDHIM_ERROR_IDX_RANGE){
    return "MDHIM_ERROR_IDX_RANGE";
  }
  else if( err == MDHIM_ERROR_MEMORY){
    return "MDHIM_ERROR_MEMORY";
  }
  else if( err == MDHIM_ERROR_INIT){
    return "MDHIM_ERROR_INIT";
  }
  else if( err == MDHIM_ERROR_NOT_FOUND){
    return "MDHIM_ERROR_NOT_FOUND";
  }
  else if( err == MDHIM_ERROR_SPAWN_SERVER){
    return "MDHIM_ERROR_SPAWN_SERVER";
  }
  else if( err == MDHIM_ERROR_DB_OPEN){
    return "MDHIM_ERROR_DB_OPEN";
  }
  else if( err == MDHIM_ERROR_DB_CLOSE){
    return "MDHIM_ERROR_DB_CLOSE";
  }
  else if( err == MDHIM_ERROR_DB_COMMIT){
    return "MDHIM_ERROR_DB_COMMIT";
  }
  else if( err == MDHIM_ERROR_DB_DELETE){
    return "MDHIM_ERROR_DB_DELETE";
  }
  else if( err == MDHIM_ERROR_DB_FIND){
    return "MDHIM_ERROR_DB_FIND";
  }
  else if( err == MDHIM_ERROR_DB_FLUSH){
    return "MDHIM_ERROR_DB_FLUSH";
  }
  else if( err == MDHIM_ERROR_DB_GET_KEY){
    return "MDHIM_ERROR_DB_GET_KEY";
  }
  else if( err == MDHIM_ERROR_DB_INSERT){
    return "MDHIM_ERROR_DB_INSERT";
  }
  else if( err == MDHIM_ERROR_DB_READ){
    return "MDHIM_MDHIM_ERROR_DB_READ";
  }
  else if( err == MDHIM_ERROR_DB_READ_DATA_LEN){
    return "MDHIM_ERROR_DB_READ_DATA_LEN";
  }
  else if( err == MDHIM_ERROR_DB_READ_KEY){
    return "MDHIM_ERROR_DB_READ_KEY";
  }
  else if( err == MDHIM_ERROR_DB_COMPARE){
    return "MDHIM_ERROR_DB_COMPARE";
  }
  else if( err == MDHIM_ERROR_DB_SET_RECORD){
    return "MDHIM_ERROR_DB_SET_RECORD";
  }
  else if( err == MDHIM_ERROR_DB_START_TRANS){
    return "MDHIM_ERROR_DB_START_TRANS";
  }
  else if( err == MDHIM_ERROR_DB_UPDATE_DATA){
    return "MDHIM_ERROR_DB_UPDATE_DATA";
  }
  else if( err == MDHIM_ERROR_DB_UPDATE_KEY){
    return "MDHIM_ERROR_DB_UPDATE_KEY";
  }
  else{
    return "Unknown";
  }

}

int getMdhimCompare(int my_rank, char *op, char *data){
  
  if(!strcmp(op, "find")){
    
    if(!strcmp(data, "MDHIM_EQ"))
      return MDHIM_EQ;
    else if(!strcmp(data, "MDHIM_EQF"))
      return MDHIM_EQF;
    else if(!strcmp(data, "MDHIM_EQL"))
      return MDHIM_EQL;
    else if(!strcmp(data, "MDHIM_GEF"))
      return MDHIM_GEF;
    else if(!strcmp(data, "MDHIM_GTF"))
      return MDHIM_GTF;
    else if(!strcmp(data, "MDHIM_LEL"))
      return MDHIM_LEL;
    else if(!strcmp(data, "MDHIM_LTL"))
      return MDHIM_LTL;
    else{
      printf("Tester Rank %d: WARNING - Compare type %s is not a valid MDHIM type.\n", my_rank, data);
      return MDHIM_ERROR_DB_COMPARE;
    }
  }
  else if(!strcmp(op, "get")){
    if(!strcmp(data, "MDHIM_LXL"))
      return MDHIM_LXL;
    else if(!strcmp(data, "MDHIM_FXF"))
      return MDHIM_FXF;
    else if(!strcmp(data, "MDHIM_PRV"))
      return MDHIM_PRV;
    else if(!strcmp(data, "MDHIM_NXT"))
      return MDHIM_NXT;
    else if(!strcmp(data, "MDHIM_CUR"))
      return MDHIM_CUR;
    else{
      printf("Tester Rank %d: WARNING - Compare type %s is not a valid MDHIM type.\n", my_rank, data);
      return MDHIM_ERROR_DB_COMPARE;
    } 
  }
  else{
    printf("Tester Rank %d: WARNING - Operation %s is not a valid MDHIM operation.\n", my_rank, op);
    return MDHIM_ERROR_DB_COMPARE;
  }
}

int callMdhimOperations(MDHIMFD_t *fd, struct options o, int num_ops, char *in_op, char *op_data, int my_rank){
  
  char *data = NULL, *key_ptr = NULL;
  char okey[KEYSIZE], pkey[KEYSIZE], skey_list[MAXCOMMANDLENGTH];
  char data_buf[DATABUFFERSIZE];
  int pkey_len = 0, num_skeys = 0, data_len = 0;
  int indx, key_indx, key_size, do_op, compare_value, rec_num;
  int i, j, rc = MDHIM_SUCCESS;
  double starttime, endtime = 0.0;
  struct keyDataList *key_data_list = NULL;
  struct listrc errs;

  PRINT_TESTER_DEBUG ("Tester Rank %d: Entered callMdhimOperations with operation %s and op data %s\n", my_rank, in_op, op_data);
  
  /* 
     Check some input parameters
  */
  if(fd == NULL){
    printf("Tester Rank %d: ERROR - MDHIM fd structure is NULL.\n", my_rank);
    return MDHIM_ERROR_INIT;
  }

  /* 
     Based on the operation, parse the num_ops operations and data
  */
  
  if(!strcmp(in_op, "close")){
    /*
      Close the MDHIM data store files	   
      If more than one close command was sent, ignore number of close commands
      (num_ops) and ignore any data sent with the operation (op_data).
    */
    rc = mdhimClose(fd);
    printf("Tester Rank %d: mdhimClose returned %s (%d)\n", my_rank, printMdhimError(rc), rc); 
    if( rc != MDHIM_SUCCESS){
      printf("Tester Rank %d: WARNING - Problem with mdhimClose; error code %d\n", my_rank,rc); 
    }
  }
  else if(!strcmp(in_op, "delete")){
    /*
      For each delete request, parse the key index and input key to delete. 
    */
    starttime = MPI_Wtime();
    data = op_data;
    indx = 0;
    for (i = 0; i < num_ops; i++){
      memset(pkey, '\0', KEYSIZE);
      memset(okey, '\0', KEYSIZE);
      key_size = -1;
      sscanf(data, "%d %d %s", &data_len, &key_indx, &(pkey[0]));
      indx += data_len + 1;
      data = &(data[indx]);
      
      rc = mdhimDelete(fd, key_indx, pkey, &rec_num, &okey, &key_size);
      printf("Tester Rank %d: mdhimDelete returned new current key %s with record number %d.\n", my_rank, okey, rec_num);
      printf("Tester Rank %d: mdhimDelete returned %s (%d)\n", my_rank, printMdhimError(rc), rc); 

      if( rc != MDHIM_SUCCESS){
	printf("Tester Rank %d: WARNING - Problem with mdhimDelete; error code %d\n", my_rank, rc); 
      }
    }
    endtime = MPI_Wtime();
    delete_time += endtime - starttime;
    
  }
  else if(!strcmp(in_op, "finalize")){
    /*
      Close data files, shut down threads and, optionally flush
      If more than one finalize command was sent, ignore number of 
      finalize commands (num_ops), but we do want to look for the flush flag. 
    */
    sscanf(op_data, "%d %d", &data_len, &indx);
    
    rc = mdhimFinalize(fd, indx);
    printf("Tester Rank %d: mdhimFinalize returned %s (%d)\n", my_rank, printMdhimError(rc), rc); 
    if( rc != MDHIM_SUCCESS){
      printf("Tester Rank %d: WARNING - Problem with mdhimFinalize; error code %d\n", my_rank, rc); 
    }
    
  }
  else if(!strcmp(in_op, "find")){
    /*
      For each find request, parse the input string to get the key index, the 
      compare operation and key value to compare to.
    */
    data = op_data;
    indx = 0;
    for (i = 0; i < num_ops; i++){
      PRINT_TESTER_DEBUG ("Tester Rank %d: indx %d data[%d] = %s.\n", my_rank, i, indx, data);
      sscanf(data, "%s ", okey); 
      indx = strlen(okey) + 1;
      memset(pkey, '\0', KEYSIZE);
      memset(okey, '\0', KEYSIZE);
      memset(skey_list, '\0', MAXCOMMANDLENGTH);
      key_size = -1;
      sscanf(data, "%d %d %s %s", &data_len, &key_indx, skey_list, &(pkey[0]));
      indx += data_len + 1; 
      data = &(data[indx]);

      compare_value = getMdhimCompare(my_rank, "find", skey_list);
      if( compare_value == MDHIM_ERROR_DB_COMPARE){
	printf("Tester Rank %d: WARNING - Comparison operation for the call to mdhimFind is not recognized.\n", my_rank);
	// xxxReturn error
      }
      
      PRINT_TESTER_DEBUG ("Tester Rank %d: Calling mdhimFind with key_indx = %d, compare value = %d, find key = %s\n", my_rank, key_indx, compare_value, pkey);
      
      rc = mdhimFind( fd, key_indx, compare_value, pkey, &rec_num, &okey, &key_size);
      printf("Tester Rank %d: mdhimFind returned key %s with record number %d.\n", my_rank, okey, rec_num); 
      printf("Tester Rank %d: mdhimFind returned %s (%d)\n", my_rank, printMdhimError(rc), rc); 
      if( rc != MDHIM_SUCCESS){
	printf("Tester Rank %d: WARNING - Problem with mdhimFind; error code %d\n", my_rank, rc); 
      }    
      PRINT_TESTER_DEBUG ("Tester Rank %d: FIND RESULTS key returned from mdhimFind ikey = %s err = %d out key %s rec_num %d with size %d\n", my_rank, pkey, rc, okey, rec_num, key_size);
    } /* end for loop */
    
  } 
  else if(!strcmp(in_op, "flush")){
    /*
      For flush, all processes have the required inputs. So, there 
      are no other variables to parse in the message received.
    */
    starttime = MPI_Wtime();
    rc = mdhimFlush(fd);
    printf("Tester Rank %d: mdhimFlush returned with %d flush ranges.\n", my_rank, fd->flush_list.num_ranges); 
    printf("Tester Rank %d: mdhimFlush returned %s (%d)\n", my_rank, printMdhimError(rc), rc); 
    if( rc != MDHIM_SUCCESS){
      printf("Tester Rank %d: ERROR - Problem in mdhimFlush with MDHIM error %d errno %d.\n", my_rank, rc, errno);
    }
    endtime = MPI_Wtime();
    flush_time += endtime - starttime;
    
  }
  else if(!strcmp(in_op, "get")){
    /*
      For each get request in the string, parse it to get the key index, 
      compare operation and key value to compare to.
    */
    data = op_data;
    indx = 0;
    for (i = 0; i < num_ops; i++){
      PRINT_TESTER_DEBUG ("Tester Rank %d: indx %d data[%d] = %s.\n", my_rank, i, indx, data);
      sscanf(data, "%s ", okey); 
      indx = strlen(okey) + 1;
      memset(pkey, '\0', KEYSIZE);
      memset(okey, '\0', KEYSIZE);
      key_size = -1;
      sscanf(data, "%d %d %s", &data_len, &key_indx, &(pkey[0]));
      indx += data_len + 1;
      data = &(data[indx]);
      
      PRINT_TESTER_DEBUG ("Tester Rank %d: Parsing get command string with data_len = %d, key_indx = %d comparison %s.\n", my_rank, data_len, key_indx, pkey);
      compare_value = getMdhimCompare(my_rank, "get", pkey);
      
      if( compare_value == MDHIM_ERROR_DB_COMPARE){
	printf("Tester Rank %d: WARNING - Comparison operation for the call to mdhimGet is not recognized.\n", my_rank);
	// xxxReturn error
      }
      
      PRINT_TESTER_DEBUG ("Tester Rank %d: Calling mdhimGet with key_indx = %d, mode = %d\n", my_rank, key_indx, compare_value);
      rc = mdhimGet( fd, key_indx, compare_value, &rec_num, &okey, &key_size);

      printf("Tester Rank %d: mdhimGet returned key %s with record number %d.\n", my_rank, okey, rec_num); 
      printf("Tester Rank %d: mdhimGet returned %s (%d)\n", my_rank, printMdhimError(rc), rc); 

      if( rc == MDHIM_ERROR_NOT_FOUND){
	printf("Tester Rank %d: WARNING - key not found with mdhimGet; error code %d\n", my_rank, rc); 
      }    
      else if( rc != MDHIM_SUCCESS) {
	printf("Tester Rank %d: WARNING - Problem with mdhimGet; error code %d\n", my_rank, rc); 
      }

    }

  }
  else if(!strcmp(in_op, "insert")){
    /*
      For insert, copy the keys and data into a keyDataList structure and 
      call mdhimInsert
    */
    starttime = MPI_Wtime();

    if( (key_data_list = (struct keyDataList *)malloc(num_ops * sizeof(struct keyDataList)) ) == NULL){
      printf("Tester Rank %d: ERROR - Cannot allocate memory for the array of key/data list structures.\n", my_rank);
      return MDHIM_ERROR_MEMORY;
    }
    
    PRINT_TESTER_DEBUG ("Tester Rank %d: fd has nkeys = %d\n", my_rank, fd->nkeys);
      
    data = op_data;
    indx = 0;
    for (i = 0; i < num_ops; i++){
      /*
	Read in the total length of the keys and data to insert and read 
	the size of the primary key. Then move the pointer to the primary key.
      */
      PRINT_TESTER_DEBUG ("Tester Rank %d: Begin loop, data = %s\n", my_rank, data);

      memset(okey, '\0', KEYSIZE);
      sscanf(data, "%s ", okey); 
      indx += strlen(okey) + 1;
      data = &(op_data[indx]);
      data_len = atoi(okey); /* data_len will be length of insert data */

      PRINT_TESTER_DEBUG ("Tester Rank %d: total length = %d data = %s\n", my_rank, data_len, data);

      memset(okey, '\0', KEYSIZE);
      sscanf(data, "%s ", okey); 
      indx += strlen(okey) + 1;
      data = &(op_data[indx]);
      pkey_len = atoi(okey);
      data_len -= atoi(okey) + strlen(okey) + 2; /* data_len will be length of insert data */
      
      PRINT_TESTER_DEBUG ("Tester Rank %d: pkey length = %d okey = %s data = %s\n", my_rank, pkey_len, okey, data);
      memset(pkey, '\0', KEYSIZE);
      strncpy(pkey, data, pkey_len);
      indx += pkey_len + 1;
      data = &(op_data[indx]);
      
      PRINT_TESTER_DEBUG ("Tester Rank %d: After parsing pkey: pkey = %s with length %d data length %d and data = %s\n", my_rank, pkey, pkey_len, data_len, data);

      /*
	For every secondary key, put the key and key length into the skey_list
	buffer
      */
      memset(skey_list, '\0', MAXCOMMANDLENGTH);
      key_indx = 0;
      key_ptr = skey_list;
      for (j = 1; j < fd->nkeys; j++){

	PRINT_TESTER_DEBUG ("Tester Rank %d: Entered secondary key loop: pkey = %s with length %d data total length %d and data = %s\n", my_rank, pkey, pkey_len, data_len, data);
	memset(okey, '\0', 256);
	sscanf(data, "%s ", okey); 
	key_size = atoi(okey);
	indx += strlen(okey) + 1;
	key_indx += strlen(okey) + 1;
	data = &(op_data[indx]);

	memset(okey, '\0', 256);
	strncpy(okey, data, key_size);
	indx += key_size + 1;
	key_indx += key_size + 1;
	data = &(op_data[indx]);
	
	sprintf(key_ptr, "%d %s", key_size, okey);	
	key_ptr = &(key_ptr[key_indx]);
      }
      
      memset(data_buf, '\0', MAXCOMMANDLENGTH);
      strncpy(data_buf, data, data_len);
      indx += data_len + 1;
      data = &(op_data[indx]);

      PRINT_TESTER_DEBUG ("Tester Rank %d: Before setKeyDataList call with: key_data_list[%d] pkey = %s pkeylen %d skey_list = %s and data_buf = %s op_data = %s\n", my_rank, i, pkey, pkey_len, skey_list, data_buf, &(op_data[indx]));

      rc = setKeyDataList(my_rank, &(key_data_list[i]), pkey, pkey_len, data_buf, fd->nkeys - 1, skey_list);
      
      PRINT_TESTER_DEBUG ("Tester Rank %d: After setKeyDataList %d with: pkey = %s pkey len = %d and data = %s\n", my_rank, i, key_data_list[i].pkey,key_data_list[i].pkey_length, key_data_list[i].data );

    } /* end for (i=0; ...*/
    
    /*
      Allocate memory for array of error structs
    */
    errs.max_return = 0;
    errs.num_ops = 0;
    errs.errors = (ERRORTYPE *) malloc(num_ops * sizeof(ERRORTYPE) + 1);
    if(errs.errors == NULL){
      printf("Tester Rank %d: ERROR - Cannot allocate memory for the array of insert error codes.\n", my_rank);
      return MDHIM_ERROR_MEMORY;
    }
    for(i=0; i<num_ops; i++){
      errs.errors[i] = -1;
    }

    rc = mdhimInsert( fd, key_data_list, num_ops, &errs);

    PRINT_TESTER_DEBUG ("Tester Rank %d: After mdhimInsert, sent %d inserts. Error struct says inserted %d records with max return = %d\n", my_rank, num_ops, errs.num_ops, errs.max_return);
    
    printf("Tester Rank %d: mdhimInsert returned %s (%d)\n", my_rank, printMdhimError(rc), rc); 

    if( rc != MDHIM_SUCCESS){
      printf("Tester Rank %d: WARNING - Problem with mdhimInsert; error code %d\n", my_rank, rc); 
      free(key_data_list);
      free(errs.errors);
    }   
    endtime = MPI_Wtime();
    insert_time += endtime - starttime;
    
    for(i=0; i < num_ops; i++){
      PRINT_TESTER_DEBUG ("Tester Rank %d: mdhimInsert error[%d] = %d\n", my_rank, i, errs.errors[i]);
    }
    
    total_inserts += errs.num_ops;
    free(key_data_list);
    free(errs.errors);
  }
  else if(!strcmp(in_op, "open")){
    /*
      For open, all processes have the required inputs. So, there 
      are no other variables to parse in the message received, i.e. we 
      ignore anything else sent in the message.
    */
    PRINT_TESTER_DEBUG ("Tester Rank %d: Before mdhimOpen call with: recordPath = %s mode %d numKeys = %d keyType = %d keymaxLen = %d keyMaxPad = %d  maxDataSize = %d maxRecsPerRange = %d\n", my_rank, o.recordPath, o.mode, o.numKeys, o.keyType[0], o.keyMaxLen[0], o.keyMaxPad[0], o.maxDataSize, o.maxRecsPerRange);

    rc = mdhimOpen(fd, o.recordPath, o.mode, o.numKeys, o.keyType, o.keyMaxLen, o.keyMaxPad, o.maxDataSize, o.maxRecsPerRange);
    printf("Tester Rank %d: mdhimOpen returned %s (%d)\n", my_rank, printMdhimError(rc), rc); 
    if( rc != MDHIM_SUCCESS){
      printf("Tester Rank %d: ERROR - Problem in mdhimOpen with MDHIM error %d errno %d\n", my_rank, rc, errno);
      //xxx Return error
    }
    
  }
  else{
    printf("Tester Rank %d: WARNING - The following operation is not recognized and will be skipped: %s\n", my_rank, in_op);
  }
  
  return rc;
}

/*
Input
Output
 */
int getInput(int my_rank, char *fname, char *mdhim_fname, struct options *o, int read_from_file){

  char wkCommand[MAXCOMMANDLENGTH + 1];
  FILE *fp = NULL;
  int i, rc = 0;
  
  if(read_from_file == 1){
    /*
      Read set up parameters from a file
    */
    if(fname == NULL){
      printf("Please enter the full path and file name containing the MDHIM input parameters: \n");
      
      fname = (char *) malloc(256 * sizeof(char));
      
      if(gets(fname) == NULL){
	printf("Rank %d: Error reading the input parameters file name.\n", my_rank);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }
    }
    
    if((fp = fopen(fname, "r")) == NULL){
      printf("Rank %d: Error - Unable to open file name %s\n", my_rank, fname);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    
    rc = fscanf(fp, "%s ", wkCommand);
    checkReturn(rc, 1, my_rank, 'f', "Problem reading from the input parameters file.");
    
    while(!feof(fp)){
      PRINT_TESTER_DEBUG ("Rank %d: Parameter just read: %s\n", my_rank, wkCommand);
      if(!strcmp(wkCommand, "rangeservers")){
	fscanf(fp, "%d", &o->numRangeSvrs);
	if( (o->rangeSvrs = (char **)malloc(o->numRangeSvrs*sizeof(char *))) == NULL){
	  printf("Rank %d: Error allocating memory for range server names.\n", my_rank);
	  fclose(fp);
	  MPI_Abort(MPI_COMM_WORLD, 100);
	}
	for(i = 0; i < o->numRangeSvrs; i++){
	  if( (o->rangeSvrs[i] = (char *)malloc(MAX_HOST_NAME + 1)) == NULL){
	    printf("Rank %d: Error allocating memory for range server %d name.\n", my_rank, i+1);
	    fclose(fp);
	    MPI_Abort(MPI_COMM_WORLD, 100);
	  }
	  memset(o->rangeSvrs[i], '\0', MAX_HOST_NAME);
	  if(fscanf(fp, "%s", o->rangeSvrs[i]) != 1){
	    printf("Rank %d: Problem reading range server name %d from the input parameters file.\n", my_rank, i+1);
	    fclose(fp);
	    MPI_Abort(MPI_COMM_WORLD, 100);
	  }
	  PRINT_TESTER_DEBUG ("o->rangeSvrs[%d] = %s\n", i,o->rangeSvrs[i]); 
	}
      }
      else if(!strcmp(wkCommand, "serversperhost")){
	if( (o->numRangeSvrsByHost = (int *)malloc(o->numRangeSvrs*sizeof(int))) == NULL){
	  printf("Rank %d: Error allocating memory for number of range servers per host.\n", my_rank);
	  fclose(fp);
	  MPI_Abort(MPI_COMM_WORLD, 100);
	}
	for(i = 0; i < o->numRangeSvrs; i++)
	  if(fscanf(fp, "%d", &(o->numRangeSvrsByHost[i])) != 1){
	    printf("Rank %d: Problem reading number of range servers per host %d from the input parameters file.\n", my_rank, i+1);
	    fclose(fp);
	    MPI_Abort(MPI_COMM_WORLD, 100);
	  }	
      }
      else if(!strcmp(wkCommand, "commtype")){
	if(fscanf(fp, "%d", &o->commType) != 1){
	  printf("Rank %d: Problem reading the communication type from the input parameters file.\n", my_rank);
	  fclose(fp);
	  MPI_Abort(MPI_COMM_WORLD, 100);
	}    	
      }
      else if(!strcmp(wkCommand, "path")){
	if(fscanf(fp, "%s", wkCommand) != 1){
	  printf("Rank %d: Problem reading path to store MDHIM files from the input parameters file.\n", my_rank);
	  fclose(fp);
	  MPI_Abort(MPI_COMM_WORLD, 100);
	}
	memset(o->recordPath, '\0', TEST_BUFLEN);
	strcpy(o->recordPath, wkCommand);
      }
      else if(!strcmp(wkCommand, "mode")){
	if(fscanf(fp, "%d", &o->mode) != 1){
	  printf("Rank %d: Problem reading mode to open MDHIM files from the input parameters file.\n", my_rank);
	  fclose(fp);
	  MPI_Abort(MPI_COMM_WORLD, 100);
	}
      }
      else if(!strcmp(wkCommand, "numkeys")){
	if(fscanf(fp, "%d", &o->numKeys) != 1){
	  printf("Rank %d: Problem reading number of keys from the input parameters file.\n", my_rank);
	  fclose(fp);
	  MPI_Abort(MPI_COMM_WORLD, 100);
	}
      }
      else if(!strcmp(wkCommand, "keytype")){
	for(i = 0; i < o->numKeys; i++){
	  if(fscanf(fp, "%d", &(o->keyType[i])) != 1){
	    printf("Rank %d: Problem reading key type %d from the input parameters file.\n", my_rank, i+1);
	    fclose(fp);
	    MPI_Abort(MPI_COMM_WORLD, 100);
	  } 
	}
      }
      else if(!strcmp(wkCommand, "keymaxlen")){
	for(i = 0; i < o->numKeys; i++){
	  if(fscanf(fp, "%d", &(o->keyMaxLen[i])) != 1){
	    printf("Rank %d: Problem reading key max length %d from the input parameters file.\n", my_rank, i+1);
	    fclose(fp);
	    MPI_Abort(MPI_COMM_WORLD, 100);
	  } 
	}
      }
      else if(!strcmp(wkCommand, "keymaxpad")){
	for(i = 0; i < o->numKeys; i++){
	  if(fscanf(fp, "%d", &(o->keyMaxPad[i])) != 1){
	    printf("Rank %d: Problem reading key max padding length %d from the input parameters file.\n", my_rank, i+1);
	    fclose(fp);
	    MPI_Abort(MPI_COMM_WORLD, 100);
	  } 
	}
      }
      else if(!strcmp(wkCommand, "datamaxlen")){
	if(fscanf(fp, "%d", &o->maxDataSize) != 1){
	  printf("Rank %d: Problem reading maximum data size from the input parameters file.\n", my_rank);
	  fclose(fp);
	  MPI_Abort(MPI_COMM_WORLD, 100);
	} 
      }
      else if(!strcmp(wkCommand, "maxrecords")){
	if(fscanf(fp, "%d", &o->maxRecsPerRange) != 1){
	  printf("Rank %d: Problem reading maximum records per range from the input parameters file.\n", my_rank);
	  fclose(fp);
	  MPI_Abort(MPI_COMM_WORLD, 100);
	} 
      }
      else{
	printf("Rank %d: Unrecognized MDHIM parameter: %s. This line will be ignored.\n", my_rank, wkCommand);
	fscanf(fp, "*");
      }	
      memset(wkCommand, '\0', MAXCOMMANDLENGTH + 1);
      fscanf(fp, "%s ", wkCommand);
      /*
	if(fscanf(fp, "%s ", wkCommand) != 1){
	printf("Rank %d: Problem reading from the input parameters file.\n", my_rank);
	return 0;
	}
      */
    } /* End while loop */
    
    fclose(fp);
  }
  else{
    /*
      Read MDHIM parameters interactively
    */
    printf("Please enter the number of (unique) range servers: \n");
    if(scanf("%d", &o->numRangeSvrs) != 1){
      printf("Rank %d: Error reading number of range servers.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    
    if( (o->rangeSvrs = (char **)malloc(o->numRangeSvrs*sizeof(char *))) == NULL){
      printf("Rank %d: Error allocating memory for range server names.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    printf("Please enter the list of hostnames of the range servers, each name separated by a space: \n");
    for(i = 0; i < o->numRangeSvrs; i++){
      if( (o->rangeSvrs[i] = (char *)malloc(MAX_HOST_NAME + 1)) == NULL){
	printf("Rank %d: Error allocating memory for range server %d name.\n", my_rank, i+1);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }
      if(scanf("%s", o->rangeSvrs[i]) != 1){
	printf("Rank %d: Problem reading range server name %d from the command line.\n", my_rank, i+1);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }
      o->rangeSvrs[i][strlen(o->rangeSvrs[i])] = '\0';
      printf("string length of %s is %d\n", o->rangeSvrs[i], (int)strlen(o->rangeSvrs[i])); 
    }
    
    printf("For each range server, please enter the number of servers per host: \n");
    if( (o->numRangeSvrsByHost = (int *)malloc(o->numRangeSvrs*sizeof(int))) == NULL){
      printf("Rank %d: Error allocating memory for number of range servers per host.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    for(i = 0; i < o->numRangeSvrs; i++)
      if(scanf("%d", &(o->numRangeSvrsByHost[i])) != 1){
	printf("Rank %d: Problem reading number of range servers per host %d from the command line.\n", my_rank, i+1);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }	
    
    printf("Please enter the communication type; MPI 1, PGAS 2: \n");
    if(scanf("%d", &o->commType) != 1){
      printf("Rank %d: Problem reading the communication type from the command line.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }    	
    
    printf("Please enter path to store the MDHIM files: \n");
    if(scanf("%s", wkCommand) != 1){
      printf("Rank %d: Problem reading path to store MDHIM files from the command line.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    memset(o->recordPath, '\0', TEST_BUFLEN);
    strcpy(o->recordPath, wkCommand);
    
    printf("Please enter the mode to open the MDHIM files create and update 1 read only 2: \n");
    if(scanf("%d", &o->mode) != 1){
      printf("Rank %d: Problem reading mode to open MDHIM files from the input parameters file.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    
    printf("Please enter the number of keys: \n");
    if(scanf("%d", &o->numKeys) != 1){
      printf("Rank %d: Problem reading number of keys from the input parameters file.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    
    printf("Please enter the key type for each of the keys; 0 alpha numeric, 1 int 2 float: \n");
    for(i = 0; i < o->numKeys; i++){
      if(scanf("%d", &(o->keyType[i])) != 1){
	printf("Rank %d: Problem reading key type %d from the command line.\n", my_rank, i+1);
	MPI_Abort(MPI_COMM_WORLD, 100);
      } 
    }
    /*
      printf("Please enter if duplicate keys are allowed 1 yes 0 no: \n");
    */
    printf("For each key, please enter the key's maximum length separated by spaces. Primary key first: \n");
    for(i = 0; i < o->numKeys; i++){
      if(scanf("%d", &(o->keyMaxLen[i])) != 1){
	printf("Rank %d: Problem reading key max length %d from the command line.\n", my_rank, i+1);
	MPI_Abort(MPI_COMM_WORLD, 100);
      } 
    }
    
    printf("For each key, please enter the key's maximum padding size. Primary key first: \n");
    for(i = 0; i < o->numKeys; i++){
      if(scanf("%d", &(o->keyMaxPad[i])) != 1){
	printf("Rank %d: Problem reading key max padding length %d from the input parameters file.\n", my_rank, i+1);
	MPI_Abort(MPI_COMM_WORLD, 100);
      } 
    }
    
    printf("Please enter the maximum record data size: \n");
    if(scanf("%d", &o->maxDataSize) != 1){
      printf("Rank %d: Problem reading maximum data size from the  command line.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    } 
    
    printf("Please enter the maximum number of records per range: \n");
    if(scanf("%d", &o->maxRecsPerRange) != 1){
      printf("Rank %d: Problem reading maximum records per range from the command line.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    } 
    
    printf("Please enter the full path and file name containing the input MDHIM commands: \n");
    if(scanf("%s", mdhim_fname) != 1){
      printf("Rank %d: Error reading the MDHIM commands file name.\n", my_rank);
      return 0;
    }
    
  }

  return TRUE;  
}

int main(int argc, char **argv){
  
  int i, j, indx = 0;
  int do_work = 0, rc = 0;
  int comm_size = 0,  my_rank = -1;
  int key_indx, key_size = 0, datalen = 0;
  int num_ops = 0, num_same_op = 0, pathLen = 0;
  int *indx_array = NULL;

  char wkCommand[MAXCOMMANDLENGTH + 1];
  char **wkBuffer = NULL; 
  char *data_buf = NULL;
  char fname[256], mdhim_fname[256];
  char temp_string[256], *str_ptr;
  char *data = NULL, new_op[256], last_op[256];
  char new_data[256], last_data[256];
  
  wk_queue *workQueue;   /* Queue variables */
  char queue_char[MAXCOMMANDLENGTH+1];
  char queue_element[MAXCOMMANDLENGTH+1];
  char dq_element[MAXCOMMANDLENGTH+1];
  int num_queues = 0, recv_int = -1, num_ready = 0;
  int queue_indx = 0, queue_number = 0, dq_element_size;
  int fill_type = 1;
  
  double starttime, endtime = 0.0, jobtime = 0.0;

  struct options o;
  FILE *fp = NULL;
  MDHIMFD_t fd;
  struct rangeDataTag *cur_range = NULL;

  MPI_Status recv_status;
  MPI_Status *mpi_status = NULL;
  MPI_Request *mpi_request = NULL;
  MPI_Request send_request;
  MPI_Comm mdhimComm;

  /*
    Start up MPI with thread support and get my rank and size of the job
  */
  indx = 0;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &indx);
  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  
  PRINT_TESTER_DEBUG ("MPI_THREAD_MULTIPLE = %d and provide = %d\n", MPI_THREAD_MULTIPLE, indx);

  /*
    Phase 1: Collect and Distribute initalization parameters
    Get the input file from the command line arguments or Rank 0 asks the 
    user how to get the input parameters; from a file or get them 
    interactively from the command line. 
  */
  if(my_rank == 0){
    
    /*
      Initalize input parameters
    */
    o.numRangeSvrs = 0;
    o.commType = 0;
    o.mode = 0;
    o.numKeys = 0;
    o.keyType[0] = -1;
    o.keyMaxLen[0] = -1;
    o.keyMaxPad[0] = -1;
    o.maxDataSize = 0;
    o.maxRecsPerRange = 0;
    o.numRangeSvrsByHost = NULL;
    strncpy(o.recordPath, "", TEST_BUFLEN-1);
    
    if(argc > 3){
      fill_type = atoi(argv[3]);
      if( (fill_type != 1) && (fill_type != 2)){
	printf("Tester: Error - Queue fill type must be 1 or 2, not %d.\n", fill_type);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }
    }
    if(argc > 2){
      indx = 1;
      i = 1;
      strcpy(fname, argv[1]);
      strcpy(mdhim_fname, argv[2]);
    }
    else{
      printf("Please enter the method of reading input parameters - from file (1) or interactive (2): \n");
      if(gets(wkCommand) == NULL){
	printf("Rank %d: Error reading input parameter method.\n", my_rank);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }
      
      indx = atoi(wkCommand);
    }

    /* 
       Get the input parameters by reading a file or interactively
    */
    getInput(my_rank, fname, mdhim_fname, &o, indx);
    pathLen = strlen(o.recordPath);

    /* 
       Check all input parameters
    */
    if(o.commType != 1 && o.commType != 2){
      printf("Rank %d: Error - Valid communication types are 1 for MPI and 2 for PGAS. Entered %d.\n", my_rank, o.commType);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    if(o.mode < 0 || o.mode > 1){
      printf("Rank %d: Error - Valid modes to open the MDHIM files are 0 for read only and 1 for reading and writing. Entered %d.\n", my_rank, o.commType);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    if(o.numKeys < 1 ){
      printf("Rank %d: Error - Number of keys entered (%d) must be greater than 1.\n", my_rank, o.numKeys);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    for(i = 0; i < o.numKeys; i++){
      if(o.keyType[i] < 0 || o.keyType[i] > 2){
	printf("Rank %d: Error key type %d entered (%d) is not valid. Use 0 for alpha-numeric, 1 for integer or 2 for float\n", my_rank, i+1, o.keyType[i]);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }
      if(o.keyMaxLen[i] < 1){
	printf("Rank %d: Error key max length %d entered (%d) is not valid. Must be greater than 0.\n", my_rank, i+1, o.keyMaxLen[i]);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }
    }
  } /* end if(my_rank == 0) */
  
  /*
    Now that rank 0 has all the input parameters, broadcast to everyone else
  */
  MPI_Barrier(MPI_COMM_WORLD);

  rc = MPI_Bcast(&o.numRangeSvrs, 1, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Number of range servers.");
  rc = MPI_Bcast(&o.commType, 1, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Communication type.");
  rc = MPI_Bcast(&pathLen, 1, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Path length.");
  rc = MPI_Bcast(&o.mode, 1, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Open mode.");
  rc = MPI_Bcast(&o.numKeys, 1, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Number of keys.");
  rc = MPI_Bcast(&o.maxDataSize, 1, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Maximum data size.");
  rc = MPI_Bcast(&o.maxRecsPerRange, 1, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Maximum records per range.");
  
  if(my_rank != 0){
    if( (o.rangeSvrs = (char **)malloc(o.numRangeSvrs*sizeof(char *))) == NULL){
      printf("Rank %d: Error allocating memory for range server names.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    for(i = 0; i < o.numRangeSvrs; i++){
      if( (o.rangeSvrs[i] = (char *)malloc(MAX_HOST_NAME+1)) == NULL){
	printf("Rank %d: Error allocating memory for range server %d name.\n", my_rank, i+1);
	MPI_Abort(MPI_COMM_WORLD, 100);
      }
      memset(o.rangeSvrs[i], '\0', MAX_HOST_NAME+1);
    }
    if( (o.numRangeSvrsByHost = (int *)malloc(o.numRangeSvrs*sizeof(int))) == NULL){
      printf("Rank %d: Error allocating memory for number of range servers per host.\n", my_rank);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
  }

  rc = MPI_Bcast(o.numRangeSvrsByHost, o.numRangeSvrs, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Number of range servers by host.");
  rc = MPI_Bcast(o.recordPath, (pathLen + 1), MPI_CHAR, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Path to MDHIM files.");
  rc = MPI_Bcast(o.keyType, o.numKeys, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Key type.");
  rc = MPI_Bcast(o.keyMaxLen, o.numKeys, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Maximum key length.");
  rc = MPI_Bcast(o.keyMaxPad, o.numKeys, MPI_INT, 0, MPI_COMM_WORLD);
  checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Maximum key padding.");

  for(i = 0; i < o.numRangeSvrs; i++){
    PRINT_TESTER_DEBUG ("o->rangeSvrs[%d] = %s\n", i,o.rangeSvrs[i]); 
    
    rc = MPI_Bcast(o.rangeSvrs[i], MAX_HOST_NAME, MPI_CHAR, 0, MPI_COMM_WORLD);
    checkReturn(rc, MPI_SUCCESS, my_rank, 'b', "Range server name.");
  }
  
  /*
    Split the world communicator into a group with just rank 0 and the 
    other with all other procs. Now, the procs issuing MDHIM commands can 
    talk together and can send this communicator to mdhimInit.
  */
  rc = MPI_Comm_split(MPI_COMM_WORLD, !(my_rank), my_rank, &mdhimComm);
  checkReturn(rc, MPI_SUCCESS, my_rank, 's', "Creating MDHIM Communicator group.");
  
  /*
    Phase 2: Read and distribute MDHIM commands
    Rank 0 now reads MDHIM commands from an input file stores them in a 
    work queue and sends those MDHIM commands to the appropriate processor. 
  */
  
  starttime = MPI_Wtime();
  if(my_rank == 0){
    int recv_counter = 0;
    MPI_Barrier(MPI_COMM_WORLD);
    
    /*
      Since rank 0 does not participate in doing work, set the number of 
      queues to be one less than the number of procs in the communication 
      group. Then initalize the work queue.
    */
    num_queues = comm_size - 1;

    mpi_status = (MPI_Status *)malloc(num_queues * sizeof(MPI_Status));
    mpi_request = (MPI_Request *)malloc(num_queues * sizeof(MPI_Request));

    wkBuffer = (char **)malloc((num_queues+1) * sizeof(char *));
    for(i = 0; i < num_queues; i++){
      wkBuffer[i] = (char *)malloc(QUEUESIZEPLUS * MAXCOMMANDLENGTH * sizeof(char));
      memset(wkBuffer[i], '\0', QUEUESIZEPLUS * MAXCOMMANDLENGTH*sizeof(char));
    }
    
    indx_array = (int *)malloc(sizeof(int) * num_queues);
    memset(indx_array, '0', sizeof(int) * num_queues);

    workQueue = (wk_queue *)malloc(sizeof(wk_queue) * (num_queues + 1));
    for(indx = 0; indx < num_queues; indx++){
      INIT_QUEUE( &(workQueue[indx]) );
    }

    /*
      Open file containing the MDHIM commands and set variables needed to 
      hold commands and control loops.
    */
    do_work = 1;
    strcpy(queue_char, "-1");
    strcpy(queue_element, "-1");
    
    if((fp = fopen(mdhim_fname, "r")) == NULL){
      printf("Rank %d: Error - Unable to open file name %s\n", my_rank, mdhim_fname);
      MPI_Abort(MPI_COMM_WORLD, 100);
    }
    
      /* 
	 Post one receive per process to see who is ready for work.
      */
    for(indx = 0; indx < num_queues; indx++){
       MPI_Irecv(&recv_int, 1, MPI_INT, MPI_ANY_SOURCE, READYTAG, MPI_COMM_WORLD, &(mpi_request[indx]));
    }

    /* 
       While there's data in the file, fill the work queues, look for procs 
       ready for work then send them their queue of work.
    */    
    while(do_work){

      /*
	If there are more elements in the file to place in the work queue,
	fill the queue. If not, break out of the loop.
      */
      rc = FILL_QUEUE(fp, workQueue, queue_char, queue_element, num_queues, fill_type);
      
      for(indx = 0; indx < num_queues; indx++){
	PRINT_QUEUE(&workQueue[indx]);
      }
      
      PRINT_TESTER_DEBUG ("Tester Rank %d: Exited FILL_QUEUE with queue_char = %s and queue_element %s\n", my_rank, queue_char, queue_element);
      
      if(rc == TRUE){
	do_work = 0;
	break;
      }
      queue_number = 0;
      for(indx = 0; indx < num_queues; indx++){
	if(workQueue[indx].data_size > 0) queue_number++;
	PRINT_TESTER_DEBUG ("Tester Rank %d: workQueue %d has %d elements in it.\n", my_rank, indx, workQueue[indx].data_size);
      }
      
      if(queue_number == 0){
	do_work = 0;
	break;
      }
      /* 
	 At least one queue is full or there is no more data to read. 
	 Wait for any of the receives and process them as they come in
      */
      //      indx = queue_number;
      indx = num_queues;
      while(indx > 0){
	num_ready = 0;
	PRINT_TESTER_DEBUG ("Tester Rank %d: indx = %d waiting for ready for work.\n", my_rank, indx);
	//	MPI_Waitsome(queue_number, mpi_request, &num_ready, indx_array, mpi_status);
	MPI_Waitsome(num_queues, mpi_request, &num_ready, indx_array, mpi_status);
	PRINT_TESTER_DEBUG ("Tester Rank %d: %d procs are ready for work.\n", my_rank, num_ready);
	
	/*
	  Need to issue more receives
	*/
	if(num_ready == MPI_UNDEFINED){
	  PRINT_TESTER_DEBUG ("Tester Rank %d: Need to ISSUE MORE RECEIVES!\n", my_rank);
	  //	  for(num_ready=0; num_ready < queue_number; num_ready++){
	  for(num_ready=0; num_ready < num_queues; num_ready++){
	    PRINT_TESTER_DEBUG ("Tester Rank %d: Called MPI_Irecv with indx %d.\n", my_rank, num_ready);
	    MPI_Irecv(&recv_int, 1, MPI_INT, MPI_ANY_SOURCE, READYTAG, MPI_COMM_WORLD, &(mpi_request[num_ready]));
	  }
	}
	else{ //if(num_ready == MPI_UNDEFINED)
	  for (i = 0; i < num_ready; i++){
	    queue_indx = mpi_status[i].MPI_SOURCE - 1;
	    PRINT_TESTER_DEBUG ("Tester Rank %d: queue_indx %d MPI_SOURCE = %d.\n", my_rank, queue_indx, mpi_status[i].MPI_SOURCE);
	    
	    memset(wkBuffer[queue_indx], '\0', QUEUESIZEPLUS * MAXCOMMANDLENGTH*sizeof(char));
	    sprintf(wkBuffer[queue_indx], "%d", workQueue[queue_indx].data_size);
	    if(workQueue[queue_indx].data_size > 0){
	      // XXX Right now, the wkCommand has enough space for the maximum number of commands that can fit in a work queue. This will change in the future and this portion of code will have to change.
	      key_indx = workQueue[queue_indx].data_size;
	      
	      for(j = 0; j < key_indx; j++){
		rc = DEQUEUE(&workQueue[queue_indx], dq_element, &dq_element_size);
		sprintf(wkBuffer[queue_indx], "%s %d %s", wkBuffer[queue_indx], dq_element_size, dq_element);
	      } /* end for(j = 0; */
	      
	      PRINT_TESTER_DEBUG ("Tester Rank %d: Going to send to rank %d workbuffer[%d] = %s.\n", my_rank, mpi_status[i].MPI_SOURCE, queue_indx, wkBuffer[queue_indx]);
	      MPI_Isend(wkBuffer[queue_indx], strlen(wkBuffer[queue_indx]), MPI_CHAR, mpi_status[i].MPI_SOURCE, WORKTAG, MPI_COMM_WORLD, &send_request);
	      
	    }/* end if(workQueue[queue_indx].data_size > 0) */
	    else{
	      
	      /* 
		 The proc is ready, but there is no work for this proc at this 
		 time. Since there is still work to be read from the file, just 
		 send the command buffer which should have '0' in it.
	      */
	      MPI_Isend(wkBuffer[queue_indx], strlen(wkBuffer[queue_indx]), MPI_CHAR, mpi_status[i].MPI_SOURCE, WORKTAG, MPI_COMM_WORLD, &send_request);
	    }
	    indx--;
	  } // end for (i = 0; i < num_ready; 
	} // end for else-if(num_ready == MPI_UNDEFINED){

	PRINT_TESTER_DEBUG ("while indx =  %d.\n", indx);
      } // end while(indx > 0)
      
      PRINT_TESTER_DEBUG ("Do work is %d.\n", do_work);
    } /* end while(do_work) */
    
    fclose(fp);
    
    /*
      There are no more lines of MDHIM commands to read from file. 
      Continue to send out work until all queues are empty
    */
    queue_number = 0;
    for(indx = 0; indx < num_queues; indx++){
      queue_number++;
    }
    
    indx = queue_number;
    while(indx > 0){
      num_ready = 0;

      PRINT_TESTER_DEBUG ("Tester Rank %d: indx %d before MPI_Waitany with queue_number %d\n", my_rank, indx, queue_number);

      MPI_Waitany(queue_number, mpi_request, &num_ready, &(mpi_status[0]));
      
      if(num_ready == MPI_UNDEFINED){
	for(num_ready=0; num_ready < queue_number; num_ready++){
	  MPI_Irecv(&recv_int, 1, MPI_INT, MPI_ANY_SOURCE, READYTAG, MPI_COMM_WORLD, &(mpi_request[num_ready]));
	}
      }
      else{
	
	queue_indx = mpi_status[0].MPI_SOURCE - 1;
	memset(wkBuffer[queue_indx], '\0', MAXCOMMANDLENGTH);
	sprintf(wkBuffer[queue_indx], "%d ", workQueue[queue_indx].data_size);
	data = &(wkBuffer[queue_indx][strlen(wkBuffer[queue_indx])]);

	if(workQueue[queue_indx].data_size > 0){
	  // XXX Right now, the wkBuffer has enough space for the maximum number of commands that can fit in a work queue. This will change in the future and this portion of code will have to change.
	  
	  key_indx = workQueue[queue_indx].data_size;
	  for(j = 0; j < key_indx; j++){
	    rc = DEQUEUE(&workQueue[queue_indx], dq_element, &dq_element_size);
	    sprintf(data, "%d %s ", dq_element_size, dq_element);
	    data = &(wkBuffer[queue_indx][strlen(wkBuffer[queue_indx])]);
	  } /* end for(j = 0; */
	  
	  MPI_Isend(wkBuffer[queue_indx], strlen(wkBuffer[queue_indx]), MPI_CHAR, mpi_status[0].MPI_SOURCE, WORKTAG, MPI_COMM_WORLD, &send_request);
	  
	  indx--;
	}
	else{
	  /* 
	     The proc is ready, but there is no work for this proc. Since 
	     there is no more work to be read from the file, tell 
	     the proc to end, i.e. value of -1. 
	  */

	  /*	  
	  printf("Rank %d: Recv indx %d source = %d is ready but has queue elements %d\n", my_rank, num_ready, mpi_status[0].MPI_SOURCE, workQueue[queue_indx].data_size);
	  strncpy(wkBuffer[queue_indx], "-1", 2);
	  wkBuffer[queue_indx][2] = '\0';
	  dq_element_size = 2;
	  MPI_Isend(wkBuffer[queue_indx], strlen(wkBuffer[queue_indx]), MPI_CHAR, mpi_status[0].MPI_SOURCE, WORKTAG, MPI_COMM_WORLD, &send_request);
	  */
	}
      } /* end else of if num_ready == MPI_UNDEFINED */
    } /* end while(indx > 0) */
    
    /* 
       Make sure all queues are empty and free them. Send an end message 
       to all procs so they stop listening for more work. This end message 
       was already sent to some procs that didn't have work above.
    */
    for(indx = 0; indx < num_queues; indx++){
      if(workQueue[indx].data_size == 0){
	FREE_QUEUE( &(workQueue[indx]) );
      }
      else
	printf("Rank %d ERROR: Queue number %d is not empty with %d elements.\n", my_rank, indx, workQueue[indx].data_size);
    }
    // XX only do this if there are still procs that haven't received -1 yet
    // Some procs are probably already waiting at the barrier before the MPI_finalize    
    strncpy(wkCommand, "-1", 2);
    wkCommand[2] = '\0';
    dq_element_size = 2;
    for(indx = 1; indx < comm_size; indx++)
      MPI_Isend(wkCommand, dq_element_size, MPI_CHAR, indx, WORKTAG, MPI_COMM_WORLD, &send_request);
    
    for(i = 0; i < num_queues; i++){
      free(wkBuffer[i]);
    }
    free(wkBuffer);
    free(indx_array);
    free(mpi_status);
    free(mpi_request);

  } /* end if(my_rank == 0) */
  else {
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    /* 
       Everyone except rank 0 calls mdhimInit to start up range servers. 
       Remember, rank 0 does not participate in the MDHIM job
    */
    printf("Calling mdhim init\n");
    rc = mdhimInit(&fd, o.numRangeSvrs, o.rangeSvrs, o.numRangeSvrsByHost, o.commType, mdhimComm);
    checkReturn(rc, MDHIM_SUCCESS, my_rank, 'm', "Problem with mdhimInit.");
    
    /*
      For all procs, send rank 0 a 'ready for work' message and 
      then post a (blocking) request for work.
    */
    data_buf = (char *)malloc(QUEUESIZEPLUS * MAXCOMMANDLENGTH * sizeof(char));
    
    do_work = 1;
    while(do_work){
      memset(data_buf, '\0', MAXCOMMANDLENGTH * sizeof(char));
      indx = 1;
      MPI_Send(&indx, 1, MPI_INT, 0, READYTAG, MPI_COMM_WORLD);
      
      MPI_Recv(data_buf, QUEUESIZEPLUS * MAXCOMMANDLENGTH, MPI_CHAR, 0, WORKTAG, MPI_COMM_WORLD, &recv_status);
      PRINT_TESTER_DEBUG ("Tester Rank %d: I received %s\n", my_rank, data_buf);
      
      /*
	Process the work sent by rank 0. If the value is 0, there is no 
	work right now, but we should check back soon. If the value is -1, 
	there is no more work and there won't be any more. So, exit.
      */
      memset(temp_string, '\0', 256);
      sscanf(data_buf, "%s ", temp_string); 
      pathLen = strlen(temp_string) + 1;
      data = &(data_buf[pathLen]);
      num_ops = atoi(temp_string);
      
      PRINT_TESTER_DEBUG ("Tester Rank %d: number of operations is %d and pathLen is %d\n", my_rank, num_ops, pathLen);
      if(num_ops == -1){
	do_work = 0;
      }
      else if(num_ops == 0){
	usleep(10);
      }
      else {
	/*
	  We need to constuct a buffer that collects all the same 
	  operations that are consequtively in the queue. 
	*/
	num_same_op = 1;
	PRINT_TESTER_DEBUG ("Tester Rank %d: data = %s, data_buf is %s\n", my_rank, data, data_buf);
	/*
	  Read in the total length of the command and data and move pointer 
	  to the MDHIM operation.
	*/
	memset(temp_string, '\0', 256);
	sscanf(data, "%s ", temp_string); 
	data = &(data[strlen(temp_string) + 1]);
	datalen = atoi(temp_string);
	PRINT_TESTER_DEBUG ("Tester Rank %d: Operation length is %d\n", my_rank, datalen);
	/*
	  Read in the MDHIM operation and all data associated with it.
	*/
	memset(temp_string, '\0', 256);
	strncpy(temp_string, data, datalen);
	data = &(data[strlen(temp_string) + 1]);
	memset(last_op, '\0', 256);
	str_ptr = strtok(temp_string, " ");
	strncpy(last_op, str_ptr, strlen(str_ptr));
	PRINT_TESTER_DEBUG ("Tester Rank %d: First operation is =%s= with length %d\n", my_rank, last_op, (int)strlen(last_op));

	/*
	  If the MDHIM operation has data associated with it, read it 
	  into the send buffer.
	*/
	memset(wkCommand, '\0', MAXCOMMANDLENGTH);
	if(datalen != strlen(last_op)){
	  sprintf(wkCommand, "%d ", (datalen - (int)strlen(last_op) - 1));
	  indx = strlen(wkCommand);
	  str_ptr = strtok('\0', "\n");
	  memset(last_data, '\0', 256);
	  strncpy(last_data, str_ptr, strlen(str_ptr));
	  PRINT_TESTER_DEBUG ("Tester Rank %d: Going to copy %d from last_data = %s to  wkCommand = %s\n", my_rank, datalen, last_data, wkCommand, indx);
	  strncpy(&(wkCommand[indx]), last_data, (datalen - (int)strlen(last_op) - 1));
	  //	  strncpy(&(wkCommand[indx]), last_data, datalen);
	  PRINT_TESTER_DEBUG ("Tester Rank %d: Last data is =%s= wkCommand = %s\n", my_rank, last_data, wkCommand);
	}

	for(i = 1; i < num_ops; i++){
	  memset(new_op, '\0', 256);
	  memset(new_data, '\0', 256);
	  memset(temp_string, '\0', 256);
	  
	  /*
	    Read in the total length of the next command and data and move 
	    pointer to the MDHIM operation.
	  */
	  sscanf(data, "%s ", temp_string); 
	  data = &(data[strlen(temp_string) + 1]);
	  datalen = atoi(temp_string);
	  PRINT_TESTER_DEBUG ("Tester Rank %d: New operation length is %d\n", my_rank, datalen);
	  /*
	    Read in the next MDHIM operation and all data associated with it.
	  */
	  memset(temp_string, '\0', 256);
	  strncpy(temp_string, data, datalen);
	  data = &(data[strlen(temp_string) + 1]);
	  str_ptr = strtok(temp_string, " ");
	  strncpy(new_op, str_ptr, strlen(str_ptr));
	  PRINT_TESTER_DEBUG ("Tester Rank %d: New operation is =%s= with length %d\n", my_rank, new_op, (int)strlen(new_op));
	  PRINT_TESTER_DEBUG ("Tester Rank %d: new_op length = %d op string length = %d\n", my_rank, (int)strlen(new_op), datalen);
	  if(datalen != strlen(new_op)){
	    memset(new_data, '\0', 256);
	    sprintf(new_data, "%d ", (datalen - (int)strlen(new_op) - 1));
	    indx = strlen(new_data);
	    str_ptr = strtok('\0', "\n");
	    strncpy(&(new_data[indx]), str_ptr, strlen(str_ptr));
	  }
	  PRINT_TESTER_DEBUG ("Tester Rank %d: New operation data is =%s=\n", my_rank, new_data);
	  if(!strncmp(last_op, new_op, 3) ){
	    num_same_op++;
	    strcat(&(wkCommand[indx++]), " ");
	    strcat(&(wkCommand[indx]), new_data);
	    indx += strlen(new_data);
	    PRINT_TESTER_DEBUG ("Tester Rank %d: Operation is %s and wkCommand =%s=\n", my_rank, last_op, wkCommand);
	  }
	  else{
	    PRINT_TESTER_DEBUG ("Tester Rank %d: Going to call callMdhimOperations with num_same_op = %d last_op = %s wkCommand = %s\n", my_rank, num_same_op, last_op, wkCommand);
	    
	    callMdhimOperations(&fd, o, num_same_op, last_op, wkCommand, my_rank);
	    memset(wkCommand, '\0', MAXCOMMANDLENGTH);
	    strcpy(last_op, new_op);
	    strcpy(wkCommand, new_data);
	    indx = strlen(new_data);
	    num_same_op = 1;
	  }
	}
	/*
	  There is one more buffer that has not been sent. Send it now.
	*/
	callMdhimOperations(&fd, o, num_same_op, last_op, wkCommand, my_rank);
	PRINT_TESTER_DEBUG ("Tester Rank %d: Last would call callMdhimOperations with num_same_op = %d last_op = %s, wkCommand = %s\n", my_rank, num_same_op, last_op, wkCommand);
	
      } /* end else { (ifelse for num_ops) */
    } /* end "while(do_work){" */

    free(data_buf);
  } /* end else{ (meaning your rank is not 1) */
  
  PRINT_TESTER_DEBUG ("Tester Rank %d: Ready to finalize- before barrier.\n", my_rank);
  MPI_Barrier(MPI_COMM_WORLD);
  
  endtime = MPI_Wtime();
  jobtime = endtime - starttime;

  printf("%d Rank %d Job time: %lf seconds.\n", comm_size, my_rank, jobtime);
  if(my_rank > 0){
    printf("%d Rank %d: Successfully insert %d records in %lf seconds.\n", comm_size, my_rank, total_inserts, insert_time);
    printf("%d Rank %d: mdhimFlush took %lf seconds.\n", comm_size, my_rank, insert_time);
    printf("%d Rank %d: mdhimDelete took %lf seconds.\n", comm_size, my_rank, delete_time);
    
    if(fd.range_data.num_ranges > 0){
      printf("%d Rank %d Range Data: %d ranges.\n", comm_size, my_rank, fd.range_data.num_ranges);
      cur_range = fd.range_data.range_list;
      for( i = 0; i < fd.range_data.num_ranges; i++){
	printf("%d Rank %d Range Data %d: %d records %ld start range.\n", comm_size, my_rank, i, cur_range->num_records, cur_range->range_start);
	cur_range = cur_range->next_range;
      }
    }
  }
  
  PRINT_TESTER_DEBUG ("Tester Rank %d: Ready to finalize- after barrier.\n", my_rank);
  MPI_Finalize();
  return 0;
}
