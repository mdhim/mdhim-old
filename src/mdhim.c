/*
Copyright (c) 2011, Los Alamos National Security, LLC. All rights reserved.
Copyright 2011. Los Alamos National Security, LLC. This software was produced under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National Laboratory (LANL), which is operated by Los Alamos National Security, LLC for the U.S. Department of Energy. The U.S. Government has rights to use, reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative works, such modified software should be clearly marked, so as not to confuse it with the version available from LANL.
 
Additionally, redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
·         Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
·         Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
·         Neither the name of Los Alamos National Security, LLC, Los Alamos National Laboratory, LANL, the U.S. Government, nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/* 
   The Multi-Dimensional Hashed Indexed Middleware (MDHIM) System core routines
   
   Author: James Nunez and Medha Bhadkamkar
   
   Date: November 29, 2011
   
   Note:
   Error handling is primitive and needs to be improved
*/

#include "mdhim.h"     
#include "range_server.h"


/* ========== mdhimClose ==========
   Close all open data and key files 

   mdhimClose is a collective call, all MDHIM process must call this routine

   Warning: The order of the pblIsamFile_t pointer array may not be the same 
   order of the final range_list because ranges are inserted into the 
   range_list and can move. The elements in the isam pointer array do not 
   track those moves. 

   Input
   fd is the MDHIM structure containing information on range servers and keys
   
   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) or pbl_error on failure
*/
int mdhimClose(MDHIMFD_t *fd) {

  int err, close_error = 0;
  char data[2048];
  MPI_Request close_request, error_request;
  
  PRINT_MDHIM_DEBUG ("****************Rank %d Entered mdhimClose ****************\n", fd->mdhim_rank);
  
  /*
    Close is a collective call. So, wait for all process to get here.
  */
  MPI_Barrier(fd->mdhim_comm); 

  /*
    Check input parameters
  */
  if(!fd){
    printf("mdhimClose Error - MDHIM FD structure is not initalized.\n");
    return MDHIM_ERROR_INIT;
  }
  
  /*
    If I"m a range server, send the command "close" 
  */
  if(fd->range_srv_flag){
    memset(data, '\0', 2048);
    strcpy(data, "close");

    /*
      Post a non-blocking received for the error codes from the close command 
      before sending data. This is just to help with deadlocking on send and 
      receives when you are sending to a range server thread that is your child.
    */

    close_error = 0;
    err = MPI_Irecv(&close_error, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &error_request);
    
    if( err != MPI_SUCCESS){
      fprintf(stderr, "Rank %d: mdhimClose ERROR - MPI_Irecv request for error code failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    /*
      Send the close command
    */
    
    if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->mdhim_rank, SRVTAG, fd->mdhim_comm, &close_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimClose: ERROR - MPI_Send of close instruction failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    /*
      Now poll until the non-blocking receive returns.
    */
    receiveReady(&error_request, MPI_STATUS_IGNORE);
    
    if(close_error > MDHIM_SUCCESS){
      fprintf(stderr, "Rank %d: ERROR -  Problem closing file with return error code %d\n", fd->mdhim_rank, close_error);
    }
    
    PRINT_MDHIM_DEBUG ("Rank %d: Close error code = %d.\n", fd->mdhim_rank, close_error);
  }
  
  PRINT_MDHIM_DEBUG ("****************Rank %d Leaving mdhimClose ****************\n", fd->mdhim_rank);
  
  return close_error;
}
  
/* ========== mdhimDelete ==========
   Delete a record

   Input:
   fd is the MDHIM structure containing information on range servers and keys
   keyIndx is the key to delete; 1 = primary key, 2 = secondary key, etc.
   ikey is the value of the key to delete

   Output:
   record_num is absolute record number of new current key
   okey is the output buffer for the new current key. Memory should be allocated for the okey prior to calling mdhimDelete.
   okey_len is the length of the output key

   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/
int mdhimDelete(MDHIMFD_t *fd, int keyIndx, void *ikey, int *record_num, void *okey, int *okey_len) {

  int key_type = -1, ikey_len = 0;
  int rc, i, indx, found = 0,  err = MDHIM_SUCCESS; 
  int server = -1, start_range = -1;
  char *data = NULL;
  char data_buffer[DATABUFFERSIZE];

  struct rangeDataTag *cur_flush, *prev_flush;
  MPI_Request delete_request, error_request;

  PRINT_DELETE_DEBUG ("****************Rank %d Entered mdhimDelete ****************\n", fd->mdhim_rank);
  /*
    Check that the input parameters are valid.
  */
  if(!fd){
    printf("Rank X mdhimDelete: Error - MDHIM fd structure is null.");
    return MDHIM_ERROR_INIT;
  }
  if(!ikey){
    printf("Rank %d mdhimDelete: Error - Input key to delete is null.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if( (keyIndx > fd->nkeys) || (keyIndx < 1)){
    printf("Rank %d mdhimDelete: Error - The input key index %d must be a value from one to the number of keys %d.\n", fd->mdhim_rank, keyIndx, fd->nkeys);
    return MDHIM_ERROR_INIT;
  }

  /*
    Make sure there is data to delete. If there is no flush data, then 
    return with a not found error.
  */
  if(fd->flush_list.num_ranges == 0){
    printf("Rank %d mdhimDelete: Error - There is no global range data. Please flush before calling delete.\n", fd->mdhim_rank);
    return MDHIM_ERROR_NOT_FOUND;
  }
  
  /*
    Find the start range for the input key
  */
  ikey_len = (int)strlen(ikey);
  
  if(keyIndx == 1)
    key_type = fd->pkey_type;
  else
    key_type = fd->alt_key_info[keyIndx - 2].type;
  
  if ( (start_range = whichStartRange(ikey, key_type, fd->max_recs_per_range)) < 0){
    printf("Rank %d mdhimDelete: Error - Unable to find start range for key %s. Record with key %s does not exist.\n", fd->mdhim_rank, (char *)ikey, (char *)ikey);
    return MDHIM_ERROR_KEY;
  }
  
  /*
    Find the index in the flush data range list for the start range. Based on 
    flush information, check if keys exist in range, if the delete key is 
    smaller or larger than the min or max, respectively.
  */
  //XXX This may be dangerous, relying on flush data to see if a key exists. Flush data is not kept up to date after a delete or insert, only on flush.
  
  cur_flush = NULL;
  rc = searchList(fd->flush_list.range_list, &prev_flush, &cur_flush, start_range);
  if(cur_flush == NULL){
    printf("Rank %d mdhimDelete: Error - Unable to find index of start range for key %s in flushed range data list.\n", fd->mdhim_rank, (char *)ikey);
    return MDHIM_ERROR_IDX_RANGE;
  }
  
  if( (cur_flush->num_records == 0) || 
      (compareKeys(ikey, cur_flush->range_min, ikey_len, key_type) < 0 ) || 
      (compareKeys(ikey, cur_flush->range_max, ikey_len, key_type) > 0) ){
    printf("Rank %d mdhimDelete: Error - Unable to find key %s to delete.\n", fd->mdhim_rank, (char *)ikey);
    return MDHIM_ERROR_NOT_FOUND;
  }
  
  /*
    Since the key may exist, compose and send the delete message
  */
  if( (server = whichServer(start_range, fd->max_recs_per_range, fd->rangeSvr_size)) < 0){
    printf("Rank %d: mdhimDelete Error - Can't find server for key %s.\n", fd->mdhim_rank, (char *)ikey);
    return MDHIM_ERROR_BASE;
  }
  
  /*
    We need to send the delete command, key index and key in a
    single message so that messages to the range servers don't get intermingled.
  */
  if( (data = (char *)malloc(ikey_len + 16)) == NULL){
    printf("Rank %d mdhimDelete: Error - Unable to allocate memory for the find message to send to the range server %d.\n", fd->mdhim_rank, server);
    return MDHIM_ERROR_MEMORY;
  }
  memset(data, '\0', ikey_len+16);
  sprintf(data, "delete %d %d %s", start_range, keyIndx - 1, (char *)ikey);
  
  PRINT_DELETE_DEBUG ("Rank %d: Input (char) key is %s with size %d\n", fd->mdhim_rank, (char *)ikey, ikey_len);
  PRINT_DELETE_DEBUG ("Rank %d: Data buffer is %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
  
  /*
    Post a non-blocking receive for any error codes
  */
  memset(data_buffer, '\0', DATABUFFERSIZE);
  
  if (MPI_Irecv(data_buffer, 2048, MPI_CHAR, fd->range_srv_info[server].range_srv_num, DONETAG, fd->mdhim_comm, &error_request) != MPI_SUCCESS){
    fprintf(stderr, "Rank %d mdimDelete: ERROR - MPI_Irecv request for delete failed.\n", fd->mdhim_rank);
    return MDHIM_ERROR_COMM;
  }
  
  /*
    Now send the delete request
  */
  PRINT_DELETE_DEBUG ("Rank %d: Sending data buffer %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
  
  if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->range_srv_info[server].range_srv_num, SRVTAG, fd->mdhim_comm, &delete_request) != MPI_SUCCESS){
    fprintf(stderr, "Rank %d mdhimDelete: ERROR - MPI_Send of delete command failed.\n", fd->mdhim_rank);
    return MDHIM_ERROR_COMM;
  }
  
  /*
    Now poll until the non-blocking receive returns.
  */
  receiveReady(&error_request, MPI_STATUS_IGNORE);
  
  /*
    Unpack the returned message with the delete error codes. Delete also 
    sends back the current key and record number.
  */
  PRINT_DELETE_DEBUG ("Rank %d mdhimDelete: Returned the data buffer: %s\n", fd->mdhim_rank, data_buffer);
  
  sscanf(data_buffer, "%d %d %d %s", &err, record_num, okey_len, (char *)okey);
  
  PRINT_DELETE_DEBUG ("Rank %d mdhimDelete: Returned error code %d and current key %s with record number %d.\n", fd->mdhim_rank, err, (char *)okey, *record_num);
  
  strcpy(fd->last_key, okey);
  fd->last_recNum = *record_num;

  PRINT_DELETE_DEBUG ("Rank %d mdhimDelete: Leaving MdhimDelete with last_key =  %s and last_recNum = %d\n", fd->mdhim_rank, fd->last_key, fd->last_recNum);
  free(data);
  
  PRINT_DELETE_DEBUG ("****************Rank %d Leaving mdhimDelete ****************\n", fd->mdhim_rank);

  return err;
}

/* ========== mdhimFinalize ==========
   A collective call to write all data to storage, shut down threads, 
   close files and, if necessary, flush the range data

   fd is the MDHIM structure containing information on range servers and keys
   flushFlag is 1 to flush and 0 otherwise
   
   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/
int mdhimFinalize(MDHIMFD_t *fd, int flushFlag) {
  
  char recv_buffer[10];
  
  PRINT_MDHIM_DEBUG ("****************Rank %d Entered mdhimFinalize ****************\n", fd->mdhim_rank);
  /*
    Check input parameters
  */
  if(!fd){
    printf("mdhimFinalize Error - MDHIM FD structure is not initalized.\n");
    return MDHIM_ERROR_INIT;
  }
  
  /* 
     Barrier here to make sure all processes are here
     Then, if necessary, flush data to all MDHIM processes 
  */
  MPI_Barrier(fd->mdhim_comm); 
  
  if(flushFlag){
    
    PRINT_MDHIM_DEBUG ("Rank %d: I want to flush the data flush flag = %d\n", fd->mdhim_rank, flushFlag);
    
    if(mdhimFlush(fd) != 0){
      printf("Rank %d mdhimFinalize: Error - Problem flushing data.\n", fd->mdhim_rank);
      return MDHIM_ERROR_BASE;
    } 
  }

  /*
    Now, close all open data files
  */
    mdhimClose(fd);
  
  /* 
     Send message to have all range server threads stop looking for 
     instructions and exit.
     The blocking receive is there to stop with potential race conditions; 
     the process may get to MPI_Finalize before the thread closes.
  */
  if(fd->range_srv_flag){
    PRINT_MDHIM_DEBUG ("Rank %d: mdhimFinalize Sending quit to mdhim_rank %d\n", fd->mdhim_rank, fd->mdhim_rank);
    MPI_Send("quit", strlen("quit"), MPI_CHAR, fd->mdhim_rank, SRVTAG, fd->mdhim_comm);
    PRINT_MDHIM_DEBUG ("Rank %d: mdhimFinalize before MPI_Recv.\n", fd->mdhim_rank);
    
    memset(recv_buffer, '\0', 10);
    
    MPI_Recv(&recv_buffer, sizeof(recv_buffer), MPI_CHAR, fd->mdhim_rank, QUITTAG, fd->mdhim_comm, MPI_STATUS_IGNORE);

    PRINT_MDHIM_DEBUG ("Rank %d: mdhimFinalize after MPI_Recv.\n", fd->mdhim_rank);
  }
  
  PRINT_MDHIM_DEBUG ("Rank %d: In mdhimFinalize before final MPI_Barrier.\n", fd->mdhim_rank);
  MPI_Barrier(fd->mdhim_comm);
  PRINT_MDHIM_DEBUG ("Rank %d: In mdhimFinalize after final MPI_Barrier.\n", fd->mdhim_rank);
  
  PRINT_MDHIM_DEBUG ("****************Rank %d Leaving mdhimFinalize****************\n", fd->mdhim_rank);
  return MDHIM_SUCCESS;
}

/* ========== mdhimFind ==========
   Find a record in the data stores
   
Input
   fd is the MDHIM structure containing information on range servers and keys
   keyIndx is the key to apply the find operation to; 1 = primary key, 2 = secondary key, etc.
   type is what key to find. Valid types are MDHIM_EQ, MDHIM_EQF, MDHIM_EQL, MDHIM_GEF, MDHIM_GTF, MDHIM_LEL, and MDHIM_LTL
   ikey is the value of the key to find

Output
   record_num is absolute record number returned by the data store
   okey is the output buffer for the key found. Memory should be allocated for the okey prior to calling mdhimFind.
   okey_len is the length of the output key

   Returns: MDHIM_SUCCESS on success, or mdhim_errno (>= 2000) on failure
*/
int mdhimFind( MDHIMFD_t *fd, int keyIndx, int ftype, void *ikey, int *record_num, void *okey, int *okey_len){
  
  int key_type = -1, ikey_len = 0;
  int rc, i, indx, found = 0,  err = MDHIM_SUCCESS; 
  int server = -1, start_range;
  char *data = NULL;
  char data_buffer[DATABUFFERSIZE];

  struct rangeDataTag *cur_flush, *prev_flush;
  MPI_Request find_request, error_request;
  
  PRINT_FIND_DEBUG ("****************Rank %d Entered mdhimFind ****************\n", fd->mdhim_rank);
  /*
    Check that the input parameters are valid.
  */
  if(!fd){
    printf("Rank X mdhimFind: Error - MDHIM fd structure is null.");
    return(MDHIM_ERROR_INIT);
  }
  if(!ikey){
    printf("Rank %d mdhimFind: Error - Input key to search on is null.\n", fd->mdhim_rank);
    return(MDHIM_ERROR_INIT);
  }
  
  PRINT_FIND_DEBUG ("Rank %d mdhimFind: input type = %d ikey = %s\n", fd->mdhim_rank, ftype, (char *)ikey);
  
  if( (keyIndx > fd->nkeys) || (keyIndx < 1)){
    printf("Rank %d mdhimFind: Error - The input key index %d must be a value from one to the number of keys %d.\n",
	   fd->mdhim_rank, keyIndx, fd->nkeys);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_IDX_RANGE ;
  }
  
  if( (ftype != MDHIM_EQ) && (ftype != MDHIM_EQF) && (ftype != MDHIM_EQL) && 
      (ftype != MDHIM_GEF) && (ftype != MDHIM_GTF) && (ftype != MDHIM_LEL) && 
      (ftype != MDHIM_LTL)){
    printf("Rank %d mdhimFind: Error - Problem finding the key %s; %d is an unrecognized mdhimFind option.\n", fd->mdhim_rank, (char *)ikey, ftype);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_INIT;
  }
  
  /*
    Find the start range for the input key
  */
  ikey_len = (int)strlen(ikey);
  
  if(keyIndx == 1)
    key_type = fd->pkey_type;
  else
    key_type = fd->alt_key_info[keyIndx - 2].type;
  
  if((start_range = whichStartRange(ikey, key_type, fd->max_recs_per_range)) < 0){
    printf("Rank %d mdhimFind: Error - Unable to find start range for key %s.\n", fd->mdhim_rank, (char *)ikey);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_BASE;
  }
  
  PRINT_FIND_DEBUG ("Rank %d mdhimFind: key %s has start range %d\n", fd->mdhim_rank, (char *)ikey, start_range);
  
  /*
    Find the index in the flush data range list for the start range.
    Based on flush information and the find operation, we may be able to 
    answer this query without sending data to the range server or may need 
    to modify the range server to send the request to.
  */
  //XXX This may be dangerous, relying on flush data to see if a key exists. Flush data is not kept up to date after a delete or insert, only on flush.

  cur_flush = NULL;
  prev_flush = NULL;
  rc = searchList(fd->flush_list.range_list, &prev_flush, &cur_flush, start_range);
  
  if(cur_flush == NULL){
    printf("Rank %d mdhimFind: Warning - Unable to find index of start range for key %s in flushed range data.\n", fd->mdhim_rank, (char *)ikey);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_KEY;
  }
  else if((ftype == MDHIM_EQ) || (ftype == MDHIM_EQF) || (ftype == MDHIM_EQL)){
    
    if((cur_flush->num_records == 0) || 
       (compareKeys(ikey, cur_flush->range_min, ikey_len, key_type) < 0) || 
       (compareKeys(ikey, cur_flush->range_max, ikey_len, key_type) > 0)){
      printf("Rank %d mdhimFind: Warning - Unable to find key equal to %s.\n", fd->mdhim_rank, (char *)ikey);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_NOT_FOUND;
    }
    
  }
  else if( (ftype == MDHIM_GEF) || (ftype == MDHIM_GTF) ){
    
    if((cur_flush->num_records == 0) || (compareKeys(ikey, cur_flush->range_max, ikey_len, key_type) > 0)){
      
      cur_flush = cur_flush->next_range;
      if(cur_flush == NULL){
	printf("Rank %d mdhimFind: Error - Unable to find key greater than or equal to %s.\n", fd->mdhim_rank, (char *)ikey);
	*record_num = -1;
	*okey_len = 0;
	return MDHIM_ERROR_NOT_FOUND;
      }
      
      strncpy(okey, cur_flush->range_min, KEYSIZE);
      *okey_len = strlen(okey);
      *record_num = 1; // XXX For ISAM, we know the absolute record number starts at 1. Need to change for other DBs
      found = 1;
    }
    
  }
  else if((ftype == MDHIM_LEL) || (ftype == MDHIM_LTL)){
    
    if((cur_flush->num_records == 0) || (compareKeys(ikey, cur_flush->range_min, ikey_len, key_type) < 0)){

      cur_flush = prev_flush;
      if(cur_flush == NULL){
	printf("Rank %d mdhimFind: Error - Unable to find key less than or equal to %s.\n", fd->mdhim_rank, (char *)ikey);
	return MDHIM_ERROR_NOT_FOUND;
      }

      strncpy(okey, cur_flush->range_max, KEYSIZE);
      *okey_len = strlen(okey);
      *record_num = cur_flush->num_records; // XXX For ISAM, we know the absolute record numbers start at 1. Need to change for other DBs
      found = 1;
    }
  }
    
  /* 
     If the key was found with flush information, we can skip finding the 
     server to send to, sending a message to the range server to get the key 
     and parsing the results. 
  */
  if(!found){
    if( (server = whichServer(start_range, fd->max_recs_per_range, fd->rangeSvr_size)) < 0){
      printf("Rank %d mdhimFind: Error - Can't find server for key %s.\n", fd->mdhim_rank, (char *)ikey);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_BASE;
    }
    
    /*
      We need to send the find command, key index, comparison type and key to 
      the range server.
    */
    if( (data = (char *)malloc(ikey_len + 15)) == NULL){
      printf("Rank %d mdhimFind: Error - Unable to allocate memory for the find message to send to the range server %d.\n", fd->mdhim_rank, server);
      *record_num = -1;
      *okey_len = 0;
      err = MDHIM_ERROR_MEMORY;
    }
    memset(data, '\0', ikey_len+15);
    sprintf(data, "find %d %d %d %s", start_range, keyIndx - 1, ftype, (char *)ikey);
    
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Input (char) key is %s with size %d\n", fd->mdhim_rank, (char *)ikey, ikey_len);
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Data buffer is %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
  
    /*
      Post a non-blocking receive for any error codes or the retrieved 
      key/data/record number.
    */
    memset(data_buffer, '\0', DATABUFFERSIZE);
    
    if (MPI_Irecv(data_buffer, 2048, MPI_CHAR, fd->range_srv_info[server].range_srv_num, DONETAG, fd->mdhim_comm, &error_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdimFind: ERROR - MPI_Irecv request for found key/data failed.\n", fd->mdhim_rank);
    }
    
    /*
      Now send the find request
    */
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Sending data buffer %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
    
    if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->range_srv_info[server].range_srv_num, SRVTAG, fd->mdhim_comm, &find_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimFind: ERROR - MPI_Send of find data failed.\n", fd->mdhim_rank);
      // XXX what to do if sending of find fails? Probably retry the send. 
    }
    
    /*
      Now poll until the non-blocking receive returns.
    */
    receiveReady(&error_request, MPI_STATUS_IGNORE);
    
    /*
      Unpack the returned message with the find results. The return string 
      should have an error code, absolute record number, found key length and 
      a string with the key it found.
    */
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Returned the data buffer: %s\n", fd->mdhim_rank, data_buffer);
    
    sscanf(data_buffer, "%d %d %d %s", &err, record_num, okey_len, (char *)okey);
    
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Returned error code %d record num %d and data buffer: %s\n", fd->mdhim_rank, err, *record_num, data);
    
    if(err == MDHIM_ERROR_NOT_FOUND){
      okey = NULL;
      *okey_len = 0;
      *record_num = -1;
    }
    else if(err < 0){
      okey = NULL;
      *okey_len = 0;
      *record_num = -1;
    }
    else{
      strncpy(fd->last_key, okey, KEYSIZE);
      *okey_len = strlen(okey);
      fd->last_key[*okey_len] = '\0';
      fd->last_recNum = *record_num;
    }
    
    PRINT_FIND_DEBUG ("Rank %d mdhimFind: Leaving MdhimFind with last_key =  %s and last_recNum = %d\n", fd->mdhim_rank, fd->last_key, fd->last_recNum);
    free(data);
  }
  
  PRINT_FIND_DEBUG ("****************Rank %d Leaving mdhimFind ****************\n", fd->mdhim_rank);
  return err;
}

/* ========== mdhimFlush ==========
   Send all information about data to all processes in the job
   
   mdhimFlush is a collective call, all MDHIM clients participating in the job 
   must call this function. All range servers send range data to the "first" 
   range server. 
   
   fd is the MDHIM structre that range server information and data

   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/
int mdhimFlush(MDHIMFD_t *fd) {
  
  int err = MDHIM_SUCCESS, flush_error = MDHIM_SUCCESS;
  int numFlushRanges = 0;
  int rc, server, len, indx; 
  char *key = NULL, *data = NULL;
  struct rangeDataTag *range_ptr, *tempRangeData, *curRange;
  MPI_Request flush_request, error_request;
  
  PRINT_FLUSH_DEBUG ("****************Rank %d Entered mdhimFlush****************\n", fd->mdhim_rank);
  PRINT_FLUSH_DEBUG ("Rank %d: Inside MDHIM FLUSH with spawned thread flag %d.\n", fd->mdhim_rank, fd->range_srv_flag); 
  
  /*
   * Check input parameters
   */
  if(!fd){
    printf("Rank X mdhimFlush: Error - MDHIM fd structure is null.\n");
    return MDHIM_ERROR_INIT;
  }
  
  /*
    Since flush is a collective call, wait for all process to get here. 
    We need to make sure all inserts are complete
  */
  PRINT_FLUSH_DEBUG ("Rank %d: Inside MDHIM FLUSH before barrier\n", fd->mdhim_rank); 
  MPI_Barrier(fd->mdhim_comm);
  PRINT_FLUSH_DEBUG ("Rank %d: Inside MDHIM FLUSH after barrier\n", fd->mdhim_rank); 
  
  /*
    If you're a range server, post a non-blocking received for the error 
    codes from the flush command before sending data. This is just to help 
    with deadlocking on send and receives when you are sending to a range 
    server thread that is your child. 
  */
  if(fd->range_srv_flag){
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Before post of Ireceive for flush error message from %d\n", fd->mdhim_rank, fd->mdhim_rank);
    
    err = MPI_Irecv(&flush_error, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &error_request);
    
    if( err != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimFlush: ERROR - MPI_Irecv request for error code failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: I am a range server with %d total ranges.\n", fd->mdhim_rank, fd->range_data.num_ranges);
    
    /*
      Send the flush command
    */
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Posted Ireceive for Flush error message from %d\n", fd->mdhim_rank, fd->mdhim_rank);
    
    err = MPI_Isend("flush", strlen("flush"), MPI_CHAR, fd->mdhim_rank,  SRVTAG, fd->mdhim_comm, &flush_request);
    
    if( err != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimFlush: ERROR - MPI_Send of flush command failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Sent data to %d successful.\n", fd->mdhim_rank, fd->mdhim_rank);
    
    /*
      Now poll until the non-blocking receive for error code returns. 
      Receiving an error code means the flush has completed.
    */
    receiveReady(&error_request, MPI_STATUS_IGNORE);
    
    if(flush_error > MDHIM_SUCCESS){
      fprintf(stderr, "Rank %d mdhimFlush: ERROR -  Problem flushing with return error code %d\n", fd->mdhim_rank, flush_error);
    }
    err = flush_error;
  }
  
  /*
    Now that one range server has collected range data from all servers, 
    send this data to all processes. 
    Only "dirty" ranges will be sent, i.e. only ranges that changed since the 
    last flush will be sent.
  */
  if(fd->mdhim_rank == fd->range_srv_info[0].range_srv_num){
    //    numFlushRanges = fd->flush_list.num_ranges;
    range_ptr = fd->flush_list.range_list;
    for(indx = 0; indx < fd->flush_list.num_ranges; indx++){
      if(range_ptr->dirty_range)
	numFlushRanges++;
      PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Flush range %d is dirty = %d.\n", fd->mdhim_rank, 
			 indx, range_ptr->dirty_range);
      range_ptr = range_ptr->next_range;
    }
    
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Master range server list - "
		       "number of flush ranges %d number of dirty flush ranges %d.\n", 
		       fd->mdhim_rank, fd->flush_list.num_ranges, numFlushRanges);
  }
  
  if (fd->flush_list.range_list) {
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Before bcast number of flush ranges" 
		       " %d and dirty flush ranges %d.\n", 
		       fd->mdhim_rank, fd->flush_list.range_list->dirty_range, numFlushRanges);
  }
  
  MPI_Bcast(&numFlushRanges, 1, MPI_INT, fd->range_srv_info[0].range_srv_num, fd->mdhim_comm);
  if (fd->flush_list.range_list) {
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: After bcast number of flush ranges %d" 
		       " and dirty flush ranges %d.\n", fd->mdhim_rank, 
		       fd->flush_list.range_list->dirty_range, numFlushRanges);
  }
  
  /* 
     If there are no dirty ranges, all the old ranges in the flush list 
     are still valid and we are done.
  */
  if(numFlushRanges == 0){
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: There are no new ranges to flush. There are %d existing ranges.\n", fd->mdhim_rank, fd->flush_list.num_ranges);

    return flush_error;
  }

  /*
    For all MDHIM clients, get the dirty range data from the range server 
    with server rank 0.
   */
  //XXX Start editing here -------------------->
  //  if(fd->mdhim_rank != fd->range_srv_info[0].range_srv_num){
  //fd->flush_list.num_ranges = numFlushRanges;
  //}

  /*
    Broadcast the dirty flush ranges
  */
  PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Before bcast from %d of struct number of flush list ranges %d.\n", fd->mdhim_rank, fd->range_srv_info[0].range_srv_num, fd->flush_list.num_ranges);
  
  if(fd->mdhim_rank == fd->range_srv_info[0].range_srv_num){
    range_ptr = fd->flush_list.range_list;
    
    for(indx=0; indx < fd->flush_list.num_ranges; indx++){
      if(range_ptr->dirty_range){
	
	MPI_Bcast(range_ptr, sizeof(range_ptr), MPI_CHAR, fd->range_srv_info[0].range_srv_num, fd->mdhim_comm);
	PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Master server bcast of range %d with start range %ld.\n", fd->mdhim_rank, indx, range_ptr->range_start);
      }
      range_ptr = range_ptr->next_range;
    }
    
  }
  else{
    if( (tempRangeData = (struct rangeDataTag *)malloc(sizeof(struct rangeDataTag))) == NULL){
      printf("Rank %d mdhimFlush: Error - Unable to allocate memory for the temporary array of range server data.\n", fd->mdhim_rank);
      err = MDHIM_ERROR_MEMORY;
    }
    
    memset(tempRangeData, 0, sizeof(struct rangeDataTag));
    range_ptr = fd->flush_list.range_list;    
    for(indx=0; indx < numFlushRanges; indx++){
      printf("Rank %d: Range srv num: %d, rank: %s", fd->mdhim_rank, fd->range_srv_info[0].range_srv_num, fd->range_srv_info[0].name);
      MPI_Bcast(tempRangeData, sizeof(struct rangeDataTag), MPI_CHAR, fd->range_srv_info[0].range_srv_num, fd->mdhim_comm);
      rc = createAndCopyNode(&(fd->flush_list.range_list), tempRangeData->range_start, tempRangeData);
      fd->flush_list.num_ranges += 1 - rc;            
    }
  }
  /*
    Get rid of any flush ranges that have no records.
  */
  // XX this really needs to be handled better, i.e. the sending of and then deleting of dirty ranges with no records. We do this so that changes can be propogated, but there's a better way.
  curRange = fd->flush_list.range_list;
  range_ptr = NULL;
  for(indx=0; indx < fd->flush_list.num_ranges; indx++){
    
    if(curRange->num_records == 0){
      deleteRange(fd->flush_list.range_list, range_ptr, curRange);
      fd->flush_list.num_ranges--;
    }
    else{
      curRange->dirty_range = 0;
      range_ptr = curRange;
      curRange = curRange->next_range;
    }
  }
  
  PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: After bcast number of flush list ranges %d\n", fd->mdhim_rank, fd->flush_list.num_ranges);
  
  range_ptr = fd->flush_list.range_list;
  for(indx=0; indx < fd->flush_list.num_ranges; indx++){
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Flush list range %d number of records %d start range %ld\n", fd->mdhim_rank, indx, range_ptr->num_records, range_ptr->range_start);
    PRINT_FLUSH_DEBUG ("Rank %d mdhimFlush: Flush list range %d min key %s max key %s\n", fd->mdhim_rank, indx, range_ptr->range_min, range_ptr->range_max);
    range_ptr = range_ptr->next_range;
  }
  
  PRINT_FLUSH_DEBUG ("****************Rank %d Leaving mdhimFlush****************\n", fd->mdhim_rank);
  
  return flush_error;
}

/* ========== mdhimGet ==========
   Get the key from the first, last, current, next or previous record
   
   Input:
   fd is the MDHIM structure containing information on range servers and keys
   keyIndx is the key to apply the find operation to; 1 = primary key, 2 = secondary key, etc.
   type is what key to find. Valid choices are first key, last key, previous, next and current key; MDHIM_FXF, MDHIM_LXL, MDHIM_PRV, MDHIM_NXT, and MDHIM_CUR respectively
   record_num is absolute record number that you just found
   
   Output:
   record_num is absolute record number returned by the data store, -1 if record not found
   okey is the output buffer for the key found
   okey_len is the length of the output key, 0 if record not found

   Returns: MDHIM_SUCCESS on success, or mdhim_errno (>= 2000) on failure
*/
int mdhimGet( MDHIMFD_t *fd, int keyIndx, int ftype, int *record_num, void *okey, int *okey_len){
  
  int key_type = -1, ikey_len = 0;
  int rc, i, indx, found = 0,  err = MDHIM_SUCCESS; 
  int server = -1, start_range;
  char *data = NULL;
  char data_buffer[DATABUFFERSIZE];
  
  struct rangeDataTag *cur_flush, *prev_flush;
  MPI_Request get_request, error_request;
  
  PRINT_GET_DEBUG ("****************Rank %d Entering mdhimGet ****************\n", fd->mdhim_rank);

  /*
    Set output variables to some default values in case there's a problem.
  */
  *record_num = -1;
  *okey_len = 0;
  
  /*
    Check that the input parameters are valid.
  */
  if(!fd){
    printf("Rank X mdhimGet: Error - MDHIM fd structure is null in mdhimGet.\n");
    return MDHIM_ERROR_INIT;
  }
  
  PRINT_GET_DEBUG ("Rank %d mdhimGet: input ftype = %d\n", fd->mdhim_rank, ftype);

  PRINT_GET_DEBUG ("Rank %d mdhimGet: fd->flush_list.num_ranges = %d\n", fd->mdhim_rank, fd->flush_list.num_ranges); 
  if(fd->flush_list.num_ranges < 1){
    printf("Rank %d mdhimGet: Error - There is no flush range data.\n", fd->mdhim_rank);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_DB_GET_KEY;
  }

  PRINT_GET_DEBUG ("Rank %d mdhimGet: fd->flush_list.num_ranges = %d num_records = %d range start = %ld\n", fd->mdhim_rank, fd->flush_list.num_ranges, fd->flush_list.range_list->num_records, fd->flush_list.range_list->range_start);
  
  if( (keyIndx > fd->nkeys) || (keyIndx < 1)){
    printf("Rank %d mdhimGet: Error - The input key index %d must be a value from one to the number of keys %d.\n",
	   fd->mdhim_rank, keyIndx, fd->nkeys);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_IDX_RANGE;
  }
  
  if( (ftype != MDHIM_PRV) && (ftype != MDHIM_NXT) && (ftype != MDHIM_CUR) && 
      (ftype != MDHIM_LXL) && (ftype != MDHIM_FXF) ){
    printf("Rank %d mdhimGet: Error - Problem getting the key; %d is an unrecognized mdhimGet option.\n",
	   fd->mdhim_rank, ftype);
    *record_num = -1;
    *okey_len = 0;
    return MDHIM_ERROR_BASE;
  }
  
  /*
    Based on the get operation, we may be able to answer this query without 
    asking the range servers. Check what the get request is and answer 
    with flush data if possible. If not, compose the get message for the range 
    servers.
  */
  if( ftype == MDHIM_FXF){
    indx = 0;

    PRINT_GET_DEBUG ("Rank %d: mdhimGet first key from list is %s\n", fd->mdhim_rank, fd->flush_list.range_list->range_min);
    strncpy(okey, fd->flush_list.range_list->range_min, KEYSIZE);

    *okey_len = strlen(okey);    
    *record_num = 1; //XXX For ISAM, we know the absolute record numbers start with 1. Need to change for other DBs.
    found = 1;

    PRINT_GET_DEBUG ("Rank %d mdhimGet: The first key from list is %s outkey is %s and okey_size %d rec_num %d\n", fd->mdhim_rank, fd->flush_list.range_list->range_min, (char *)okey, *okey_len, *record_num);
  }
  else if( ftype == MDHIM_LXL){
    /* 
       Find the last range
    */
    indx = fd->flush_list.num_ranges - 1;
    cur_flush = fd->flush_list.range_list;
    for(i=0; i < indx; i++){
      cur_flush = cur_flush->next_range;
    }

    strncpy(okey, cur_flush->range_max, KEYSIZE);
    *okey_len = strlen(okey);    
    *record_num = cur_flush->num_records; //XXX For ISAM, we know the absolute record numbers start at 1. Need to change for other DBs.
    found = 1;
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: The last key from list is %s outkey is %s and okey_size = %d and rec_num %d\n", fd->mdhim_rank, cur_flush->range_max, (char *)okey, *okey_len, *record_num);
    
  }
  else if( ftype == MDHIM_CUR ){
    memset(okey, '\0', KEYSIZE);
    strncpy(okey, fd->last_key, KEYSIZE);
    *okey_len = strlen(okey);
    *record_num = fd->last_recNum;
    found = 1;
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: The current key is %s outkey is %s and okey_size = %d and rec_num %d\n", fd->mdhim_rank, fd->last_key, (char *)okey, *okey_len, *record_num);
  }
  else{  
    /*
      For get next and get previous, find the start range from the stored 
      last key found
    */
    if((fd->last_key == NULL) || (fd->last_recNum < 0)){
      printf("Rank %d mdhimGet: Error - Unable to get next or previous. Must first get or find a key.\n", fd->mdhim_rank);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_BASE;
    }
    
    ikey_len = (int)strlen(fd->last_key);
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: last_key seen = %s with rec num %d ikey_len = %d\n", fd->mdhim_rank, fd->last_key, fd->last_recNum, ikey_len);

    if(keyIndx == 1)
      key_type = fd->pkey_type;
    else
      key_type = fd->alt_key_info[keyIndx - 2].type;
    
    if ( (start_range = whichStartRange(fd->last_key, key_type, fd->max_recs_per_range)) < 0){
      printf("Rank %d mdhimGet: Error - Unable to find start range for key %s.\n", fd->mdhim_rank, (char *)fd->last_key);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_BASE;
    }
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: last key seen %s has start range %d\n", fd->mdhim_rank, fd->last_key, start_range);

    /*
      Find the index in the flush data range list for the start range.
      Based on flush information and the get operation, we may be able to 
      answer this query without sending data to the range server or may need 
      to modify the range server to send the request to.
    */
    indx = -1;
    cur_flush = NULL;
    prev_flush = NULL;
    rc = searchList(fd->flush_list.range_list, &prev_flush, &cur_flush, start_range);
    
    if(cur_flush == NULL){
      printf("Rank %d mdhimGet: Error - Unable to find index of start range for key %s in flushed range list.\n", fd->mdhim_rank, (char *)fd->last_key);
      *record_num = -1;
      *okey_len = 0;
      return MDHIM_ERROR_IDX_RANGE;
    }
    
    else if( ftype == MDHIM_PRV ){
      
      PRINT_GET_DEBUG ("Rank %d mdhimGet: flush list found start range %d\n", fd->mdhim_rank, start_range);
      
      if((indx == 0) && ( (cur_flush->num_records == 0) || (compareKeys(fd->last_key, cur_flush->range_min, ikey_len, key_type) <= 0))){
	/*
	  The last key seen is in the first flush range and either there are no records in this range or the last key seen is less than or equal to the first record in this range, i.e. the first key. Thus, there is no previous key.
	*/

	printf("Rank %d mdhimGet: Warning - Unable to get previous key to %s because no previous key exists.\n", fd->mdhim_rank, (char *)fd->last_key);
	*record_num = -1;
	*okey_len = 0;
	return MDHIM_ERROR_NOT_FOUND;
      }
      else if((cur_flush->num_records == 0) || ( compareKeys(fd->last_key, cur_flush->range_min, ikey_len, key_type) == 0)){
	cur_flush = prev_flush;
	
	if(cur_flush == NULL){
	  printf("Rank %d mdhimGet: Warning - Unable to get previous key to %s because no previous key exists.\n", fd->mdhim_rank, (char *)fd->last_key);
	  *record_num = -1;
	  *okey_len = 0;
	  return MDHIM_ERROR_NOT_FOUND;
	}
	
	strncpy(okey, cur_flush->range_max, KEYSIZE);
	*okey_len = strlen(okey);
	*record_num = cur_flush->num_records; //XXX For ISAM, we know the absolute record numbers start at 1. Need to change for other DBs
	found = 1;

	PRINT_GET_DEBUG ("Rank %d mdhimGet: The previous key from list is %s outkey is %s and okey_size = %d and rec_num %d\n", fd->mdhim_rank, cur_flush->range_max, (char *)okey, *okey_len, *record_num);
      }
      
    }
    else if( ftype == MDHIM_NXT ){

      PRINT_GET_DEBUG ("Rank %d mdhimGet: NXT flush_list.num_ranges =  %d cur_flush->num_records %d cur_flush->range_max = %s\n", fd->mdhim_rank, fd->flush_list.num_ranges, cur_flush->num_records, cur_flush->range_max);
      
      if((indx == (fd->flush_list.num_ranges - 1)) && ( (cur_flush->num_records == 0) || (compareKeys(fd->last_key, cur_flush->range_max, ikey_len, key_type) >= 0))){
	/*
	  The last key seen is in the last flush range and either there are no records in this range or the last key seen is greater than or equal to the last record in this range, i.e. the last key. Thus, there is no next key.
	*/

	printf("Rank %d mdhimGet: Error - Unable to get next key to %s because no next key exists in list.\n", fd->mdhim_rank, (char *)fd->last_key);
	*record_num = -1;
	*okey_len = 0;
	return MDHIM_ERROR_NOT_FOUND;
      }
      else if((cur_flush->num_records == 0) || ( compareKeys(fd->last_key, cur_flush->range_max, ikey_len, key_type) == 0)){
	cur_flush = cur_flush->next_range;

	if(cur_flush == NULL ){
	  printf("Rank %d mdhimGet: Error - Unable to get next key to %s because no next key exists in list.\n", fd->mdhim_rank, (char *)fd->last_key);
	  *record_num = -1;
	  *okey_len = 0;
	  return MDHIM_ERROR_NOT_FOUND;
	}
	
	PRINT_GET_DEBUG ("Rank %d mdhimGet: NXT flush_list.range_min =  %s\n", fd->mdhim_rank, cur_flush->range_min);
	
	strncpy(okey, cur_flush->range_min, KEYSIZE);
	*okey_len = strlen(okey);
	*record_num = 1; //XXX For ISAM, we know the absolute record numbers start at 1. Need to change for other DBs
	found = 1;
	PRINT_GET_DEBUG ("Rank %d mdhimGet: The next key from list is %s outkey is %s and okey_size = %d and rec_num %d\n", fd->mdhim_rank, cur_flush->range_min, (char *)okey, *okey_len, *record_num);
      }
      
    }
  }
  
  /* 
     If the key was found with flush information, we can skip finding the 
     server to send to, sending a message to the range server to get the key 
     and parsing the results. 
  */
  if(!found){
    if( (server = whichServer(cur_flush->range_start, fd->max_recs_per_range, fd->rangeSvr_size)) < 0){
      printf("Rank %d: mdhimGet Error - Can't find server for key %s.\n", fd->mdhim_rank, (char *)fd->last_key);
      return MDHIM_ERROR_BASE;
    }
    
    /*
      We need to send the get command, key index, comparison type and current 
      record number that we want in a single message so that messages to the 
      range servers don't get intermingled.
    */
    if( (data = (char *)malloc(25)) == NULL){
      printf("Rank %d mdhimGet: Error - Unable to allocate memory for the get message to send to the range server %d.\n", fd->mdhim_rank, server);
      err = MDHIM_ERROR_MEMORY;
    }
    memset(data, '\0', 25);
    sprintf(data, "get %d %d %d %d", start_range, keyIndx - 1, ftype, fd->last_recNum);    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: Input (char) key is %s with number %d size %d\n", fd->mdhim_rank, (char *)fd->last_key, fd->last_recNum, ikey_len);
    PRINT_GET_DEBUG ("Rank %d mdimGet: Data buffer is %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
    
    /*
      Post a non-blocking receive for any error codes or the retrieved 
      key/data/record number.
    */
    memset(data_buffer, '\0', DATABUFFERSIZE);
    if (MPI_Irecv(data_buffer, 2048, MPI_CHAR, fd->range_srv_info[server].range_srv_num, DONETAG, fd->mdhim_comm, &error_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdimGet: ERROR - MPI_Irecv request for found key/data failed.\n", fd->mdhim_rank);
    }
    
    /*
      Now send the get request
    */
    PRINT_GET_DEBUG ("Rank %d mdhimGet: Sending data buffer %s with size %u\n", fd->mdhim_rank, data, (unsigned int)strlen(data));
    if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->range_srv_info[server].range_srv_num, SRVTAG, fd->mdhim_comm, &get_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimGet: ERROR - MPI_Send of find data failed.\n", fd->mdhim_rank);
      // XXX what to do if sending of get fails? Probably retry the send. 
    }
    
    /*
      Now poll until the non-blocking receive returns.
    */
    receiveReady(&error_request, MPI_STATUS_IGNORE);

    /*
      Unpack the returned message with the get results. The return string 
      should have an error code, absolute record number, found key length and 
      a string with the key it found.
    */
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: Returned the data buffer: %s\n", fd->mdhim_rank, data_buffer);
    
    sscanf(data_buffer, "%d %d %d %s", &err, record_num, okey_len, (char *)okey);
    
    PRINT_GET_DEBUG ("Rank %d mdhimGet: Returned error code %d record number %d and found key %s\n", fd->mdhim_rank, err, *record_num, (char *)okey);
    
    if(err != MDHIM_SUCCESS){
      okey = NULL;
      *record_num = -1;
    }
    else{
      strncpy(fd->last_key, okey, *okey_len);
      fd->last_key[*okey_len] = '\0';
      fd->last_recNum = *record_num;
    }

    free(data);
  } /* end if(!found) */
  else{
    strncpy(fd->last_key, okey, *okey_len);
    fd->last_key[*okey_len] = '\0';
    fd->last_recNum = *record_num;
  }
  
  PRINT_GET_DEBUG ("Rank %d mdhimGet: last record number %d and last key %s\n", fd->mdhim_rank, fd->last_recNum, fd->last_key);
  PRINT_GET_DEBUG ("****************Rank %d Leaving mdhimGet****************\n", fd->mdhim_rank);
  return err;
}

/* ========== mdhimInit ==========
   Initialization routine for MDHIM. 
   
   mdhimInit is a collective call, all processes participating in the job 
   must call this function, and it must be called before any other MDHIM 
   routine. Threads on range servers will be started and MDHIM fd 
   structures will be initalized with range server information. 
   
   fd is the MDHIMFD_t structre that will be initalized with range server information
   numRangeSvrs is the number of unique range server hosts. 
   rangeSvrs array of range server (host) names. No duplicate names.
   numRangeSvrsByHost is an array of the number of range servers on each of 
   the hosts in the rangeSvrs array.
   commType is the type of communication between processes; 
   1 is MPI, 2 PGAS (only MPI is currently supported)

   Returns: MDHIM_SUCCESS on success or one of the following on failure 
   MDHIM_ERROR_INIT, MDHIM_ERROR_BASE or MDHIM_ERROR_MEMORY;
*/
int mdhimInit(MDHIMFD_t *fd, int numRangeSvrs, char **rangeSvrs, int *numRangeSvrsByHost, 
	      int commType, MPI_Comm inComm ) {  
  int i, indx, j;
  int mdhimRank = -1;
  int *rangeNameIndx = NULL;
  int *rangeSvrsByHostDup = NULL;
  char hostName[HOSTNAMELEN];
  MPI_Group mdhim_group, range_svr_group;

  PRINT_INIT_DEBUG ("****************Rank Entered mdhimInit****************\n");

  /*
    Check input parameters
  */
  if(!fd){
    printf("mdhimInit Error - MDHIM FD structure is not initalized.\n");
    return MDHIM_ERROR_INIT;
  }
  if(commType != 1){
    printf("mdhimInit Error - MPI is the only communication method supported.\n");
    return MDHIM_ERROR_BASE;
  }
  if(!rangeSvrs){
    printf("mdhimInit Error - Array of range server names is not initalized.\n");
    return MDHIM_ERROR_INIT;
  }
  
  /* 
     Get information on the MPI job and fill in the MDHIMFD_t struct
  */
  MPI_Comm_rank(inComm, &mdhimRank);	 
  MPI_Comm_size(inComm, &(fd->mdhim_size));
  /*
  MPI_Comm_dup(inComm, &(fd->mdhim_comm));
  */

  fd->rangeSvr_size = 0;
  for(i = 0; i < numRangeSvrs; i++)
    fd->rangeSvr_size += numRangeSvrsByHost[i];
  
  PRINT_INIT_DEBUG ("Rank %d: Number of range servers = %d\n", mdhimRank, fd->rangeSvr_size);

  if(numRangeSvrs < 1 || fd->rangeSvr_size > fd->mdhim_size){
    printf("Rank %d: mdhimInit Error - There must be at least one range server and less than the number of process in the job (%d). " 
	   "Total number of range servers entered was %d.\n", mdhimRank, fd->mdhim_size, fd->rangeSvr_size);
    return MDHIM_ERROR_INIT;
  }
  
  /* 
     Let's figure out who the range servers are:
     Everyone reads the input array of host names of the range servers. 
     If your host name is in the array, send to rank zero the index in 
     the range server name list of what host you are on starting with 1 not 0.
  */
  
  gethostname(hostName, HOSTNAMELEN);
  indx = 0;
  for (i = 0; i < numRangeSvrs; i++) {
    if (strncmp(hostName, rangeSvrs[i], strlen(rangeSvrs[i])) == 0) {
      indx = i+1;
    }
  }

  PRINT_INIT_DEBUG ("Rank %d: My host name is %s found at index %d\n", mdhimRank, hostName, indx);
  if(mdhimRank == 0){
    if( (rangeNameIndx = (int *)malloc(sizeof(int) * fd->mdhim_size)) == NULL){
      printf("Rank %d: mdhimInit Error - Unable to allocate memory for the array of range server membership array.\n", mdhimRank);
      return MDHIM_ERROR_MEMORY;
    }
  }

  /*
    Rank 0 gathers info on all procs on if they are on a range server.
    Compute number of occurances of hostname in the range server list.
    Rank 0 then fills in the array of range server information in the 
    MDHIMFD_t struct.
  */
  MPI_Gather(&indx, 1, MPI_INT, rangeNameIndx, 1, MPI_INT, 0, inComm);
  
  if(mdhimRank == 0){  
    if( (rangeSvrsByHostDup = (int *)malloc(numRangeSvrs * sizeof(int))) == NULL){  
      printf("Rank %d: mdhimInit Error - Unable to allocate memory for the array of range server name count.\n", mdhimRank);
      return MDHIM_ERROR_MEMORY;
    }
    
    for(i = 0; i < numRangeSvrs; i++)
      rangeSvrsByHostDup[i] = numRangeSvrsByHost[i];
    
    if( (fd->range_srv_info = (RANGE_SRV *)malloc(fd->rangeSvr_size * sizeof(RANGE_SRV)) ) == NULL){
      printf("Rank %d: mdhimInit Error - Unable to allocate memory for the array of range server name count.\n", mdhimRank);
      return MDHIM_ERROR_MEMORY;
    }
    
    j = 0;
    for(i = 0; i < fd->mdhim_size; i++){
      indx = rangeNameIndx[i];
      
      if(indx > 0){
	indx--;
	if(rangeSvrsByHostDup[indx] > 0){
	  fd->range_srv_info[j].range_srv_num = i; /* rank in mdhim_comm */
	  fd->range_srv_info[j].name = (char *)malloc(HOSTNAMELEN);
	  memset(fd->range_srv_info[j].name, '\0', HOSTNAMELEN);
	  strcpy(fd->range_srv_info[j].name, rangeSvrs[indx]);
	  j++;
	  rangeSvrsByHostDup[indx]--;
	}
      }
    }
    
    /* 
       Check that the total number and number of range servers per host 
       are correct.
    */
    if(j != fd->rangeSvr_size){
      printf("Rank %d: mdhimInit Error - Number of range server assigned (%d) does not equal user supplied number (%d).\n", mdhimRank, j, fd->rangeSvr_size);
      return MDHIM_ERROR_BASE;
    }
    
    for(i = 0; i < numRangeSvrs; i++) {
      if(rangeSvrsByHostDup[indx] != 0){
	printf("Rank %d: mdhimInit Error - Range server %s does not have enough procs to be a range server.\n", mdhimRank, rangeSvrs[i]);
	return MDHIM_ERROR_BASE;
      }
    }

    free(rangeSvrsByHostDup);
    free(rangeNameIndx);

    /*
      Now initalize the entries on keys and last record seen. Many of these 
      will be set in the open routine.
    */
    fd->mdhim_comm = NULL;
    fd->mdhim_rank = -1;
    fd->max_recs_per_range = 0;
    fd->pkey_type = -1;
    fd->max_pkey_length = 0;    
    fd->max_pkey_padding_length = 0; 
    fd->max_data_length = 0;    
    fd->nkeys = 0;
    fd->update = -1;
    fd->keydup = NULL;
    memset(fd->last_key, '\0', KEYSIZE);
    fd->last_recNum = -1;
    fd->path = NULL;
    fd->alt_key_info = NULL;

    /* 
       Set the range data and flush data to NULL and initalize number of ranges.
    */
    fd->range_data.num_ranges = 0;
    fd->range_data.range_list = NULL;
    fd->flush_list.num_ranges = 0;
    fd->flush_list.range_list = NULL;
  } /* end if(mdhimRank == 0 */
  
  /* 
     Broadcast the MDHIMFD_t struct from rank 0.
  */
  PRINT_INIT_DEBUG ("Rank %d mdhimInit: Before bcast of fd. size of *fd %d size of fd %d\n", mdhimRank, (int)sizeof(*fd), (int)sizeof(fd));
  MPI_Bcast(fd, sizeof(*fd), MPI_BYTE, 0, inComm);

  if(mdhimRank > 0){
    if( (fd->range_srv_info = (RANGE_SRV *)malloc(fd->rangeSvr_size * sizeof(RANGE_SRV)) ) == NULL){
      printf("Rank %d: mdhimInit Error - Unable to allocate memory for the array of range server name count.\n", mdhimRank);
      return MDHIM_ERROR_MEMORY;
    }
  }

  MPI_Bcast(fd->range_srv_info, sizeof(RANGE_SRV) * fd->rangeSvr_size, MPI_BYTE, 0, inComm);
  
  PRINT_INIT_DEBUG ("Rank %d mdhimInit: Number of range servers %d\n", mdhimRank, fd->rangeSvr_size);

  for(j=0; j < fd->rangeSvr_size; j++){
    if(mdhimRank > 0){
      fd->range_srv_info[j].name = (char *)malloc(HOSTNAMELEN);
      memset(fd->range_srv_info[j].name, '\0', HOSTNAMELEN);
    }
    MPI_Bcast(fd->range_srv_info[j].name, HOSTNAMELEN, MPI_BYTE, 0, inComm);
    PRINT_INIT_DEBUG ("Rank %d mdhimInit: Hostname %d = %s\n", mdhimRank, j, fd->range_srv_info[j].name);
  }

  /* 
     Fill in rank specific values in the MDHIMFD_t struct
  */
  fd->mdhim_rank = mdhimRank;

  if(MPI_Comm_dup(inComm, &(fd->mdhim_comm)) != MPI_SUCCESS){
    printf("Rank %d mdhimInit: Error - Unable to duplicate the incoming MPI Communicator group.\n", fd->mdhim_rank);
    return MDHIM_ERROR_BASE;
  }
  
  PRINT_INIT_DEBUG ("Rank %d mdhimInit: After MPI_Comm_dup\n", fd->mdhim_rank);

  PRINT_INIT_DEBUG ("Rank %d: Number of range servers %d\n", fd->mdhim_rank, fd->rangeSvr_size);
  
  /*
    Create an MPI Communicator for range servers
  */
  fd->range_srv_flag = 0;
  for(i = 0; i < fd->rangeSvr_size; i++) {
    if(fd->mdhim_rank == fd->range_srv_info[i].range_srv_num){
      fd->range_srv_flag = 1;      
    }
  }

  MPI_Comm_split(fd->mdhim_comm, fd->range_srv_flag, fd->mdhim_rank, &(fd->rangeSrv_comm));
  
  /* 
     Start up a thread on each range server to accept data store operations.
  */
  for(i = 0; i < fd->rangeSvr_size; i++){
    if(fd->mdhim_rank == fd->range_srv_info[i].range_srv_num){
      
      if(spawn_mdhim_server(fd) != 0){  
	fprintf( stderr, "Rank %d: mdhimInit Error - Spawning thread failed on host %s with error %s\n", 
		 fd->mdhim_rank, hostName, strerror( errno ));
	return MDHIM_ERROR_BASE;
      }
      
      PRINT_INIT_DEBUG ("Rank %d: Done spawning thread.\n", fd->mdhim_rank);
    }
  }

  PRINT_INIT_DEBUG ("****************Rank %d Leaving mdhimInit****************\n", fd->mdhim_rank);

  return MDHIM_SUCCESS;
} 

/* ========== mdhimInsert ==========
   Insert a record into the data store

   fd is the MDHIM structure containing information on range servers and keys
   key_data_list is an array of keyDataList structures each with a primary key 
   value, any number of secondary keys and the record data
   num_key_data is the number of elements in the key_data_list array
   ierrors is the structure containing the highest insert error, number of operations that succeeded and an array of error codes for each insert. 

   Returns: MDHIM_SUCCESS on success or one of the following on failure 
   MDHIM_ERROR_INIT, MDHIM_ERROR_BASE or MDHIM_ERROR_MEMORY;
*/
int mdhimInsert(MDHIMFD_t *fd, struct keyDataList *key_data_list, int num_key_data, LISTRC *ierrors) {
  
  int err = MDHIM_SUCCESS;
  int k, j, i, start_range, server; 
  int *num_inserts = NULL, **insert_errors = NULL, **perrors = NULL;
  unsigned int len = 0;
  char **insert_data = NULL, **pdata = NULL;
  MPI_Request insert_request;
  MPI_Request *error_requests = NULL;
  
  PRINT_INSERT_DEBUG ("****************Rank %d Entered mdhimInsert****************\n", fd->mdhim_rank);
  /*
    Check input parameters
  */
  if(!fd){
    printf("Rank %d mdhimInsert: Error - MDHIM FD structure is not initalized.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!key_data_list){
    printf("Rank %d mdhimInsert: Error - The array of key values and data is not initalized.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!ierrors->errors){
    printf("Rank %d mdhimInsert: Error - The error array is not initalized.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  
  /*
    Allocate memory for the array of pointers to the insert commands for each range server.
  */
  if( (insert_data = (char **)malloc(fd->rangeSvr_size * sizeof(char *))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of insert" 
	   " commands.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  if( (pdata = (char **)malloc(fd->rangeSvr_size * sizeof(char *))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of pointers" 
	   " to the insert commands.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  if( (num_inserts = (int *)malloc(fd->rangeSvr_size * sizeof(int))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of number" 
	   " of insert commands per server.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }

  ierrors->num_ops = 0;
  ierrors->max_return = 0;
  for(i = 0; i < fd->rangeSvr_size; i++){
    num_inserts[i] = 0;
    insert_data[i] = NULL;
  }
  /*
    For each record to insert, figure out what server and start range to send 
    to based on the primary (first) key. 
  */
  for(i = 0; i < num_key_data; i++){
    PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Before whichStartRange with key = %s, " 
			"key_type = %d, size = %d, max_recs = %d\n", fd->mdhim_rank, 
			key_data_list[i].pkey, fd->pkey_type, key_data_list[i].pkey_length, 
			fd->max_recs_per_range);
    
    err = getServerAndStartRange((void *)key_data_list[i].pkey, fd->pkey_type, fd->max_recs_per_range, 
				 fd->rangeSvr_size, &start_range, &server);
    
    ierrors->errors[i] = server;

    PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: After whichStartRange with key = %s, server %d" 
			" start_range %d\n", fd->mdhim_rank, key_data_list[i].pkey, server, start_range);
    /*
      If this is the first insert command for this server, allocate memory 
      for the insert command and initalize the string.
    */
    if(insert_data[server] == NULL) {
      len = 0;
      
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Insert data for server %d not allocated.\n", fd->mdhim_rank, server);
      
      if(fd->nkeys > 1){
	len = strlen(key_data_list[i].secondary_keys);
      }
      
      len += key_data_list[i].pkey_length + strlen(key_data_list[i].data) + 21;

      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Insert data %d for server %d has length %d.\n", fd->mdhim_rank, i, server, len);

      if( (insert_data[server] = (char *)malloc(num_key_data * len * sizeof(char))) == NULL){
	printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the insert" 
	       " commands for range server %d.\n", fd->mdhim_rank, server);
	return MDHIM_ERROR_MEMORY;
      }

      /*
	Compose the beginning of the insert command "insert"
      */
      memset(insert_data[server], '\0', num_key_data * len * sizeof(char));
      sprintf(insert_data[server], "insert %d ", num_key_data);
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Server %d insert command: %s with length %d.\n", 
			  fd->mdhim_rank, server, insert_data[server], (int)strlen(insert_data[server]));

      pdata[server] = &(insert_data[server][strlen(insert_data[server])]);

      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Server %d insert command: %s.\n", fd->mdhim_rank, server, insert_data[server]);
    }
    
    /*
      Compose the insert command; start range to insert key at, the 
      primary key, primary key length, secondary keys with secondary key 
      lengths, data length and data. Append successive records to insert.
    */
    num_inserts[server]++;
    
    PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Server %d has %d inserts.\n", fd->mdhim_rank, server, num_inserts[server]);
    
    if(key_data_list[i].secondary_keys){
      sprintf(pdata[server], "%d %d %s %s %d %s", start_range, key_data_list[i].pkey_length, key_data_list[i].pkey, 
	      key_data_list[i].secondary_keys, (int)strlen(key_data_list[i].data), key_data_list[i].data);
    }
    else{
      sprintf(pdata[server], "%d %d %s %d %s", start_range, key_data_list[i].pkey_length, key_data_list[i].pkey, 
	      (int)strlen(key_data_list[i].data), key_data_list[i].data);
    }

    // shouldn't this work?
    //    pdata[server] += strlen(insert_data[server]);
    pdata[server] = &(insert_data[server][strlen(insert_data[server])]);

    PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: data buffer for server %d is %s with size %u\n", 
			fd->mdhim_rank, server, insert_data[server], (unsigned int)strlen(insert_data[server]));
    
  }

  /*
    Allocate memory for the array of return MDHIM errors and for the array 
    of MPI request structure for the MPI_Isend
  */
  if( (insert_errors = (int **)malloc(fd->rangeSvr_size * sizeof(int *))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of insert errors for server %d.\n", fd->mdhim_rank, i);
    return MDHIM_ERROR_MEMORY;
  }
  
  if( (error_requests = (MPI_Request *)malloc(fd->rangeSvr_size * sizeof(MPI_Request))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of error MPI Request structures.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }

  /*
    For each range server, if there are records to insert, post receives 
    for error messages and send insert data.
  */
  for(i = 0; i < fd->rangeSvr_size; i++){
    if(num_inserts[i] > 0){
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Before post of Ireceive for Insert error" 
			  " message from %d\n", fd->mdhim_rank, fd->range_srv_info[i].range_srv_num);
      
      if( (insert_errors[i] = (int *)malloc( (num_inserts[i]+1) * sizeof(int))) == NULL){
	printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of insert " 
	       "errors for server %d.\n", fd->mdhim_rank, i);
	return MDHIM_ERROR_MEMORY;
      }

      err = MPI_Irecv(insert_errors[i], num_inserts[i], MPI_INT, 
		      fd->range_srv_info[server].range_srv_num, DONETAG, 
		      fd->mdhim_comm, &(error_requests[i]));
      
      if( err != MPI_SUCCESS){
	fprintf(stderr, "Rank %d mdhimInsert: ERROR - MPI_Irecv request for error code" 
		" failed with error %d\n", fd->mdhim_rank, err);
	return MDHIM_ERROR_COMM;
      }
      
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Posted Ireceive for Insert error message from %d\n", fd->mdhim_rank, fd->range_srv_info[server].range_srv_num);

      PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert sending message %s with size %d to server with rank %d\n", fd->mdhim_rank, insert_data[i], 
			  (int)strlen(insert_data[i]), fd->range_srv_info[i].range_srv_num); 
      
      err = MPI_Isend(insert_data[i], strlen(insert_data[i]), MPI_CHAR, fd->range_srv_info[i].range_srv_num, 
		      SRVTAG, fd->mdhim_comm, &insert_request);
      if( err != MPI_SUCCESS){
	fprintf(stderr, "Rank %d mdhimInsert: ERROR - MPI_Send of insert data for range server %d failed with error %d\n", fd->mdhim_rank, i, err);
	return MDHIM_ERROR_COMM;
      }
      PRINT_INSERT_DEBUG ("Rank %d mdhimInsert: Sent data to %d successful.\n", fd->mdhim_rank, fd->range_srv_info[i].range_srv_num);
    }
  }
    
    /*
      Now poll until ALL the non-blocking receives return.
    */
  //XXX This really should be a wait all and not a sequential wait for each request
  for(i = 0; i < fd->rangeSvr_size; i++)
    if(num_inserts[i] > 0)
      receiveReady(&(error_requests[i]), MPI_STATUS_IGNORE);
  
  for(i = 0; i < fd->rangeSvr_size; i++){
    for(j = 0; j < num_inserts[i]; j++){
      PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - server %d error %d = %d.\n", fd->mdhim_rank, i, j, insert_errors[i][j]);
    }
  }
    /*
      Now that all inserts are done, put the error codes in the correct place 
      in the output LISTRC struct
    */
  if( (perrors = (int **)malloc(fd->rangeSvr_size * sizeof(int *))) == NULL){
    printf("Rank %d mdhimInsert: Error - Unable to allocate memory for the array of error pointers.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }

  for(i = 0; i < fd->rangeSvr_size; i++){
      perrors[i] = insert_errors[i];
  }

  k = 0;
  for(i = 0; i < fd->rangeSvr_size; i++){

    for(j = 0; j < num_inserts[i]; j++){
      if(num_inserts[i] > 0){
	ierrors->errors[k] = *perrors[ierrors->errors[k]]++;
	PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - server %d error %d = %d.\n", fd->mdhim_rank, i, j, ierrors->errors[k]);
	
	if(ierrors->errors[k] > ierrors->max_return){
	  ierrors->max_return = ierrors->errors[k];
	}
	if(ierrors->errors[k] == MDHIM_SUCCESS){
	  ierrors->num_ops++;
	}
	k++;
      }
    }
  }
  PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - %d successful inserts with max error %d.\n", fd->mdhim_rank,ierrors->num_ops, ierrors->max_return);
  PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - Inserting error code = %d.\n", fd->mdhim_rank, err);
  
  PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - fd->range_data.num_ranges = %d\n", fd->mdhim_rank, fd->range_data.num_ranges);
  
  for(i = 0; i < num_key_data; i++){
    PRINT_INSERT_DEBUG ("Rank %d: mdhimInsert - errors[%d] = %d\n", fd->mdhim_rank, i, ierrors->errors[i]);
  }
  for(i = 0; i < fd->rangeSvr_size; i++){
    if(num_inserts[i] > 0){
      free(insert_errors[i]);
      free(insert_data[i]);
    }
  }
  free(error_requests);
  free(insert_data);
  free(pdata);
  free(perrors);
  free(insert_errors);
  
  PRINT_INSERT_DEBUG ("****************Rank %d Leaving mdhimInsert****************\n", fd->mdhim_rank);
  
  return err;
}

/* ========== mdhimOpen ==========
   Open key and data files on range servers
   
   mdhimOpen is a collective call, all processes participating in the job 
   must call this function.
   
   fd is the MDHIM structre that will be initalized with key information
   recordPath is the full path to the data store files
   mode is the mode to open the record files; 
   0 for create, 1 for update, 2 for read only 
   numKeys is the number of keys; primary key and any number of secondary keys
   keyType is an array specifying the key type for each of the numKeys, first 
   key is the primary key; 0 is alpha-numeric, 1 integer, 2 float
   keyMaxLen array of maximum length for each key of the numKeys
   keyMaxPad array of maximum padding for each key of the numKeys
   maxDataSize is the maximum size of the data in each record
   maxRecsPerRange is the size of each range; for integer keys the number 
   of records in the range, for float keys, this is ...

   Warning: The order of the pblIsamFile_t pointer array may not be the same 
   order of the final range_list because ranges are inserted into the 
   range_list and can move. The elements in the isam pointer array do not 
   track those moves. 

   Returns: MDHIM_SUCCESS on success, mdhim_errno (>= 2000) on failure
*/
int mdhimOpen(MDHIMFD_t *fd, char *recordPath, int mode, int numKeys, int *keyType, int *keyMaxLen, int *keyMaxPad, int maxDataSize, int maxRecsPerRange){

  char data[10];
  int err, open_error = MDHIM_SUCCESS;
  int i, j;
  struct stat st;
  MPI_Request open_request, error_request;

  PRINT_OPEN_DEBUG ("****************Rank %d Entered mdhimOpen****************\n", fd->mdhim_rank);
  /*
    Since open is a collective call, wait for all process to get here. 
  */
  PRINT_OPEN_DEBUG ("Rank %d: Inside MDHIM  Open before barrier\n", fd->mdhim_rank); 
  MPI_Barrier(fd->mdhim_comm);
  PRINT_OPEN_DEBUG ("Rank %d: Inside MDHIM Open after barrier\n", fd->mdhim_rank); 

  /*
    Check input parameters
  */
  if(!fd){
    printf("Rank %d: mdhimOpen Error - MDHIM FD structure is not initalized.\n", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!recordPath){
    printf("Rank %d: mdhimOpen Error - Path to store records is not initalized.\n.", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!keyType){
    printf("Rank %d: mdhimOpen Error - Array of key types is not initalized.\n.", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!keyMaxLen){
    printf("Rank %d: mdhimOpen Error - Array of maximum key lengths is not initalized.\n.", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(!keyMaxPad){
    printf("Rank %d: mdhimOpen Error - Array of maximum key padding is not initalized.\n.", fd->mdhim_rank);
    return MDHIM_ERROR_INIT;
  }
  if(mode < 0 || mode > 2){
    printf("Rank %d: mdhimOpen Error - Invalid open mode (%d); 0 for open with create, 1 for update and 2 for read only.", fd->mdhim_rank, mode);
    return MDHIM_ERROR_INIT;
  }
  if(numKeys < 1){
    printf("Rank %d: mdhimOpen Error - Invalid total number of keys (%d); must be 1 or more.", fd->mdhim_rank, numKeys);
    return MDHIM_ERROR_INIT;
  }
  if(maxDataSize < 1){
    printf("Rank %d: mdhimOpen Error - Invalid maximum size of record data (%d); must be greater than 1.", fd->mdhim_rank, maxDataSize);
    return MDHIM_ERROR_INIT;
  }
  if( maxRecsPerRange < 1){
    printf("Rank %d: mdhimOpen Error - Invalid number of records per host (%d); must be 1 or more.", fd->mdhim_rank,  maxRecsPerRange);
    return MDHIM_ERROR_INIT;
  }
  
  /* 
     Fill in key information in the MDHIM fd structure and create the 
     structure for all alternate keys.
  */
  fd->max_recs_per_range = maxRecsPerRange;
  fd->pkey_type = keyType[0];
  fd->max_pkey_length = keyMaxLen[0];    
  fd->max_pkey_padding_length = keyMaxPad[0]; 
  fd->max_data_length = maxDataSize;    
  fd->nkeys = numKeys;
  fd->update = mode;

  if( (fd->alt_key_info = (struct altKeys *)malloc((numKeys - 1) * sizeof(struct altKeys)) ) == NULL){
    printf("Rank %d: mdhimOpen Error - Unable to allocate memory for the array of alternate key information.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  for(i=1; i < numKeys; i++){
    j = i - 1;
    fd->alt_key_info[j].type = keyType[i]; 
    fd->alt_key_info[j].max_key_length = keyMaxLen[i]; 
    fd->alt_key_info[j].max_pad_length = keyMaxPad[i]; 
  }
  
  //XXX For now assume no duplicate keys
  if( (fd->keydup = (int *)malloc(numKeys * sizeof(int)) ) == NULL){
    printf("Rank %d mdhimOpen: Error - Unable to allocate memory for the array of key duplicates.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  
  for(i=0; i < numKeys; i++)
    fd->keydup[i] = 0;
  
  /*
    For each range server, create the path to the data and key files.
  */
  PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: Path to data files is %s\n", fd->mdhim_rank, recordPath);

  if( (fd->path = (char *)malloc(strlen(recordPath) + 20) ) == NULL){
    printf("Rank %d mdhimOpen: Error - Unable to allocate memory for the file path.\n", fd->mdhim_rank);
    return MDHIM_ERROR_MEMORY;
  }
  memset(fd->path, '\0', strlen(recordPath) + 20);

  if(strncmp(&recordPath[strlen(recordPath) - 1], "/", 1) != 0)
    sprintf(fd->path, "%s/mdhimrank%d/", recordPath, fd->mdhim_rank);
  else
    sprintf(fd->path, "%smdhimrank%d/", recordPath, fd->mdhim_rank);
  
  PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: fd->path to data files is %s\n", fd->mdhim_rank, fd->path);

  /*
    Post a non-blocking received for the error codes from the open command 
    before sending data. This is just to help with deadlocking on send and 
    receives when you are sending to a range server thread that is your child. 
  */
  if( fd->range_srv_flag){

    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: I am a range server!\n", fd->mdhim_rank);

    // Check if the file exists.  If it does and it is a directory, then don't try to make the directory
    if ((err = stat(fd->path, &st)) < 0 || S_ISDIR(st.st_mode) == 0) {
      if(mkdir(fd->path,S_IRWXU) != 0) {
	printf("Rank %d: mdhimOpen Error - Unable to create the directory %s.\n", 
	       fd->mdhim_rank, fd->path, err);
	return MDHIM_ERROR_BASE;
      }
    }
    
    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: Before error MPI_Irecv.\n", fd->mdhim_rank);
    err = MPI_Irecv(&open_error, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &error_request);
    
    if( err != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimOpen: ERROR - MPI_Irecv request for error code failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }

    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: After error MPI_Irecv.\n", fd->mdhim_rank);
    /*
      Send the open command
    */
    memset(data, '\0', 10);
    strncpy(data, "open", 4);
    
    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: Before MPI_Send of %s\n", fd->mdhim_rank, data);

    if( MPI_Isend(data, strlen(data), MPI_CHAR, fd->mdhim_rank, SRVTAG, fd->mdhim_comm, &open_request) != MPI_SUCCESS){
      fprintf(stderr, "Rank %d mdhimOpen: ERROR - MPI_Send of open data failed with error %d\n", fd->mdhim_rank, err);
      return MDHIM_ERROR_BASE;
    }
    
    /*
      Now poll until the non-blocking receive returns.
    */
    PRINT_OPEN_DEBUG ("Rank %d mdhimOpen: Before receiveRequest.\n", fd->mdhim_rank);
    receiveReady(&error_request, MPI_STATUS_IGNORE);
    
    if(open_error > 0){
      fprintf(stderr, "Rank %d mdhimOpen: ERROR -  Problem opening files with return error code %d.\n", fd->mdhim_rank, open_error);
      MPI_Abort(MPI_COMM_WORLD, 10);
    }
  }

  PRINT_OPEN_DEBUG ("Rank %d: Leaving MDHIM Open before barrier\n", fd->mdhim_rank); 
  MPI_Barrier(fd->mdhim_comm);
  PRINT_OPEN_DEBUG ("Rank %d: Leaving MDHIM Open after barrier\n", fd->mdhim_rank); 
  PRINT_OPEN_DEBUG ("****************Rank %d Leaving mdhimOpen****************\n", fd->mdhim_rank);

  //Force a flush to make sure range data created on open is propagated 
  mdhimFlush(fd);
  
  return open_error;
}          

/* ========== receiveReady ==========
   Wait for and check if MPI_Irecv completed

   Input:
   inRequest is the MPI_Request struct
   inStatus is the MPI_Status struct

   Output:

   Returns: 0 on success
*/
int receiveReady( MPI_Request *inRequest, MPI_Status *inStatus) {
  
  int recv_ready = 0;
  int wait_count = 0;
  
  while( recv_ready == 0){
    MPI_Test(inRequest, &recv_ready, inStatus); // Should test for error
    wait_count++;
    
    if(wait_count%2 == 0){
      usleep(1000);
      //      printf("**mdhim_commands Waiting for recv_ready to be true. Wait count %d.\n", wait_count);
    }
    
  }
 
  return MDHIM_SUCCESS;
}

/* ========== setKeyDataList ==========
   Set the variables of the KeyDataList struct

   Input:

   Output:

   Returns: MDHIM_SUCCESS on success
*/
 int setKeyDataList( int my_rank, struct keyDataList *key_data_list, char *pkey, int pkey_len, char *data, int num_secondary_keys, char *skey_list) {
   
   int rc = MDHIM_SUCCESS, scanf_rc = 0;
   int i, data_len = 0, key_len = 0;
   char skeys[KEYSIZE];
   
   PRINT_MDHIM_DEBUG ("Rank %d Entered setKeyDataList with data = %s\n", my_rank, data);
   /*
     Check input parameters
   */
   if(key_data_list == NULL){
     printf("Rank %d setKeyDataList: Error - Input keyDataList struct is null.\n", my_rank);
     return(MDHIM_ERROR_INIT);
   }
   if(pkey == NULL){
     printf("Rank %d setKeyDataList: Error - Input primary key is null.\n", my_rank);
     return(MDHIM_ERROR_INIT);
   }
   if(data == NULL){
     printf("Rank %d setKeyDataList: Error - Input data is null.\n", my_rank);
     return MDHIM_ERROR_INIT;
   }
   if(num_secondary_keys > 0){
     if(key_data_list == NULL){
       printf("Rank %d setKeyDataList: Error - Input keyDataList structis null.\n", my_rank);
       return(MDHIM_ERROR_INIT);
     }
   }
   
   /*
     Set variables in the keyDataList structure
   */
   key_data_list->pkey_length = pkey_len;
   
   if( (key_data_list->pkey = (char *)malloc((pkey_len+1) * sizeof(char))) == NULL){
     printf("Rank %d setKeyDataList: Error - Problem allocating memory for the primary key.\n", my_rank);
     return MDHIM_ERROR_MEMORY;
   }
   memset(key_data_list->pkey, '\0', pkey_len+1);
   strncpy(key_data_list->pkey, pkey, pkey_len);
   
   data_len = strlen(data);
   if( (key_data_list->data = (char *)malloc((data_len+1) * sizeof(char))) == NULL){
     printf("Rank %d setKeyDataList: Error - Problem allocating memory for the records data.\n", my_rank);
     return MDHIM_ERROR_MEMORY;
   }
   memset(key_data_list->data, '\0', data_len+1);
   strncpy(key_data_list->data, data, data_len);
   
   /*
     Let's check and make sure we have all the secondary keys we think we 
     should. If so, copy them to the Key Data List.
   */
   if(num_secondary_keys > 0){
     if( (key_data_list->secondary_keys = (char *)malloc((strlen(skey_list)+1) * sizeof(char))) == NULL){
       printf("Rank %d setKeyDataList: Error - Problem allocating memory for the secondary keys.\n", my_rank);
       return MDHIM_ERROR_MEMORY;
     }
     memset(key_data_list->secondary_keys, '\0', strlen(skey_list)+1);
     strncpy(key_data_list->secondary_keys, skey_list, strlen(skey_list));
     
   } /* end if(num_secondary_keys) */
   else{
     key_data_list->secondary_keys = NULL;
   }
   
   return rc;
 }
