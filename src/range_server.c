#include "mdhim.h"
#include "range_server.h"

/* ========== getCommands ==========
   
   Routine called by threads to act on range server commands 
   
   fd is the input MDHIM fd struct with information on range servers

   Valid commands are:
   flush - moves range data from all range servers to the first range server 
   in the range servers array

   Warning: The order of the pblIsamFile_t pointer array may not be the same 
   order of the final range_list because ranges are inserted into the 
   range_list and can move. The elements in the isam pointer array do not 
   track those moves. 

   Return: 
*/

void *getCommands(void *infd){
  char *command = NULL, *databuf = NULL, *keybuf = NULL;
  char *p;
  char recv_buf[DATABUFFERSIZE];
  char recv_output[DATABUFFERSIZE];
  char *fileSettag = NULL;
  char *range_str, *key_str;
  char start_range_ff[26];
  int indx, f_indx, r_indx, do_work;
  int rc, i, j, err = MDHIM_SUCCESS, dowork=1;      
  int server_rank = -1;
  int skip_len, len, totalLen;
  int out_int, recordNum;
  int parse_int1 = 0, parse_int2 = 0, *keyLen = NULL;
  int start_range, num_ranges = 0, max_ops = 0;
  int *intArray = NULL, *errArray = NULL;
  int num_dirty = 0, numFlushRanges = 0;
  unsigned long tempULong = 0, bitVector = 0;
  DIR *dir;
  struct dirent *dirent;
  struct rangeDataTag *curRange, *tempRangeData;
  struct rangeDataTag *range_ptr, *cur_flush, *prev_flush;
  MDHIMFD_t *fd = (MDHIMFD_t* )infd;
  FILE *filed = NULL;
  int isamfd_counter = 0;
  pblIsamFile_t **isamfds;

  MPI_Status status;
  MPI_Request op_request, recv_request;
  
  while(dowork){
    memset(recv_buf, '\0', 2048);
    PRINT_MDHIM_DEBUG ("Rank %d: THREAD in get_mdhim_commands\n", fd->mdhim_rank);
    
    /*
      Post a non-blocking receive and wait/sleep until it returns. Also 
      wait/sleep for the operation Isend to complete.
    */
    if (MPI_Irecv(recv_buf, 2048, MPI_CHAR, MPI_ANY_SOURCE, SRVTAG, fd->mdhim_comm, &recv_request) 
	!= MPI_SUCCESS){
      fprintf(stderr, "Rank %d: getCommands Error -  MPI_Irecv error.\n", fd->mdhim_rank);
    }

    receiveReady(&recv_request, &status);
    
    /*
      Look for recognized commands
    */
    
    if (!strncmp(recv_buf, "close", strlen("close"))){  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      
      PRINT_MDHIM_DEBUG ("Rank %d: On close: bitVector is %lu\n", fd->mdhim_rank, bitVector);
      curRange = fd->range_data.range_list;
      for(j = 0; j < fd->range_data.num_ranges; j++){
	// XXX Make this an error array	
	err = MDHIM_SUCCESS;
	indx = (curRange->range_start/fd->max_recs_per_range)/fd->rangeSvr_size;
	curRange = curRange->next_range;
	tempULong = 1 << indx;
	//	tempULong = bitVector >> indx;
	
	if( tempULong & bitVector){
	  //	if( tempULong%2){
	  PRINT_MDHIM_DEBUG ("Rank %d: closing isam indx %d.\n", fd->mdhim_rank, indx);
	  
	  err = dbClose(isamfds[indx]);
	  /*
	    If we closed the file, zero out the bitVector at bit indx
	  */
	  if (!err) {
	    tempULong = 1 << indx;
	    bitVector  = bitVector ^ tempULong;
	    
	    PRINT_MDHIM_DEBUG ("Rank %d: End of close, now bitvector = %lu\n", 
			       fd->mdhim_rank, bitVector);
	  }
	  
	  PRINT_MDHIM_DEBUG ("Rank %d: returned from dbClose with error = %d.\n", 
			     fd->mdhim_rank, err);
	}
      } /* end for(j = 0; j < fd->range_data.num_ranges; j++){ */
      //XXX send back an error array
      MPI_Isend(&err, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &op_request);      
    } else if (!strncmp(recv_buf, "create", strlen("create"))) {  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
    } else if (!strncmp(recv_buf, "dbflush", strlen("dbflush"))) {  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
    } else if (!strncmp(recv_buf, "delete", strlen("delete"))) {  
      PRINT_DELETE_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);

      /*
	Unpack the delete command message. The "delete" string is first, then 
	the start range, key index to search on and the key to delete.
      */
      if ((databuf = (char *)malloc(sizeof(recv_buf))) == NULL) {
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the input key to delete.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }
      
      memset(databuf, '\0', sizeof(recv_buf));
      memset(recv_output, '\0', 2048);
      sscanf(recv_buf, "%*s %d %d %s", &start_range, &parse_int1, databuf);
      
      /* 
	 Delete the key from the database
      */
      indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;  
      err = dbDeleteKey(isamfds[indx], databuf, parse_int1, recv_output, &recordNum);      
      PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - dbDeleteKey exited with deleted key %s with" 
			  " record number %d with err %d\n", fd->mdhim_rank, recv_output, recordNum, err);      
      memset(recv_output, '\0', 2048);
      if(err != MDHIM_SUCCESS){
	/*
	  Key does not exist in the DB or there was another problem. 
	  Pass on the error value.
	*/
	recordNum = -1;
	out_int = -1;
      } else{
	/* 
	   The key was successfully deleted from the database. We need to 
	   modify the range data and set the current record. Flush data is 
	   not updated on delete.
	   
	   For all cases, set the current record to the one after the 
	   deleted key. If the deleted key is the last record, set the 
	   current record to the one before the deleted key. 
	*/
	rc = searchList(fd->flush_list.range_list, &prev_flush, &cur_flush, start_range);
	
	/* XXX I could point curRange to the node with deleted key using searchList, but not get index
	   back. */
	r_indx = -1;
	curRange = fd->range_data.range_list;
	for(i = 0; i < fd->range_data.num_ranges; i++){
	  if( curRange->range_start == start_range){
	    r_indx = i;
	    i = fd->range_data.num_ranges;
	  }
	  else{
	    curRange = curRange->next_range;
	  }
	}

	if((r_indx < 0) || (cur_flush == NULL)){
	  printf("Rank %d getCommands: Error - Cannot find flush or range (%d) information" 
		 " for the current deleted key with start range %d.\n", fd->mdhim_rank, r_indx, start_range);
	  err = MDHIM_ERROR_BASE;
	}
	
	PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - Range indx %d server has/had %d records" 
			    " and is dirty %d.\n", fd->mdhim_rank, r_indx, curRange->num_records, 
			    curRange->dirty_range);
	
	curRange->num_records--;
	curRange->dirty_range = 1;
	
	/*
	  If we deleted only key in range, don't delete the range. We will 
	  delete any empty ranges during flush.
	*/
	// XXX start modifying deleting only key here
	if(curRange->num_records == 0){
	  
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - deleted only key in range. Range %d has %d keys and server" 
			      " has %d ranges.\n", fd->mdhim_rank, r_indx, curRange->num_records, 
			      fd->range_data.num_ranges);
	  
	  // comment this out - don't want to delete
	  //	  searchAndDeleteNode(&(fd->range_data.range_list), start_range);
	  
	  // comment this out - don't want to change number of ranges
	  //	  fd->range_data.num_ranges--;

	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - after delete node, server has %d ranges.\n", 
			      fd->mdhim_rank, fd->range_data.num_ranges);
	  
	  // change this to fd->range_data.num_ranges == 1 (deleted only range)
	  //	  if(fd->range_data.num_ranges == 0){ 
	  if(fd->range_data.num_ranges == 1){ 
	    
	    /*
	      We deleted the only key from the only range.
	    */
	    
	    out_int = -1;
	    recordNum = -1;
	    recv_output[0] = '\0';
	  }
	  else if(cur_flush->next_range != NULL ){ 
	    
	    /*
	      We deleted a range other than the last range. The new current 
	      key is the minimum key of the next range.
	    */
	    
	    strcpy(recv_output, cur_flush->next_range->range_min);
	    recordNum = 1;
	    out_int = strlen(recv_output);
	  } else{ 
	    
	    /*
	      We deleted the last range. The new current key is maximum 
	      of the previous range.
	    */
	    
	    strcpy(recv_output, prev_flush->range_max);
	    recordNum = prev_flush->num_records;
	  }
	} else if(!strcmp(databuf, curRange->range_min)){
	  /*
	    More than one record in range and we deleted the minimum record. 
	    Replace the minimum key in the range with the one found. 
	  */
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - More than one key and delete range min %s" 
			      " with deleted key %s with record number %d\n", fd->mdhim_rank, 
			      curRange->range_min, databuf, recordNum);

	  parse_int2 = recordNum;
	  err = dbGetKey(isamfds[indx], parse_int2, 0, parse_int1, recv_output, &out_int, 
			 &recordNum);
	  strcpy(curRange->range_min, recv_output);

	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - After dbgetKey new range min %s with new" 
			      " current key %s with record number %d\n", fd->mdhim_rank, 
			      curRange->range_min, recv_output, recordNum);
	} else if(!strcmp(databuf, curRange->range_max)){
	  /*
	    More than one record in the range and we deleted the maximum record.
	    Replace the maximum key for the range with the found key. 
	    If we deleted last key in the last range, set the current key to 
	    the found key else set it to the minimum of the next range. 
	  */
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - More than one key and delete range" 
			      " max %s with deleted key %s with record number %d\n", 
			      fd->mdhim_rank, curRange->range_max, databuf, recordNum);

	  parse_int2 = recordNum - 1;
	  err = dbGetKey(isamfds[indx], parse_int2, 0, parse_int1, recv_output, &out_int, 
			 &recordNum);
	  strcpy(curRange->range_max, recv_output);

	  if(cur_flush->next_range != NULL ){ 
	    /*
	      We deleted a range other than the last range. The new current 
	      key is the minimum key of the next range.
	    */
	    memset(recv_output, '\0', 2048);
	    strcpy(recv_output, cur_flush->next_range->range_min);
	    recordNum = 1;
	    out_int = strlen(recv_output);
	  }
	  
	} else{
	  /*
	    More than one record in the range and it was not the minimum nor 
	    maximum key. So, get the current key.
	  */
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - More than one key and middle" 
			      " key with deleted key %s with record number %d\n", 
			      fd->mdhim_rank, databuf, recordNum);	  
	  memset(recv_output, '\0', 2048);
	  parse_int2 = recordNum;
	  err = dbGetKey(isamfds[indx], parse_int2, 0, parse_int1, recv_output, &out_int, 
			 &recordNum);	  
	  PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - Delete middle of the range with" 
			      " current key %s with record number %d.\n", fd->mdhim_rank, 
			      recv_output, recordNum);
	}
      }
      
      /*
	Now pack and send the error code to the requesting process. 
	The send can be non-blocking since this process doesn't 
	need to wait to see or act on if the send is received.
      */
      memset(databuf, '\0', sizeof(databuf));
      if(sprintf(databuf, "%d %d %d %s", err, recordNum, out_int, recv_output) < 0){
	printf("Rank %d getCommands: Error - problem packing output results for delete.\n", fd->mdhim_rank);
      }
      
      PRINT_DELETE_DEBUG ("Rank %d: THREAD Delete - going to send output %s with size %d to" 
			  " source with rank %d\n", fd->mdhim_rank, databuf, 
			  (int)strlen(databuf), status.MPI_SOURCE);      
      MPI_Isend(databuf, strlen(databuf), MPI_BYTE, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);      
      free(databuf);      
    } else if (!strncmp(recv_buf, "find", strlen("find"))){  
      PRINT_FIND_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      err = MDHIM_SUCCESS;
      
      /*
	Unpack the find command message. First is "find", then the start range, 
	key index to search on, type of find comparison and a search key.
      */
      if((databuf = (char *)malloc( sizeof(recv_buf))) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory" 
		" for the input key to find.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }

      memset(databuf, '\0', sizeof(recv_buf));      
      sscanf(recv_buf, "%*s %d %d %d %s", &start_range, &parse_int1, &parse_int2, databuf);      
      indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;
      err = isamFindKey(isamfds[indx], parse_int2, parse_int1, databuf, (int)strlen(databuf), 
			recv_output, &out_int, &recordNum);      
      PRINT_FIND_DEBUG ("Rank %d: THREAD Find - going to send found key %s, return code %d," 
			" and found key len %d and record number %d.\n", fd->mdhim_rank, 
			recv_output, err, out_int, recordNum);
      
      /*
	Now pack and send the error code and key found to the requesting 
	process. The send can be non-blocking since this process doesn't 
	need to wait to see or act on if the send is received.
      */
      memset(databuf, '\0', sizeof(recv_buf));
      if(sprintf(databuf, "%d %d %d %s", err, recordNum, out_int, recv_output) < 0){
	printf("Rank %d getCommands: Error - problem packing output results for find\n", 
	       fd->mdhim_rank);
      }
      
      PRINT_FIND_DEBUG ("Rank %d: THREAD Find - going to send output %s with size %d" 
			" to source with rank %d\n", fd->mdhim_rank, databuf, 
			(int)strlen(databuf), status.MPI_SOURCE);      
      MPI_Isend(databuf, strlen(databuf), MPI_BYTE, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, 
		&op_request);      
      PRINT_FIND_DEBUG ("Rank %d: THREAD Find - Returned from sending done to %d. DONE with FIND\n", 
			fd->mdhim_rank, status.MPI_SOURCE);

      free(databuf);      
    } else if(!strncmp(recv_buf, "flush", strlen("flush"))){
      err = MDHIM_SUCCESS;
      MPI_Comm_rank(fd->rangeSrv_comm, &server_rank);
      PRINT_FLUSH_DEBUG ("Rank %d: THREAD Inside %s with Range server comm size %d" 
			 " and server rank %d\n", fd->mdhim_rank, recv_buf, 
			 fd->rangeSvr_size, server_rank);
      
      /*
	Compute the number of dirty ranges each server has and then send it 
	to the first range server in the range_srv_info array. Then sum up 
	the number of ranges that will be sent you.
      */
      num_dirty = 0;
      range_ptr = fd->range_data.range_list;
      for(i = 0; i < fd->range_data.num_ranges; i++){
	if(range_ptr->dirty_range){
	  num_dirty++;
	}
	range_ptr = range_ptr->next_range;
      }
      
      PRINT_FLUSH_DEBUG ("Rank %d getCommands: Server rank %d has %d dirty ranges.\n", 
			 fd->mdhim_rank, server_rank, num_dirty);
      
      if (server_rank == 0){
	if ((intArray = (int *)malloc(sizeof(int) * fd->rangeSvr_size)) == NULL) {
	  printf("Rank %d getCommands: Error - Unable to allocate memory for the array" 
		 " of range server data.\n", fd->mdhim_rank);
	  err = MDHIM_ERROR_MEMORY;
	}
      }
      
      PRINT_FLUSH_DEBUG ("Rank %d getCommands: Server rank %d before gather.\n", 
			 fd->mdhim_rank, server_rank);
      MPI_Gather(&num_dirty, 1, MPI_INT, intArray, 1, MPI_INT, 0, fd->rangeSrv_comm);      
      if(server_rank == 0){
	numFlushRanges = 0;	
	for (i = 0; i < fd->rangeSvr_size; i++){
	  numFlushRanges += intArray[i];
	  PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range server %d has %d dirty ranges\n", 
			     fd->mdhim_rank, i, intArray[i]);
	}
	
	/* 
	   If there are no dirty ranges, just send error codes to 
	   the requesting process.
	*/
	if (numFlushRanges == 0){
	  PRINT_FLUSH_DEBUG ("Rank %d: THREAD - There are no new ranges to flush. There are %d existing ranges" 
			     " in flush list.\n", fd->mdhim_rank, fd->flush_list.num_ranges);	  
	  MPI_Isend(&err, 1, MPI_INT, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
	  continue;
	}
	
	PRINT_FLUSH_DEBUG ("Rank %d getCommands: Total number of dirty ranges %d\n", 
			   fd->mdhim_rank, numFlushRanges);
	
	/*
	  Copy all my range data into the flush data range linked list. If 
	  a range has no keys, still copy it to the flush data list, but 
	  delete it from my range data list.
	*/
	range_ptr = NULL;
	curRange = fd->range_data.range_list;
	for(indx = 0; indx < fd->range_data.num_ranges; indx++){
	  
	  PRINT_FLUSH_DEBUG ("Rank %d getCommands: Looking at range %d with min %s and start" 
			     " range %ld is dirty = %d.\n", fd->mdhim_rank, indx, 
			     curRange->range_min, curRange->range_start, curRange->dirty_range);	  
	  if (curRange->dirty_range) {
	    rc = createAndCopyNode(&(fd->flush_list.range_list), curRange->range_start, curRange);
	    if(rc < 0){
	      printf("Rank %d getCommands: Error - Problem creating flush list.\n", fd->mdhim_rank);
	      err = MDHIM_ERROR_BASE;
	    }
	    fd->flush_list.num_ranges += 1 - rc;
	    curRange->dirty_range = 0;
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range %d was dirty. flush list has %d ranges. \n", 
			       fd->mdhim_rank, indx, fd->flush_list.num_ranges);
	  }

	  /*
	    Delete ranges with no records from the range list; not from the 
	    flush list yet.
	  */
	  if(curRange->num_records == 0){
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range %d was dirty and has %d records." 
			       " Going to delete. \n", fd->mdhim_rank,indx, curRange->num_records);
	    deleteRange(fd->range_data.range_list, range_ptr, curRange);
	  } else{
	    range_ptr = curRange;
	    curRange = curRange->next_range;
	  }
	}
	
	/*
	  Now collect range data from all other range servers.
	*/
	if( (tempRangeData = (struct rangeDataTag *)malloc(sizeof(struct rangeDataTag))) == NULL){
	  printf("Rank %d getCommands: Error - Unable to allocate memory for the temporary" 
		 " array of range server data.\n", fd->mdhim_rank);
	  err = MDHIM_ERROR_MEMORY;
	}
	
	for(i = 1; i < fd->rangeSvr_size; i++){	  
	  for(j = 0; j < intArray[i]; j++){
	    if( MPI_Recv(&tempRangeData, sizeof(struct rangeDataTag), MPI_CHAR, 
			 fd->range_srv_info[i].range_srv_num, SRVTAG, 
			 fd->mdhim_comm, &status) != MPI_SUCCESS){
	      printf("Rank %d getCommands: Error - Unable to receive range server" 
		     " data from process %d.\n", fd->mdhim_rank, 
		     fd->range_srv_info[i].range_srv_num);
	      err = MDHIM_ERROR_BASE;
	    }

	    /*
	      Insert the new range into the flush linked list
	    */
	    rc = createAndCopyNode(&(fd->flush_list.range_list), 
				   tempRangeData->range_start, tempRangeData);
	    
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: copied range with start %ld and min" 
			       " %s to flush range list\n", fd->mdhim_rank, 
			       tempRangeData->range_start, tempRangeData->range_min);
	  }
	}
	
	printList(fd->flush_list.range_list);	
	free(intArray);
	free(tempRangeData);
      } else{
	/*
	  All other procs send your range data to server with rank 0
	*/
	
	range_ptr = NULL;
	curRange = fd->range_data.range_list;
	for(i=0; i < fd->range_data.num_ranges; i++){
	  if(curRange->dirty_range){
	    if (MPI_Send(curRange, sizeof(struct rangeDataTag), MPI_BYTE, 
			 fd->range_srv_info[0].range_srv_num, SRVTAG, fd->mdhim_comm) != 
		MPI_SUCCESS){
	      printf("Rank %d getCommands: Error - Unable to send range server data" 
		     " to process %d.\n", fd->mdhim_rank, fd->range_srv_info[0].range_srv_num);
	      err = MDHIM_ERROR_BASE;
	    }

	    curRange->dirty_range = 0;
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range %d was dirty. flush list has" 
			       " %d ranges. \n", fd->mdhim_rank, i, fd->flush_list.num_ranges);
	  }
	  
	  /*
	    Delete ranges with no records from the range list; not from the 
	    flush list yet.
	  */
	  if(curRange->num_records == 0){
	    PRINT_FLUSH_DEBUG ("Rank %d getCommands: Range %d was dirty and has %d records." 
			       " Going to delete. \n", fd->mdhim_rank,indx, curRange->num_records);
	    deleteRange(fd->range_data.range_list, range_ptr, curRange);
	  } else{
	    range_ptr = curRange;
	    curRange = curRange->next_range;
	  }
	}
	
      }/* end else */
      
      /*
	Send the error code to the requesting process
      */
      MPI_Isend(&err, 1, MPI_INT, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
      
    } else if(!strncmp(recv_buf, "get", 3)){
      /*      
	      At this point, we should only see next and previous requests. 
	      All other possible "get" options were taken care of with flush 
	      data in mdhimGet
      */
      PRINT_GET_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      err = MDHIM_SUCCESS;
      
      /*
	Unpack the get command message. First is "get", then the start range, 
	key index to search on, type of get comparison, current record number.
      */
      sscanf(recv_buf, "%*s %d %d %d %d", &start_range, &parse_int1, &parse_int2, &recordNum);
      memset(recv_buf, '\0', 2048);
      
      indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;
      
      PRINT_GET_DEBUG ("Rank %d getCommands: Going to call dbGetKey with isam file indx = %d," 
		       " key_indx = %d, get type %d recordNum = %d\n", fd->mdhim_rank, 
		       indx, parse_int1, parse_int2, recordNum);      
      if(parse_int2 == MDHIM_NXT){
	parse_int2 = recordNum;
	err = dbGetKey(isamfds[indx], parse_int2, 1, parse_int1, recv_buf, &out_int, &recordNum);
      }
      else if(parse_int2 == MDHIM_PRV){
	parse_int2 = recordNum;
	err = dbGetKey(isamfds[indx], parse_int2, -1, parse_int1, recv_buf, &out_int, &recordNum);
      }


      PRINT_GET_DEBUG ("Rank %d getCommands: Going to send got key %s, return code %d, found key" 
		       " len %d and record number %d.\n", fd->mdhim_rank, recv_buf, err, out_int, 
		       recordNum);
      
      /*
	Now pack and send the error code, the new current record number, 
	length of found key and the key found to the requesting 
	process. The send can be non-blocking since this process doesn't 
	need to wait to see or act on if the send is received.
      */
      if( (databuf = (char *)malloc( out_int + 15)) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the" 
		" output key to get.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }
      
      memset(databuf, '\0', sizeof(databuf));
      if(sprintf(databuf, "%d %d %d %s", err, recordNum, out_int, recv_buf) < 0){
	printf("Rank %d getCommands: Error - problem packing output results for get\n", 
	       fd->mdhim_rank);
      }
      
      PRINT_GET_DEBUG ("Rank %d: THREAD Get - going to send output %s with size %d to source" 
		       " with rank %d\n", fd->mdhim_rank, databuf, (int)strlen(databuf), 
		       status.MPI_SOURCE);      
      MPI_Isend(databuf, strlen(databuf), MPI_BYTE, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, 
		&op_request);
      
      PRINT_GET_DEBUG ("Rank %d: THREAD Get - Returned from sending done to %d. DONE with FIND\n", 
		       fd->mdhim_rank, status.MPI_SOURCE);
      free(databuf);      

    } else if(!strncmp(recv_buf, "insert", strlen("insert"))){
      /*
	The insert string may contain multiple records. The insert message 
	looks like "insert start_range key_1_length key_1 
	key_2_length key_2 ... key_numkeys_length key_numkeys data_length data 
	[ ... start_range key_1_length key_1 key_2_length key_2 ... 
	key_numkeys_length key_numkeys data_length data]"
      */
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      err = MDHIM_SUCCESS;
      
      if( (keyLen = (int *)malloc(sizeof(int) * fd->nkeys)) == NULL){
	printf("Rank %d getCommands: Error - Problem allocating memory for the array" 
	       " of key lengths.\n", fd->mdhim_rank);
	err =  MDHIM_ERROR_MEMORY;
      }
      if( (keybuf = (char *)malloc(sizeof(char) * (fd->nkeys + 1) * KEYSIZE)) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the" 
		" array of keys.\n", fd->mdhim_rank);
	err =  MDHIM_ERROR_MEMORY;
      }
      if( (databuf = (char *)malloc(sizeof(char) * DATABUFFERSIZE)) == NULL){
	fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for" 
		" the record data.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }
      
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Allocated keyLen of size %d and keybuf" 
			  " size %d\n", fd->mdhim_rank, fd->nkeys, (int)sizeof(keybuf));
      
      /*
	Get the max number of inserts and the start range for the first insert 
	and enter loop for remaining inserts
      */
      memset(recv_output, '\0', DATABUFFERSIZE);
      p = &(recv_buf[sizeof("insert")]);

      sscanf(p, "%s ", recv_output);
      len = strlen(recv_output) + 1;
      max_ops = atoi(recv_output);
      p += len;

      if( (errArray = (int *)malloc(sizeof(int) * max_ops)) == NULL){
	printf("Rank %d getCommands: Error - Problem allocating memory for the array" 
	       " of insert errors.\n", fd->mdhim_rank);
	err =  MDHIM_ERROR_MEMORY;
      }
      PRINT_INSERT_DEBUG ("Rank %d: After skip p=%s.\n", fd->mdhim_rank, p);
      sscanf(p, "%s ", recv_output);
      len = strlen(recv_output) + 1;
      start_range = atoi(recv_output);
      p += len;
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Pointer to data p=%s and start" 
			  " range %d\n", fd->mdhim_rank, p, start_range);
      
      out_int = 0; // Number of inserts
      do_work = 1;
      while( do_work ){
	
	memset(keybuf, '\0', sizeof(char) * (fd->nkeys + 1) * KEYSIZE);
	
	/*
	  For the primary key and each secondary key, get the size and key 
	  pairs. After the keys is the data length and data. 
	  Place the keys and keylengths into separate arrays. 
	*/
	totalLen = 0;
	for(i = 0; i < fd->nkeys; i++){
	  memset(recv_output, '\0', DATABUFFERSIZE);
	  sscanf(p, "%s ", recv_output);
	  len = strlen(recv_output) + 1;
	  keyLen[i] = atoi(recv_output);
	  p += len;	  
	  PRINT_INSERT_DEBUG ("Rank %d: THREAD Pointer to data p=%s and key" 
			      " len %d is %d\n", fd->mdhim_rank, p, i, keyLen[i]);	  
	  len = keyLen[i] + 1;
	  strncpy(&(keybuf[totalLen]), p, len);
	  totalLen += len;
	  p += len;	  
	  PRINT_INSERT_DEBUG ("Rank %d: For key %d: p =%s totalLen = %d keybuf=%s\n", 
			      fd->mdhim_rank, i, p, totalLen, keybuf);
	}
	
	PRINT_INSERT_DEBUG ("Rank %d: Total len is %d and strlen = %d\n", 
			    fd->mdhim_rank, totalLen, (int)strlen(keybuf));
	PRINT_INSERT_DEBUG ("Rank %d: p is %s with length %d\n", fd->mdhim_rank, p, 
			    (int)strlen(p));
	
	memset(recv_output, '\0', DATABUFFERSIZE);
	sscanf(p, "%s ", recv_output);
	len = strlen(recv_output) + 1;
	p += len;
	len = atoi(recv_output);
	
	memset(databuf, '\0', DATABUFFERSIZE);
	strncpy(databuf, p, len);
	p += len;
	PRINT_INSERT_DEBUG ("Rank %d: THREAD Insert - key buffer=%s data buffer=%s p=%s\n", 
			    fd->mdhim_rank, keybuf, databuf, p);
	
	/* 
	   For each insert command, check if file is already open.
	   
	   The directory containing all the key and data files should exist.
	   Check if the range (key and data) file is already open by checking 
	   the correspondng bit in the bit vector; 1 means file exists and is 
	   open. We assume if the data file exists, then all key files exist and 
	   are already open. If the file does not exist, open the files and if 
	   necessary, set the compare function. 	
	*/
	indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;
	err = open_db_files(fd, indx, start_range, isamfd_counter, isamfds, &bitVector, 0);
	if (err != MDHIM_SUCCESS) {
	  PRINT_INSERT_DEBUG ("Rank %d: Error opening database: %d\n", fd->mdhim_rank, err);
	}

	/*
	  If no errors have occurred, insert the record into the data store 
	*/	
	if(isamfds[indx] != NULL){
	  PRINT_INSERT_DEBUG ("Rank %d: Going to call isamInsert with nkeys %d, keybuf = %s," 
			      " databuf = %s\n", fd->mdhim_rank, fd->nkeys, keybuf, databuf);
	  
	  errArray[out_int] = isamInsert(isamfds[indx], fd->nkeys, keyLen, keybuf, databuf, 
					 &recordNum);
	  
	  PRINT_INSERT_DEBUG ("Rank %d: isamInsert record number %d return code = %d\n", 
			      fd->mdhim_rank, recordNum, errArray[out_int]);
	} else{	  
	  printf("Rank %d: An error has occurred and the insert of keys %s and data %s was aborted." 
		 " Please try again.\n", fd->mdhim_rank, keybuf, databuf);
	}
	
	PRINT_INSERT_DEBUG ("Rank %d: returned from isamInsert with error = %d.\n", fd->mdhim_rank, err);
	/*
	  Now that the insert completed, we need to update, extend or create 
	  the array of range information.
	*/	
	if(errArray[out_int] == MDHIM_SUCCESS){
	  PRINT_INSERT_DEBUG ("Rank %d: Adding to the range list of size %d\n", fd->mdhim_rank, 
			      fd->range_data.num_ranges);
	  /*
	    curRange = fd->range_data.range_list;
	    for(i=0; i < fd->range_data.num_ranges; i++){
	    printf("Rank %d: Before insert, Range %d with start range %d and num_records %d\n", fd->mdhim_rank, i, curRange->range_start, curRange->num_records);
	    curRange = curRange->next_range;
	    }
	  */
	  if( (indx = searchInsertAndUpdateNode(&(fd->range_data.range_list), start_range, 
						keybuf, keyLen[0], fd->pkey_type)) != -1){
	    fd->range_data.num_ranges += 1 - indx;
	  }
	  else{
	    printf("Rank %d: An error has occurred and the insert of the range was aborted. Range" 
		   " data may not accurately reflect contents of data store.\n", fd->mdhim_rank);
	  }
	  
	  PRINT_INSERT_DEBUG ("Rank %d: Done adding to the range list of size %d.\n", 
			      fd->mdhim_rank, fd->range_data.num_ranges);
	}
	
	// XXX Comment out when timing
	curRange = fd->range_data.range_list;
	for(i=0; i < fd->range_data.num_ranges; i++){
	  PRINT_INSERT_DEBUG ("Rank %d: After insert, Range %d with start range %ld, num_records %d," 
			      " range min %s and range max %s\n", fd->mdhim_rank, i, 
			      curRange->range_start, curRange->num_records, 
			      curRange->range_min, curRange->range_max );
	  curRange = curRange->next_range;
	}
	
	/*
	  Look for another insert command by looking for the next start range.
	*/
	out_int++;
	memset(recv_output, '\0', DATABUFFERSIZE);
	len = sscanf(p, "%s ", recv_output);

	if(len != 1){
	  do_work = 0;
	} else{
	  len = strlen(recv_output) + 1;
	  start_range = atoi(recv_output);
	  p += len;
	  PRINT_INSERT_DEBUG ("Rank %d: DO MORE WORK! Pointer to data p=%s and" 
			      " start range %d\n", fd->mdhim_rank, p, start_range);
	}	
      }
      
      PRINT_INSERT_DEBUG ("Rank %d: FINAL key buffer=%s data buffer=%s p=%s\n", 
			  fd->mdhim_rank, keybuf, databuf, p);
      /*
	Send the ISAM insert error code to requesting process. The send can 
	be non-blocking since this process doesn't need to wait to see or 
	act on if the send is received.
      */
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Insert - going to send error code %d to %d\n", 
			  fd->mdhim_rank, err, status.MPI_SOURCE);

      MPI_Isend(errArray, out_int, MPI_INT, status.MPI_SOURCE, DONETAG, fd->mdhim_comm, &op_request);
      
      PRINT_INSERT_DEBUG ("Rank %d: THREAD Insert - Returned from sending done to %d." 
			  " DONE with INSERT\n", fd->mdhim_rank, status.MPI_SOURCE);
      
      free(keybuf);
      free(databuf);
      //XXX Make sure errors were received before freeing error array
      receiveReady(&op_request, MPI_STATUS_IGNORE);
      free(errArray);
    }
    else if (!strncmp(recv_buf, "open", strlen("open"))){  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      err = MDHIM_SUCCESS;

      /*
	(Pre)Allocate memory for the array of ISAM file descriptor pointers.
      */
      isamfd_counter = 100;
      if( (isamfds = (pblIsamFile_t **)malloc(sizeof(pblIsamFile_t *) * 
					      isamfd_counter)) == NULL){
	printf("Rank %d getCommands: Error - Unable to allocate memory for the array" 
	       " of range server data.\n", fd->mdhim_rank);
	err = MDHIM_ERROR_MEMORY;
      }

      if (err != MDHIM_SUCCESS) {
	MPI_Isend("quit", strlen("quit"), MPI_CHAR, fd->mdhim_rank, QUITTAG, fd->mdhim_comm, 
		  &op_request);
	pthread_exit(&err);
      }
      
      dir = opendir(fd->path);
      if (dir == NULL) {
	err = MDHIM_ERROR_DB_OPEN;
	MPI_Isend("quit", strlen("quit"), MPI_CHAR, fd->mdhim_rank, QUITTAG, fd->mdhim_comm, 
		  &op_request);
	pthread_exit(&err);
      }

      bitVector = 0;
      while (dirent = readdir(dir)) {
	range_str = strstr(dirent->d_name, "Range");
	key_str = strstr(dirent->d_name, "Key");
	if (range_str == NULL || key_str == NULL) {
	  continue;
	}
	
	snprintf(start_range_ff, key_str - (range_str + 4), "%s", dirent->d_name + 5); 
	start_range = atoi(start_range_ff);
	PRINT_MDHIM_DEBUG ("Rank %d: Opening existing db files with start_range: %d\n", fd->mdhim_rank, start_range);
	indx = (start_range/fd->max_recs_per_range)/fd->rangeSvr_size;
	err = open_db_files(fd, indx, start_range, isamfd_counter, isamfds, &bitVector, 1);	
      }

      closedir(dir);      
      MPI_Isend(&err, 1, MPI_INT, fd->mdhim_rank, DONETAG, fd->mdhim_comm, &op_request);     
    }
    else if (!strcmp (recv_buf, "quit")){  
      PRINT_MDHIM_DEBUG ("Rank %d: THREAD Inside %s\n", fd->mdhim_rank, recv_buf);
      /*
	Send the message acknowledging the quit message then exit.
       */
      dowork=0;
      MPI_Isend("quit", strlen("quit"), MPI_CHAR, fd->mdhim_rank, QUITTAG, fd->mdhim_comm, 
		&op_request);
      pthread_exit(&err);
    }
    else{
      /* XX Problem with this if an error occurs. You will wait for the irecv to complete 
	 forever or something like that. Check this with sending bad command to this routine. */
      printf("Rank %d - ERROR: Unrecognized command. The following command will be ignored: %s\n", 
	     fd->mdhim_rank, recv_buf);
    }

    receiveReady(&op_request, &status);
  }
  
  return 0;
}

/* ========== spawn_mdhim_server ==========
   Spawns a thread running get_mdhim_commands

   fd is the input MDHIM fd struct with information on range servers

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_SPAWN_SERVER on failure   
*/
int spawn_mdhim_server (MDHIMFD_t * fd){
  int err = MDHIM_SUCCESS;
  pthread_t threads; 
  
  PRINT_MDHIM_DEBUG ("Rank %d: Entered spawn_mdhim_server\n",fd->mdhim_rank);
  
  err = pthread_create(&threads, NULL, getCommands, (void *)(fd));
  if (err) {
    fprintf(stderr, "Pthread create error while spawning server thread: %d\n", err);
    err = MDHIM_ERROR_SPAWN_SERVER;
  }
  
  return err;
}

/* ========== populate_range_data ==========
   Populates the range data from the stored data on an open

   Input:
   fd is the input MDHIM fd struct with information on range servers
   isam is a PBL ISAM file open for update 
   start_range  the target index 

   Returns: void
*/
void populate_range_data(MDHIMFD_t *fd, pblIsamFile_t *isamfd, int start_range) {
  char key[KEYSIZE];
  int key_len = 0;
  int record_num = 0 ;
  int err;
  struct rangeDataTag *curRange;
  int indx, rc;

  memset(key, 0, KEYSIZE);
  err = isamGetKey(isamfd, MDHIM_FXF, 0, key, &key_len, &record_num);
  PRINT_OPEN_DEBUG ("Rank %d: populating range data on open return: %d " 
		    " key: %s, key_len: %d, record_num: %d\n", fd->mdhim_rank, err, 
		    key, key_len, record_num);
  if ((err = searchInsertAndUpdateNode(&(fd->range_data.range_list), start_range, 
				       key, key_len, fd->pkey_type)) != -1) {
    fd->range_data.num_ranges += 1 - err;
  }

  while ((err = isamGetKey(isamfd, MDHIM_NXT, 0, key, &key_len, &record_num)) == MDHIM_SUCCESS) {
    PRINT_OPEN_DEBUG ("Rank %d: populating range data on open return: %d " 
		      " key: %s, key_len: %d, record_num: %d\n", fd->mdhim_rank, err, 
		      key, key_len, record_num);
    if ((err = searchInsertAndUpdateNode(&(fd->range_data.range_list), start_range, 
					 key, key_len, fd->pkey_type)) != -1) {
      fd->range_data.num_ranges += 1 - err;
    }
  }   
}

/* ========== open_db_files ==========
   Populates the range data from the stored data on an open

   Input:
   fd               is the input MDHIM fd struct with information on range servers
   indx             the index into the isamfds array
   start_range      the target index 
   isamfd_counter   the number of isamfds in the array
   isamfds          is an array of PBL ISAM file fds
   bitVector        stores which indexes in the isamfds array have been used
   populate         whether to populate the range data from the database

   Returns:  MDHIM_ERROR_MEMORY or MDHIM_SUCCESS
*/
int open_db_files(MDHIMFD_t *fd, int indx, int start_range, int isamfd_counter, 
		  pblIsamFile_t **isamfds, unsigned long *bitVector, int populate) {
  char **filenames, *dataname;
  unsigned long tempULong = 0;
  int err = MDHIM_SUCCESS;
  int i;

  tempULong = 1 << indx;	
  PRINT_INSERT_DEBUG ("Rank %d: Range indx = %d bitVector is %lu and shifted by indx %d is" 
		      " %lu\n", fd->mdhim_rank, indx, *bitVector, indx, tempULong);
	
  if(tempULong & (*bitVector)){
    return 0;
  }

  PRINT_INSERT_DEBUG ("Rank %d: File is NOT already open. Open file and set bitVector" 
		      " (%lu).\n", fd->mdhim_rank, *bitVector);
	  
  /*
    Check if you are creating an ISAM fd beyond the end of the array
    If so, realloc the array. Since this is a new file, allocate memory 
    for the ISAM fd.
  */
  if(indx > isamfd_counter){
    // For now, fail if isamfd_counter is not enough. 
    // Later, make this a b-tree or linked list.
	    
    printf("Rank %d getCommands: Error - exceeding the number of ISAM files" 
	   " allowed (%d). \n", fd->mdhim_rank, isamfd_counter);
    MPI_Abort(MPI_COMM_WORLD, 10);
    return 1;
  }
	  
  if( (isamfds[indx] = (pblIsamFile_t *)malloc(sizeof(pblIsamFile_t)) ) == NULL){
    fprintf(stderr, "Rank %d getCommands: Error - Unable to allocate memory for the" 
	    " ISAM fd at index %d.\n", fd->mdhim_rank, indx);
    err =  MDHIM_ERROR_MEMORY;
    return err;
  }
	  
  if( (dataname = (char *)malloc(sizeof(char) * (strlen(fd->path) + 16) )) == NULL){
    fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the" 
	    " array of data file name.\n", fd->mdhim_rank);
    err =  MDHIM_ERROR_MEMORY;
    return err;
  }
  memset(dataname, '\0', sizeof(char) * (strlen(fd->path) + 16));
  sprintf(dataname, "%sdataRange%d", fd->path, start_range);
  PRINT_INSERT_DEBUG ("Rank %d: Going to try and open file name %s\n", 
		      fd->mdhim_rank, dataname);
	  
  if( (filenames = (char **)malloc(sizeof(char *) * (fd->nkeys + 1))) == NULL){
    printf("Rank %d getCommands: Error - Problem allocating memory for the array" 
	   " of key file names.\n", fd->mdhim_rank);
    err =  MDHIM_ERROR_MEMORY;
    return err;
  }
	  
  for(i = 0; i < fd->nkeys; i++){
    if( (filenames[i] = (char *)malloc(sizeof(char) * 25)) == NULL){
      fprintf(stderr, "Rank %d getCommands: Error - Problem allocating memory for the" 
	      " array of key file name %d.\n", fd->mdhim_rank, i+1);
      err =  MDHIM_ERROR_MEMORY;
      return err;
    }
    memset(filenames[i], '\0', sizeof(char) * 25);
    sprintf(filenames[i], "Range%dKey%d", start_range, i);
    PRINT_INSERT_DEBUG ("Rank %d: filenames[%d] = %s\n", fd->mdhim_rank, i, filenames[i]);
  }
	  
  err = MDHIM_SUCCESS;
  err = dbOpen(&(isamfds[indx]), dataname, fd->update, NULL, fd->nkeys, filenames, 
	       fd->keydup);
  PRINT_INSERT_DEBUG ("Rank %d: returned from dbOpen with error = %d.\n", fd->mdhim_rank, err);
  if (populate) {
    populate_range_data(fd, isamfds[indx], start_range);
  }
  
  /*
    If necessary, set the ISAM setcompare function
  */
  //XXX	err = isamSetCompareFunction(&isam, 1, fd->pkey_type);
	  
  for(i = 0; i < fd->nkeys; i++)
    free(filenames[i]);
  free(filenames);
  free(dataname);
	  
  /*
    Set the bit vector to reflect that the file with indx is open
  */
  *bitVector |= tempULong;
  PRINT_INSERT_DEBUG ("Rank %d: After open, bitVector is %lu and 2^%d is %lu\n", 
		      fd->mdhim_rank, *bitVector, indx, tempULong);
  return err;
	  	
}


