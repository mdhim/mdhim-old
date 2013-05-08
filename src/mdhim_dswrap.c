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
   Wrapper functions around the PBL ISAM API

   Author: James Nunez

   Date: October 27, 2011

*/

#include "mdhim.h"

/* ========== dbClose ==========
   Close a PBL ISAM file
   
   isam is the ISAM file to close
   
   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_CLOSE on failure
*/

int dbClose(pblIsamFile_t *isam){
  
  int rc = MDHIM_SUCCESS;
  
  if(isam == NULL){
    printf("dbClose Error - Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;
  }
  
  rc = pblIsamClose(isam);
  
  if(rc != 0){
    printf("dbClose Error - PBL ISAM Error %d\n", pbl_errno);
    return MDHIM_ERROR_DB_CLOSE;
  }

  return rc;
}

/* ========== dbDeleteKey ==========
   Delete the specified key
   
   Input:
   isam 
   record_num is the record number of the current record
   offset is the number of recrods from the current record number to get; 0 means get the current record, 1 means get the next record, -1 means get the previous record, etc.
   key_ndx

   Output:
   out_key is the previous key
   out_record_num is the previous record number; -1 on error

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_XXX on failure
*/
int dbDeleteKey(pblIsamFile_t *isam, char *ikey, int key_ndx, char *out_key, int *out_record_num){

  int new_record_num = -1, okey_size = 0;
  int err = MDHIM_SUCCESS, which = MDHIM_EQ;
  
 /*
    Check input parameters
  */
  if( !isam ){
    *out_record_num = -1;
    printf( "dbDeleteKey Error - PBL ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  
  /* 
     Set the current record to the one we want to delete
  */
  err = isamFindKey(isam, which, key_ndx, ikey, (int)strlen(ikey), out_key, &okey_size, out_record_num);

  if(err != MDHIM_SUCCESS){
    *out_record_num = -1;
    return err;
  }

  err = isamDeleteRecord(isam);

  return err;
}

/* ========== dbGetKey ==========
   Get the key from record offset records from the current record
   
   Input:
   isam 
   record_num is the record number of the current record
   offset is the number of recrods from the current record number to get; 0 means get the current record, 1 means get the next record, -1 means get the previous record, etc.
   key_ndx

   Output:
   out_key is the previous key
   okey_size is the size of the found key; -1 on error
   out_record_num is the previous record number; -1 on error

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_XXX on failure
*/
int dbGetKey(pblIsamFile_t *isam, int record_num, int offset, int key_ndx, char *out_key, int *okey_size, int *out_record_num){

  int new_record_num = -1;
  int err = MDHIM_SUCCESS, which = MDHIM_CUR;

  /*
    Check input parameters
  */
  if( !isam ){
    *out_record_num = -1;
    *okey_size = -1;
    printf( "dbGetKey Error - PBL ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  
  if(record_num < 1){
    *out_record_num = -1;
    *okey_size = -1;
    printf( "dbGetKey Error - INput record number (%d) is not valid.\n", record_num);
    return MDHIM_ERROR_REC_NUM;    
  }

  new_record_num = record_num + offset;
  
  /*
    Set the current record to the one we are looking for and get it.
  */
  err = isamSetRecord(isam, key_ndx, new_record_num, out_key, okey_size);
  
  //  printf("dbGetKey: Just exited isamSetRecord with input keyIndx = %d, new_record_num = %d, output key %s length %d with return code %ld\n", key_ndx, new_record_num, out_key, *okey_size, err);
  
  *out_record_num =  record_num + offset;
  
  return err;
}


/* ========== dbOpen ==========
   Open and, if necessary, create a PBL ISAM file

   isam is the returned ISAM file descriptor
   path is a pointer to the path of file to create
   update is a flag, 0 for opening isam file for read acess only, 
   not 0 for read and write access 
   filesettag is for "flushing multiple files consistently" ?
   nkeys is the number of key files to create
   filenames is a list of file names to create/open
   keydup is a flag list where the ith index key signifies a duplicate key
   
   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_OPEN on failure
*/
int dbOpen(pblIsamFile_t **isam, char *path, int update, void *fileSettag, int numKeys, char **fileNames, int *keyDup){

  /*
    Check input parameters
  */

  if(!path){
    printf("dbOpen Error - Path to ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }

  if(!keyDup){
    printf("dbOpen Error - Key duplication array is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  
  *isam = pblIsamOpen( path, update, fileSettag, numKeys, fileNames, keyDup );
  
  if(*isam == NULL){
    printf("dbOpen Error - PBL ISAM Error %d errno %d %s\n", pbl_errno, errno, strerror(errno));
    return MDHIM_ERROR_DB_OPEN;
  }

  return MDHIM_SUCCESS;
}

/* ========== isamCommit ==========
   Commit or rollback changes done during a transaction
   
   isam is the list of ISAM files
   numFiles is the number of files to start transaction
   rollBack is 0 for commit and != 0 for rollback

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_COMMIT on failure
*/

int isamCommit(pblIsamFile_t **isam, int numFiles, int rollBack){
  
  int i, rc = MDHIM_SUCCESS;
  
  for(i=0; i < numFiles; i++)
    if(isam[i] == NULL){
      printf("isamCommit Error - Input ISAM file %d is NULL.\n", i+1);
      return MDHIM_ERROR_INIT;
    }
  if( numFiles < 1 ){
    printf("isamCommit Error - Number of input ISAM files (%d) must be greater than zero.\n", numFiles);
    return MDHIM_ERROR_INIT;
  }
  
  rc = pblIsamCommit(numFiles, isam, rollBack);
  
  if(rc < 0){
    printf("isamCommit Error - PBL ISAM Error %d\n", pbl_errno);
    return MDHIM_ERROR_DB_COMMIT;
  }
  
  return MDHIM_SUCCESS;
}

/* ========== isamDeleteRecord ==========
   Delete the current record from a PBL_ISAM file

  isam is the ISAM file to delete from

  Returns: MDHIM_SUCCESS or MDHIM_ERROR_DB_DELETE on failure
*/
int isamDeleteRecord(pblIsamFile_t *isam){

  int rc = MDHIM_SUCCESS;
  
  if(isam == NULL){
    printf("isamDeleteRecord Error - Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;
  }

  rc = pblIsamDelete(isam);
  
  if(rc != 0){
    printf("isamDeleteRecord Error - PBL ISAM Error %d with return code %d.\n", pbl_errno, rc);
    return MDHIM_ERROR_DB_DELETE;
  }
  
  return MDHIM_SUCCESS;
}

/* ========== isamFindKey ==========
   Find a key from a PBL ISAM file relative to an input key and set the 
   current record

   Input:
   isam is the ISAM file to search
   which is one of the following find operations:
        MDHIM_EQ - find a record whose key is equal to searchKey
	MDHIM_EQF - find the first key that is equal to searchKey
	MDHIM_EQL - find the last key that is equal to searchKey
	MDHIM_GEF - find the last record that is equal or the smallest record that is greater
	MDHIM_GTF - find the smallest record that is greater than searchKey
	MDHIM_LEL - find the first record that is equal or the biggest record that is smaller
	MDHIM_LTL - find the biggest record whose key that is smaller than searchKey 
   
   keyIndx is the index of the key to use starts at 0
   searchKey is the key for the record to search for
   searchKeyLen is the length of the key

   Output:
   outKey is the key found; max length of a PBL ISAM key is 255
   outKeyLen is length of the key found
   recordNum is the absolute record number of the found key
   
   Returns: MDHIM_SUCCESS or MDHIM_ERROR_DB_FIND on failure
*/
int isamFindKey(pblIsamFile_t *isam, int which, int keyIndx, char *searchKey, int searchKeyLen, char *outKey, int *outKeyLen, int *recordNum){
  
  int isamType = 0;
  int rc;

  MDHIMFILE_t *mfile;
  MDHIMKFILE_t *mkeyfile;
  /*
    Check input parameters
  */
  if( !isam ){
    printf( "isamFindKey Error - PBL ISAM file is NULL.\n");
    return(MDHIM_ERROR_INIT);    
  }
  if( !searchKey ){
    printf( "isamFindKey Error - The buffer for the input search key is NULL.\n");
    return(MDHIM_ERROR_INIT);    
  }
  if( !outKey ){
    printf( "isamFindKey Error - The buffer for the output key is NULL.\n");
    return(MDHIM_ERROR_INIT);    
  }

  /* 
     Convert MDHIM find type to PBL ISAM find type and make sure the 
     type is valid
  */ 
  switch(which){
  case MDHIM_EQ:
    isamType = PBLEQ;
    break;
  case MDHIM_EQF:
    isamType = PBLFI;
    break;
  case MDHIM_EQL:
    isamType = PBLLA;
    break;
  case MDHIM_GEF:
    isamType = PBLGE;
    break;
  case MDHIM_GTF:
    isamType = PBLGT;
    break;
  case MDHIM_LEL:
    isamType = PBLLE;
    break;
  case MDHIM_LTL:
    isamType = PBLLT;
    break;
  default:
    printf("isamFindKey Error - Find comparison type %d is not recognized.\n", which);
    return(MDHIM_ERROR_DB_FIND);
  }

  /*
    Find a key and check return code. If return code is >= key was found and find completed normally. If return code < 0, some error occured including PBL_ERROR_NOT_FOUND meaning the key was not found.
  */
  *outKeyLen = pblIsamFind(isam, isamType, keyIndx, (void *)searchKey, (size_t)searchKeyLen, (void *)outKey);
  outKey[*outKeyLen] = '\0';
  
  if(*outKeyLen < 0){
    printf("isamFindKey Error - Problem finding key %s; PBL ISAM Error %d error string %s with errno %d.\n", searchKey, pbl_errno, pbl_errstr, errno);
    return MDHIM_ERROR_NOT_FOUND;
  }
  
  /*
    Now get absolute record number
  */
  mfile = (MDHIMFILE_t *)isam;
  mkeyfile = (MDHIMKFILE_t *)mfile->keyfiles[keyIndx]; 
  *recordNum = mkeyfile->index;
  //  printf("After pblIsamFind index = %d\n",mkeyfile->index);

  return MDHIM_SUCCESS;
}

/* ========== isamFlush ==========
   Flush a PBL ISAM file to disk
   
   isam is the ISAM file to flush
   
   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_FLUSH on failure
*/
int isamFlush(pblIsamFile_t *isam){

  int rc = MDHIM_SUCCESS;
  
  if(isam == NULL){
    printf("isamFlush Error - Input ISAM file is NULL.\n");
    return(MDHIM_ERROR_INIT);    
  }
  
  rc = pblIsamFlush(isam);
  
  if(rc != 0){
    printf("isamFlush Error - PBL ISAM Error %d with return code %d\n", pbl_errno, rc);
    return MDHIM_ERROR_DB_FLUSH;
  }
  
  return MDHIM_SUCCESS;
}

/* ========== isamGetKey ==========
   Get a key and key length of a record from a PBL ISAM file

   Input:
   isam is a PBL ISAM file open for update 
   which is the key to find relative to one of the folowing search values:
   MDHIM_CUR - get key and keylen of current record
   MDHIM_NXT - get key and keylen of next record
   MDHIM_PRV - get key and keylen of previous record
   MDHIM_FXF - get key and keylen of first record
   MDHIM_LXL - get key and keylen of last record 
   keyIndx is the index of the key to use starts at 0
   Output:
   outKey is the key found; max length of a PBL ISAM key is 255
   outKeyLen is the length of the key found
   recordNum is the absolute record number of the found key

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_GET_KEY on failure
*/
int isamGetKey(pblIsamFile_t *isam, int which, int keyIndx, char *outKey, int *outKeyLen, int *recordNum){

  int isamType = 0;
  
  MDHIMFILE_t *mfile;
  MDHIMKFILE_t *mkeyfile;

  /*
    Check input parameters
  */
  if( !isam ){
    printf( "isamGet Error - PBL ISAM file is NULL.\n");
    return(MDHIM_ERROR_INIT);    
  }
  if( !outKey ){
    printf( "isamGet Error - The buffer for the output key is NULL.\n");
    return(MDHIM_ERROR_INIT);    
  }

  /*
    Convert MDHIM get type to PBL ISAM get type and make sure the type is valid
  */
  switch(which){
  case MDHIM_CUR:
    isamType = PBLTHIS;
    break;
  case MDHIM_FXF:
    isamType = PBLFIRST;
    break;
  case MDHIM_LXL:
    isamType = PBLLAST;
    break;
  case MDHIM_NXT:
    isamType = PBLNEXT;
    break;
  case MDHIM_PRV:
    isamType = PBLPREV;
    break;
  default:
    printf("isamGet Error - The key to get %d is not recognized.\n", which);
    return(MDHIM_ERROR_DB_GET_KEY);
  }
  
  /*
    Get a key and check return code.
  */
  *outKeyLen = pblIsamGet( isam, isamType, keyIndx, (void *)outKey);
  
  if( *outKeyLen < 0){
    printf( "isamGet Error - Unable to get requested key and key length pbl_errno %d, errno %d\n", pbl_errno, errno );
    return(MDHIM_ERROR_DB_GET_KEY);
  }
  
  /*
    Now get absolute record number
  */
  mfile = (MDHIMFILE_t *)isam;
  mkeyfile = (MDHIMKFILE_t *)mfile->keyfiles[keyIndx]; 
  *recordNum = mkeyfile->index;

  printf("After pblIsamGet new record num = %d\n", *recordNum);

  return MDHIM_SUCCESS;
}


/* ========== isamInsert ==========
   Insert a new record with given keys and data into a PBL ISAM file

   Input:
   isam is a PBL ISAM file open for update
   numKeys is the number of keys to insert
   keySize is an array of length of keys
   keyBuf is an array of keys. Each key is separated by a space
   dataBuf is the data of the record to insert

   Output:
   recordNum is the absolute record number of the found key

   This function uses pblKfGetAbs(), which is a function one level below 
   the published PBL_ISAM API. Thus, it could change without notice.

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_INSERT on failure
*/
int isamInsert(pblIsamFile_t *isam, int numKeys, int *keySize, char * keyBuf, char *dataBuf, int *recordNum){

  char *allKeys = NULL;
  char *p = NULL;

  int i, rc = 0, indx = 0; 
  int allKeyLen = 0, dataLen = 0;
  
  MDHIMFILE_t *mfile;
  MDHIMKFILE_t *mkeyfile;

  /*
    Check input parameters
  */
  if( !isam ){
    printf( "isamInsert Error: Input PBL ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  if( !keySize ){
    printf( "isamInsert Error: Array of input key lengths is empty.\n");
    return MDHIM_ERROR_INIT;    
  }
  if( !keyBuf ){
    printf( "isamInsert Error: Array of input keys is NULL.\n");
    return MDHIM_ERROR_INIT ;    
  }
  if( !dataBuf ){
    printf( "isamInsert Error: The input data string is NULL.\n");
    return MDHIM_ERROR_INIT ;    
  }
  
  /* 
     We need to convert the array of keys to what PBL ISAM requires; a 
     string with the length of the key, in binary, preceeding the key. 
  */
  dataLen = strlen(dataBuf);

  allKeyLen = strlen(keyBuf);
  allKeyLen++;
  allKeys = (char *)malloc(allKeyLen + 2);
  strcpy(allKeys, " ");
  strcpy(&allKeys[1], keyBuf);

  indx = 0;
  for(i=0; i < numKeys; i++){
    if(strncmp(&allKeys[indx], " ", 1)){
      printf( "isamInsert Error: Input key string requires a blank space between key values, not %c.\n", allKeys[indx]);
    return MDHIM_ERROR_INIT;    
    }
    
    allKeys[indx] = (char)(0xff & keySize[i]);
    indx += keySize[i] + 1;
  }

  /*
    For PBL ISAM, check key string length; must be between 0 and 1024.
  */
  if(allKeyLen <= 0 || allKeyLen >= 1024){
    printf( "isamInsert Error: Combined length of keys must be greater than 0 and less than 1024; input keylength = %d.\n", allKeyLen);
    return MDHIM_ERROR_DB_INSERT;    
  }

  /*
    Insert key(s) and check return code.
  */
  rc = pblIsamInsert( isam, (void *)allKeys, allKeyLen, (void *)dataBuf, dataLen);

  if( rc != 0){
    printf( "isamInsert Error: Unable to insert keys %s (all keys %s) pbl_errno %d, errno %d\n", keyBuf, allKeys, pbl_errno, errno );
    free(allKeys);
    return MDHIM_ERROR_DB_INSERT;    
  }
  
  /*
    Now get the record number for the record just inserted
  */
  mfile = (MDHIMFILE_t *)isam;
  mkeyfile = (MDHIMKFILE_t *)mfile->keyfiles[0]; 
  *recordNum = mkeyfile->index;
  //  printf("After pblIsamInsert index = %d for keys %s\n",mkeyfile->index, allKeys);

  free(allKeys);
  return MDHIM_SUCCESS;
}

/* ========== isamReadData ==========
   Get the data of the current record
   
   isam is the ISAM file to read the current record
   dataLen specifies the number of bytes to read
   outBuf is the data for the currect record. A terminating string character is added to the end of the data.
   outBufLen is the length of the output data
   
   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_READ on failure
*/

int isamReadData(pblIsamFile_t *isam, int dataLen, char *outBuf, long *outBufLen){
  
  if(isam == NULL){
    printf("isamReadData Error - Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  if( !outBuf ){
    printf( "isamReadData Error - The buffer for the output data is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }

  *outBufLen = pblIsamReadData(isam, (void *)outBuf, dataLen);
  
  outBuf[*outBufLen] = '\0';

  printf("outBuf = %s with bytes read %ld bytes requested %d\n", outBuf, *outBufLen, dataLen);

  if(*outBufLen < 0){
    printf("isamReadData Error - PBL ISAM Error %d with error string %s\n", pbl_errno, pbl_errstr );
    return MDHIM_ERROR_DB_READ;
  }

  return MDHIM_SUCCESS;
}

/* ========== isamReadDataLen ==========
   Get the length of the data of the current record
   
   isam is the ISAM file to read the current record
   dataLen is the length of the data of the current record

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_READ_DATA_LEN on failure
*/

int isamReadDataLen(pblIsamFile_t *isam, long *dataLen){
  
  if(isam == NULL){
    printf("isamReadDataLen Error - Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  
  *dataLen = pblIsamReadDatalen(isam);
  
  if(*dataLen < 0){
    printf("isamReadDataLen Error - PBL ISAM Error %d with error string %s\n", pbl_errno, pbl_errstr );
    return MDHIM_ERROR_DB_READ_DATA_LEN;
  }

  return MDHIM_SUCCESS;
}

/* ========== isamReadKey ==========
   Get the key and key langth of the current record
   
   isam is the ISAM file to read the current record
   keyIndx is the index of the key to read, index starts at 0
   outKey value of the key of the current record
   outKeyLen the length of the key of the current record

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_READ_KEY on failure
*/

int isamReadKey(pblIsamFile_t *isam, int keyIndx, char *outKey, int *outKeyLen){
  
  if(isam == NULL){
    printf("isamReadKey Error - Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  if( !outKey ){
    printf( "isamReadKey Error - The buffer for the output key is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }

  *outKeyLen = pblIsamReadKey(isam, keyIndx, (void *)outKey);
  
  if(*outKeyLen < 0){
    printf("isamReadKey Error - PBL ISAM Error %d with error string %s\n", pbl_errno, pbl_errstr );
    
    return MDHIM_ERROR_DB_READ_KEY;
  }

  return MDHIM_SUCCESS;
}
// XXX
// compare function for long ints, uses same returns as memcmp.
// @return   int rc  < 0: left is smaller than right
// @return   int rc == 0: left and right are equal
// @return   int rc  > 0: left is bigger than right
int pblIsamFloatComp(void * left, size_t llen, void * right, size_t rlen ){
    return(PBL_ERROR_NOT_FOUND);
}
int pblIsamDoubleComp(void * left, size_t llen, void * right, size_t rlen ){
    return(PBL_ERROR_NOT_FOUND);
}
int pblIsamLongComp(void * left, size_t llen, void * right, size_t rlen ){
    long lleft;
    long lright;
    char strBuffer[PBLKEYLENGTH + 1];// +1 for \0 terminator

    if(left != NULL){

        if(right == NULL)
            return(1);
        
        //((char*)left)[(llen-1)] = '\0';
        if(strlen(left) != llen){
            strncpy(strBuffer, left, llen);
            strBuffer[llen] = '\0';
            lleft = atol(strBuffer);
        }
        else
            lleft = atol(left);
    }
    
    if(right != NULL){
        
        if(left == NULL)
            return(-1);
        
        //((char*)right)[(rlen-1)] = '\0';
        if(strlen(right) != rlen){
            strncpy(strBuffer, right, rlen);
            strBuffer[rlen] = '\0';
            lright = atol(strBuffer);
        }
        else
            lright = atol(right);
        
                
    }

    if(lleft < lright)
        return(-1); 

    if(lleft > lright)
        return(1);

    if(lleft == lright)
        return(0);

    return(PBL_ERROR_NOT_FOUND);
}

/* ========== isamSetCompareFunction ==========
   Set an application specific compare function for a specified 
   key of an ISAM file
   
   isam is the ISAM file that contains data
   keyIndx index to apply custom function to 
   compare_type is char (0), integer (1), float (2) or double (3) 

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_COMPARE on failure
*/

int isamSetCompareFunction(pblIsamFile_t *isam, int keyIndx, int compare_type){
  
  int rc = 0;
  int (*compareFunction)(void*, size_t, void*, size_t);

  if(isam == NULL){
    printf("isamSetCompareFunction Error - Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }

  if(compare_type == 0){ /* use default memcmp compare function */
    return 0;
  }
  else if(compare_type == 1){
    compareFunction = pblIsamLongComp;
  }
  else if(compare_type == 1){
    compareFunction = pblIsamFloatComp;
  }
  else if(compare_type == 1){
    compareFunction = pblIsamDoubleComp;
  }
  else{
    printf("isamSetCompareFunction Error - Unknown input compare data type (%d).\n", compare_type);
    return MDHIM_ERROR_DB_COMPARE;
  }
  
  rc = pblIsamSetCompareFunction(isam, keyIndx, compareFunction);
  
  if(rc != 0){
    printf("isamSetCompareFunction Error - PBL ISAM Error %d\n", pbl_errno);
    return MDHIM_ERROR_DB_COMPARE;
  }
  
  return MDHIM_SUCCESS;
}

/* ========== isamSetRecord ==========
   Set the current record to the input record number
   
   isam is the ISAM file that contains data
   keyIndx is the index of the key to use; starts at 0
   recordNum is the record number to set to

   Output:
   outKey is the key for record recordNum
   outKeyLen is length of the key found
   
   This function uses pblKfGetAbs(), which is a function one level below 
   the published PBL_ISAM API. Thus, it could change without notice.
   
   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_SET_RECORD on failure
*/

int isamSetRecord(pblIsamFile_t *isam, int keyIndx, int recordNum, char *outKey, int *outKeyLen){
  
  long rc = 0;
  
  MDHIMFILE_t *mfile;
  MDHIMKFILE_t *mkeyfile;
  
  if(isam == NULL){
    printf("isamSetRecord Error: Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  if(recordNum < 1){
    printf("isamSetRecord Error: Input record number (%d)is not valid.\n", recordNum);
    return MDHIM_ERROR_INIT;    
  }
  
  /*
    Now set the ISAM data structures to reveal the key files and call pblKfGetAbs
  */
  mfile = (MDHIMFILE_t *)isam;
  rc = pblKfGetAbs(mfile->keyfiles[keyIndx], recordNum - 1, outKey, (size_t *)outKeyLen);
  
  outKey[*outKeyLen] = '\0';
  
  //  printf("isamSetRecord: input keyIndx = %d, recordNum = %d, output key %s length %d with return code %ld\n", keyIndx, recordNum - 1, outKey, *outKeyLen, rc);
  
  if(rc < 0){
    printf("isamSetRecord Error - PBL ISAM Error %d\n", pbl_errno);
    return MDHIM_ERROR_DB_SET_RECORD;
  }
  
  return MDHIM_SUCCESS;
}

/* ========== isamStartTransaction ==========
   Start a transaction on a set of ISAM files. Allows you to take 
   advantage of buffer cache.
   
   isam is the list of ISAM files
   numFiles is the number of files to start transaction

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_START_TRANS on failure
*/

int isamStartTransaction(pblIsamFile_t **isam, int numFiles){
  
  int i, rc = 0;
  
  for(i=0; i < numFiles; i++)
    if(isam[i] == NULL){
      printf("isamStartTransaction Error - Input ISAM file %d is NULL.\n", i+1);
      return MDHIM_ERROR_INIT;    
    }
  if( numFiles < 1 ){
    printf("isamStartTransaction Error - Number of input ISAM files (%d) must be greater than zero.\n", numFiles);
    return MDHIM_ERROR_INIT;    
  }
 
  rc = pblIsamStartTransaction(numFiles, isam);
  
  if(rc != 0){
    printf("isamStartTransaction Error - PBL ISAM Error %d\n", pbl_errno);
    return MDHIM_ERROR_DB_START_TRANS;
  }
  
  return MDHIM_SUCCESS;
}

/* ========== isamUpdateData ==========
   Update the data of the current record in a PBL ISAM file
   
   isam is the ISAM file that contains data
   newData is the new value of the data for the current record
   newDataLen is the length of the new data
   outDataLen is the length of the output data

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_UPDATE_DATA on failure
*/

int isamUpdateData(pblIsamFile_t *isam, int newDataLen, char *newData, long *outDataLen){
  
  int rc = 0;
  
  if(isam == NULL){
    printf("isamUpdateData Error - Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  if(newData == NULL){
    printf("isamUpdateData Error - Input update data string is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  
  *outDataLen = pblIsamUpdateData(isam, (void *)newData, (size_t)newDataLen);
  
  if(*outDataLen < 0){
    printf("isamUpdateData Error - PBL ISAM Error %d\n", pbl_errno);
    return MDHIM_ERROR_DB_UPDATE_DATA;
  }
  
  return MDHIM_SUCCESS;
}

/* ========== isamUpdateKey ==========
   Update a key value for the current record in a PBL ISAM file
   
   isam is the ISAM file that contains data
   keyIndx is the index of the key to use starts at 0
   newKey is the new value of the key
   newKeyLen is the length of the new key value

   Returns: MDHIM_SUCCESS on success or MDHIM_ERROR_DB_UPDATE_KEY on failure
*/

int isamUpdateKey(pblIsamFile_t *isam, int keyIndx, char *newKey, int newKeyLen){
  
  int rc = 0;
  
  if(isam == NULL){
    printf("isamUpdateKey Error - Input ISAM file is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  if(newKey == NULL){
    printf("isamUpdateKey Error - Input update key is NULL.\n");
    return MDHIM_ERROR_INIT;    
  }
  
  rc = pblIsamUpdateKey(isam, keyIndx, (void *)newKey, (size_t)newKeyLen);
  
  if(rc != 0){
    printf("isamUpdateKey Error - PBL ISAM Error %d\n", pbl_errno);
    return MDHIM_ERROR_DB_UPDATE_KEY;
  }

  return MDHIM_SUCCESS;
}
