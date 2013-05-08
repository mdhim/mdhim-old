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
  A collection of functions to manipulate linked lists; insert, delete, 
  print and search. The list consists of the RANGE_NODE data structure defined
  in mdhim.h. 

  Programmer: James Nunez
  Date: October 26, 2011
  
  Reference: Behrouz A. Forouzan and Richard F. Gilberg, "Computer Science A 
  Structured Programming Approach Using C", pp. 707 - 737, Second Edition

  Used:
  cc -o test_link test_link.c -I../../data_stores/pbl_1_04_04/src
  use "-g -O0" for valgrind
*/

#include "mdhim.h"
//#include "mdhim_link.h"

/* ========= combineRanges ==========
   combineRanges combines two linked lists into a single a linked list. 
   We assume that key values are distinct in the two lists. If they are not, 
   two nodes with the same key will remain in the combined list.
   
   firstList is the pointer to the head of the list. 
   secondList is the pointer to the head of the second list.

   Return: returns the head pointer
*/
struct rangeDataTag *combineRanges(struct rangeDataTag *aList, struct rangeDataTag *bList ){
  
  struct rangeDataTag *pcur1, *pcur2, c;
  struct rangeDataTag *clist = &c;
  
  pcur1 = aList;
  pcur2 = bList;

  while(pcur1 != NULL && pcur2 != NULL){
    if(pcur1->range_start < pcur2->range_start){
      clist->next_range = pcur1;
      pcur1 = pcur1->next_range;
    }
    else{
      clist->next_range = pcur2;
      pcur2 = pcur2->next_range;
    }

    clist = clist->next_range;
  }

  if(pcur1 == NULL)
    clist->next_range = pcur2;
  else
    clist->next_range = pcur1;

  return c.next_range;
}

/* ========= deleteRange ==========
   This function deletes a single range from a linked list. 
   
   rangeList is the pointer to the head of the list
   rangePre points to the range before the delete range
   rangeCur is the range to be deleted
   
   Return: returns the head pointer
*/
struct rangeDataTag *deleteRange(struct rangeDataTag *rangeList, struct rangeDataTag *rangePre, struct rangeDataTag *rangeCur){
  
  if(rangePre == NULL){
    /*
      Delete the first, possibly only, range in the list
    */
    rangeList = rangeCur->next_range;
  }
  else{
    /*
      Delete a range in the middle or the end of the list
    */
    rangePre->next_range = rangeCur->next_range;
  }
  
  free(rangeCur);

  return rangeList;
}

/* ========= insertRange ==========
   This function inserts a single range into a linked list. Does not modify 
   or act on information in the RANGE_DATA struct, i.e. it will insert 
   multiple nodes with the same start range. The information in the RANGE_DATA 
   struct does not get updated. 
   
   input:
   rangeList is the pointer to the head of the list
   rangePre is a pointer to the new range's logical predecessor
   data is the node to be inserted.
   
   Return: returns the head pointer to the list
*/
struct rangeDataTag *insertRange(struct rangeDataTag **rangeList, struct rangeDataTag *rangePre, struct rangeDataTag *rangeNew){

  if(rangePre == NULL){

    /*
      Insert into an empty list or before the first range
    */
    rangeNew->next_range = *rangeList;
    *rangeList = rangeNew;
  }
  else{

    /*
      Insert into the middle or the end of the list
    */
    rangeNew->next_range = rangePre->next_range;
    rangePre->next_range = rangeNew;
  }

  return *rangeList;
}

/* ========= printList ==========
   Traverse a linked list and print the RANGE contents
   
   input:
   rangeList is the pointer to the head of the list
   
   Return: Nothing
*/

void printList(struct rangeDataTag *rangeList){
  
  struct rangeDataTag *walker;
  int lineCount = 0;
  
  walker = rangeList;
  
  printf("Range list (start range, num records, min , max): ");
  
  while(walker){
    if(++lineCount > 10){
      printf("\n");
      lineCount = 1;
    }
    
    printf("%3ld, %3d %s %s ", walker->range_start, walker->num_records, walker->range_min, walker->range_max);
    walker = walker->next_range;
  }
  printf("\n");
  return;
}

/* ========= searchAndDeleteNode ==========
   Given key value, find the location of a Range node. If the node does not 
   exist, do nothing. If the nodes exists, remove it. Range data is updated.
   
   Input:
   rangeList is the pointer to the head of the list
   target is key being sought

   Output:
   rangeList is the pointer to the head of the updated list
   
   Return: 2 is target is found, removed and this is the only record in range
   1 if target is found and removed, 
   0 if target not found, 
   -1 if an error occurs
*/
int searchAndDeleteNode(struct rangeDataTag **rangeList, int target){
  
  int found = 0;
  
  struct rangeDataTag *rangeCur = NULL;
  struct rangeDataTag *rangePre = NULL;
  struct rangeDataTag *rangeNew = NULL;
  
  found = searchList(*rangeList, &rangePre, &rangeCur, target);
  
  if(!found){
    
    PRINT_MLINK_DEBUG("Target (%d) not found. Returning found = %d\n", target, found);
    
  }
  else{

    PRINT_MLINK_DEBUG("Target (%d) was found. Number of records is %d\n", target, rangeCur->num_records);

    if(rangeCur->num_records == 1){
      *rangeList = deleteRange(*rangeList, rangePre, rangeCur);
      found = 2; 
    }
    else{
      rangeCur->num_records--;
    }
  }

  //  printf("At the end of search and delete going to print list\n");
  //  printList(*rangeList);
  
  return found;
}

/* ========= searchAndInsertNode ==========
   Given key value, find the location of a Range node. If the node does not 
   exist, add it to the list. If it exists, range data is updated.
   
   Input:
   rangeList is the pointer to the head of the list
   target is key being sought
   
   Output:
   rangeList is the pointer to the head of the updated list
   
   Return: 1 if target is found, 0 if not, -1 if an error occurs
*/
int searchAndInsertNode(struct rangeDataTag **rangeList, int target, struct rangeDataTag *irange){
  int found = 0;
  
  struct rangeDataTag *rangeCur = NULL;
  struct rangeDataTag *rangePre = NULL;
  struct rangeDataTag *rangeNew = NULL;
  
  found = searchList(*rangeList, &rangePre, &rangeCur, target);
  
  PRINT_MLINK_DEBUG("searchAndInsertNode: Range with target %d was found = %d\n", target, found);
  
  if(!found){
    
    PRINT_MLINK_DEBUG("searchAndInsertNode: Target not found. Going to insert\n");
    
    if(!(rangeNew = (struct rangeDataTag *)malloc(sizeof(struct rangeDataTag)))){
      printf("searchAndInsertNode ERROR: Problem with allocating memory for RANGE_DATA data structure\n");
      return -1;
    }
    
    rangeNew->range_start = target;
    rangeNew->num_records = 1;
    rangeCur = rangeNew;
    irange = rangeNew;
    *rangeList = insertRange(rangeList, rangePre, rangeNew);
    if(irange == NULL)
      PRINT_MLINK_DEBUG("searchAndInsertNode: irange is NULL.\n");
  }
  else{
    PRINT_MLINK_DEBUG("searchAndInsertNode: Target was found. increment number of records from %d with min %s\n", rangeCur->num_records, rangeCur->range_min);
    rangeCur->num_records++;
    irange = rangeCur;
    PRINT_MLINK_DEBUG("searchAndInsertNode: Incremented number of records to %d with min %s\n", rangeCur->num_records, rangeCur->range_min);
  }
  
  return found;
}

/* ========= searchInsertAndUpdateNode ==========
   Given key value, find the location of a Range node. If the node does not 
   exist, add it to the list. If it exists, range data is updated.
   
   Input:
   rangeList is the pointer to the head of the list
   target is key being sought
   ikey
   key_size
   key_type

   Output:
   rangeList is the pointer to the head of the updated list
   
   Return: 1 if target is found, 0 if not, -1 if an error occurs
*/
int searchInsertAndUpdateNode(struct rangeDataTag **rangeList, int target, char *ikey, int key_size, int key_type){
  
  int found = 0;
  
  struct rangeDataTag *rangeCur = NULL;
  struct rangeDataTag *rangePre = NULL;
  struct rangeDataTag *rangeNew = NULL;
  
  //XXX Replace most of this routine with searchAndInsertNode above.
  //  searchAndInsertNode(struct rangeDataTag **rangeList, int target);

  found = searchList(*rangeList, &rangePre, &rangeCur, target);
  
  PRINT_MLINK_DEBUG("Range with target %d was found = %d\n", target, found);
  
  if(!found){

    PRINT_MLINK_DEBUG("Target (%d) not found. Going to insert\n", target);

    if(!(rangeNew = (struct rangeDataTag *)malloc(sizeof(struct rangeDataTag)))){
      printf("Problem with allocating memory for RANGE_DATA data structure\n");
      return -1;
    }
    
    rangeNew->range_start = target;
    rangeNew->num_records = 1;
    rangeNew->dirty_range = 1;
    rangeCur = rangeNew;
    *rangeList = insertRange(rangeList, rangePre, rangeNew);
    if(rangeCur == NULL)
      PRINT_MLINK_DEBUG("searchAndInsertNode: rangeCur is NULL.\n");
  }
  else{
    PRINT_MLINK_DEBUG("Target (%d) was found. increment number of records from %d with min %s\n", target, rangeCur->num_records, rangeCur->range_min);
    rangeCur->num_records++;
    PRINT_MLINK_DEBUG("Target (%d) was found. incremented number of records to %d with min %s\n", target, rangeCur->num_records, rangeCur->range_min);
  }

  /*
    Now update the statistics of the current node
  */
  updateNodeStats(rangeCur, ikey, key_size, key_type);
  PRINT_MLINK_DEBUG("After updateNodeStats: number of records is %d with min %s\n", rangeCur->num_records, rangeCur->range_min);

  //  printList(*rangeList);
  
  return found;
}

/* ========= createAndCopyNode ==========
   Create the node and copy the range data. If a node already exists, then 
   all the data will be replaced with the input range data.

   Input:


   Output:

   
   Return: 1 if node exists in list, 0 if not and -1 on error

*/
int createAndCopyNode(struct rangeDataTag **rangeList, int target, struct rangeDataTag *node_data){
  
  int found = 0;
   
  struct rangeDataTag *rangeCur = NULL;
  struct rangeDataTag *rangePre = NULL;
  struct rangeDataTag *rangeNew = NULL;
  
  /*
    Find if the node exists in the input list
  */
  found = searchList(*rangeList, &rangePre, &rangeCur, target);
  PRINT_MLINK_DEBUG("Target found = %d\n", target);

  if(!found){
    PRINT_MLINK_DEBUG("Target (%d) not found. Going to insert\n", target);
  
    if(!(rangeNew = (struct rangeDataTag *)malloc(sizeof(struct rangeDataTag)))){
      printf("Problem with allocating memory for RANGE_DATA data structure\n");
      return -1;
    }
    
    rangeNew->range_start = target;
    rangeNew->num_records = 1;
    rangeNew->dirty_range = 1;
    rangeCur = rangeNew;
    *rangeList = insertRange(rangeList, rangePre, rangeNew);
    // XXX update number of ranges in list?
  }

  /*
    Now copy the input node data to the current range
  */
  copyNodeData(rangeCur, node_data);
  
  PRINT_MLINK_DEBUG("After copyNodeData number of records is %d with min %s\n", rangeCur->num_records, rangeCur->range_min);
  
  //  printList(*rangeList);
  
  return found;
}

/* ========= searchList ==========
   Given value, finds the location of the node with start_range equal to input
   value

   Input:
   rangeList is the pointer to the head of the list
   target is key being sought

   Output:
   rangePre is a pointer to a variable to receive predecessor
   rangeCur is a pointer to a variable to receive the current range
   
   Return: 1 if target is found, 0 if not
   rangeCur points to the range with equal or greater key 
   or NULL if target > key of last range
   rangePre points to the largest range smaller than key 
   or NULL if target < key of first node
*/
int searchList(struct rangeDataTag *rangeList, struct rangeDataTag **rangePre, struct rangeDataTag **rangeCur, int target){
  
  int found = 0;
  
  *rangePre = NULL;
  *rangeCur = rangeList;
  
  if(rangeList == NULL){
    *rangeCur = NULL;
  }
  else{
    while(*rangeCur != NULL && target > (*rangeCur)->range_start){
      PRINT_MLINK_DEBUG("Range Cur != NULL and target (%d) > range cur range_start (%ld)\n", target, (*rangeCur)->range_start);
      *rangePre = *rangeCur;
      *rangeCur = (*rangePre)->next_range;
    }
    
    if(*rangeCur != NULL && target == (*rangeCur)->range_start)
      found = 1;
  }
  
  return found;
}

/* ========= copyNodeData ==========
   Copy the range data from src to dest

   Input:
   struct rangeDataTag *src Source data for the range data structure

   Output:
   struct rangeDataTag *dest Destination for the range data
   
   Return: 1 on success, 0 otherwise

*/
int copyNodeData(struct rangeDataTag *dest, struct rangeDataTag *src){
  
  if(dest == NULL || src == NULL){
    printf("copyNodeData Error: Input range data structure is NULL.\n");
    return 0;
  }

  dest->num_records = src->num_records;
  dest->range_start = src->range_start;
  dest->dirty_range = src->dirty_range;
  strcpy(dest->range_min, src->range_min);
  strcpy(dest->range_max, src->range_max);

  return 1;
}

/* ========= compareKeys ==========
   Compare two keys. Equality is not tested for.

   Input:
   oneKey is the first key to compare
   twoKey is the second key to compare
   sizeKey is the number of chars to compare. Ignored for int and floats
   typeKey is the type of key; 1 = char, 2 = int, 3 = float
   
   Return: 0 if keys are equal, -1 if oneKey is less than twoKey or 1 is oneKey is more than twoKey
*/
int compareKeys(char *oneKey, char *twoKey, int sizeKey, int typeKey){
  
  int rc = 0;
  int oneInt, twoInt;
  float oneFloat, twoFloat;
  
  PRINT_MLINK_DEBUG("compareKeys: keytype = %d keysize = %d\n", typeKey, sizeKey); 
  
  if(typeKey == 0){
    rc = memcmp(oneKey, twoKey, sizeKey);
    PRINT_MLINK_DEBUG("compareKeys: memcmp(%s, %s, %d) = %d\n",oneKey, twoKey, sizeKey, rc); 
    
    if(rc < 0)
      return -1;
    else if(rc > 0)
      return 1;
    else
      return 0;
  }
  else if (typeKey == 1){
    oneInt = atoi(oneKey);
    twoInt = atoi(twoKey);
    if(oneInt < twoInt)
      return -1;
    else if(oneInt < twoInt)
      return 1;
    else
      return 0;
  }
  else if (typeKey == 2){
    oneFloat = atof(oneKey);
    twoFloat = atof(twoKey);
    // For float, should do float1 - float2 = e, if e < small_num, floats equal
    if(oneFloat < twoFloat)
      return -1;
    else if(oneFloat < twoFloat)
      return 1;
    else
      return 0;

  }
  /*
    If key type isn't recognized, return error code.
  */
  // XXX What error code should be returned for an error?
  return 0;
}

/* ========= updateNodeStats ==========
   Update the information found in the range list nodes.

   Input:
   rangeDataTag is the node that needs to be updated.
   key is the values to update the stats with
   key_size is length of key in characters
   key_type is the type of key; 1 = char, 2 = int, 3 = float

   Output:
   rangeDataTag is the node with updated data.
   
   Return: -1 if error, 1 is no errors 
*/
int updateNodeStats(struct rangeDataTag *rangeList, char *key, int key_size, int key_type){
  
  int rc = 0;
  
  if(rangeList == NULL){
    printf("updateNodeStats Error: Input rage data structure is NULL.\n");
    return -1;
  }
  
  if(rangeList->num_records == 1){

    memset(rangeList->range_min, '\0', KEYSIZE);
    memset(rangeList->range_max, '\0', KEYSIZE);
    strncpy(rangeList->range_min, key, key_size);
    strncpy(rangeList->range_max, key, key_size);
  }
  else{
    
    rc = compareKeys(key, rangeList->range_min, key_size, key_type);
    
    if(rc < 0){
      strncpy(rangeList->range_min, key, key_size);
      rangeList->range_min[key_size] = '\0';
    }
    else{
      rc = compareKeys(key, rangeList->range_max, key_size, key_type);
      if(rc > 0){
	strncpy(rangeList->range_max, key, key_size);
	rangeList->range_max[key_size] = '\0';
      }
    }

  }
  
  return 1;
}
