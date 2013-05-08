#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "pbl.h"                

/*
  Since PBL_ISAM hides the details of the ISAM file descriptor, pblIsamFile_t, 
  we need to have a data structure to shadow the internal PBL_ISAM data 
  structures. We need to to get the absolute record number. 
 */
typedef struct MDHIMKFILE_s
{
    char * magic;                /* magic string                              */
    int    bf;                   /* file handle from bigfile handling         */
    int    update;               /* flag: file open for update                */

    long   blockno;              /* of block current record is on             */
    int    index;                /* of current record on this block           */

    long   saveblockno;          /* block number of saved position            */
    int    saveindex;            /* item index of saved position              */

    void * filesettag;           /* file set tag attached to file             */

    int    transactions;         /* number of transactions active for file    */
    int    rollback;             /* next commit should lead to a rollback     */

    void * writeableListHead;    /* head and tail of writeable blocks         */
    void * writeableListTail;

                                 /* a user defined key compare function       */
    int (*keycompare)( void * left, size_t llen, void * right, size_t rlen );

} MDHIMKFILE_t;

typedef struct MDHIMFILE_s
{
    char          * magic;        /* magic string pointing to file descriptor */

    pblKeyFile_t  * mainfile;     /* file desriptor of main isam file         */
    int             update;       /* flag: file open for update               */

    int             transactions; /* number of transactions active for file   */
    int             rollback;     /* next commit should lead to a rollback    */

    int             nkeys;        /* number of key files of file              */
    pblKeyFile_t ** keyfiles;     /* file descriptors of key files            */

    int           * keydup;       /* flag array does the key allow duplicates */
    void         ** keycompare;   /* compare functions for keyfile            */

} MDHIMFILE_t;

extern int isamClose(pblIsamFile_t *isam);
extern int isamCommit(pblIsamFile_t **isam, int numFiles, int rollBack);
extern int isamDeleteRecord(pblIsamFile_t *isam);
extern int isamFindKey(pblIsamFile_t *isam, int which, int keyIndx, char *searchKey, int searchKeyLen, char *outKey, int *outKeyLen, int *recordNum);
extern int isamFlush(pblIsamFile_t *isam);
extern int isamGetKey(pblIsamFile_t *isam, int which, int keyIndx, char *outKey, int *outKeyLen, int *recordNum);
extern int isamInsert(pblIsamFile_t *isam, int numKeys, int *keySize, char * keyBuf, char *dataBuf, int *recordNum);
extern int isamOpen(pblIsamFile_t **isam, char *path, int update, void *fileSettag, int numKeys, char **fileNames, int *keyDup);
extern int isamReadData(pblIsamFile_t *isam, int dataLen, char *outBuf, long *outBufLen);
extern int isamReadDataLen(pblIsamFile_t *isam, long *dataLen);
extern int isamReadKey(pblIsamFile_t *isam, int keyIndx, char *outKey, int *outKeyLen);
extern int isamSetRecord(pblIsamFile_t *isam, int keyIndx, int recordNum, char *outKey, int *outKeyLen);
extern int isamStartTransaction(pblIsamFile_t **isam, int numFiles);
extern int isamUpdateData(pblIsamFile_t *isam, int newDataLen, char *newData, long *outDataLen);
extern int isamUpdateKey(pblIsamFile_t *isam, int keyIndx, char *newKey, int newKeyLen);
