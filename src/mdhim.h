/* ******************* #includes *********************** */
#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "debug.h"

/* MPI */
#include <mpi.h>

/* key/data stores */
#include <pbl.h>
#include "mdhim_dswrap.h"
#include "mdhim_link.h"

/* **************** #defines ****************************/
#define CLIENTTAG 2
#define DATATAG 99
#define DONETAG 100
#define QUITTAG 101
#define HOSTNAMELEN 255
#define KEYTAG 89
#define MAX_PROCS_PER_SRV 10
#define MAX_RANGES_PER_SRV 100
#define MAX_RANGE_SRVS 1024
#define MAX_START_RANGES MAX_RANGES_PER_SRV*MAX_RANGE_SRVS
#define PISAMTAG 79
#define RANGE 50
#define SERVERS 5
#define SRVTAG 1
#define DATABUFFERSIZE 2048
#define KEYSIZE 255
#define ERRORTYPE unsigned short

/* ********************** MDHIM ERROR CODES ************************ */

#define MDHIM_SUCCESS                   0
#define MDHIM_ERROR_BASE		2000
#define MDHIM_ERROR_IDX_PATH 		( MDHIM_ERROR_BASE + 1 )		// path to index data not found
#define MDHIM_ERROR_CTRL_INFO_PATH 	( MDHIM_ERROR_BASE + 2 )		// control information path not found
#define MDHIM_ERROR_CTRL_INFOR_WRITE 	( MDHIM_ERROR_BASE + 3 )		// write of control info on close error
#define MDHIM_ERROR_COMM 		( MDHIM_ERROR_BASE + 4 )		// communicator bad
#define MDHIM_ERROR_REC_NUM 		( MDHIM_ERROR_BASE + 5 )		// invalid absolute record number
#define MDHIM_ERROR_KEY  		( MDHIM_ERROR_BASE + 6 )		// invalid key
#define MDHIM_ERROR_IDX_RANGE 		( MDHIM_ERROR_BASE + 7 )		// Index out of bounds
#define MDHIM_ERROR_MEMORY 		( MDHIM_ERROR_BASE + 8 )		// out of memory
#define MDHIM_ERROR_INIT 		( MDHIM_ERROR_BASE + 9 )		// structure or memory not initalized properly
#define MDHIM_ERROR_NOT_FOUND 		( MDHIM_ERROR_BASE + 10 )		// Requested item, i.e. key, not found/not in DB
#define MDHIM_ERROR_SPAWN_SERVER        ( MDHIM_ERROR_BASE + 11 )               // Error while spawning range server
#define MDHIM_ERROR_DB_OPEN             ( MDHIM_ERROR_BASE + 12 )               // Database open error
#define MDHIM_ERROR_DB_CLOSE            ( MDHIM_ERROR_BASE + 13 )               // Database close error
#define MDHIM_ERROR_DB_COMMIT           ( MDHIM_ERROR_BASE + 14 )               // Database commit error
#define MDHIM_ERROR_DB_DELETE           ( MDHIM_ERROR_BASE + 15 )               // Database delete error
#define MDHIM_ERROR_DB_FIND             ( MDHIM_ERROR_BASE + 16 )               // Database find error
#define MDHIM_ERROR_DB_FLUSH            ( MDHIM_ERROR_BASE + 17 )               // Database flush error
#define MDHIM_ERROR_DB_GET_KEY          ( MDHIM_ERROR_BASE + 18 )               // Database get key error     
#define MDHIM_ERROR_DB_INSERT           ( MDHIM_ERROR_BASE + 19 )               // Database insert error
#define MDHIM_ERROR_DB_READ             ( MDHIM_ERROR_BASE + 20 )               // Database read record error
#define MDHIM_ERROR_DB_READ_DATA_LEN    ( MDHIM_ERROR_BASE + 21 )               // Database read data length error
#define MDHIM_ERROR_DB_READ_KEY         ( MDHIM_ERROR_BASE + 22 )               // Database read key error
#define MDHIM_ERROR_DB_COMPARE          ( MDHIM_ERROR_BASE + 23 )               // Database compare error
#define MDHIM_ERROR_DB_SET_RECORD       ( MDHIM_ERROR_BASE + 24 )               // Database set record error
#define MDHIM_ERROR_DB_START_TRANS      ( MDHIM_ERROR_BASE + 25 )               // Database start transaction error
#define MDHIM_ERROR_DB_UPDATE_DATA      ( MDHIM_ERROR_BASE + 26 )               // Database update data error
#define MDHIM_ERROR_DB_UPDATE_KEY       ( MDHIM_ERROR_BASE + 27 )               // Database update key error

/* ************* mdhimFind key comparison types ***********/

#define MDHIM_EQ    0 /* Find first record that equals input key */
#define MDHIM_EQF   1 /* Find first record that equals input key */
#define MDHIM_EQL   2 /* Find last record that equals input key */
#define MDHIM_GEF   3 /* Find the last record that equals or the first record that is greater than input key */
#define MDHIM_GTF   6 /* Find first record that is greater than the input key */
#define MDHIM_LEL   9 /* Find first record equal to or the last record that is less than the input key */
#define MDHIM_LTL   11 /* Find last record that is less than the input key */

/* ************* mdhimGet types ***********/
#define MDHIM_LXL   14 /* Last record, last one if duplicates exist */
#define MDHIM_FXF   15 /* First record, first one if duplicates exist */
#define MDHIM_PRV   17 /* Previous key */
#define MDHIM_NXT   18 /* Next key */
#define MDHIM_CUR   19 /* Current key */

/* **************** MDHIM structures *******************/
typedef struct rangeDataTag{
  short dirty_range;        /* Has range been modified since last flush? */
  int num_records;          /* Number of records in this range           */
  long range_start;         /* Starting (hashed) value of range          */
  char range_min[KEYSIZE];            /* Minimum key in this range */
  char range_max[KEYSIZE];            /* Maximum key in this range */
  struct rangeDataTag *next_range;        
} RANGE_DATA;

typedef struct rangeListTag{
  int num_ranges;         /* Number of ranges on this server/in this list */
  struct rangeDataTag *range_list;  /* list of ranges on this server */
} RANGE_LIST;

typedef struct rangeSrvTag{
  // XXX why not just make this char name[HOSTNAMELEN]?
  char *name;              /* hostname of range server      */
  int range_srv_num;       /* Range server rank in mdhim_comm Communicator */
}RANGE_SRV;

typedef struct keyDataList{
  char *pkey;              // Primary key
  int pkey_length;         // Primary key length   
  char *data;              // Record data 
  char *secondary_keys;    // list of secondary keys; key length followed by key value
}KEY_DATA_LIST;

typedef struct altKeys{
  int type;                // type of alternate key; char, int, float, etc.
  int max_key_length;
  int max_pad_length;
}ALT_KEYS;

typedef struct listrc{
  ERRORTYPE max_return; // Maximum MDHIM_ERROR
  int num_ops;          // Number of operations completed
  ERRORTYPE *errors;    // Array of MDHIM_ERROR codes
} LISTRC;

typedef struct MDHIMFD_s{
  MPI_Comm mdhim_comm;
  int mdhim_size;           /* Number of processes  */
  int mdhim_rank;           /* Rank in mdhim_comm   */
  MPI_Comm rangeSrv_comm;
  int rangeSvr_size;        /* Number of procs in rangeSrv_comm     */
  RANGE_SRV *range_srv_info;/* Info on range servers and names      */
  RANGE_LIST range_data;    /* Info on ranges and number of records */
  RANGE_LIST flush_list;    
  int max_data_length;      /* Max length of record data            */
  int max_pkey_length;      /* Max length of primary key (used for padding) */
  int max_pkey_padding_length; /* Padding used for ints and floats   */
  int max_recs_per_range;
  int nkeys;
  int pkey_type;        /* primary key type; 0 alpha-numeric, 1 int, 2 float */
  struct altKeys *alt_key_info; /*array of information on all non primary keys*/
  char *path;           /* path to record files */
  int update;
  int range_srv_flag;   /* denotes process is a range server */
  int *keydup;
  char last_key[KEYSIZE];
  int last_recNum;
} MDHIMFD_t;

typedef struct FLUSH_s
{
  int g_min;  /* global min value */
  int g_max;  /* global max value */
  int ranges_per_server[MAX_RANGE_SRVS]; /* number of ranges ordered by the index number */
  int start_ranges[MAX_START_RANGES];
  int entries_per_range[MAX_START_RANGES];
} FLUSH_t;

extern int mdhimDelete(MDHIMFD_t *fd, int keyIndx, void *ikey, int *record_num, void *okey, int *okey_len);
extern int mdhimFinalize(MDHIMFD_t *fd, int flush);
extern int mdhimFind( MDHIMFD_t *fd, int key_indx, int type, void *ikey, int *rec_number, void *okey, int *okey_len);
extern int mdhimFlush(MDHIMFD_t *fd);
extern int mdhimGet( MDHIMFD_t *fd, int key_indx, int ftype, int *record_num, void *okey, int *okey_len);
extern int mdhimInit(MDHIMFD_t *fd, int numRangeSvrs, char **rangeSvrs, int *numRangeSvrsByHost, int commType, MPI_Comm inComm );
extern int mdhimInsert(MDHIMFD_t *fd, struct keyDataList *key_data_list, int num_key_data, LISTRC *ierrors);
extern int mdhimOpen(MDHIMFD_t *fd, char *recordPath, int mode, int numKeys, int *keyType, int *keyMaxLen, int *keyMaxPad, int maxDataSize, int maxRecsPerRange);
extern int spawn_mdhim_server (MDHIMFD_t *fd);
