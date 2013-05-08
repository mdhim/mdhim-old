#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <mpi.h>
#include "mdhim.h"
#include "debug.h"

#define TEST_BUFLEN       2048
#define MAX_HOST_NAME 255
#define MAX_KEY_LEN 256
#define MAX_KEYS 2048
#define MAXCOMMANDS 5
#define MAXCOMMANDLENGTH 2048
#define QUEUESIZE 5
#define QUEUESIZEPLUS (QUEUESIZE+1)
#define READYTAG 10
#define WORKTAG 11
#define TRUE 1
#define FALSE 0

struct work_queue{
  char **buf;
  int head;
  int tail;
  int data_size;
};

typedef struct work_queue wk_queue;

struct options {
  int numRangeSvrs;
  char **rangeSvrs;
  int commType;
  int mode;
  int numKeys;
  int keyType[MAX_KEYS];
  int keyMaxLen[MAX_KEYS];
  int keyMaxPad[MAX_KEYS];
  int maxDataSize;
  int maxRecsPerRange;
  int *numRangeSvrsByHost;
  char recordPath[TEST_BUFLEN];
};

