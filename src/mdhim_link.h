#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

struct rangeDataTag *combineRanges(struct rangeDataTag *aList, struct rangeDataTag *bList );
struct rangeDataTag *deleteRange(struct rangeDataTag *rangeList, struct rangeDataTag *rangePre, struct rangeDataTag *rangeCur);
struct rangeDataTag *insertRange(struct rangeDataTag **rangeList, struct rangeDataTag *rangePre, struct rangeDataTag *data);
void printList(struct rangeDataTag *rangeList);
int searchList(struct rangeDataTag *rangeList, struct rangeDataTag **rangePre, struct rangeDataTag **rangeCur, int target);
int searchAndInsertNode(struct rangeDataTag **rangeList, int target, struct rangeDataTag *irange);
int searchInsertAndUpdateNode(struct rangeDataTag **rangeList, int target, char *ikey, int key_size, int key_type);
int searchAndDeleteNode(struct rangeDataTag **rangeList, int target);
int createAndCopyNode(struct rangeDataTag **rangeList, int target, struct rangeDataTag *node_data);
