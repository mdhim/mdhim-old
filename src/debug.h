#ifndef  __MDHIM_DEBUG_H
#define __MDHIM_DEBUG_H

/*
  Define debugs
*/

//#define DELETE_DEBUG
//#define FIND_DEBUG
//#define FLUSH_DEBUG
//#define GET_DEBUG
//#define HASH_DEBUG
//#define INIT_DEBUG
//#define INSERT_DEBUG
//#define MLINK_DEBUG
//#define MDHIM_DEBUG
//#define OPEN_DEBUG
//#define QUEUE_DEBUG
//#define TESTER_DEBUG

/*
  Define debug print statements
*/

#ifdef DELETE_DEBUG
#define PRINT_DELETE_DEBUG(format, args...) printf("MDHIM_DELETE_DEBUG: "format, ##args);
#else
#define PRINT_DELETE_DEBUG(format, args...) 
#endif

#ifdef FIND_DEBUG
#define PRINT_FIND_DEBUG(format, args...) printf("MDHIM_FIND_DEBUG: "format, ##args);
#else
#define PRINT_FIND_DEBUG(format, args...) 
#endif

#ifdef FLUSH_DEBUG
#define PRINT_FLUSH_DEBUG(format, args...) printf("MDHIM_FLUSH_DEBUG: "format, ##args);
#else
#define PRINT_FLUSH_DEBUG(format, args...) 
#endif

#ifdef GET_DEBUG
#define PRINT_GET_DEBUG(format, args...) printf("MDHIM_GET_DEBUG: "format, ##args);
#else
#define PRINT_GET_DEBUG(format, args...) 
#endif

#ifdef HASH_DEBUG
#define PRINT_HASH_DEBUG(format, args...) printf("MDHIM_HASH_DEBUG: "format, ##args);
#else
#define PRINT_HASH_DEBUG(format, args...) 
#endif

#ifdef INIT_DEBUG
#define PRINT_INIT_DEBUG(format, args...) printf("MDHIM_INIT_DEBUG: "format, ##args);
#else
#define PRINT_INIT_DEBUG(format, args...) 
#endif

#ifdef INSERT_DEBUG
#define PRINT_INSERT_DEBUG(format, args...) printf("MDHIM_INSERT_DEBUG: "format, ##args);
#else
#define PRINT_INSERT_DEBUG(format, args...) 
#endif

#ifdef MDHIM_DEBUG
#define PRINT_MDHIM_DEBUG(format, args...) printf("MDHIM_DEBUG: "format, ##args);
#else
#define PRINT_MDHIM_DEBUG(format, args...) 
#endif

#ifdef MLINK_DEBUG
#define PRINT_MLINK_DEBUG(format, args...) printf("MDHIM_LINK_DEBUG: "format, ##args);
#else
#define PRINT_MLINK_DEBUG(format, args...) 
#endif

#ifdef OPEN_DEBUG
#define PRINT_OPEN_DEBUG(format, args...) printf("MDHIM_OPEN_DEBUG: "format, ##args);
#else
#define PRINT_OPEN_DEBUG(format, args...) 
#endif

#ifdef QUEUE_DEBUG
#define PRINT_QUEUE_DEBUG(format, args...) printf("QUEUE_DEBUG: "format, ##args);
#else
#define PRINT_QUEUE_DEBUG(format, args...) 
#endif

#ifdef TESTER_DEBUG
#define PRINT_TESTER_DEBUG(format, args...) printf("TEST_DEBUG: "format, ##args);
#else
#define PRINT_TESTER_DEBUG(format, args...) 
#endif

#endif
