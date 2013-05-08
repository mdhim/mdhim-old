I Introduction
II Installing the MDHIM Test Program
III Running the MDHIM Test Program
IV Tests 


I Introduction
II Installing the MDHIM Test Program
III Running the MDHIM Test Program
IV Tests 
The following directories contain tests to run against MDHIM to check for correctness. The output files provided are what the output of the test should look like. Any times reported in the output files will not match what you will see.
In each of the config files, the host name or names must be changed from 'ubuntu' to the host name of the node running the MDHIM range server or servers.

In all cases, the files:
test_op_i_input.txt - The input MDHIM commands file for op test i
test_op_i_config.txt - The input configuration file for op test i
test_op_i_output.txt - What output of op test i should look like with no debug messages.
where i is the test number and op is any of the MDHIM operations; delete, flush, insert, etc.


test_delete/test_delete_1

Purpose: 
Delete a record. Delete a record in the middle of a range for a single range server with a single range. "Middle" of the range means not the record with the minimum key and not the maximum key of the range. 

Command:
mpirun -n 2 ./tester.x ./test_delete/test_delete_1_config.txt ./test_delete/test_delete_1_input.txt

test_delete/test_delete_2

Purpose: 
Delete the only record. Delete the only record in a range for a single range server with a single range.

Command:
mpirun -n 2 ./tester.x ./test_delete/test_delete_2_config.txt ./test_delete/test_delete_2_input.txt

test_delete/test_delete_3

Purpose: 
Delete the first record. Delete the first record in MDHIM for a single range server with a single range.

Command:
mpirun -n 2 ./tester.x ./test_delete/test_delete_3_config.txt ./test_delete/test_delete_3_input.txt

test_delete/test_delete_4

Purpose: 
Delete the minimum record in the second range for a single range server with two ranges.

Command:
mpirun -n 2 ./tester.x ./test_delete/test_delete_4_config.txt ./test_delete/test_delete_4_input.txt

test_delete/test_delete_5

Purpose: 
Delete with no inserts and no flush. Try and delete a key that does not exist with there is no data in MDHIM. Should error gracefully.

Command:
mpirun -n 2 ./tester.x ./test_delete/test_delete_5_config.txt ./test_delete/test_delete_5_input.txt

test_delete/test_delete_6
Purpose: 
Delete the maximum record in the first range for a single range server with two ranges.

Command:
mpirun -n 2 ./tester.x ./test_delete/test_delete_6_config.txt ./test_delete/test_delete_6_input.txt

test_delete/test_delete_7
Purpose: 
Delete the last record. Delete the last record in MDHIM for a single range server with a single range. Make sure that the new current record is the new last record.

Command:
mpirun -n 2 ./tester.x ./test_delete/test_delete_7_config.txt ./test_delete/test_delete_7_input.txt

test_delete/test_delete_8
Purpose: 
Delete a key that does not exist in MDHIM. Code should gracefully fail.

Command:
mpirun -n 2 ./tester.x ./test_delete/test_delete_8_config.txt ./test_delete/test_delete_8_input.txt



test_insert/test_insert_1
Purpose:
Insert three keys and flush from a single node into a single range server

Command:
mpirun -n 2 ./tester.x ./test_insert/test_insert_1_config.txt ./test_insert/test_insert_1_input.txt

test_insert/test_insert_2
Purpose:
Insert four keys from a single nodes into a single range server where one key is a duplicate and should fail to insert.

Command:
mpirun -n 2 ./tester.x ./test_insert/test_insert_2_config.txt ./test_insert/test_insert_2_input.txt

test_flush/test_flush_1
Purpose: 
Flush after open with no records inserted.

Command:
mpirun -n 2 ./tester.x ./test_flush/test_flush_1_config.txt ./test_flush/test_flush_1_input.txt


Planned:
test_insert/test_insert_3
Purpose:
Insert four keys from two different nodes into two separate range servers where one key is a duplicate and should fail to insert.


test_flush/test_flush_1
Purpose: 
Flush with multiple range servers with two ranges each. Do something else so taht we know that the merged flush data is in order and contains everything from the individual ranges.

test_flush/test_flush_2
Purpose: 
Flush with a second flush with fewer ranges on one server. Make sure number of flush ranges is correct.

test_flush/test_flush_3
Purpose: 
Flush with a second flush immediately after, i.e. with not modification to range data inbetween. Make sure that flush data remains the same if flush is called multiple times.


test_get/test_get_0
Purpose:
Call mdhimGet when there is no data. Should error gracefully

test_get/test_get_1
Purpose:
Insert some records, get the first record then get previous. Should error gracefully

test_get/test_get_2
Purpose:
Insert some records, get the last record then get next. Should error gracefully

test_find/test_find_0
Purpose:
Call mdhimFind when there is no data. Should error gracefully
