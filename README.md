# database_systems_implementation
 Database systems implementation course project
#Contributers
Tushar Kaley UFID: 9126-1421
Sharmilee Desai UFID: 0519-9796

#Instructions
1. To run main.cc (on Linux) just run the following commands in sequence 
make clean
make main
./main


2. To run the tests provided with the original zip (on Linux) just run the following commands in sequence 
make clean
make test_mac
./test.out


3. To run the tests provided with the original zip (on Linux) just run the following commands in sequence 
make clean
make test.out
./test.out

4. To run gtests on Mac OS run the following commands
make clean
make test_mac
clang++ -std=c++11 -stdlib=libc++ DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread
or 
just run make gtest_mac

5. To run gtests on Linux run the following commands
make clean
make test.out
g++ -std=c++11 DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread
or
just run make gtest_linux

PS: The location of the gtest folder should be included in the your PATH for this to run
PPS: Also the runs.bin file for the orders table should be present in the P1 folder for the gtests for project to go through

6. To run Project 1 on Linux just run the following commands
make clean
make test_dbfile
./test_dbfile.out

7.  To run Project 1 on macOS just run the following commands
make clean
make test_dbfile_mac
./test_dbfile.out

8. To run Project 2 Milestone 1 on Linux just run the following commands
make clean
make test_bigq
./test_bigq.out

9.  To run Project 2 Milestone 1 on macOS just run the following commands
make clean
make test_bigq_mac
./test_bigq.out

7. To run Project 2 Milestone 2 on Linux just run the following commands
make clean
make test_sorted
./test_sorted.out

8.  To run Project 2 Milestone 2 on macOS just run the following commands
make clean
make test_sorted_mac
./test_sorted.out

9. To run Project 2 Milestone 2 on Linux just run the following commands
make clean
make test_relops
./test_relops.out

10.  To run Project 2 Milestone 2 on macOS just run the following commands
make clean
make test_relops_mac
./test_relops.out

#Overview
Aim: To implement the DBFile class specifically a heap. 
The job of the DBFile class within the database system is simply to store and retrieve records from the disk.

Project 2: Milestone 1
Our aim was to build a Sorted File Implementation using the external sort algorithm TPMMS

Project 2: Milestone 2
To implement the DBFile class for Sorted file. 
The job of the DBFile class within the database system is simply to store and retrieve records from the disk.
Now with a possibility for heap type and Sorted type both

Project 3
To implement the following relational operations
SelectPipe, SelectFile, Project, Join, DuplicateRemoval, Sum, GroupBy, and WriteOut
We build on top of previous three projects here making extensive use of BigQ and DBFile

Project 4.1
Implement the Statistics class which calculates the cost in terms of number of tuples for the given query


Project 3 Note:
Please note that for test Q6 the schema used to create the order maker for the groupby is the complete join schema 
essentially making every record distinct with no specific grouping attribute thus making grouping redundant and returning all rows
We have tested it with a specific grouping attribute(s_nationkey) and function performing the following operation-> (s_acctbal + (s_acctbal * 1.05)) 
Have included the screenshot for the same with the project.
The result in the screenshot returns 25 results since it groups on s_nationkey(with 25 unique values).

Project 4 Note:
Please note that for tests Q8 and Q11 we get different results than those checked against in the test case.

Project 4.2
Aim: To compile and optimize an input SQL statement, and then print the resulting, optimized query plan to the screen.
These are the queries that the data is setup for

SELECT l_orderkey, l_partkey, l_suppkey FROM lineitem AS l WHERE  (l.l_returnflag = 'R') AND (l.l_discount < 0.04 OR l.l_returnflag = 'R') AND (l.l_shipmode = 'MAIL')

SELECT SUM(p.akash), p.sharmilee, ps.tushar FROM part AS p, supplier AS s, partsupp AS ps WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey=ps.ps_suppkey) AND (p.p_partkey = ps.ps_partkey)

