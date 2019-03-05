# database_systems_implementation
 Database systems implementation course project
#Contributers
Tushar Kaley UFID: 9126-1421
Sharmilee Desai UFID: 0519-9796

#Instructions
1. To run the tests provided with the original zip (on Linux) just run the following commands in sequence 
make clean
make test_macos
./test.out


2. To run the tests provided with the original zip (on Linux) just run the following commands in sequence 
make clean
make test.out
./test.out

3. To run gtests on Mac OS run the following commands
make clean
make test_macos
clang++ -std=c++11 -stdlib=libc++ DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread
or 
just run make gtest_macos

4. To run gtests on Linux run the following commands
make clean
make test.out
g++ -std=c++11 DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread
or
just run make gtest_linux

PS: The location of the gtest folder should be included in the your PATH for this to run
PPS: Also the runs.bin file for the orders table should be present in the P1 folder for the gtests for project to go through

5. To run Project 1 on Linux just run the following commands
make clean
make test_dbfile
./test_dbfile.out

6.  To run Project 1 on Macos just run the following commands
make clean
make test_dbfile_mac
./test_dbfile.out

7. To run Project 2 Milestone 1 on Linux just run the following commands
make clean
make test_bigq
./test_bigq.out

8.  To run Project 2 Milestone 1 on Macos just run the following commands
make clean
make test_bigq_macos
./test_bigq.out

#Overview
Aim: To implement the DBFile class specifically a heap. 
The job of the DBFile class within the database system is simply to store and retrieve records from the disk.

Project 2: Milestone 1
Our aim was to build a Sorted File Implementation using the external sort algorithm TPMMS

Project 2: Milestone 2
To implement the DBFile class for Sorted file. 
The job of the DBFile class within the database system is simply to store and retrieve records from the disk.
Now with a possibility for heap type and Sorted type both

