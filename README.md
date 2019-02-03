# database_systems_implementation
 Database systems implementation course project
#Contributers
Tushar Kaley UFID: 9126-1421
Sharmilee Desai UFID: 0519-9796

#Instructions
1. To run the tests provided with the original zip (on Linux) just run the following commands in sequence 
make clean
make test_macos.out
./test.out

2. To run the tests provided with the original zip (on Linux) just run the following commands in sequence 
make clean
make test.out
./test.out
3. To run gtests on Mac OS run the following commands
make clean
make main
clang++ -std=c++11 -stdlib=libc++ DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread
or 
just run make gtest_macos

4. To run gtests on Linux run the following commands
make clean
make main
g++ -std=c++11 DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread
or
just run make gtest_linux

PS: The location of the gtest folder should be included in the your PATH for this to run

#Overview
Aim: To implement the DBFile class specifically a heap. 
The job of the DBFile class within the database system is simply to store and retrieve records from the disk.
