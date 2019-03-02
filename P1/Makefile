CC = g++ -O0 -Wno-deprecated 
tag = -i
ifdef linux
tag = -n
endif

gtest_macos:  clean test_macos
	clang++ -std=c++11 -stdlib=libc++ DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread

gtest_linux: clean test.out
	g++ -std=c++11 DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread

test_macos: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab_macos.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o -ll -lpthread -fsanitize=address

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test.o -lfl -lpthread 

main_macos: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab_macos.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o main.o -ll -lpthread

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o Pipe.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o Pipe.o y.tab.o lex.yy.o main.o -lfl -lpthread 

test_bigq_macos: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab_macos.o lex.yy.o test_p21.o
	$(CC) -o test_bigq.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p21.o -ll -lpthread -fsanitize=address

test_bigq: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p21.o
	$(CC) -o test_bigq.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p21.o -lfl -lpthread 

test_dbfile_mac: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab_macos.o lex.yy.o test_p1.o
	$(CC) -o test_dbfile.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p1.o -ll -lpthread

test_dbfile: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o DBFile.o test_p1.o
	$(CC) -o test_dbfile.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p1.o -lfl

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

main.o: main.cc
	$(CC) -g -c main.cc
test.o: test.cc
	$(CC) -g -c test.cc

test_p1.o: test_p1.cc
	$(CC) -g -c test_p1.cc


test_p21.o: test_p21.cc
	$(CC) -g -c test_p21.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab_macos.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f main
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	
