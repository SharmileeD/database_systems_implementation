CC = g++ -O0 -Wno-deprecated 

tag = -i
ifdef linux
tag = -n
endif

gtest_mac:  clean test_mac
	clang++ -std=c++11 -stdlib=libc++ DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread

gtest_linux: clean test.out
	g++ -std=c++11 DBFile_test.cc -o  dbfile_test.o -ll -lgtest -lpthread

main_mac:   y.tab_mac.o lex.yy.o Record.o Comparison.o Schema.o Statistics.o Function.o TreeNode.o main.o
	$(CC) -o main y.tab.o lex.yy.o Record.o Comparison.o Schema.o Statistics.o Function.o TreeNode.o main.o -ll

main:   y.tab.o lex.yy.o Record.o Comparison.o Schema.o Statistics.o Function.o TreeNode.o y.tab.o yyfunc.tab.o lex.yyfunc.o main.o
	$(CC) -o main y.tab.o lex.yy.o Record.o Comparison.o Schema.o Statistics.o Function.o TreeNode.o yyfunc.tab.o lex.yyfunc.o main.o -lfl

test_mac: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o Statistics.o y.tab_mac.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o Statistics.o y.tab.o lex.yy.o test.o -ll

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o Statistics.o y.tab.o lex.yy.o test.o -lfl -lpthread

test_relops_mac: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab_mac.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p3.o
	$(CC) -o test_relops.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p3.o -ll -lpthread

test_relops: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p3.o
	$(CC) -o test_relops.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test_p3.o -lfl -lpthread

test_sorted_mac: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab_mac.o lex.yy.o test_p22.o
	$(CC) -o test_sorted.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p22.o -ll -lpthread -fsanitize=address

test_sorted: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test_p22.o
	$(CC) -o test_sorted.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test_p22.o -lfl -lpthread 

test_bigq_mac: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab_mac.o lex.yy.o test_p21.o
	$(CC) -o test_bigq.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p21.o -ll -lpthread -fsanitize=address

test_bigq: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p21.o
	$(CC) -o test_bigq.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p21.o -lfl -lpthread 

test_dbfile_mac: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab_mac.o lex.yy.o test_p1.o
	$(CC) -o test_dbfile.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test_p1.o -ll -lpthread

test_dbfile: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o DBFile.o test_p1.o BigQ.o
	$(CC) -o test_dbfile.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o BigQ.o test_p1.o -lfl -lpthread

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

main.o: main.cc
	$(CC) -g -c main.cc
test.o: test.cc
	$(CC) -g -c test.cc

test_p1.o: test_p1.cc
	$(CC) -g -c test_p1.cc

test_p3.o: test_p3.cc
	$(CC) -g -c test_p3.cc

test_p21.o: test_p21.cc
	$(CC) -g -c test_p21.cc

test_p22.o: test_p22.cc
	$(CC) -g -c test_p22.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc
	
RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

TreeNode.o: TreeNode.cc
	$(CC) -g -c TreeNode.cc
	
y.tab_mac.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
		
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c
	
lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean: 
	rm -f main
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
