build:
	javac -d ./bin/  -cp ./lib/commons-math-2.2.jar:./lib/commons-lang-2.6.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar:./lib/guava-11.0.1.jar:./lib/pig-0.9.2.jar:./lib/commons-logging-commons-logging-1.1.1.jar:./lib/hadoop-core-1.0.0.jar src/com/twitter/dataservice/*/*.java

clean:
	rm -rf ./bin/

deploy-lib:
	scp ./lib/*.jar orm@cocoa.csail.mit.edu:~/thesis/lib

deploy-all:
	scp -r ./bin Benchmark.properties *og4j.properties server.policy runmany.sh Makefile orm@cocoa.csail.mit.edu:~/thesis
#scp -r -q (q for no prints)

deploy-benchmark:
	scp -r  ./bin orm@cocoa.csail.mit.edu:~/thesis/

deploy-conf:
	scp -r Benchmark.properties *og4j.properties server.policy runmany.sh Makefile orm@cocoa.csail.mit.edu:~/thesis/

#example work node command:
#java -cp ./bin/:./lib/commons-math-2.2.jar:./lib/commons-lang-2.6.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar:./lib/guava-11.0.1.jar -Djava.security.policy=server.policy -Djava.rmi.server.codebase=file:./bin/ com.twitter.dataservice.simulated.WorkNodeMain node0-lookuptablehashbackwardpart0 &

#server:
#java -cp ./bin/:./lib/commons-math-2.2.jar:./lib/commons-lang-2.6.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar:./lib/guava-11.0.1.jar -Djava.security.policy=server.policy -Djava.rmi.server.codebase=file:./bin/ com.twitter.dataservice.simulated.Benchmark log4j.properties Benchmark.properties