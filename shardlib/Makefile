build:
	javac -d ./bin/  -cp ./lib/commons-math-2.2.jar:./lib/commons-math-2.2.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar  src/com/twitter/dataservice/*/*.java
clean:
	rm -rf ./bin/
deploy:
	scp -r  ./bin Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agn-11-sr1:~
	scp -r -q ./bin Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agm-15-sr1:~
	scp -r -q ./bin Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agn-29-sr1:~
	scp -r -q ./bin Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agq-31-sr1:~
	scp -r -q ./bin Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agt-31-sr1:~
	scp -r -q ./bin Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agm-35-sr1:~

deploy-conf:
	scp -r Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agn-11-sr1:~
	scp -r -q Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agm-15-sr1:~
	scp -r -q Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agn-29-sr1:~
	scp -r -q Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agq-31-sr1:~
	scp -r -q Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agt-31-sr1:~
	scp -r -q Benchmark.properties log4j.properties server.policy runmany.sh Makefile smf1-agm-35-sr1:~

#machines smf1-agn-11-sr1 smf1-agm-15-sr1 smf1-agn-29-sr1 smf1-agq-31-sr1 smf1-agt-31-sr1 smf1-agm-35-sr1
#java -cp ./bin/  -Djava.security.policy=server.policy -Djava.rmi.server.codebase=file:./bin/ com.twitter.dataservice.simulated.WorkNodeMain 'node0' &
#java -cp ./bin/:./lib/commons-math-2.2.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar  -Djava.security.policy=server.policy -Djava.rmi.server.codebase=file:./bin/ com.twitter.dataservice.simulated.Benchmark log4j.properties Benchmark.properties



