build:
	javac -d ./bin/  -cp ./lib/commons-math-2.2.jar:./lib/commons-math-2.2.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar  src/com/twitter/dataservice/*/*.java
clean:
	rm -rf ./bin/
deploy:
	scp -r ./bin Benchmark.properties log4j.properties server.policy Makefile oscarm@smf1-agm-15-sr1:~/
	scp -r ./bin Benchmark.properties log4j.properties server.policy Makefile oscarm@smf1-agm-35-sr1:~/
	scp -r ./bin Benchmark.properties log4j.properties server.policy Makefile oscarm@smf1-agn-11-sr1:~/

#java -cp ./bin/  -Djava.security.policy=server.policy -Djava.rmi.server.codebase=file:./bin/ com.twitter.dataservice.simulated.WorkNodeMain 'node1' &
#java -cp ./bin/:./lib/commons-math-2.2.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar  -Djava.security.policy=server.policy -Djava.rmi.server.codebase=file:/Users/oscarm/workspace/oscarmeng/shardlib/bin/ com.twitter.dataservice.simulated.APIServer node1 localhost 1099
