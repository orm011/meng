#!/bin/bash

CP="./bin/:./lib/commons-math-2.2.jar:./lib/commons-lang-2.6.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar:./lib/guava-11.0.1.jar"

FLAGS='-Djava.security.policy=server.policy -Djava.rmi.server.codebase=file:./bin/'

java -cp $CP $FLAGS com.twitter.dataservice.simulated.Benchmark $1 $2