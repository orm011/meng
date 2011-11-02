dist-test: build
	. start.sh
build:
	javac -d ./bin/ -cp ./lib/commons-math-2.2/commons-math-2.2.jar:./lib/jfreechart-0.5.6/jfreechart.jar src/com/twitter/dataservice/*/*
clean:
	. cleanps.sh
	rm -rf ./bin/*

build-unit: build
	javac -d ./bin/ -cp ./bin/:./lib/commons-math-2.2/commons-math-2.2.jar:./lib/junit-4.10/junit-4.10.jar:.//lib/jfreechart-0.5.6/jfreechart.jar /test/com/twitter/dataservice/*/*

unit-test: build
	echo 'use eclipse?'
	java -cp ./bin/:./lib/junit-4.10.jar org.junit.runner.JUnitCore com.twitter.dataservice.sharding.RoundRobinShardLibTest

lib: /lib
	. ./lib/buildlib.sh

remote: build
	jar -cf ./bin/remotes.jar ./bin/src/com/twitter/dataservice/remotes/*.class