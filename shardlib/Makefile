dist-test: build
	. start.sh
build:
	javac -d ./bin/ -cp ./lib/commons-math-2.2/commons-math-2.2.jar src/com/twitter/dataservice/*/*
clean:
	. cleanps.sh
	rm -rf ./bin/*

build-unit: build
	javac -d ./bin/ -cp ./bin/:./lib/commons-math-2.2/commons-math-2.2.jar:./lib/junit-4.10.jar test/com/twitter/dataservice/*/*

unit-test: build
	echo 'use eclipse?'
	java -cp ./bin/:./lib/junit-4.10.jar org.junit.runner.JUnitCore com.twitter.dataservice.sharding.RoundRobinShardLibTest

lib: /lib
	. buildlib.sh