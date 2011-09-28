build:
	javac -d ./bin/ -cp ./lib/junit-4.9b1.jar src/com/twitter/dataservice/*/*

clean:
	. cleanps.sh
	rm -rf ./bin/*

unit-test: build
	java -cp ./bin/:./lib/junit-4.9b1.jar org.junit.runner.JUnitCore com.twitter.dataservice.shardlib.DirectHashTest

dist-test: build
	. start.sh
