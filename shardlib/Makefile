#note: farm1-4 have 2 processors only (vs 4) farm10 has 3GB ram, vs 2 normally. other machines cocoa vise
#farm5 farm6 farm7 farm8 farm9 farm11 farm12 farm13 farm14 farm10
machines :=  farm1 farm2 farm3 farm4 farm5 farm6 farm7 farm8 farm9 farm11 farm12 farm13 farm14 farm10

destination_folder := $(addsuffix .csail.mit.edu:~/thesis, $(machines))

configuration := benchmark.properties log4j.properties server.policy *.sh

jars := commons-math3-3.0 commons-lang-2.6 junit-4.10 log4j-1.2.16 slf4j-api-1.6.4 slf4j-log4j12-1.6.4 guava-11.0.1 pig-0.9.2 commons-logging-commons-logging-1.1.1 hadoop-core-1.0.0 fastutil-6.4.3

classpath := $(shell echo $(addprefix ./lib/, $(addsuffix .jar, $(jars))) | sed 's/ /:/g')

build: 
	javac -d ./bin/ -cp $(classpath) src/com/twitter/dataservice/*/*.java 

clean:
	rm -rf ./bin/

deploy-lib:
	$(foreach dest, $(addsuffix /lib/,$(destination_folder)), scp -r ./lib/*.jar $(dest);)

deploy-all:
	$(foreach dest, $(destination_folder), scp -r -q ./bin $(configuration) $(dest);)

deploy-conf:
	$(foreach dest, $(destination_folder), scp -r -q $(configuration) $(dest);)