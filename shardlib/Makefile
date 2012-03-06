machines := cocoa vise5

destination_folder := $(addsuffix .csail.mit.edu:~/thesis, $(machines))

configuration := Benchmark.properties log4j.properties dataNodelog4j.properties server.policy *.sh

jars := commons-math-2.2 commons-lang-2.6 junit-4.10 log4j-1.2.16 slf4j-api-1.6.4 slf4j-log4j12-1.6.4 guava-11.0.1 pig-0.9.2 commons-logging-commons-logging-1.1.1 hadoop-core-1.0.0

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