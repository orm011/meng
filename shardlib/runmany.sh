#runs the benchmark many times, labels outputs with input KEYWORD for easy processing later
#TODO:make it print less
trap handler SIGINT

handler()
{
echo ''
exit 1
}

if [[ $# < 1 ]]
then
echo "usage: $0 logKeyword"
exit 1
fi

KEYWORD=$1
shift
CP='./bin/:./lib/commons-math-2.2.jar:./lib/junit-4.10.jar:./lib/log4j-1.2.16.jar:./lib/slf4j-api-1.6.4.jar:./lib/slf4j-log4j12-1.6.4.jar'
FLAGS='-Djava.security.policy=server.policy -Djava.rmi.server.codebase=file:./bin/'

#DEGREE="10000 30000 100000 300000 1000000"
DEGREE="10000 20000 40000 80000 160000 320000 640000 1280000"
PARALLEL="5 4 3 2 1"

LOGPROP="log4j.temp.properties"
BENCHPROP="Benchmark.temp.properties"

cp log4j.properties $LOGPROP
cp Benchmark.properties $BENCHPROP

echo 'using base properties:'
cat Benchmark.properties
cat log4j.temp.properties

echo $DEGREE
for d in $DEGREE
do
echo "now doing graph.averageDegree=$d"
sed "s/graph.averageDegree=.*/graph.averageDegree=$d/" Benchmark.properties > $BENCHPROP

for p in $PARALLEL
do
echo "with system.numDataNodes=$p"
#must do sed with replacement to not lose both changes
sed -i.sedtmp "s/system.numDataNodes=.*/system.numDataNodes=$p/" $BENCHPROP
sed "s/log4j.appender.R.File=logs\/saved.log/log4j.appender.R.File=logs\/$KEYWORD\/$KEYWORD-degree-$d-parallel-$p.log/" log4j.properties > $LOGPROP
(time java -cp $CP $FLAGS com.twitter.dataservice.simulated.Benchmark $LOGPROP $BENCHPROP) 2>> $KEYWORD.times 1>> $KEYWORD.out
done

done
