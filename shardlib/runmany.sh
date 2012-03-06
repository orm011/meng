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

VAR1="system.numDataNodes"
VAR1VALS="5 1"
echo $VAR1: $VAR1VALS

VAR2="graph.degreeSkew"
VAR2VALS="0.1 1.0 3.0"
echo $VAR2: $VAR2VALS

LOGPROP="log4j.temp.properties"
BENCHPROP="Benchmark.temp.properties"

cp log4j.properties $LOGPROP
cp Benchmark.properties $BENCHPROP

for d in $VAR1VALS
do
echo "now doing $VAR1=$d"
sed "s/$VAR1=.*/$VAR1=$d/" Benchmark.properties > $BENCHPROP

for p in $VAR2VALS
do
echo "with $VAR2=$p"
#must do sed with replacement to not lose both changes
sed -i.sedtmp "s/$VAR2=.*/$VAR2=$p/" $BENCHPROP
sed "s/log4j.appender.R.File=logs\/saved.log/log4j.appender.R.File=logs\/$KEYWORD\/$VAR1-$d\/$KEYWORD-$VAR1-$d-$VAR2-$p.log/" log4j.properties > $LOGPROP
cat $BENCHPROP | grep -v "^#"
cat $LOGPROP | grep 'R.File'
time java -cp $CP $FLAGS com.twitter.dataservice.simulated.Benchmark $LOGPROP $BENCHPROP
echo '--------------'
done

done

