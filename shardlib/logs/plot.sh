#!/bin/bash
#discarding the last 3 digits (nanotime seems to have less than microsecond accuracy)
#taking only 11 digits from the rest (enough to distinguish the instants in a day 10^6 musec/sec* 10^5 sec/ day)
#issue: octave has issues reading big numbers, it does not treat them as numbers if they go beyond a certain size

# for log format: $37, now much less after removing prefix
# argument is the number of rows written the last time

#NOTE: $2 is the number of lines of the log to read, ie number of vertices to put in the histogram or number of queries
# $3 is maxX, $4 is maxY
#example:
#1    [main] DEBUG parameters.WorkloadParams  2011-11-26 23:35:07,787 SystemParameters edgePayload 50 workNodes 1 
#2    [main] DEBUG parameters.WorkloadParams  2011-11-26 23:35:07,788 GraphParameters numberVertices 10 numberEdges 30 actualNumberEdges 30 maxDegree 10 actualMaxDegree 5 degreeSkew 1.0 
#3    [main] DEBUG parameters.WorkloadParams  2011-11-26 23:35:07,789 WorkloadParams numberOfQueries 10 querySkew 0.0010 skewCorrelationWithGraph 0.0 percentEdge 0 percentVertex 100 percentIntersection 0 
#3    [main] DEBUG simulated.Graph  2011-11-26 23:35:07,789 1
#18   [main] DEBUG simulated.MetricsCollector  2011-11-26 23:35:07,804 148000

if [[ $# < 1 ]]
then
   echo "usage $0 --[latency|throughput|degree] NumberOfRowsToRead maxX maxY fileName"
   exit 0
fi

MAXX=$3
MAXY=$4
FILE=$5

SYSTEMMARKER='SystemParameters'
GRAPHMARKER='GraphParameters'
WORKLOADMARKER='WorkloadParameters'
GRAPHDATA='simulated.Graph'
QUERYDATA='simulated.MetricsCollector'

#TODO: change octave to read from stdin for everything, remove intermediate file

#NOTE: will have to change this according to Benchmark.java

#TODO: make into function
SYSTEM=$(cat $FILE | grep $SYSTEMMARKER | tail -n 1 | cut -d ' ' -f 6-)
GRAPH=$(cat $FILE | grep $GRAPHMARKER | tail -n 1 | cut -d ' ' -f 6-)
WORKLOAD=$(cat $FILE | grep $WORKLOADMARKER | tail -n 1 | cut -d ' ' -f 6-)

echo $SYSTEM
echo $GRAPH
echo $WORKLOAD

if [[ $1 == --latency ]]
then
echo 'latencyplot'
cat $FILE | grep $QUERYDATA | awk '{print $7}' |\
octave --silent plot_latency.m "$SYSTEM" "$GRAPH" "$WORKLOAD" 'latency (nanos)' 'latency histogram' $MAXX $MAXY
else
if [[ $1 == --throughput ]]
then
#broken need to reimplement based on new format
TITLE=$(cat $FILE | grep $WORKLOADMARKER | tail -n 1 | cut -d ' ' -f 15-30)
#probably needs update
tail -n $2 $FILE | awk '{print $7}' | cut -c 8-16  > $OUTPUTFILE;
octave --silent plot_throughput.m $OUTPUTFILE "$TITLE"
else
if [[ $1 == --degree ]] 
then
echo 'degree plot'
cat $FILE | grep $GRAPHDATA | awk '{print $7}' |\
octave --silent plot_latency.m "$SYSTEM" "$GRAPH" "$WORKLOAD" 'degree' 'degree histogram' $MAXX $MAXY
fi 
fi
fi