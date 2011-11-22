#!/bin/bash
#discarding the last 3 digits (nanotime seems to have less than microsecond accuracy)
#taking only 11 digits from the rest (enough to distinguish the instants in a day 10^6 musec/sec* 10^5 sec/ day)
#issue: octave has issues reading big numbers, it does not treat them as numbers if they go beyond a certain size

# for log format: $37, now much less after removing prefix
# argument is the number of rows written the last time

OUTPUTFILE='intermediatedata.csv'
METRICSMARKER='MetricsCollector'
ENVMARKER='WorkloadParams'

#TODO: figure out way of including all plot details
TITLE=$(cat example.log | grep $ENVMARKER | tail -n 1 | cut -d ' ' -f 15-30)
echo $TITLE


if [[ $1 == --latency ]]
then
cat example.log | grep $METRICSMARKER | tail -n $2  | awk '{print $7}'  > $OUTPUTFILE;
octave --silent plot_latency.m $OUTPUTFILE "$TITLE"
else
if [[ $1 == --throughput ]]
then
tail -n $2 example.log | awk '{print $7}' | cut -c 8-16  > $OUTPUTFILE;
octave --silent plot_throughput.m $OUTPUTFILE "$TITLE"
else
echo "usage: $0 --[latency|throughput] [num. lines]"
fi
fi