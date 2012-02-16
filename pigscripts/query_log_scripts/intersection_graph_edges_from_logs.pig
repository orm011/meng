LOGS = LOAD 'test-flock-logs' USING PigStorage(' ') AS (pos:int, operation:chararray, page:chararray, nodea:int, nodeb:int);  

#offsets and pages summary
INTERSECTIONS = ORDER (FOREACH (GROUP (FILTER LOGS BY $1 == 'SimpleQuery_SimpleQuery_Intersection') BY $2) GENERATE $0, COUNT($1)) BY $1;
SIMPLEQUERIES = ORDER (FOREACH (GROUP (FILTER LOGS BY $1 == 'SimpleQuery') BY $2) GENERATE $0, COUNT($1)) BY $1;
store INTERSECTIONS into 'intersections' USING PigStorage(',');
store SIMPLEQUERIES into 'simplequeries' USING PigStorage(',');



