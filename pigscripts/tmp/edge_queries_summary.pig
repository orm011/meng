logs = load 'flock-request-logs' using PigStorage(' ') as (logpos:int, operation:chararray, nodea:int, nodeb:int);
edgeq = foreach (filter logs by (operation == 'Edge')) generate nodea, nodeb;
popularity = order (foreach (group edgeq by (nodea,nodeb)) generate group.nodea as nodea, group.nodeb as nodeb, COUNT(edgeq) as counted) by counted;
store popularity into 'popular-edge-queries';

idsqueried  = foreach (group (union edgeq, (foreach edgeq generate nodeb as nodea, nodea as nodeb)) by nodea) generate group, COUNT($1) as counted;
popularity_individual = order idsqueried by counted;
store popularity_individual into 'ids-in-edge-queries';

counted = foreach (group idsqueried all) generate COUNT(idsqueried);
store counted into 'ids-in-edge-queries-count';