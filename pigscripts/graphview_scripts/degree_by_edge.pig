edges = load '$EDGES' using PigStorage(' ') as (nodea:int, nodeb:int);
degrees = foreach (group edges by nodea) generate *, COUNT($1) as degree;
histo = foreach (group degrees by FLOOR(degree/$BIN)*$BIN) generate *, SUM($1.degree) as total;
ordered_histo = order (foreach histo generate group, total) by group;

cumulative = stream ordered_histo through `awk -F '\t' '{total2 += $2; print $1,$2,total2}'`;
store cumulative into '$EDGES-cumulative-output/degree_by_edge_histogram';