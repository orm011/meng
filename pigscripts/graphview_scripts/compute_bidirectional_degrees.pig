forward = load 'forward_edges.txt' using PigStorage(' ') as (nodea:int, nodeb:int);
lefte = foreach (group forward by nodea) generate group as id, COUNT($1) as leftcount;
righte = foreach (group forward by nodeb) generate group as id, COUNT($1) as rightcount;

mixed = join lefte by id FULL OUTER, righte by id;
summix = foreach mixed generate ((lefte::id is null)?righte::id:lefte::id) as id, ((leftcount is null)?0:leftcount) as fordegree, ((rightcount is null)?0:rightcount) as backdegree;
summix = foreach summix generate *, fordegree + backdegree;
summix = stream (order summix by id) through `nl -n ln`;

store summix into 'graph_degree_table_orderedbyid-2';