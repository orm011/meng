partmap = load 'partitions_with_original_id5.graph.part' as (vertexid:int, partid:int);
forwards = load 'forward_edges.txt' using PigStorage(' ') as (nodea:int, nodeb:int);

backforth = union forwards, (foreach forwards generate nodeb, nodea);
parts = join backforth by nodea FULL OUTER, partmap by vertexid;
split parts into missing_parts if (nodea is null OR vertexid is null), matched_all if (nodea is not null AND vertexid is not null);

redux = foreach matched_all generate partid, nodea, nodeb;
parts = foreach (group redux by partid) generate group, COUNT($1);

store missing_parts into 'foo/missing_parts';
store parts into 'foo/back-forth-counts';

/*
split joined_parts into node0 if (partid == 0), node1 if (partid == 1), node2 if (partid == 2), node3 if (partid == 3), node4 if (partid == 4);

store node0 into '$KEYWORD/node0';
store node1 into '$KEYWORD/node1';
store node2 into '$KEYWORD/node2';
store node3 into '$KEYWORD/node3';
store node4 into '$KEYWORD/node4';
*/