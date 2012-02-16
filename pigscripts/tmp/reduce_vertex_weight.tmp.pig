list = load 'numbered_query_graph_adjacency_list' using PigStorage(' ');
divided = foreach list generate $0, (int)CEIL($1/5.0), $2 ..;
better_divided = foreach divided generate $1 ..;
store divided into 'reweighted_adjacency_list' using  PigStorage(' ');
store better_divided into 'TMP_reweighted_adjacency_list' using PigStorage(' ');

