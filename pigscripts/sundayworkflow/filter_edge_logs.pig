edges = load 'query_log_views/edge_queries' as (stamp:int, op:chararray, leftid:int, rightid:int);
forward_edges = load 'forward_edges.txt' as (leftid:int, rightid;int);

filtered_left = foreach (join edges by leftid, idtable by realid) generate $0..$3;
filtered_all = foreach (join filtered_left by rightid, idtable by realid) generate $0..$3;

store filtered_all into 'query_log_views/filtered_edge_queries2';