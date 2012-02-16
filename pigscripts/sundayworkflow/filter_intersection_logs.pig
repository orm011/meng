intersections = load 'query_log_views/intersection_queries' as (stamp:int, op:chararray, page:int, offset:long, leftid:int, rightid:int);
idtable = load 'graphviews/graph_degree_table_ordered_by_id' as (denseid:int, realid:int, indeg:int, outdeg:int, total:int);
filtered_left = foreach (join intersections by leftid, idtable by realid) generate $0..$5;
filtered_all = foreach (join filtered_left by rightid, idtable by realid) generate $0..$5;

store filtered_all into 'query_log_views/filtered_intersection_queries2';