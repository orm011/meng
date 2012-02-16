fanouts = load 'query_log_views/fanout_queries' as (stamp:int, oper:chararray, page:int, offset:int, vertexid:int);
idtable = load 'graphviews/graph_degree_table_ordered_by_id' as (denseid:int, realid:int, indeg:int, outdeg:int, total:int);  

filtered = foreach (join fanouts by vertexid, idtable by realid) generate $0..$4;
store filtered into 'query_log_views/filtered_fanout_queries';