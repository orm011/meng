idtable = load 'graphviews/graph_degree_table_ordered_by_id' as (denseid:int, realid:int, indeg:int, outdeg:int, total:int);
ordered = order (foreach idtable generate realid, total) by total DESC;
store ordered into 'graphviews/vertex_ids_ordered_by_total_degree';

