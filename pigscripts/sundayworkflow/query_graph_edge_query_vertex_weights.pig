edgequery = load 'query_log_views/edge_query_distribution_on_pairs_sorted_by_popularity' as (leftid:int, rightid:int, total:int);
filteredbyid = filter edgequery by leftid <= 187083288  and rightid <= 187083288 and not (leftid == rightid);
symmetric_edges = distinct (union filteredbyid, (foreach filteredbyid generate rightid, leftid, total));
group_id_and_weights = group (foreach symmetric_edges generate leftid, total) by leftid;
id_total = foreach group_id_and_weights generate group, SUM($1.$1);

store id_total into 'query_log_views/query_log_graph_edge_contribution_to_vertex_weight';