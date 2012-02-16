adjacency_list = load 'TMP__adjacency_list' using PigStorage(' ') as (id:int, weight:int);
degreetable  = load 'graphviews/graph_degree_table_ordered_by_id' as (denseid:int, sparseid:int, indeg:int, outdeg:int, total:int);

jointids = foreach (join adjacency_list by id FULL OUTER, degreetable by denseid) generate id, weight, denseid, total;
discrepancy = union (filter jointids by (id is null)), (filter jointids by (denseid is null)), (filter jointids by NOT(total == (int)CEIL(weight/5.0)));

store discrepancy into 'check_ordering_and_degree_sored_intcast.tmp';