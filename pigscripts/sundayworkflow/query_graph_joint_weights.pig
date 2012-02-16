edgeq = load 'query_log_views/query_log_graph_edge_contribution_to_vertex_weight' as (id:int, edgecontrib:int);
fanoutq = load 'query_log_views/query_log_graph_fanout_contribution_to_vertex_weight' as (id:int, peek:int, neg:int, tiny:int, small:int, total:int);
intersectionq = load 'query_log_views/query_log_graph_intersection_contribution_to_vertex_weight' as (id:int, intercontrib:int);

selected_fanoutq = foreach fanoutq generate id, total as fanoutcontrib;
together0 = join edgeq by id FULL OUTER, intersectionq by id;
cleaned0 = foreach together0 generate ((edgeq::id is null)?intersectionq::id:edgeq::id) as id, ((edgecontrib is null)?0:edgecontrib) as edgecontrib, ((intercontrib is null)?0:intercontrib) as intercontrib;

together = join selected_fanoutq by id FULL OUTER, cleaned0 by id;
cleaned = foreach together generate ((cleaned0::id is null)?selected_fanoutq::id:cleaned0::id) as id, ((cleaned0::id is null)?0:edgecontrib) as edgew, ((cleaned0::id is null)?0:intercontrib) as interw, ((selected_fanoutq::id is null)?0:fanoutcontrib) as fanoutw;

totals = foreach cleaned generate *, edgew+interw+fanoutw, edgew+50*(interw + fanoutw);
orderedtotal = order totals by id;
streamed = stream totals through `nl -nln -s'\t' -w 1`;

store streamed into 'metis_inputs/joint_vertex_weights_table_sorted_id';