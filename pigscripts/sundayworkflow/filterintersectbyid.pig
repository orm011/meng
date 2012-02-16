intersections = load 'query_log_views/intersection_popularity_per_id_pair_ordered_by_popularity' as (args:(leftarg:int, rightarg:int), counted:int);

-- prefer to filter out ids not present in my snapshot, and edges on the same id (as they have no meaning in the new model)
filteredbyid = filter intersections by args.leftarg <= 187083288  and args.rightarg <= 187083288 and not (args.leftarg == args.rightarg);
flatter_rep = foreach filteredbyid generate args.leftarg as leftarg, args.rightarg as rightarg, counted;
symmetric_edges = distinct (union flatter_rep, (foreach flatter_rep generate rightarg, leftarg, counted));
group_id_and_weights = group (foreach symmetric_edges generate leftarg, counted) by leftarg;
id_total = foreach group_id_and_weights generate group, SUM($1.$1);

store symmetric_edges into 'query_log_views/query_log_graph_intersection_edges';
store id_total into 'query_log_views/query_log_graph_intersection_contribution_to_vertex_weight';
