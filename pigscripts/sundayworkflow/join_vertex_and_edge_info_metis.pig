edges = load 'query_log_views/query_log_graph_intersection_edges' as (leftid:int, rightid:int, edgew:int);
--leftid and rightid are plain old ids

vertices = load 'metis_inputs/joint_vertex_weights_table_sorted_id' as (denseid:int, sparseid:int, edgew:int, interw:int, fanoutw:int, total1:int, total2:int);
--here dense id is simply the position on the weight table. NOT the the denseid in the graph degree table.
--sparse id is the plain old id

--trying to keep weights small
projectedvertex = foreach vertices generate denseid, sparseid, interw + fanoutw as vertexw;

--translate edgetable to dense id
joint_left = join edges by leftid, projectedvertex by sparseid;
clean_left = foreach joint_left generate denseid as newleft, rightid, edgew;
joint_right = join clean_left by rightid, projectedvertex by sparseid;
translated = foreach joint_right generate newleft, denseid as newright, edgew;

grouped_edges = foreach (group translated by newleft) generate group as id, $1.(newright, edgew) as edgelist;
finaljoin = join projectedvertex by denseid LEFT OUTER, grouped_edges by id;
cleanedjoin = foreach (order (foreach finaljoin generate denseid, vertexw, edgelist) by denseid) generate vertexw, edgelist;
final = stream cleanedjoin through `sed 's/[{|}|(|)]//g; s/,/ /g'`;

--this is in the same order as denseid
store final into 'metis_inputs/graph_list_query_weight' using PigStorage(' ');

