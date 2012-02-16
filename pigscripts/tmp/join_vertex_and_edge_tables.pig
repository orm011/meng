vertex_degrees = load 'translated_degree_table' as (vertexid:int, degree:int);
id_table = load 'vertex_id_table' as (newid:int, oldid:int);
edge_table = load 'translated-edges' as (nodea:int, nodeb:int, edgew:int);

symmetric_edge_table = filter (union edge_table, (foreach edge_table generate nodeb as nodea, nodea as nodeb, edgew)) by NOT(nodea == nodeb);
grouped_edges = foreach (group symmetric_edge_table by nodea) generate group as source, symmetric_edge_table.(nodeb, edgew) as destinations;
all_weights = foreach (join vertex_degrees by vertexid FULL, grouped_edges by source) generate vertexid, degree, destinations;

split all_weights into missing_source_weight if (vertexid is null), vertex_list if NOT(vertexid is null);

num_list_edges = foreach (group (foreach vertex_list generate (($2 is null)?0:COUNT($2))) all) generate SUM($1);
num_list_vertices = foreach (group vertex_list all) generate COUNT(vertex_list);
store num_list_edges into 'num_list_edges';
store num_list_vertices into 'num_list_vertices';

cleaned_symbols = stream vertex_list through `sed 's/[{}()]//g; s/,/ /g'` as (vertex:int, degree:int, edges:chararray);
adjacency_list = foreach (order cleaned_symbols by vertex) generate degree, edges;

store adjacency_list into 'adjacency_list' using PigStorage(' ');
store missing_source_weight into 'missing_source_weight';