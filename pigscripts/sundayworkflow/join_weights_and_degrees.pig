weight_table = load 'metis_inputs/joint_vertex_weights_table_sorted_id' as (denseid:int, sparseid:int, edgew:int, interw:int, fanoutw:int, total1:int, total2:int);
degree_table = load 'graphviews/graph_degree_table_ordered_by_id' as (numbered:int, sparseid:int, indeg:int, outdeg:int, total:int);

weight_table = foreach weight_table generate denseid, sparseid, edgew, interw, fanoutw, interw + fanoutw as weight;
degree_table = foreach degree_table generate sparseid, indeg, outdeg, total;

degreeweight =  foreach (join weight_table by sparseid LEFT OUTER, degree_table by sparseid) generate denseid, weight_table::sparseid as sparseid, edgew, interw, fanoutw, weight, ((indeg is null)?0:indeg), ((outdeg is null)?0:outdeg), ((total is null)?0:total);

--store degreeweight into 'stats/joint_degree_and_weight';


--- added this new part to get the the mapping from realid, partition. move elsewhere.
partitions = load 'metis_outputs/joint_partition_file' as (id:int, part2:int, part4:int, part5:int, part8:int, part16:int);
joint = join degreeweight by denseid, partitions by id;
joint =  order joint by denseid;

--- can preprocess to put all stuff in the same part together
--- still need to join this with the forward edges file, in order to get each part separately.
store degreeweight into 'stats/joint_degree_and_weight_including_sparseid';
store joint into 'metis_outputs/sparse_vertexid_vs_partid_ordered_by_vertexid';

/*
partmap = foreach joint generate denseid, part8;
edges = load 'forward_edges.txt' using PigStorage(' ') as (leftid:int, rightid:int);
forward_edges = foreach edges generate leftid, rightid, 1;
back_edges = foreach edges generate rightid, leftid, 0;
all_edges = union edges, back_edges;
partitioned = join partmap by denseid, all_edges by leftid;


--at this point need to use the 'official hash' so that every vertex gets it hash and gets loaded in parallel
order partitioned by part8
*/