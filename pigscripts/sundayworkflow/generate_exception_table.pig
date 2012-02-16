--arguments $INPUT: the edge list input, $OUTPUT: the keyword for output files
degreetab =  load 'graphviews/graph_degree_table_ordered_by_id' as (denseid:int, realid:int, indeg:int, outdeg:int, total:int);
parts = load 'metis_outputs/translated_partition_table' as  (part2:int, part4:int, part5:int, part8:int, part16:int, vertexid:int, realid:int);

--need to change this if change #nodes
exceptions = foreach parts generate realid, part5;
degreetab = foreach degreetab generate realid;

filtered = foreach (join exceptions by realid, degreetab by realid) generate $0, part5;

store filtered into 'metis_outputs/filtered_exception_table_5parts';


