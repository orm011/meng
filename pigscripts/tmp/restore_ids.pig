parts = load 'numbered_graph_partitions5.graph.part' as (denseid:int, part:int);
ids = load 'vertex_id_table' as (newid:int, oldid:int);
partition_table = foreach (join parts by denseid, ids by newid USING 'merge') generate oldid, part;
store partition_table into 'partitions_with_original_id5.graph.part';