exceptions_denseid = load 'metis_outputs/joint_partition_file' as (denseid:int, part2:int, part4:int, part5:int, part8:int, part16:int);
idtable = load 'metis_inputs/joint_vertex_weights_table_sorted_id';
idtable = foreach idtable generate (int)$0 as denseid, (int)$1 as realid;
--idtable here is the translation table,not the id table vs. degree but id table vs workload weight.

translated_exceptions = foreach (join exceptions_denseid by denseid, idtable by denseid) generate $1..$7;
store translated_exceptions into 'metis_outputs/translated_partition_table';