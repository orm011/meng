degrees = load 'translated_degree_table' as (vertexid:int, degree:int);
parts = load 'numbered_graph_partitions5.graph.part' as (newid:int, partnum:int);
joint = join degrees by vertexid FULL, parts by newid;
split joint into missing if (newid is null OR vertexid is null), complete if (newid is not null AND vertexid is not null);

degree_counts = foreach (group complete by partnum) generate group, COUNT(complete), SUM(complete.degree);
store degree_counts into '$NAME/partition_degree_weights';
store missing into '$NAME/missing_degree_weights';