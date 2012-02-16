parts = load 'graph_partitions_from_metis5' as (part:int);
numbered_parts = stream parts through `nl` as (index:int, part:int);

