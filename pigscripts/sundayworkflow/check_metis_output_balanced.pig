degreeweight  = load 'stats/joint_degree_and_weight' as (denseid:int, edgew:int, interw:int, fanoutw:int, weight:int, indeg:int, outdeg:int, totaldeg:int);
partitions = load 'metis_outputs/joint_partition_file' as (id:int, part2:int, part4:int, part5:int, part8:int, part16:int);

joint = join degreeweight by denseid, partitions by id;
store joint into 'debugging/joint_table2';

part2total = foreach (group joint by part2) generate group, COUNT($1), SUM($1.weight), SUM($1.totaldeg);
part4total = foreach (group joint by part4) generate group, COUNT($1), SUM($1.weight), SUM($1.totaldeg);
part5total = foreach (group joint by part5) generate group, COUNT($1), SUM($1.weight), SUM($1.totaldeg);
part8total = foreach (group joint by part8) generate group, COUNT($1), SUM($1.weight), SUM($1.totaldeg);
part16total = foreach (group joint by part16) generate group, COUNT($1), SUM($1.weight), SUM($1.totaldeg);

store part2total into 'metis_outputs/part2total';
store part4total into 'metis_outputs/part4total';
store part5total into 'metis_outputs/part5total';
store part8total into 'metis_outputs/part8total';
store part16total into 'metis_outputs/part16total';

