/* script to verify the metis output is balanced with respect to weight 
inputs: a partition file with numbered lines, the degree table.
outputs: a partition weight count inlucing per part stats. like #ets, sum weights, avg
*/

outparts = load '$PARTITIONRESULT' as (id:int, partno:int);
degreetable = load 'graphviews/graph_degree_table_ordered_by_id' as (denseid:int, sparseid:int, indeg:int, outdeg:int, total:int);
merged = join outparts by id FULL OUTER, degreetable by denseid;
partsizes = foreach (group merged by partno) generate group as partno, COUNT(merged), SUM(merged.total), AVG(merged.total), MAX(merged.total);
partsizes = order partsizes by partno;

store partsizes into '$PREFIX-part-parameters';