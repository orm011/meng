--arguments $INPUT: the edge list input, $OUTPUT: the keyword for output files
REGISTER /home/oscarm/pig/sundayworkflow/test.jar;
forward = load '$INPUT' using PigStorage(' ') as (leftid:int, rightid:int);
exceptions = load 'metis_outputs/translated_partition_table' as  
	(part2:int, part4:int, part5:int, part8:int, part16:int, densevertexid:int, vertexid:int);

--need to change this if change #nodes
exceptions = foreach exceptions generate vertexid, part5;

joint = join forward by leftid LEFT OUTER, exceptions by vertexid;
split joint into edge_cases if (exceptions::vertexid is not null), common_case if (exceptions::vertexid is null);

--common case is by vertex id. so we save work by doing it only once per fanout. node: 1 shard guarantees the second argument does not matter.
mapped_common = foreach (group common_case by forward::leftid) generate com.twitter.dataservice.sharding.VERTEXHASHSHARD(group, 0, $NUMNODES, 1) as node, group as leftid, common_case.rightid as rightids;
mapped_common = foreach mapped_common generate node, leftid, FLATTEN(rightids) as rightid;

--special case is with different hash parameters OR via lookup table.
mapped_special = foreach edge_cases generate part5 as node, leftid, rightid;

---done, now reunite
reunited = union mapped_common, mapped_special;

mapped_all =  foreach (group reunited by (node, leftid)) {
	o = order reunited by rightid;
	generate group, o.rightid;
};

DEFINE store_formatted(part, outputkeyword) RETURNS void {
	a = stream (foreach $part generate group.leftid, $1) through `sed 's/[{|}|(|)]//g' | sed 's/,/\t/g'`;
	store a into '$outputkeyword$part';
}

split mapped_all into part0 if group.node == 0, part1 if group.node == 1, part2 if group.node == 2, part3 if group.node == 3, part4 if group.node == 4, part5 if group.node == 5, part6 if group.node == 6, part7 if group.node == 7;


store_formatted(part0, $OUTPUT);
store_formatted(part1, $OUTPUT);
store_formatted(part2, $OUTPUT);
store_formatted(part3, $OUTPUT);
store_formatted(part4, $OUTPUT);
store_formatted(part5, $OUTPUT);
store_formatted(part6, $OUTPUT);
store_formatted(part7, $OUTPUT);