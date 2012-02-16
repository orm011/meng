--arguments $INPUT: the edge list input, $OUTPUT: the keyword for output files
REGISTER /home/oscarm/pig/sundayworkflow/test.jar;
forward = load '$INPUT' using PigStorage(' ') as (leftid:int, rightid:int);
exceptions0 = load 'metis_outputs/translated_partition_table' as  
	(part2:int, part4:int, part5:int, part8:int, part16:int, vertexid:int, realid:int);

--need to change this if change #nodes
exceptions = foreach exceptions0 generate vertexid, part5;

joint = join forward by leftid LEFT OUTER, exceptions by vertexid;
split joint into edge_cases if (exceptions::vertexid is not null), common_case if (exceptions::vertexid is null);

--common case is by vertex id. so we save work by doing it only once per fanout. node: 1 shard guarantees the second argument does not matter.
mapped_common = foreach (group common_case by forward::leftid) generate com.twitter.dataservice.sharding.VERTEXHASHSHARD(group, 0, $NUMNODES, 1) as node, group as leftid, common_case.rightid as rightids;
mapped_common = foreach mapped_common generate node, leftid, FLATTEN(rightids) as rightid;

--special case is with different hash parameters OR via lookup table.
mapped_special = foreach edge_cases generate part5 as node, leftid, rightid;

---done, now reunite
reunited = union mapped_common, mapped_special;

mapped_all0 =  foreach (group reunited by (node, leftid)) {
	o = order reunited by rightid;
	generate group, o.rightid;
};

mapped_all = foreach mapped_all0 generate $0.node as node, $0.leftid as leftid;
checktable = join mapped_all by leftid, exceptions by vertexid;
store checktable into 'debugging/checklookuptable';