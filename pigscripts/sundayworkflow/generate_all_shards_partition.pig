REGISTER /home/oscarm/pig/sundayworkflow/test.jar;
forward = load '$INPUT' using PigStorage(' ') as (leftid:int, rightid:int);

mapped = foreach forward generate com.twitter.dataservice.sharding.VERTEXHASHSHARD(leftid, rightid, $NUMNODES, $NUMNODES) as node, leftid, rightid;

mapped_all =  foreach (group mapped by (node, leftid)) {
	o = order mapped by rightid;
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