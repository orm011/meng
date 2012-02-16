intersectids =  load 'popular-ids-in-intersects' as (vertexid:int, counted:int);
simplequeryids = load ' popular-ids-in-fanouts' as (vertexid:int, counted:int);
edgequeryids = load 'ids-in-edge-queries' as (vertexid:int, counted:int);

forward = load 'forward_edges.txt' using PigStorage(' ') as (nodea:int, nodeb:int);
backward = foreach forward generate nodeb as nodea, nodea as nodeb;

--activeids = distinct(foreach (union intersectids, simplequeryids, edgequeryids) generate vertexid);
activeids = foreach simplequeryids generate vertexid; -- check if fanouts themselves are too big.

numactiveids = foreach (group activeids all) generate COUNT($1);

alledges = union forward, backward;

activeandmore = join alledges by nodea RIGHT OUTER, activeids by vertexid;
split activeandmore into activeedges if (nodea is not null), missing if (nodea is null);

-- remove the extra id from joining
activeedges = foreach activeedges generate nodea, nodeb;

-- symmetry may now be lost if the left end is in the set but the right end is not. restore it.
symmetricactive = distinct(union activeedges, (foreach activeedges generate nodeb, nodea));

datasize = foreach (group symmetricactive all) generate COUNT($1);
store datasize into '$KEYWORD/datasetize';
store symmetricactive into '$KEYWORD/symmetric_active_edges';
store activeids into '$KEYWORD/activeids';
store numactiveids into '$KEYWORD/activeids-count';
store missing into '$KEYWORD/missing-actives';