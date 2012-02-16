intersects = load 'popular-intersects' using PigStorage() as (pair:(nodea:int, nodeb:int), counted:int);
pairs = union (foreach intersects generate pair.nodea, counted), (foreach intersects generate pair.nodeb, counted);
stuff = foreach (group pairs by $0) generate group as vertexid, SUM(pairs.counted) as total;
ranked = order stuff by total;
store ranked into 'popular-ids-in-intersects';

