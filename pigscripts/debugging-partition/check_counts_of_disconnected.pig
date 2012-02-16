adjlist = load 'adjacency_list' using PigStorage(' ');
isolated = foreach (group (filter adjlist by $1 is null) all) generate COUNT($1), SUM($1.$0);
store isolated into '$OUT/isolated-nodes-weights';
