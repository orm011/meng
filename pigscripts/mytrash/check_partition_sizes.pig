edgetable = load '$FOO' as (partnum:int, nodea:int, nodeb:int);
partweight = foreach (group edgetable all) generate COUNT($1);
tempunique = foreach (group edgetable by nodea) generate group as nodea, COUNT($1) as deg;
uniquenodea = order tempunique by deg;
countvertices = foreach (group uniquenodea all) generate COUNT($1);

store uniquenodea into 'out-$FOO/degrees';
store countvertices into 'out-$FOO/countvertices';
store partweight into 'out-$FOO/weight';