group_edges = load 'popular-intersects' using PigStorage() as (edge:(nodea:int, nodeb:int), count:int);
id_table = load 'vertex_id_table' using PigStorage() as (newid:int, oldid:int);

edges = foreach group_edges generate FLATTEN(edge) as (nodea:int, nodeb:int), count;

translated_source = foreach (join edges by nodea LEFT, id_table by oldid) generate newid as newnodea, nodeb, count;
translated_dest = foreach (join translated_source by nodeb LEFT, id_table by oldid) generate newnodea, newid as newnodeb, count;

split translated_dest into missing if (newnodea is null OR newnodeb is null), translated if NOT(newnodea is null OR newnodeb is null);
store missing into 'missing-ids';
store translated into 'translated-edges';