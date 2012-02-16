fanouts = load 'query_log_views/fanout_queries' as (logpos:int, name:chararray, pagesize:int, offset:long, argid:int);
filtered = foreach fanouts generate argid, ((pagesize == 1)?1:0) as tiny_page, ((pagesize < 50)?1:0) as small_page, ((offset < 0)?1:0) as negative_offset;
correlated = foreach filtered generate *, tiny_page*negative_offset as peek;

--id, 'peeking query', pagesize==1, offset < 0, pagesize < 50, total;
countall =  foreach (group correlated by argid) generate group,  SUM($1.peek), SUM($1.negative_offset), SUM($1.tiny_page), SUM($1.small_page), COUNT($1);
store countall into 'query_log_views/query_graph_fanout_query_vertex_weights';

