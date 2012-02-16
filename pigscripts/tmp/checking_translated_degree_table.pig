-- intended for use with translated_degree_table
vertex_degrees = load 'translated_degree_table' as (vertexid:int, degree:int);
maxid = foreach (group vertex_degrees all) generate MAX(vertex_degrees.vertexid);
store maxid into 'maxid-vertex';
sortedtabid = order vertex_degrees by vertexid;
store sortedtabid into 'ordered-vertex-table';
sorteddegree = order vertex_degrees by degree DESC;
store sorteddegree into 'ordered-vertex-by-degree';