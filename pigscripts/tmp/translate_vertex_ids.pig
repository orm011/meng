vertex_degrees = load 'degrees' as (vertexid:int, degree:int);
id_table = load 'vertex_id_table' as (newid:int, oldid:int);

translated_vertex = foreach (join vertex_degrees by vertexid LEFT, id_table by oldid) generate newid as newvertexid, degree;
split translated_vertex into missing if (newvertexid is null), translated_degree_table if NOT(newvertexid is null);
store missing into 'missing-ids-in-vertex-table';
store translated_degree_table into 'translated_degree_table';