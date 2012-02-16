big_edge_table = load 'translated-edges' as (nodea:int, nodeb:int, edgew:int);
edge_table = foreach big_edge_table generate nodea, nodeb;
symmetric_edge_table = union edge_table, (foreach edge_table generate nodeb as nodea, nodea as nodeb);
x = order (distinct (foreach symmetric_edge_table generate nodea)) by nodea;
store x into 'check-edge-table';