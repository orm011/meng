edges = load '$FILE' as (partid:int, nodea:int, nodeb:int);
groupededges = order (foreach (group (foreach edges generate nodea, nodeb) by nodea) generate $0, $1.$1) by $0;
better = stream groupededges through `sed 's/[(){}]//g; s/,/ /g'`;
store better into '$FILE-reorg';