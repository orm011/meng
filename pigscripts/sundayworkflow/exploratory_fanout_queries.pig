fanouts = load 'query_log_views/fanout_queries' as (logpos:int, name:chararray, pagesize:int, offset:long, argid:int);
page_sizes = order (foreach (group (foreach fanouts generate pagesize) by pagesize) generate group, COUNT($1)) by $1;
offsets = order (foreach (group (foreach fanouts generate offset) by offset) generate group, COUNT($1)) by $1;

combos = foreach fanouts generate pagesize, offset;
ordered_combos  = order  combos by offset;
numbered_combos = stream ordered_combos through `nl -nln -s',' -w1`;

combo_counts = order (foreach (group combos by (pagesize, offset)) generate group.$0, group.$1, COUNT($1)) by $1;

store page_sizes into 'stats/fanout_frequency_count';
store offsets into 'stats/offsets_frequency_count';
store numbered_combos into 'query_log_views/numbered_pagesize_offset_combination';
store combo_counts into 'stats/fanout_combo_count';