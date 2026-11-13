-- Grouping 13 of Table I of the paper: 47988996 groups at SF 8.
SETTINGS (query_out_of_memory_behavior = 'spilling', max_query_memory_usage = 536870912)
SELECT
    l_suppkey,
    l_partkey,
    l_orderkey
FROM lineitem_sf8
GROUP BY l_suppkey, l_partkey, l_orderkey
OFFSET 47988995;
