-- Grouping 5 of Table I of the paper: 12000000 groups at SF 8.
SETTINGS (query_out_of_memory_behavior = 'spilling', max_query_memory_usage = 2147483648)
SELECT
    l_orderkey,
    any_value(l_partkey),
    any_value(l_suppkey),
    any_value(l_linenumber),
    any_value(l_quantity),
    any_value(l_extendedprice),
    any_value(l_discount),
    any_value(l_tax),
    any_value(l_returnflag),
    any_value(l_linestatus),
    any_value(l_shipdate),
    any_value(l_commitdate),
    any_value(l_receiptdate),
    any_value(l_shipinstruct),
    any_value(l_shipmode),
    any_value(l_comment)
FROM lineitem_sf8
GROUP BY l_orderkey
OFFSET 11999999;
