-- Grouping 2 of Table I of the paper: 1600000 groups at SF 8.
SELECT
    l_partkey,
    any_value(l_orderkey),
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
GROUP BY l_partkey
OFFSET 1599999;
