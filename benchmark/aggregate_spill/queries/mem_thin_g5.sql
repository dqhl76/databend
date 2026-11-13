-- Grouping 5 of Table I of the paper: 12000000 groups at SF 8.
SELECT
    l_orderkey
FROM lineitem_sf8
GROUP BY l_orderkey
OFFSET 11999999;
