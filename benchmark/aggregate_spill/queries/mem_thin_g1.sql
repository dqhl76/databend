-- Grouping 1 of Table I of the paper: 4 groups at SF 8.
SELECT
    l_returnflag,
    l_linestatus
FROM lineitem_sf8
GROUP BY l_returnflag, l_linestatus
OFFSET 3;
