SELECT l_orderkey, l_linenumber, l_shipmode, l_quantity
FROM lineitem
WHERE l_shipmode = 'SHIP'
ORDER BY l_orderkey, l_linenumber
LIMIT 3;