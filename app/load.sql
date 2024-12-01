\copy nation FROM 'nation.tbl' DELIMITER '|' CSV;
\copy region FROM 'region.tbl' DELIMITER '|' CSV;
\copy customer FROM 'customer.tbl' DELIMITER '|' CSV;
\copy orders FROM 'orders.tbl' DELIMITER '|' CSV;
\copy lineitem FROM 'lineitem.tbl' DELIMITER '|' CSV;
\copy part FROM 'part.tbl' DELIMITER '|' CSV;
\copy partsupp FROM 'partsupp.tbl' DELIMITER '|' CSV;
\copy supplier FROM 'supplier.tbl' DELIMITER '|' CSV;