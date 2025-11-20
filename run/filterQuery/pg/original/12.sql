
SELECT COUNT(*) FILTER (where ol_receipdate <  '1994-01-01 00:00:00')::NUMERIC / COUNT(*) FROM vodka_order_line where ol_receipdate IS NOT NULL;


