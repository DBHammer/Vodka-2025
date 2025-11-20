SELECT 
  COUNT(*) FILTER (
    WHERE ol_quantity >= 1 and ol_quantity <= 11 or ol_quantity >= 10 and ol_quantity <= 20 or ol_quantity >= 20 and ol_quantity <= 30
  )::NUMERIC 
  / COUNT(*) AS ratio
FROM vodka_order_line;

