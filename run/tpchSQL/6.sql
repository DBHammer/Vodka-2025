SELECT SUM(ol_amount * ol_discount) AS revenue
FROM vodka_order_line
WHERE ol_delivery_d < TIMESTAMP '1994-01-01 00:00:00'
  AND ol_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND ol_quantity < 24;
