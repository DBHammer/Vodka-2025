SELECT count(*)
FROM vodka_order_line
JOIN vodka_item        ON i_id    = ol_i_id
WHERE i_brand    = 'Brand#23'
  AND i_container = 'MED BOX'
  AND ol_quantity < (
    SELECT 0.2 * avg(ol_quantity)
    FROM vodka_order_line
    WHERE ol_i_id = i_id
  );