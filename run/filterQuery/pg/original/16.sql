SELECT 
  COUNT(*) FILTER (
    WHERE i_brand <> 'Brand#45'
  and i_type not like 'MEDIUM POLISHED%' and i_size in (49, 14, 23, 45, 19, 3, 36, 9,
                    1, 2, 4, 5, 6, 7, 8,
                    10, 11, 12, 13, 15, 16, 17,
                    31, 20, 50, 22, 24, 33, 28)
  )::NUMERIC 
  / COUNT(*) AS ratio
FROM vodka_item;

