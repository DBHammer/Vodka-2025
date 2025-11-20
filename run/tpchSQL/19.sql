SELECT
    SUM(ol_amount * (1 - ol_discount)) AS revenue
FROM   vodka_order_line,
       vodka_item
WHERE  i_id = ol_i_id                               -- 连接条件
  AND  ol_shipmode IN ('AIR', 'AIR REG')            -- 公共谓词
  AND  ol_shipinstruct = 'DELIVER IN PERSON'
  AND (
        /* ─── Brand#12 ───────────────────────────── */
        ( i_brand = 'Brand#12'
          AND i_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
          AND ol_quantity BETWEEN 1 AND 11          -- 1 ≤ qty ≤ 1+10
          AND i_size BETWEEN 1 AND 5 )

        OR
        /* ─── Brand#23 ───────────────────────────── */
        ( i_brand = 'Brand#23'
          AND i_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
          AND ol_quantity BETWEEN 10 AND 20         -- 10 ≤ qty ≤ 10+10
          AND i_size BETWEEN 1 AND 10 )

        OR
        /* ─── Brand#34 ───────────────────────────── */
        ( i_brand = 'Brand#34'
          AND i_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
          AND ol_quantity BETWEEN 20 AND 30         -- 20 ≤ qty ≤ 20+10
          AND i_size BETWEEN 1 AND 15 )
      );