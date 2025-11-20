select ol_shipmode,
       sum(case
               when o_carrier_id = 1
                   or o_carrier_id = 2
                   then 1
               else 0
           end) as high_line_count,
       sum(case
               when o_carrier_id <> 1
                   and o_carrier_id <> 2
                   then 1
               else 0
           end) as low_line_count
from vodka_oorder,
     vodka_order_line
where ol_w_id = o_w_id
  and ol_d_id = o_d_id
  and ol_o_id = o_id
  and ol_shipmode in ('MAIL', 'SHIP')
  and ol_commitdate < ol_receipdate
  and ol_delivery_d < ol_commitdate
  and ol_receipdate < TIMESTAMP '1994-01-01 00:00:00' 
group by ol_shipmode
order by ol_shipmode;