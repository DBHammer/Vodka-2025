select ol_w_id,
       ol_d_id,
       ol_o_id,
       sum(ol_amount * (1 - ol_discount)) as revenue,
       o_entry_d,
       o_carrier_id
from vodka_customer,
     vodka_order_line,
     vodka_oorder
where c_mktsegment = 'BUILDING'
  and c_w_id = o_w_id
  and c_d_id = o_d_id
  and c_id = o_c_id
  and ol_w_id = o_w_id
  and ol_d_id = o_d_id
  and ol_o_id = o_id
  and ol_delivery_d < TIMESTAMP '1995-03-15 00:00:00'
group by ol_w_id, ol_d_id, ol_o_id,
         o_entry_d,
         o_carrier_id
order by revenue desc,
         o_entry_d limit 10;