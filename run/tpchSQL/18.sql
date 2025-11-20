select c_last,
       c_w_id,
       c_d_id,
       c_id,
       o_w_id,
       o_d_id,
       o_id,
       o_entry_d,
       sum(ol_quantity) AS o_totalprice
from vodka_customer,
     vodka_oorder,
     vodka_order_line
where (o_w_id, o_d_id, o_id) in (select ol_w_id,
                                        ol_d_id,
                                        ol_o_id
                                 from vodka_order_line
                                 group by ol_w_id, ol_d_id, ol_o_id
                                 having sum(ol_quantity) > 300)
  and c_w_id = o_w_id
  and c_d_id = o_d_id
  and c_id = o_c_id
  and ol_w_id = o_w_id
  and ol_d_id = o_d_id
  and ol_o_id = o_id
group by c_last,
         c_w_id, c_d_id, c_id,
         o_w_id, o_d_id, o_id,
         o_entry_d
order by
    o_totalprice DESC,
    o_entry_d
limit 5;

