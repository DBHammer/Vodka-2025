select n_name,
       sum(ol_amount * (1 - ol_discount)) as revenue, count(*)
from vodka_customer,
     vodka_oorder,
     vodka_order_line,
     vodka_supplier_col2,
     vodka_nation_col,
     vodka_region_col
where c_w_id = o_w_id
  and c_d_id = o_d_id
  and c_id = o_c_id
  and ol_w_id = o_w_id
  and ol_d_id = o_d_id
  and ol_o_id = o_id
  and ol_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_entry_d < TIMESTAMP '1994-01-01'
group by n_name
order by revenue desc;