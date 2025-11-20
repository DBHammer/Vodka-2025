select nation,
       o_year,
       sum(amount) as sum_profit
from (select n_name                                                     as nation, extract(year from o_entry_d)                               as o_year,
             ol_amount * (1 - ol_discount) - s_supplycost * ol_quantity as amount
      from vodka_item,
           vodka_supplier,
           vodka_order_line,
           vodka_stock,
           vodka_oorder,
           vodka_nation
      where vodka_supplier.s_suppkey = ol_suppkey
        and s_tocksuppkey = ol_suppkey
        and s_w_id = ol_supply_w_id
        and s_i_id = ol_i_id
        and i_id = ol_i_id
        and o_w_id = ol_w_id
        and o_d_id = ol_d_id
        and o_id = ol_o_id
        and s_nationkey = n_nationkey
        and i_name like '%green%') as profit
group by nation,
         o_year
order by nation,
         o_year desc;