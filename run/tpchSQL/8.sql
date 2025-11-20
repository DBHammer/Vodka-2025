select o_year,
       sum(case
               when nation = 'BRAZIL' then volume
               else 0
           end) / sum(volume) as mkt_share
from (select extract(year from o_entry_d)  as o_year,
             ol_amount * (1 - ol_discount) as volume,
             n2.n_name                     as nation
      from vodka_item,
           vodka_supplier,
           vodka_order_line,
           vodka_oorder,
           vodka_customer,
           vodka_nation n1,
           vodka_nation n2,
           vodka_region
      where i_id = ol_i_id
        and s_suppkey = ol_suppkey
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
        and c_w_id = o_w_id
        and c_d_id = o_d_id
        and c_id = o_c_id
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_entry_d < '1995-01-01 00:00:00'
        and i_type = 'ECONOMY ANODIZED STEEL') as all_nations
group by o_year
order by o_year;