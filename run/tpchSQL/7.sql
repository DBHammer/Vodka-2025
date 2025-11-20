select supp_nation,
       cust_nation,
       l_year,
       sum(volume) as revenue
from (select n1.n_name                        as supp_nation,
             n2.n_name                        as cust_nation,
             extract(year from ol_delivery_d) as l_year,
             ol_amount * (1 - ol_discount)    as volume
      from vodka_supplier,
           vodka_order_line,
           vodka_oorder,
           vodka_customer,
           vodka_nation n1,
           vodka_nation n2
      where s_suppkey = ol_suppkey
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
        and c_w_id = o_w_id
        and c_d_id = o_d_id
        and c_id = o_c_id
        and s_nationkey = n1.n_nationkey
        and c_nationkey = n2.n_nationkey
        and (
              (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
              or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
          )
        and ol_delivery_d < TIMESTAMP '1995-01-01 00:00:00') as shipping
group by supp_nation,
         cust_nation,
         l_year
order by supp_nation,
         cust_nation,
         l_year;