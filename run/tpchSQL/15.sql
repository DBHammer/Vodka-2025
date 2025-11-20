with revenue (supplier_no, total_revenue) as
         (select ol_suppkey,
                 sum(ol_amount * (1 - ol_discount))
          from vodka_order_line
          where ol_delivery_d < TIMESTAMP '1996-01-01 00:00:00'
          group by ol_suppkey)
select s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
from vodka_supplier,
     revenue
where s_suppkey = supplier_no
  and total_revenue = (select max(total_revenue)
                       from revenue)
order by s_suppkey;