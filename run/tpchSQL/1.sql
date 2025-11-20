select ol_number,
       ol_returnflag,
       sum(ol_quantity)                   as sum_qty,
       sum(ol_amount)                     as sum_base_price,
       sum(ol_amount * (1 - ol_discount)) as sum_disc_price,
       avg(ol_quantity)                   as avg_qty,
       avg(ol_amount)                     as avg_price,
       avg(ol_discount)                   as avg_disc,
       count(*)                           as count_order
from vodka_order_line
where ol_delivery_d <= TIMESTAMP '1996-09-02 00:00:00'
group by ol_returnflag,
         ol_number
order by ol_returnflag,
         ol_number;