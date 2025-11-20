select 100.00 * sum(case
                        when i_type like 'PROMO%'
                            then ol_amount * (1 - ol_discount)
                        else 0
    end) / (sum(ol_amount * (1 - ol_discount)) + 0.001) as promo_revenue
from vodka_order_line,
     vodka_item
where ol_i_id = i_id
    and ol_delivery_d < TIMESTAMP '1995-09-01 00:00:00';
