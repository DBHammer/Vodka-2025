select ( (select count(*) from vodka_order_line where ol_delivery_d <  '1995-09-01 00:00:00' ) /
        (select count(*) from vodka_order_line where ol_delivery_d is not null) );

