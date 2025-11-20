select ((select count(*) from vodka_order_line where ol_delivery_d < '1994-01-01 00:00:00') /
                                                                        (select count(*) from vodka_order_line where ol_delivery_d is not null));

