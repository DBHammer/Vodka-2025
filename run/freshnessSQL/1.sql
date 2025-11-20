SELECT ol_delivery_d, ol_receipdate, (ol_w_id, ol_d_id, ol_o_id, ol_number), ver
FROM vodka_order_line
WHERE vodka_order_line.ts BETWEEN ? AND ?;