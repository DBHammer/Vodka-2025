select o_carrier_id,
       count(*) as order_count
from vodka_oorder
where o_entry_d < TIMESTAMP '1993-07-01'
  and exists(
        select *
        from vodka_order_line
        where ol_w_id = o_w_id
          and ol_d_id = o_d_id
          and ol_o_id = o_id
          and ol_commitdate < ol_receipdate
    )
group by o_carrier_id
order by o_carrier_id;