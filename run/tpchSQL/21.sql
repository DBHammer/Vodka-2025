select s_name,
       count(*) as numwait
from vodka_supplier,
     vodka_order_line l1,
     vodka_oorder,
     vodka_nation
where s_suppkey = l1.ol_suppkey
  and l1.ol_w_id = o_w_id
  and l1.ol_d_id = o_d_id
  and l1.ol_o_id = o_id
  and l1.ol_receipdate > l1.ol_commitdate
  and exists(
        select *
        from vodka_order_line l2
        where l2.ol_w_id = l1.ol_w_id
          and l2.ol_d_id = l1.ol_w_id
          and l2.ol_o_id = l1.ol_o_id
          and l2.ol_suppkey <> l1.ol_suppkey
    )
  and not exists(
        select *
        from vodka_order_line l3
        where l3.ol_w_id = l1.ol_w_id
          and l3.ol_d_id = l1.ol_w_id
          and l3.ol_o_id = l1.ol_o_id
          and l3.ol_suppkey <> l1.ol_suppkey
          and l3.ol_receipdate > l3.ol_commitdate
    )
  and s_nationkey = n_nationkey
  and n_name = 'SAUDI ARABIA'
group by s_name
order by numwait desc,
         s_name  limit 10;