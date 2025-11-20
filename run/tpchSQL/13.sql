select c_count,
       count(*) as custdist
from (select c_w_id,
             c_d_id,
             c_id,
             count(o_id)
      from vodka_customer
               left outer join vodka_oorder on
                  c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id
              and o_comment not like '%special%requests%'
      group by c_w_id, c_d_id, c_id) as c_orders (c_custkey, c_count)
group by c_count
order by custdist desc,
         c_count desc;