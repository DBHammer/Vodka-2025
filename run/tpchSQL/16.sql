select i_brand,
       i_type,
       i_size,
       count(distinct s_tocksuppkey) as supplier_cnt
from vodka_stock,
     vodka_item
where i_id = s_i_id
  and i_brand <> 'Brand#45'
  and i_type not like 'MEDIUM POLISHED%'
  and i_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and s_tocksuppkey not in (select s_suppkey
                            from vodka_supplier
                            where s_comment like '%Customer%Complaints%'
                              and s_suppkey < 500)
group by i_brand,
         i_type,
         i_size
order by supplier_cnt desc,
         i_brand,
         i_type,
         i_size;