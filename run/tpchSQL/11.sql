select s_i_id,
       sum(s_supplycost * s_quantity) as value
from
    vodka_stock, vodka_supplier, vodka_nation
where
    s_tocksuppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by
    s_i_id
having
    sum (s_supplycost * s_quantity)
     > (
    select
    sum (s_supplycost * s_quantity) * 0.0001000000
    from
    vodka_stock,
    vodka_supplier,
    vodka_nation
    where
    s_tocksuppkey = s_suppkey
   and s_nationkey = n_nationkey
   and n_name = 'GERMANY'
    )
order by
    value desc;