select cntrycode,
       count(*)       as numcust,
       sum(c_balance) as totacctbal
from (select substring(c_phone from 1 for 2) as cntrycode,
             c_balance
      from vodka_customer
      where substring(c_phone from 1 for 2) in
            ('13', '31', '23', '29', '30', '18', '17')
        and c_balance > (select avg(c_balance)
                         from vodka_customer
                         where c_balance > 0.00
                           and substring(c_phone from 1 for 2) in
                               ('13', '31', '23', '29', '30', '18', '17'))
        and not exists(
              select *
              from vodka_oorder
              where c_w_id = o_w_id
                and c_d_id = o_d_id
                and c_id = o_c_id
          )) as custsale
group by cntrycode
order by cntrycode;
