select ((select count(*) from vodka_oorder where o_entry_d <=  '1993-07-01 00:00:00' )/
        (select count(*) from vodka_oorder) );