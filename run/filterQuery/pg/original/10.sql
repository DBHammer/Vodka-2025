
SELECT COUNT(*) FILTER (where o_entry_d <  '1993-10-01 00:00:00' )::NUMERIC / COUNT(*) FROM vodka_oorder;


