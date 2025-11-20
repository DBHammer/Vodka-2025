SELECT 
  COUNT(*) FILTER (
    WHERE c_balance > 0
      AND substring(c_phone FROM 1 FOR 2) IN (
            '13', '31', '23', '29', '30', '18', '17', '02', '40', '52', '78', '76', '46', '62',
            '81', '83', '70', '19', '94', '98', '61', '09', '99', '97', '67', '43', '91', '21'
          )
  )::NUMERIC 
  / COUNT(*) AS ratio
FROM vodka_customer;

