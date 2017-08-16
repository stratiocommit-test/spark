--
-- © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
--
-- This software is a modification of the original software Apache Spark licensed under the Apache 2.0
-- license, a copy of which is below. This software contains proprietary information of
-- Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
-- otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
-- without express written authorization from Stratio Big Data Inc., Sucursal en España.
--

SELECT
  count(DISTINCT ws_order_number) AS `order count `,
  sum(ws_ext_ship_cost) AS `total shipping cost `,
  sum(ws_net_profit) AS `total net profit `
FROM
  web_sales ws1, date_dim, customer_address, web_site
WHERE
  d_date BETWEEN '1999-02-01' AND
  (CAST('1999-02-01' AS DATE) + INTERVAL 60 days)
    AND ws1.ws_ship_date_sk = d_date_sk
    AND ws1.ws_ship_addr_sk = ca_address_sk
    AND ca_state = 'IL'
    AND ws1.ws_web_site_sk = web_site_sk
    AND web_company_name = 'pri'
    AND EXISTS(SELECT *
               FROM web_sales ws2
               WHERE ws1.ws_order_number = ws2.ws_order_number
                 AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
    AND NOT EXISTS(SELECT *
                   FROM web_returns wr1
                   WHERE ws1.ws_order_number = wr1.wr_order_number)
ORDER BY count(DISTINCT ws_order_number)
LIMIT 100
