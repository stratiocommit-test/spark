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
  substr(w_warehouse_name, 1, 20),
  sm_type,
  cc_name,
  sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30)
    THEN 1
      ELSE 0 END)  AS `30 days `,
  sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) AND
    (cs_ship_date_sk - cs_sold_date_sk <= 60)
    THEN 1
      ELSE 0 END)  AS `31 - 60 days `,
  sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) AND
    (cs_ship_date_sk - cs_sold_date_sk <= 90)
    THEN 1
      ELSE 0 END)  AS `61 - 90 days `,
  sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) AND
    (cs_ship_date_sk - cs_sold_date_sk <= 120)
    THEN 1
      ELSE 0 END)  AS `91 - 120 days `,
  sum(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 120)
    THEN 1
      ELSE 0 END)  AS `>120 days `
FROM
  catalog_sales, warehouse, ship_mode, call_center, date_dim
WHERE
  d_month_seq BETWEEN 1200 AND 1200 + 11
    AND cs_ship_date_sk = d_date_sk
    AND cs_warehouse_sk = w_warehouse_sk
    AND cs_ship_mode_sk = sm_ship_mode_sk
    AND cs_call_center_sk = cc_call_center_sk
GROUP BY
  substr(w_warehouse_name, 1, 20), sm_type, cc_name
ORDER BY substr(w_warehouse_name, 1, 20), sm_type, cc_name
LIMIT 100
