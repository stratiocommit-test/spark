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
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  COUNT(*) sales_cnt,
  SUM(ext_sales_price) sales_amt
FROM (
       SELECT
         'store' AS channel,
         ss_store_sk col_name,
         d_year,
         d_qoy,
         i_category,
         ss_ext_sales_price ext_sales_price
       FROM store_sales, item, date_dim
       WHERE ss_store_sk IS NULL
         AND ss_sold_date_sk = d_date_sk
         AND ss_item_sk = i_item_sk
       UNION ALL
       SELECT
         'web' AS channel,
         ws_ship_customer_sk col_name,
         d_year,
         d_qoy,
         i_category,
         ws_ext_sales_price ext_sales_price
       FROM web_sales, item, date_dim
       WHERE ws_ship_customer_sk IS NULL
         AND ws_sold_date_sk = d_date_sk
         AND ws_item_sk = i_item_sk
       UNION ALL
       SELECT
         'catalog' AS channel,
         cs_ship_addr_sk col_name,
         d_year,
         d_qoy,
         i_category,
         cs_ext_sales_price ext_sales_price
       FROM catalog_sales, item, date_dim
       WHERE cs_ship_addr_sk IS NULL
         AND cs_sold_date_sk = d_date_sk
         AND cs_item_sk = i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
LIMIT 100
