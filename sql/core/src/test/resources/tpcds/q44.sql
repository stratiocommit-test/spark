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
  asceding.rnk,
  i1.i_product_name best_performing,
  i2.i_product_name worst_performing
FROM (SELECT *
FROM (SELECT
  item_sk,
  rank()
  OVER (
    ORDER BY rank_col ASC) rnk
FROM (SELECT
  ss_item_sk item_sk,
  avg(ss_net_profit) rank_col
FROM store_sales ss1
WHERE ss_store_sk = 4
GROUP BY ss_item_sk
HAVING avg(ss_net_profit) > 0.9 * (SELECT avg(ss_net_profit) rank_col
FROM store_sales
WHERE ss_store_sk = 4
  AND ss_addr_sk IS NULL
GROUP BY ss_store_sk)) V1) V11
WHERE rnk < 11) asceding,
  (SELECT *
  FROM (SELECT
    item_sk,
    rank()
    OVER (
      ORDER BY rank_col DESC) rnk
  FROM (SELECT
    ss_item_sk item_sk,
    avg(ss_net_profit) rank_col
  FROM store_sales ss1
  WHERE ss_store_sk = 4
  GROUP BY ss_item_sk
  HAVING avg(ss_net_profit) > 0.9 * (SELECT avg(ss_net_profit) rank_col
  FROM store_sales
  WHERE ss_store_sk = 4
    AND ss_addr_sk IS NULL
  GROUP BY ss_store_sk)) V2) V21
  WHERE rnk < 11) descending,
  item i1, item i2
WHERE asceding.rnk = descending.rnk
  AND i1.i_item_sk = asceding.item_sk
  AND i2.i_item_sk = descending.item_sk
ORDER BY asceding.rnk
LIMIT 100
