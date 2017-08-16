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
  sum(ws_net_paid) AS total_sum,
  i_category,
  i_class,
  grouping(i_category) + grouping(i_class) AS lochierarchy,
  rank()
  OVER (
    PARTITION BY grouping(i_category) + grouping(i_class),
      CASE WHEN grouping(i_class) = 0
        THEN i_category END
    ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
FROM
  web_sales, date_dim d1, item
WHERE
  d1.d_month_seq BETWEEN 1200 AND 1200 + 11
    AND d1.d_date_sk = ws_sold_date_sk
    AND i_item_sk = ws_item_sk
GROUP BY ROLLUP (i_category, i_class)
ORDER BY
  lochierarchy DESC,
  CASE WHEN lochierarchy = 0
    THEN i_category END,
  rank_within_parent
LIMIT 100
