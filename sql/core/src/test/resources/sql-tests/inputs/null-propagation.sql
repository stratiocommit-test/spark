--
-- © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
--
-- This software is a modification of the original software Apache Spark licensed under the Apache 2.0
-- license, a copy of which is below. This software contains proprietary information of
-- Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
-- otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
-- without express written authorization from Stratio Big Data Inc., Sucursal en España.
--


-- count(null) should be 0
SELECT COUNT(NULL) FROM VALUES 1, 2, 3;
SELECT COUNT(1 + NULL) FROM VALUES 1, 2, 3;

-- count(null) on window should be 0
SELECT COUNT(NULL) OVER () FROM VALUES 1, 2, 3;
SELECT COUNT(1 + NULL) OVER () FROM VALUES 1, 2, 3;

