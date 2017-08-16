--
-- © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
--
-- This software is a modification of the original software Apache Spark licensed under the Apache 2.0
-- license, a copy of which is below. This software contains proprietary information of
-- Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
-- otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
-- without express written authorization from Stratio Big Data Inc., Sucursal en España.
--


-- limit on various data types
select * from testdata limit 2;
select * from arraydata limit 2;
select * from mapdata limit 2;

-- foldable non-literal in limit
select * from testdata limit 2 + 1;

select * from testdata limit CAST(1 AS int);

-- limit must be non-negative
select * from testdata limit -1;

-- limit must be foldable
select * from testdata limit key > 3;

-- limit must be integer
select * from testdata limit true;
select * from testdata limit 'a';

-- limit within a subquery
select * from (select * from range(10) limit 5) where id > 3;
