--
-- © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
--
-- This software is a modification of the original software Apache Spark licensed under the Apache 2.0
-- license, a copy of which is below. This software contains proprietary information of
-- Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
-- otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
-- without express written authorization from Stratio Big Data Inc., Sucursal en España.
--

CREATE TABLE t (a STRING, b INT) PARTITIONED BY (c STRING, d STRING);

ALTER TABLE t ADD PARTITION (c='Us', d=1);

DESCRIBE t;

DESC t;

DESC TABLE t;

-- Ignore these because there exist timestamp results, e.g., `Create Table`.
-- DESC EXTENDED t;
-- DESC FORMATTED t;

DESC t PARTITION (c='Us', d=1);

-- Ignore these because there exist timestamp results, e.g., transient_lastDdlTime.
-- DESC EXTENDED t PARTITION (c='Us', d=1);
-- DESC FORMATTED t PARTITION (c='Us', d=1);

-- NoSuchPartitionException: Partition not found in table
DESC t PARTITION (c='Us', d=2);

-- AnalysisException: Partition spec is invalid
DESC t PARTITION (c='Us');

-- ParseException: PARTITION specification is incomplete
DESC t PARTITION (c='Us', d);

-- DROP TEST TABLE
DROP TABLE t;
