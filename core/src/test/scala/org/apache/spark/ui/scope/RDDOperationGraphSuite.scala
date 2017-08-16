/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ui.scope

import org.apache.spark.SparkFunSuite

class RDDOperationGraphSuite extends SparkFunSuite {
  test("Test simple cluster equals") {
    // create a 2-cluster chain with a child
    val c1 = new RDDOperationCluster("1", "Bender")
    val c2 = new RDDOperationCluster("2", "Hal")
    c1.attachChildCluster(c2)
    c1.attachChildNode(new RDDOperationNode(3, "Marvin", false, "collect!"))

    // create an equal cluster, but without the child node
    val c1copy = new RDDOperationCluster("1", "Bender")
    val c2copy = new RDDOperationCluster("2", "Hal")
    c1copy.attachChildCluster(c2copy)

    assert(c1 == c1copy)
  }
}
