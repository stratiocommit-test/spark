/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.attribute

import org.apache.spark.SparkFunSuite

class AttributeGroupSuite extends SparkFunSuite {

  test("attribute group") {
    val attrs = Array(
      NumericAttribute.defaultAttr,
      NominalAttribute.defaultAttr,
      BinaryAttribute.defaultAttr.withIndex(0),
      NumericAttribute.defaultAttr.withName("age").withSparsity(0.8),
      NominalAttribute.defaultAttr.withName("size").withValues("small", "medium", "large"),
      BinaryAttribute.defaultAttr.withName("clicked").withValues("no", "yes"),
      NumericAttribute.defaultAttr,
      NumericAttribute.defaultAttr)
    val group = new AttributeGroup("user", attrs)
    assert(group.size === 8)
    assert(group.name === "user")
    assert(group(0) === NumericAttribute.defaultAttr.withIndex(0))
    assert(group(2) === BinaryAttribute.defaultAttr.withIndex(2))
    assert(group.indexOf("age") === 3)
    assert(group.indexOf("size") === 4)
    assert(group.indexOf("clicked") === 5)
    assert(!group.hasAttr("abc"))
    intercept[NoSuchElementException] {
      group("abc")
    }
    assert(group === AttributeGroup.fromMetadata(group.toMetadataImpl, group.name))
    assert(group === AttributeGroup.fromStructField(group.toStructField()))
  }

  test("attribute group without attributes") {
    val group0 = new AttributeGroup("user", 10)
    assert(group0.name === "user")
    assert(group0.numAttributes === Some(10))
    assert(group0.size === 10)
    assert(group0.attributes.isEmpty)
    assert(group0 === AttributeGroup.fromMetadata(group0.toMetadataImpl, group0.name))
    assert(group0 === AttributeGroup.fromStructField(group0.toStructField()))

    val group1 = new AttributeGroup("item")
    assert(group1.name === "item")
    assert(group1.numAttributes.isEmpty)
    assert(group1.attributes.isEmpty)
    assert(group1.size === -1)
  }
}
