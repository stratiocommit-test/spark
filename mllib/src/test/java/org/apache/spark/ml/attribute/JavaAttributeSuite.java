/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.attribute;

import org.junit.Assert;
import org.junit.Test;

public class JavaAttributeSuite {

  @Test
  public void testAttributeType() {
    AttributeType numericType = AttributeType.Numeric();
    AttributeType nominalType = AttributeType.Nominal();
    AttributeType binaryType = AttributeType.Binary();
    Assert.assertEquals(numericType, NumericAttribute.defaultAttr().attrType());
    Assert.assertEquals(nominalType, NominalAttribute.defaultAttr().attrType());
    Assert.assertEquals(binaryType, BinaryAttribute.defaultAttr().attrType());
  }

  @Test
  public void testNumericAttribute() {
    NumericAttribute attr = NumericAttribute.defaultAttr()
      .withName("age").withIndex(0).withMin(0.0).withMax(1.0).withStd(0.5).withSparsity(0.4);
    Assert.assertEquals(attr.withoutIndex(), Attribute.fromStructField(attr.toStructField()));
  }

  @Test
  public void testNominalAttribute() {
    NominalAttribute attr = NominalAttribute.defaultAttr()
      .withName("size").withIndex(1).withValues("small", "medium", "large");
    Assert.assertEquals(attr.withoutIndex(), Attribute.fromStructField(attr.toStructField()));
  }

  @Test
  public void testBinaryAttribute() {
    BinaryAttribute attr = BinaryAttribute.defaultAttr()
      .withName("clicked").withIndex(2).withValues("no", "yes");
    Assert.assertEquals(attr.withoutIndex(), Attribute.fromStructField(attr.toStructField()));
  }
}
