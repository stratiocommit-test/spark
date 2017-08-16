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

public class JavaAttributeGroupSuite {

  @Test
  public void testAttributeGroup() {
    Attribute[] attrs = new Attribute[]{
      NumericAttribute.defaultAttr(),
      NominalAttribute.defaultAttr(),
      BinaryAttribute.defaultAttr().withIndex(0),
      NumericAttribute.defaultAttr().withName("age").withSparsity(0.8),
      NominalAttribute.defaultAttr().withName("size").withValues("small", "medium", "large"),
      BinaryAttribute.defaultAttr().withName("clicked").withValues("no", "yes"),
      NumericAttribute.defaultAttr(),
      NumericAttribute.defaultAttr()
    };
    AttributeGroup group = new AttributeGroup("user", attrs);
    Assert.assertEquals(8, group.size());
    Assert.assertEquals("user", group.name());
    Assert.assertEquals(NumericAttribute.defaultAttr().withIndex(0), group.getAttr(0));
    Assert.assertEquals(3, group.indexOf("age"));
    Assert.assertFalse(group.hasAttr("abc"));
    Assert.assertEquals(group, AttributeGroup.fromStructField(group.toStructField()));
  }
}
