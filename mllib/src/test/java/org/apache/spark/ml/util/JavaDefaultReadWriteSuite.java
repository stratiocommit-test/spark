/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.util;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.util.Utils;

public class JavaDefaultReadWriteSuite extends SharedSparkSession {
  File tempDir = null;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    tempDir = Utils.createTempDir(
      System.getProperty("java.io.tmpdir"), "JavaDefaultReadWriteSuite");
  }

  @Override
  public void tearDown() {
    super.tearDown();
    Utils.deleteRecursively(tempDir);
  }

  @Test
  public void testDefaultReadWrite() throws IOException {
    String uid = "my_params";
    MyParams instance = new MyParams(uid);
    instance.set(instance.intParam(), 2);
    String outputPath = new File(tempDir, uid).getPath();
    instance.save(outputPath);
    try {
      instance.save(outputPath);
      Assert.fail(
        "Write without overwrite enabled should fail if the output directory already exists.");
    } catch (IOException e) {
      // expected
    }
    instance.write().session(spark).overwrite().save(outputPath);
    MyParams newInstance = MyParams.load(outputPath);
    Assert.assertEquals("UID should match.", instance.uid(), newInstance.uid());
    Assert.assertEquals("Params should be preserved.",
      2, newInstance.getOrDefault(newInstance.intParam()));
  }
}
