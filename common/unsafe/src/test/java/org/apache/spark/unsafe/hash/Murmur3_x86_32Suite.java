/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.unsafe.hash;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.spark.unsafe.Platform;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test file based on Guava's Murmur3Hash32Test.
 */
public class Murmur3_x86_32Suite {

  private static final Murmur3_x86_32 hasher = new Murmur3_x86_32(0);

  @Test
  public void testKnownIntegerInputs() {
    Assert.assertEquals(593689054, hasher.hashInt(0));
    Assert.assertEquals(-189366624, hasher.hashInt(-42));
    Assert.assertEquals(-1134849565, hasher.hashInt(42));
    Assert.assertEquals(-1718298732, hasher.hashInt(Integer.MIN_VALUE));
    Assert.assertEquals(-1653689534, hasher.hashInt(Integer.MAX_VALUE));
  }

  @Test
  public void testKnownLongInputs() {
    Assert.assertEquals(1669671676, hasher.hashLong(0L));
    Assert.assertEquals(-846261623, hasher.hashLong(-42L));
    Assert.assertEquals(1871679806, hasher.hashLong(42L));
    Assert.assertEquals(1366273829, hasher.hashLong(Long.MIN_VALUE));
    Assert.assertEquals(-2106506049, hasher.hashLong(Long.MAX_VALUE));
  }

  @Test
  public void randomizedStressTest() {
    int size = 65536;
    Random rand = new Random();

    // A set used to track collision rate.
    Set<Integer> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int vint = rand.nextInt();
      long lint = rand.nextLong();
      Assert.assertEquals(hasher.hashInt(vint), hasher.hashInt(vint));
      Assert.assertEquals(hasher.hashLong(lint), hasher.hashLong(lint));

      hashcodes.add(hasher.hashLong(lint));
    }

    // A very loose bound.
    Assert.assertTrue(hashcodes.size() > size * 0.95);
  }

  @Test
  public void randomizedStressTestBytes() {
    int size = 65536;
    Random rand = new Random();

    // A set used to track collision rate.
    Set<Integer> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int byteArrSize = rand.nextInt(100) * 8;
      byte[] bytes = new byte[byteArrSize];
      rand.nextBytes(bytes);

      Assert.assertEquals(
        hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
        hasher.hashUnsafeWords(bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));

      hashcodes.add(hasher.hashUnsafeWords(
        bytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
    }

    // A very loose bound.
    Assert.assertTrue(hashcodes.size() > size * 0.95);
  }

  @Test
  public void randomizedStressTestPaddedStrings() {
    int size = 64000;
    // A set used to track collision rate.
    Set<Integer> hashcodes = new HashSet<>();
    for (int i = 0; i < size; i++) {
      int byteArrSize = 8;
      byte[] strBytes = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
      byte[] paddedBytes = new byte[byteArrSize];
      System.arraycopy(strBytes, 0, paddedBytes, 0, strBytes.length);

      Assert.assertEquals(
        hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize),
        hasher.hashUnsafeWords(paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));

      hashcodes.add(hasher.hashUnsafeWords(
        paddedBytes, Platform.BYTE_ARRAY_OFFSET, byteArrSize));
    }

    // A very loose bound.
    Assert.assertTrue(hashcodes.size() > size * 0.95);
  }
}
