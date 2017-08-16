/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.io;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

/**
 * Tests functionality of {@link NioBufferedFileInputStream}
 */
public class NioBufferedFileInputStreamSuite {

  private byte[] randomBytes;

  private File inputFile;

  @Before
  public void setUp() throws IOException {
    // Create a byte array of size 2 MB with random bytes
    randomBytes =  RandomUtils.nextBytes(2 * 1024 * 1024);
    inputFile = File.createTempFile("temp-file", ".tmp");
    FileUtils.writeByteArrayToFile(inputFile, randomBytes);
  }

  @After
  public void tearDown() {
    inputFile.delete();
  }

  @Test
  public void testReadOneByte() throws IOException {
    InputStream inputStream = new NioBufferedFileInputStream(inputFile);
    for (int i = 0; i < randomBytes.length; i++) {
      assertEquals(randomBytes[i], (byte) inputStream.read());
    }
  }

  @Test
  public void testReadMultipleBytes() throws IOException {
    InputStream inputStream = new NioBufferedFileInputStream(inputFile);
    byte[] readBytes = new byte[8 * 1024];
    int i = 0;
    while (i < randomBytes.length) {
      int read = inputStream.read(readBytes, 0, 8 * 1024);
      for (int j = 0; j < read; j++) {
        assertEquals(randomBytes[i], readBytes[j]);
        i++;
      }
    }
  }

  @Test
  public void testBytesSkipped() throws IOException {
    InputStream inputStream = new NioBufferedFileInputStream(inputFile);
    assertEquals(1024, inputStream.skip(1024));
    for (int i = 1024; i < randomBytes.length; i++) {
      assertEquals(randomBytes[i], (byte) inputStream.read());
    }
  }

  @Test
  public void testBytesSkippedAfterRead() throws IOException {
    InputStream inputStream = new NioBufferedFileInputStream(inputFile);
    for (int i = 0; i < 1024; i++) {
      assertEquals(randomBytes[i], (byte) inputStream.read());
    }
    assertEquals(1024, inputStream.skip(1024));
    for (int i = 2048; i < randomBytes.length; i++) {
      assertEquals(randomBytes[i], (byte) inputStream.read());
    }
  }

  @Test
  public void testNegativeBytesSkippedAfterRead() throws IOException {
    InputStream inputStream = new NioBufferedFileInputStream(inputFile);
    for (int i = 0; i < 1024; i++) {
      assertEquals(randomBytes[i], (byte) inputStream.read());
    }
    // Skipping negative bytes should essential be a no-op
    assertEquals(0, inputStream.skip(-1));
    assertEquals(0, inputStream.skip(-1024));
    assertEquals(0, inputStream.skip(Long.MIN_VALUE));
    assertEquals(1024, inputStream.skip(1024));
    for (int i = 2048; i < randomBytes.length; i++) {
      assertEquals(randomBytes[i], (byte) inputStream.read());
    }
  }

  @Test
  public void testSkipFromFileChannel() throws IOException {
    InputStream inputStream = new NioBufferedFileInputStream(inputFile, 10);
    // Since the buffer is smaller than the skipped bytes, this will guarantee
    // we skip from underlying file channel.
    assertEquals(1024, inputStream.skip(1024));
    for (int i = 1024; i < 2048; i++) {
      assertEquals(randomBytes[i], (byte) inputStream.read());
    }
    assertEquals(256, inputStream.skip(256));
    assertEquals(256, inputStream.skip(256));
    assertEquals(512, inputStream.skip(512));
    for (int i = 3072; i < randomBytes.length; i++) {
      assertEquals(randomBytes[i], (byte) inputStream.read());
    }
  }

  @Test
  public void testBytesSkippedAfterEOF() throws IOException {
    InputStream inputStream = new NioBufferedFileInputStream(inputFile);
    assertEquals(randomBytes.length, inputStream.skip(randomBytes.length + 1));
    assertEquals(-1, inputStream.read());
  }
}
