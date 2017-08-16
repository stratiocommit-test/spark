/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.launcher;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import static org.apache.spark.launcher.SparkSubmitOptionParser.*;

public class SparkSubmitOptionParserSuite extends BaseSuite {

  private SparkSubmitOptionParser parser;

  @Before
  public void setUp() {
    parser = spy(new DummyParser());
  }

  @Test
  public void testAllOptions() {
    int count = 0;
    for (String[] optNames : parser.opts) {
      for (String optName : optNames) {
        String value = optName + "-value";
        parser.parse(Arrays.asList(optName, value));
        count++;
        verify(parser).handle(eq(optNames[0]), eq(value));
        verify(parser, times(count)).handle(anyString(), anyString());
        verify(parser, times(count)).handleExtraArgs(eq(Collections.<String>emptyList()));
      }
    }

    for (String[] switchNames : parser.switches) {
      int switchCount = 0;
      for (String name : switchNames) {
        parser.parse(Arrays.asList(name));
        count++;
        switchCount++;
        verify(parser, times(switchCount)).handle(eq(switchNames[0]), same((String) null));
        verify(parser, times(count)).handle(anyString(), any(String.class));
        verify(parser, times(count)).handleExtraArgs(eq(Collections.<String>emptyList()));
      }
    }
  }

  @Test
  public void testExtraOptions() {
    List<String> args = Arrays.asList(parser.MASTER, parser.MASTER, "foo", "bar");
    parser.parse(args);
    verify(parser).handle(eq(parser.MASTER), eq(parser.MASTER));
    verify(parser).handleUnknown(eq("foo"));
    verify(parser).handleExtraArgs(eq(Arrays.asList("bar")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testMissingArg() {
    parser.parse(Arrays.asList(parser.MASTER));
  }

  @Test
  public void testEqualSeparatedOption() {
    List<String> args = Arrays.asList(parser.MASTER + "=" + parser.MASTER);
    parser.parse(args);
    verify(parser).handle(eq(parser.MASTER), eq(parser.MASTER));
    verify(parser).handleExtraArgs(eq(Collections.<String>emptyList()));
  }

  private static class DummyParser extends SparkSubmitOptionParser {

    @Override
    protected boolean handle(String opt, String value) {
      return true;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      return false;
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {

    }

  }

}
