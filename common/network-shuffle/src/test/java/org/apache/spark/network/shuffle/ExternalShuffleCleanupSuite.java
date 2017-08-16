/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.network.shuffle;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ExternalShuffleCleanupSuite {

  // Same-thread Executor used to ensure cleanup happens synchronously in test thread.
  private Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
  private TransportConf conf = new TransportConf("shuffle", new SystemPropertyConfigProvider());
  private static final String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";

  @Test
  public void noCleanupAndCleanup() throws IOException {
    TestShuffleDataContext dataContext = createSomeData();

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);
    resolver.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));
    resolver.applicationRemoved("app", false /* cleanup */);

    assertStillThere(dataContext);

    resolver.registerExecutor("app", "exec1", dataContext.createExecutorInfo(SORT_MANAGER));
    resolver.applicationRemoved("app", true /* cleanup */);

    assertCleanedUp(dataContext);
  }

  @Test
  public void cleanupUsesExecutor() throws IOException {
    TestShuffleDataContext dataContext = createSomeData();

    final AtomicBoolean cleanupCalled = new AtomicBoolean(false);

    // Executor which does nothing to ensure we're actually using it.
    Executor noThreadExecutor = new Executor() {
      @Override public void execute(Runnable runnable) { cleanupCalled.set(true); }
    };

    ExternalShuffleBlockResolver manager =
      new ExternalShuffleBlockResolver(conf, null, noThreadExecutor);

    manager.registerExecutor("app", "exec0", dataContext.createExecutorInfo(SORT_MANAGER));
    manager.applicationRemoved("app", true);

    assertTrue(cleanupCalled.get());
    assertStillThere(dataContext);

    dataContext.cleanup();
    assertCleanedUp(dataContext);
  }

  @Test
  public void cleanupMultipleExecutors() throws IOException {
    TestShuffleDataContext dataContext0 = createSomeData();
    TestShuffleDataContext dataContext1 = createSomeData();

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);

    resolver.registerExecutor("app", "exec0", dataContext0.createExecutorInfo(SORT_MANAGER));
    resolver.registerExecutor("app", "exec1", dataContext1.createExecutorInfo(SORT_MANAGER));
    resolver.applicationRemoved("app", true);

    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);
  }

  @Test
  public void cleanupOnlyRemovedApp() throws IOException {
    TestShuffleDataContext dataContext0 = createSomeData();
    TestShuffleDataContext dataContext1 = createSomeData();

    ExternalShuffleBlockResolver resolver =
      new ExternalShuffleBlockResolver(conf, null, sameThreadExecutor);

    resolver.registerExecutor("app-0", "exec0", dataContext0.createExecutorInfo(SORT_MANAGER));
    resolver.registerExecutor("app-1", "exec0", dataContext1.createExecutorInfo(SORT_MANAGER));

    resolver.applicationRemoved("app-nonexistent", true);
    assertStillThere(dataContext0);
    assertStillThere(dataContext1);

    resolver.applicationRemoved("app-0", true);
    assertCleanedUp(dataContext0);
    assertStillThere(dataContext1);

    resolver.applicationRemoved("app-1", true);
    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);

    // Make sure it's not an error to cleanup multiple times
    resolver.applicationRemoved("app-1", true);
    assertCleanedUp(dataContext0);
    assertCleanedUp(dataContext1);
  }

  private static void assertStillThere(TestShuffleDataContext dataContext) {
    for (String localDir : dataContext.localDirs) {
      assertTrue(localDir + " was cleaned up prematurely", new File(localDir).exists());
    }
  }

  private static void assertCleanedUp(TestShuffleDataContext dataContext) {
    for (String localDir : dataContext.localDirs) {
      assertFalse(localDir + " wasn't cleaned up", new File(localDir).exists());
    }
  }

  private static TestShuffleDataContext createSomeData() throws IOException {
    Random rand = new Random(123);
    TestShuffleDataContext dataContext = new TestShuffleDataContext(10, 5);

    dataContext.create();
    dataContext.insertSortShuffleData(rand.nextInt(1000), rand.nextInt(1000), new byte[][] {
        "ABC".getBytes(StandardCharsets.UTF_8),
        "DEF".getBytes(StandardCharsets.UTF_8)});
    return dataContext;
  }
}
