/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
/**
 * Library for launching Spark applications.
 *
 * <p>
 * This library allows applications to launch Spark programmatically. There's only one entry
 * point to the library - the {@link org.apache.spark.launcher.SparkLauncher} class.
 * </p>
 *
 * <p>
 * The {@link org.apache.spark.launcher.SparkLauncher#startApplication(
 * org.apache.spark.launcher.SparkAppHandle.Listener...)} can be used to start Spark and provide
 * a handle to monitor and control the running application:
 * </p>
 *
 * <pre>
 * {@code
 *   import org.apache.spark.launcher.SparkAppHandle;
 *   import org.apache.spark.launcher.SparkLauncher;
 *
 *   public class MyLauncher {
 *     public static void main(String[] args) throws Exception {
 *       SparkAppHandle handle = new SparkLauncher()
 *         .setAppResource("/my/app.jar")
 *         .setMainClass("my.spark.app.Main")
 *         .setMaster("local")
 *         .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
 *         .startApplication();
 *       // Use handle API to monitor / control application.
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>
 * It's also possible to launch a raw child process, using the
 * {@link org.apache.spark.launcher.SparkLauncher#launch()} method:
 * </p>
 *
 * <pre>
 * {@code
 *   import org.apache.spark.launcher.SparkLauncher;
 *
 *   public class MyLauncher {
 *     public static void main(String[] args) throws Exception {
 *       Process spark = new SparkLauncher()
 *         .setAppResource("/my/app.jar")
 *         .setMainClass("my.spark.app.Main")
 *         .setMaster("local")
 *         .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
 *         .launch();
 *       spark.waitFor();
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>This method requires the calling code to manually manage the child process, including its
 * output streams (to avoid possible deadlocks). It's recommended that
 * {@link org.apache.spark.launcher.SparkLauncher#startApplication(
 *   org.apache.spark.launcher.SparkAppHandle.Listener...)} be used instead.</p>
 */
package org.apache.spark.launcher;
