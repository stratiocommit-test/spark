/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package test.org.apache.spark;

import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;
import org.apache.spark.util.TaskFailureListener;

/**
 * Something to make sure that TaskContext can be used in Java.
 */
public class JavaTaskContextCompileCheck {

  public static void test() {
    TaskContext tc = TaskContext.get();

    tc.isCompleted();
    tc.isInterrupted();

    tc.addTaskCompletionListener(new JavaTaskCompletionListenerImpl());
    tc.addTaskFailureListener(new JavaTaskFailureListenerImpl());

    tc.attemptNumber();
    tc.partitionId();
    tc.stageId();
    tc.taskAttemptId();
  }

  /**
   * A simple implementation of TaskCompletionListener that makes sure TaskCompletionListener and
   * TaskContext is Java friendly.
   */
  static class JavaTaskCompletionListenerImpl implements TaskCompletionListener {
    @Override
    public void onTaskCompletion(TaskContext context) {
      context.isCompleted();
      context.isInterrupted();
      context.stageId();
      context.partitionId();
      context.addTaskCompletionListener(this);
    }
  }

  /**
   * A simple implementation of TaskCompletionListener that makes sure TaskCompletionListener and
   * TaskContext is Java friendly.
   */
  static class JavaTaskFailureListenerImpl implements TaskFailureListener {
    @Override
    public void onTaskFailure(TaskContext context, Throwable error) {
    }
  }

}
