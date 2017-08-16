/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
* See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui.exec

import scala.collection.mutable.{LinkedHashMap, ListBuffer}

import org.apache.spark.{ExceptionFailure, Resubmitted, SparkConf, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.{StorageStatus, StorageStatusListener}
import org.apache.spark.ui.{SparkUI, SparkUITab}

private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {
  val listener = parent.executorsListener
  val sc = parent.sc
  val threadDumpEnabled =
    sc.isDefined && parent.conf.getBoolean("spark.ui.threadDumpsEnabled", true)

  attachPage(new ExecutorsPage(this, threadDumpEnabled))
  if (threadDumpEnabled) {
    attachPage(new ExecutorThreadDumpPage(this))
  }
}

private[ui] case class ExecutorTaskSummary(
    var executorId: String,
    var totalCores: Int = 0,
    var tasksMax: Int = 0,
    var tasksActive: Int = 0,
    var tasksFailed: Int = 0,
    var tasksComplete: Int = 0,
    var duration: Long = 0L,
    var jvmGCTime: Long = 0L,
    var inputBytes: Long = 0L,
    var inputRecords: Long = 0L,
    var outputBytes: Long = 0L,
    var outputRecords: Long = 0L,
    var shuffleRead: Long = 0L,
    var shuffleWrite: Long = 0L,
    var executorLogs: Map[String, String] = Map.empty,
    var isAlive: Boolean = true
)

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the ExecutorsTab
 */
@DeveloperApi
class ExecutorsListener(storageStatusListener: StorageStatusListener, conf: SparkConf)
    extends SparkListener {
  var executorToTaskSummary = LinkedHashMap[String, ExecutorTaskSummary]()
  var executorEvents = new ListBuffer[SparkListenerEvent]()

  private val maxTimelineExecutors = conf.getInt("spark.ui.timeline.executors.maximum", 1000)
  private val retainedDeadExecutors = conf.getInt("spark.ui.retainedDeadExecutors", 100)

  def activeStorageStatusList: Seq[StorageStatus] = storageStatusListener.storageStatusList

  def deadStorageStatusList: Seq[StorageStatus] = storageStatusListener.deadStorageStatusList

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, ExecutorTaskSummary(eid))
    taskSummary.executorLogs = executorAdded.executorInfo.logUrlMap
    taskSummary.totalCores = executorAdded.executorInfo.totalCores
    taskSummary.tasksMax = taskSummary.totalCores / conf.getInt("spark.task.cpus", 1)
    executorEvents += executorAdded
    if (executorEvents.size > maxTimelineExecutors) {
      executorEvents.remove(0)
    }

    val deadExecutors = executorToTaskSummary.filter(e => !e._2.isAlive)
    if (deadExecutors.size > retainedDeadExecutors) {
      val head = deadExecutors.head
      executorToTaskSummary.remove(head._1)
    }
  }

  override def onExecutorRemoved(
      executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    executorEvents += executorRemoved
    if (executorEvents.size > maxTimelineExecutors) {
      executorEvents.remove(0)
    }
    executorToTaskSummary.get(executorRemoved.executorId).foreach(e => e.isAlive = false)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    applicationStart.driverLogs.foreach { logs =>
      val storageStatus = activeStorageStatusList.find { s =>
        s.blockManagerId.executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER ||
        s.blockManagerId.executorId == SparkContext.DRIVER_IDENTIFIER
      }
      storageStatus.foreach { s =>
        val eid = s.blockManagerId.executorId
        val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, ExecutorTaskSummary(eid))
        taskSummary.executorLogs = logs.toMap
      }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val eid = taskStart.taskInfo.executorId
    val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, ExecutorTaskSummary(eid))
    taskSummary.tasksActive += 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = info.executorId
      val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, ExecutorTaskSummary(eid))
      taskEnd.reason match {
        case Resubmitted =>
          // Note: For resubmitted tasks, we continue to use the metrics that belong to the
          // first attempt of this task. This may not be 100% accurate because the first attempt
          // could have failed half-way through. The correct fix would be to keep track of the
          // metrics added by each attempt, but this is much more complicated.
          return
        case e: ExceptionFailure =>
          taskSummary.tasksFailed += 1
        case _ =>
          taskSummary.tasksComplete += 1
      }
      if (taskSummary.tasksActive >= 1) {
        taskSummary.tasksActive -= 1
      }
      taskSummary.duration += info.duration

      // Update shuffle read/write
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        taskSummary.inputBytes += metrics.inputMetrics.bytesRead
        taskSummary.inputRecords += metrics.inputMetrics.recordsRead
        taskSummary.outputBytes += metrics.outputMetrics.bytesWritten
        taskSummary.outputRecords += metrics.outputMetrics.recordsWritten

        taskSummary.shuffleRead += metrics.shuffleReadMetrics.remoteBytesRead
        taskSummary.shuffleWrite += metrics.shuffleWriteMetrics.bytesWritten
        taskSummary.jvmGCTime += metrics.jvmGCTime
      }
    }
  }

}
