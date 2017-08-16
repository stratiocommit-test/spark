/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.scheduler.cluster.mesos

import scala.collection.JavaConverters._

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NoNodeException

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Persistence engine factory that is responsible for creating new persistence engines
 * to store Mesos cluster mode state.
 */
private[spark] abstract class MesosClusterPersistenceEngineFactory(conf: SparkConf) {
  def createEngine(path: String): MesosClusterPersistenceEngine
}

/**
 * Mesos cluster persistence engine is responsible for persisting Mesos cluster mode
 * specific state, so that on failover all the state can be recovered and the scheduler
 * can resume managing the drivers.
 */
private[spark] trait MesosClusterPersistenceEngine {
  def persist(name: String, obj: Object): Unit
  def expunge(name: String): Unit
  def fetch[T](name: String): Option[T]
  def fetchAll[T](): Iterable[T]
}

/**
 * Zookeeper backed persistence engine factory.
 * All Zk engines created from this factory shares the same Zookeeper client, so
 * all of them reuses the same connection pool.
 */
private[spark] class ZookeeperMesosClusterPersistenceEngineFactory(conf: SparkConf)
  extends MesosClusterPersistenceEngineFactory(conf) with Logging {

  lazy val zk = SparkCuratorUtil.newClient(conf)

  def createEngine(path: String): MesosClusterPersistenceEngine = {
    new ZookeeperMesosClusterPersistenceEngine(path, zk, conf)
  }
}

/**
 * Black hole persistence engine factory that creates black hole
 * persistence engines, which stores nothing.
 */
private[spark] class BlackHoleMesosClusterPersistenceEngineFactory
  extends MesosClusterPersistenceEngineFactory(null) {
  def createEngine(path: String): MesosClusterPersistenceEngine = {
    new BlackHoleMesosClusterPersistenceEngine
  }
}

/**
 * Black hole persistence engine that stores nothing.
 */
private[spark] class BlackHoleMesosClusterPersistenceEngine extends MesosClusterPersistenceEngine {
  override def persist(name: String, obj: Object): Unit = {}
  override def fetch[T](name: String): Option[T] = None
  override def expunge(name: String): Unit = {}
  override def fetchAll[T](): Iterable[T] = Iterable.empty[T]
}

/**
 * Zookeeper based Mesos cluster persistence engine, that stores cluster mode state
 * into Zookeeper. Each engine object is operating under one folder in Zookeeper, but
 * reuses a shared Zookeeper client.
 */
private[spark] class ZookeeperMesosClusterPersistenceEngine(
    baseDir: String,
    zk: CuratorFramework,
    conf: SparkConf)
  extends MesosClusterPersistenceEngine with Logging {
  private val WORKING_DIR =
    conf.get("spark.deploy.zookeeper.dir", "/spark_mesos_dispatcher") + "/" + baseDir

  SparkCuratorUtil.mkdir(zk, WORKING_DIR)

  def path(name: String): String = {
    WORKING_DIR + "/" + name
  }

  override def expunge(name: String): Unit = {
    zk.delete().forPath(path(name))
  }

  override def persist(name: String, obj: Object): Unit = {
    val serialized = Utils.serialize(obj)
    val zkPath = path(name)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(zkPath, serialized)
  }

  override def fetch[T](name: String): Option[T] = {
    val zkPath = path(name)

    try {
      val fileData = zk.getData().forPath(zkPath)
      Some(Utils.deserialize[T](fileData))
    } catch {
      case e: NoNodeException => None
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(zkPath)
        None
    }
  }

  override def fetchAll[T](): Iterable[T] = {
    zk.getChildren.forPath(WORKING_DIR).asScala.flatMap(fetch[T])
  }
}
