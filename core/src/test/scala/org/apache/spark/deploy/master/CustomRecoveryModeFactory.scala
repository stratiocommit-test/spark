/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
// This file is placed in different package to make sure all of these components work well
// when they are outside of org.apache.spark.
package other.supplier

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master._
import org.apache.spark.serializer.Serializer

class CustomRecoveryModeFactory(
  conf: SparkConf,
  serializer: Serializer
) extends StandaloneRecoveryModeFactory(conf, serializer) {

  CustomRecoveryModeFactory.instantiationAttempts += 1

  /**
   * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
   * is handled for recovery.
   *
   */
  override def createPersistenceEngine(): PersistenceEngine =
    new CustomPersistenceEngine(serializer)

  /**
   * Create an instance of LeaderAgent that decides who gets elected as master.
   */
  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent =
    new CustomLeaderElectionAgent(master)
}

object CustomRecoveryModeFactory {
  @volatile var instantiationAttempts = 0
}

class CustomPersistenceEngine(serializer: Serializer) extends PersistenceEngine {
  val data = mutable.HashMap[String, Array[Byte]]()

  CustomPersistenceEngine.lastInstance = Some(this)

  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   */
  override def persist(name: String, obj: Object): Unit = {
    CustomPersistenceEngine.persistAttempts += 1
    val serialized = serializer.newInstance().serialize(obj)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    data += name -> bytes
  }

  /**
   * Defines how the object referred by its name is removed from the store.
   */
  override def unpersist(name: String): Unit = {
    CustomPersistenceEngine.unpersistAttempts += 1
    data -= name
  }

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   */
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    CustomPersistenceEngine.readAttempts += 1
    val results = for ((name, bytes) <- data; if name.startsWith(prefix))
      yield serializer.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
    results.toSeq
  }
}

object CustomPersistenceEngine {
  @volatile var persistAttempts = 0
  @volatile var unpersistAttempts = 0
  @volatile var readAttempts = 0

  @volatile var lastInstance: Option[CustomPersistenceEngine] = None
}

class CustomLeaderElectionAgent(val masterInstance: LeaderElectable) extends LeaderElectionAgent {
  masterInstance.electedLeader()
}

