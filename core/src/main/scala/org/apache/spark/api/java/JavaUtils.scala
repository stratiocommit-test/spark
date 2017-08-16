/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.api.java

import java.{util => ju}
import java.util.Map.Entry

import scala.collection.mutable

private[spark] object JavaUtils {
  def optionToOptional[T](option: Option[T]): Optional[T] =
    if (option.isDefined) {
      Optional.of(option.get)
    } else {
      Optional.empty[T]
    }

  // Workaround for SPARK-3926 / SI-8911
  def mapAsSerializableJavaMap[A, B](underlying: collection.Map[A, B]): SerializableMapWrapper[A, B]
    = new SerializableMapWrapper(underlying)

  // Implementation is copied from scala.collection.convert.Wrappers.MapWrapper,
  // but implements java.io.Serializable. It can't just be subclassed to make it
  // Serializable since the MapWrapper class has no no-arg constructor. This class
  // doesn't need a no-arg constructor though.
  class SerializableMapWrapper[A, B](underlying: collection.Map[A, B])
    extends ju.AbstractMap[A, B] with java.io.Serializable { self =>

    override def size: Int = underlying.size

    override def get(key: AnyRef): B = try {
      underlying.getOrElse(key.asInstanceOf[A], null.asInstanceOf[B])
    } catch {
      case ex: ClassCastException => null.asInstanceOf[B]
    }

    override def entrySet: ju.Set[ju.Map.Entry[A, B]] = new ju.AbstractSet[ju.Map.Entry[A, B]] {
      override def size: Int = self.size

      override def iterator: ju.Iterator[ju.Map.Entry[A, B]] = new ju.Iterator[ju.Map.Entry[A, B]] {
        val ui = underlying.iterator
        var prev : Option[A] = None

        def hasNext: Boolean = ui.hasNext

        def next(): Entry[A, B] = {
          val (k, v) = ui.next()
          prev = Some(k)
          new ju.Map.Entry[A, B] {
            import scala.util.hashing.byteswap32
            override def getKey: A = k
            override def getValue: B = v
            override def setValue(v1 : B): B = self.put(k, v1)
            override def hashCode: Int = byteswap32(k.hashCode) + (byteswap32(v.hashCode) << 16)
            override def equals(other: Any): Boolean = other match {
              case e: ju.Map.Entry[_, _] => k == e.getKey && v == e.getValue
              case _ => false
            }
          }
        }

        def remove() {
          prev match {
            case Some(k) =>
              underlying match {
                case mm: mutable.Map[A, _] =>
                  mm.remove(k)
                  prev = None
                case _ =>
                  throw new UnsupportedOperationException("remove")
              }
            case _ =>
              throw new IllegalStateException("next must be called at least once before remove")
          }
        }
      }
    }
  }
}
