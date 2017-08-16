/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.ml.linalg

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse => parseJson, render}

private[ml] object JsonVectorConverter {

  /**
   * Parses the JSON representation of a vector into a [[Vector]].
   */
  def fromJson(json: String): Vector = {
    implicit val formats = DefaultFormats
    val jValue = parseJson(json)
    (jValue \ "type").extract[Int] match {
      case 0 => // sparse
        val size = (jValue \ "size").extract[Int]
        val indices = (jValue \ "indices").extract[Seq[Int]].toArray
        val values = (jValue \ "values").extract[Seq[Double]].toArray
        Vectors.sparse(size, indices, values)
      case 1 => // dense
        val values = (jValue \ "values").extract[Seq[Double]].toArray
        Vectors.dense(values)
      case _ =>
        throw new IllegalArgumentException(s"Cannot parse $json into a vector.")
    }
  }

  /**
   * Coverts the vector to a JSON string.
   */
  def toJson(v: Vector): String = {
    v match {
      case SparseVector(size, indices, values) =>
        val jValue = ("type" -> 0) ~
          ("size" -> size) ~
          ("indices" -> indices.toSeq) ~
          ("values" -> values.toSeq)
        compact(render(jValue))
      case DenseVector(values) =>
        val jValue = ("type" -> 1) ~ ("values" -> values.toSeq)
        compact(render(jValue))
    }
  }
}
