/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst

import java.io._
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{NumericType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

package object util {

  /** Silences output to stderr or stdout for the duration of f */
  def quietly[A](f: => A): A = {
    val origErr = System.err
    val origOut = System.out
    try {
      System.setErr(new PrintStream(new OutputStream {
        def write(b: Int) = {}
      }))
      System.setOut(new PrintStream(new OutputStream {
        def write(b: Int) = {}
      }))

      f
    } finally {
      System.setErr(origErr)
      System.setOut(origOut)
    }
  }

  def fileToString(file: File, encoding: String = "UTF-8"): String = {
    val inStream = new FileInputStream(file)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    new String(outStream.toByteArray, encoding)
  }

  def resourceToBytes(
      resource: String,
      classLoader: ClassLoader = Utils.getSparkClassLoader): Array[Byte] = {
    val inStream = classLoader.getResourceAsStream(resource)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    outStream.toByteArray
  }

  def resourceToString(
      resource: String,
      encoding: String = "UTF-8",
      classLoader: ClassLoader = Utils.getSparkClassLoader): String = {
    new String(resourceToBytes(resource, classLoader), encoding)
  }

  def stringToFile(file: File, str: String): File = {
    val out = new PrintWriter(file)
    out.write(str)
    out.close()
    file
  }

  def sideBySide(left: String, right: String): Seq[String] = {
    sideBySide(left.split("\n"), right.split("\n"))
  }

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.length).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    leftPadded.zip(rightPadded).map {
      case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.length) + 3)) + r
    }
  }

  def stackTraceToString(t: Throwable): String = {
    val out = new java.io.ByteArrayOutputStream
    val writer = new PrintWriter(out)
    t.printStackTrace(writer)
    writer.flush()
    new String(out.toByteArray, StandardCharsets.UTF_8)
  }

  def stringOrNull(a: AnyRef): String = if (a == null) null else a.toString

  def benchmark[A](f: => A): A = {
    val startTime = System.nanoTime()
    val ret = f
    val endTime = System.nanoTime()
    // scalastyle:off println
    println(s"${(endTime - startTime).toDouble / 1000000}ms")
    // scalastyle:on println
    ret
  }

  // Replaces attributes, string literals, complex type extractors with their pretty form so that
  // generated column names don't contain back-ticks or double-quotes.
  def usePrettyExpression(e: Expression): Expression = e transform {
    case a: Attribute => new PrettyAttribute(a)
    case Literal(s: UTF8String, StringType) => PrettyAttribute(s.toString, StringType)
    case Literal(v, t: NumericType) if v != null => PrettyAttribute(v.toString, t)
    case e: GetStructField =>
      val name = e.name.getOrElse(e.childSchema(e.ordinal).name)
      PrettyAttribute(usePrettyExpression(e.child).sql + "." + name, e.dataType)
    case e: GetArrayStructFields =>
      PrettyAttribute(usePrettyExpression(e.child) + "." + e.field.name, e.dataType)
  }

  def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }

  def toPrettySQL(e: Expression): String = usePrettyExpression(e).sql

  /* FIX ME
  implicit class debugLogging(a: Any) {
    def debugLogging() {
      org.apache.log4j.Logger.getLogger(a.getClass.getName).setLevel(org.apache.log4j.Level.DEBUG)
    }
  } */
}
