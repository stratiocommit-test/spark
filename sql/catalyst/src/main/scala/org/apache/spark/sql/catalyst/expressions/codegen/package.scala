/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.rules
import org.apache.spark.util.Utils

/**
 * A collection of generators that build custom bytecode at runtime for performing the evaluation
 * of catalyst expression.
 */
package object codegen {

  /** Canonicalizes an expression so those that differ only by names can reuse the same code. */
  object ExpressionCanonicalizer extends rules.RuleExecutor[Expression] {
    val batches =
      Batch("CleanExpressions", FixedPoint(20), CleanExpressions) :: Nil

    object CleanExpressions extends rules.Rule[Expression] {
      def apply(e: Expression): Expression = e transform {
        case Alias(c, _) => c
      }
    }
  }

  /**
   * Dumps the bytecode from a class to the screen using javap.
   */
  object DumpByteCode {
    import scala.sys.process._
    val dumpDirectory = Utils.createTempDir()
    dumpDirectory.mkdir()

    def apply(obj: Any): Unit = {
      val generatedClass = obj.getClass
      val classLoader =
        generatedClass
          .getClassLoader
          .asInstanceOf[scala.tools.nsc.interpreter.AbstractFileClassLoader]
      val generatedBytes = classLoader.classBytes(generatedClass.getName)

      val packageDir = new java.io.File(dumpDirectory, generatedClass.getPackage.getName)
      if (!packageDir.exists()) { packageDir.mkdir() }

      val classFile =
        new java.io.File(packageDir, generatedClass.getName.split("\\.").last + ".class")

      val outfile = new java.io.FileOutputStream(classFile)
      outfile.write(generatedBytes)
      outfile.close()

      // scalastyle:off println
      println(
        s"javap -p -v -classpath ${dumpDirectory.getCanonicalPath} ${generatedClass.getName}".!!)
      // scalastyle:on println
    }
  }
}
