/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

trait AnalysisTest extends PlanTest {

  protected val caseSensitiveAnalyzer = makeAnalyzer(caseSensitive = true)
  protected val caseInsensitiveAnalyzer = makeAnalyzer(caseSensitive = false)

  private def makeAnalyzer(caseSensitive: Boolean): Analyzer = {
    val conf = new SimpleCatalystConf(caseSensitive)
    val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
    catalog.createTempView("TaBlE", TestRelations.testRelation, overrideIfExists = true)
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules = EliminateSubqueryAliases :: Nil
    }
  }

  protected def getAnalyzer(caseSensitive: Boolean) = {
    if (caseSensitive) caseSensitiveAnalyzer else caseInsensitiveAnalyzer
  }

  protected def checkAnalysis(
      inputPlan: LogicalPlan,
      expectedPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    val analyzer = getAnalyzer(caseSensitive)
    val actualPlan = analyzer.execute(inputPlan)
    analyzer.checkAnalysis(actualPlan)
    comparePlans(actualPlan, expectedPlan)
  }

  protected def assertAnalysisSuccess(
      inputPlan: LogicalPlan,
      caseSensitive: Boolean = true): Unit = {
    val analyzer = getAnalyzer(caseSensitive)
    val analysisAttempt = analyzer.execute(inputPlan)
    try analyzer.checkAnalysis(analysisAttempt) catch {
      case a: AnalysisException =>
        fail(
          s"""
            |Failed to Analyze Plan
            |$inputPlan
            |
            |Partial Analysis
            |$analysisAttempt
          """.stripMargin, a)
    }
  }

  protected def assertAnalysisError(
      inputPlan: LogicalPlan,
      expectedErrors: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    val analyzer = getAnalyzer(caseSensitive)
    val e = intercept[AnalysisException] {
      analyzer.checkAnalysis(analyzer.execute(inputPlan))
    }

    if (!expectedErrors.map(_.toLowerCase).forall(e.getMessage.toLowerCase.contains)) {
      fail(
        s"""Exception message should contain the following substrings:
           |
           |  ${expectedErrors.mkString("\n  ")}
           |
           |Actual exception message:
           |
           |  ${e.getMessage}
         """.stripMargin)
    }
  }
}
