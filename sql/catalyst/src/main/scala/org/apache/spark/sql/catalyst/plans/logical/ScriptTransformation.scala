/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 * @param ioschema the input and output schema applied in the execution of the script.
 */
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: LogicalPlan,
    ioschema: ScriptInputOutputSchema) extends UnaryNode {
  override def references: AttributeSet = AttributeSet(input.flatMap(_.references))
}

/**
 * Input and output properties when passing data to a script.
 * For example, in Hive this would specify which SerDes to use.
 */
case class ScriptInputOutputSchema(
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    inputSerdeClass: Option[String],
    outputSerdeClass: Option[String],
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    recordReaderClass: Option[String],
    recordWriterClass: Option[String],
    schemaLess: Boolean) {

  def inputRowFormatSQL: Option[String] =
    getRowFormatSQL(inputRowFormat, inputSerdeClass, inputSerdeProps)

  def outputRowFormatSQL: Option[String] =
    getRowFormatSQL(outputRowFormat, outputSerdeClass, outputSerdeProps)

  /**
   * Get the row format specification
   * Note:
   * 1. Changes are needed when readerClause and writerClause are supported.
   * 2. Changes are needed when "ESCAPED BY" is supported.
   */
  private def getRowFormatSQL(
    rowFormat: Seq[(String, String)],
    serdeClass: Option[String],
    serdeProps: Seq[(String, String)]): Option[String] = {
    if (schemaLess) return Some("")

    val rowFormatDelimited =
      rowFormat.map {
        case ("TOK_TABLEROWFORMATFIELD", value) =>
          "FIELDS TERMINATED BY " + value
        case ("TOK_TABLEROWFORMATCOLLITEMS", value) =>
          "COLLECTION ITEMS TERMINATED BY " + value
        case ("TOK_TABLEROWFORMATMAPKEYS", value) =>
          "MAP KEYS TERMINATED BY " + value
        case ("TOK_TABLEROWFORMATLINES", value) =>
          "LINES TERMINATED BY " + value
        case ("TOK_TABLEROWFORMATNULL", value) =>
          "NULL DEFINED AS " + value
        case o => return None
      }

    val serdeClassSQL = serdeClass.map("'" + _ + "'").getOrElse("")
    val serdePropsSQL =
      if (serdeClass.nonEmpty) {
        val props = serdeProps.map{p => s"'${p._1}' = '${p._2}'"}.mkString(", ")
        if (props.nonEmpty) " WITH SERDEPROPERTIES(" + props + ")" else ""
      } else {
        ""
      }
    if (rowFormat.nonEmpty) {
      Some("ROW FORMAT DELIMITED " + rowFormatDelimited.mkString(" "))
    } else {
      Some("ROW FORMAT SERDE " + serdeClassSQL + serdePropsSQL)
    }
  }
}
