/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved
 *
 * This software is a modification of the original software Apache Spark licensed under the Apache 2.0
 * license, a copy of which is below. This software contains proprietary information of
 * Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or
 * otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled,
 * without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package org.apache.spark.sql.catalyst.expressions.codegen

import java.util.regex.Matcher

/**
 * An utility class that indents a block of code based on the curly braces and parentheses.
 * This is used to prettify generated code when in debug mode (or exceptions).
 *
 * Written by Matei Zaharia.
 */
object CodeFormatter {
  val commentHolder = """\/\*(.+?)\*\/""".r

  def format(code: CodeAndComment): String = {
    val formatter = new CodeFormatter
    code.body.split("\n").foreach { line =>
      val commentReplaced = commentHolder.replaceAllIn(
        line.trim,
        m => code.comment.get(m.group(1)).map(Matcher.quoteReplacement).getOrElse(m.group(0)))
      formatter.addLine(commentReplaced)
    }
    formatter.result()
  }

  def stripExtraNewLines(input: String): String = {
    val code = new StringBuilder
    var lastLine: String = "dummy"
    input.split('\n').foreach { l =>
      val line = l.trim()
      val skip = line == "" && (lastLine == "" || lastLine.endsWith("{") || lastLine.endsWith("*/"))
      if (!skip) {
        code.append(line)
        code.append("\n")
      }
      lastLine = line
    }
    code.result()
  }

  def stripOverlappingComments(codeAndComment: CodeAndComment): CodeAndComment = {
    val code = new StringBuilder
    val map = codeAndComment.comment

    def getComment(line: String): Option[String] = {
      if (line.startsWith("/*") && line.endsWith("*/")) {
        map.get(line.substring(2, line.length - 2))
      } else {
        None
      }
    }

    var lastLine: String = "dummy"
    codeAndComment.body.split('\n').foreach { l =>
      val line = l.trim()

      val skip = getComment(lastLine).zip(getComment(line)).exists {
        case (lastComment, currentComment) =>
          lastComment.substring(3).contains(currentComment.substring(3))
      }

      if (!skip) {
        code.append(line).append("\n")
      }

      lastLine = line
    }
    new CodeAndComment(code.result().trim(), map)
  }
}

private class CodeFormatter {
  private val code = new StringBuilder
  private val indentSize = 2

  // Tracks the level of indentation in the current line.
  private var indentLevel = 0
  private var indentString = ""
  private var currentLine = 1

  // Tracks the level of indentation in multi-line comment blocks.
  private var inCommentBlock = false
  private var indentLevelOutsideCommentBlock = indentLevel

  private def addLine(line: String): Unit = {

    // We currently infer the level of indentation of a given line based on a simple heuristic that
    // examines the number of parenthesis and braces in that line. This isn't the most robust
    // implementation but works for all code that we generate.
    val indentChange = line.count(c => "({".indexOf(c) >= 0) - line.count(c => ")}".indexOf(c) >= 0)
    var newIndentLevel = math.max(0, indentLevel + indentChange)

    // Please note that while we try to format the comment blocks in exactly the same way as the
    // rest of the code, once the block ends, we reset the next line's indentation level to what it
    // was immediately before entering the comment block.
    if (!inCommentBlock) {
      if (line.startsWith("/*")) {
        // Handle multi-line comments
        inCommentBlock = true
        indentLevelOutsideCommentBlock = indentLevel
      } else if (line.startsWith("//")) {
        // Handle single line comments
        newIndentLevel = indentLevel
      }
    }
    if (inCommentBlock) {
      if (line.endsWith("*/")) {
        inCommentBlock = false
        newIndentLevel = indentLevelOutsideCommentBlock
      }
    }

    // Lines starting with '}' should be de-indented even if they contain '{' after;
    // in addition, lines ending with ':' are typically labels
    val thisLineIndent = if (line.startsWith("}") || line.startsWith(")") || line.endsWith(":")) {
      " " * (indentSize * (indentLevel - 1))
    } else {
      indentString
    }
    code.append(f"/* ${currentLine}%03d */")
    if (line.trim().length > 0) {
      code.append(" ") // add a space after the line number comment.
      code.append(thisLineIndent)
      if (inCommentBlock && line.startsWith("*") || line.startsWith("*/")) code.append(" ")
      code.append(line)
    }
    code.append("\n")
    indentLevel = newIndentLevel
    indentString = " " * (indentSize * newIndentLevel)
    currentLine += 1
  }

  private def result(): String = code.result()
}
