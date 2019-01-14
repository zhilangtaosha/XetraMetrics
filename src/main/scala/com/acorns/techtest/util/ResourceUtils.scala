package com.acorns.techtest.util

import scala.io.Source

object ResourceUtils {
  def getTextFromResource(filePath: String, delimiter: String): String = {
    val stream = getClass.getResourceAsStream(filePath)
    Source.fromInputStream(stream).getLines.mkString(delimiter)
  }
}