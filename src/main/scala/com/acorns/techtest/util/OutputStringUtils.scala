package com.acorns.techtest.util

object OutputStringUtils {
  def repeatChar(n: Int, c: Char): String = List.fill(n)(c).mkString

  def get30Dashes: String = repeatChar(30, '-')

  def getSampleTitle(label: String): String = {
    val newLabel = s"$label sample".toUpperCase()
    val numDashes = get30Dashes.length - newLabel.length

    if (numDashes % 2 == 0) {
      val dashes = repeatChar(numDashes/2, '-')
      s"$dashes$newLabel$dashes"
    }
    else {
      val dashes1 = repeatChar(numDashes/2, '-')
      val dashes2 = repeatChar(numDashes/2 + 1, '-')
      s"$dashes1$newLabel$dashes2"
    }
  }
}