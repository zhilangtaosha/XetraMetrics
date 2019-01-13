package com.acorns.techtest.util

object ResourceUtils {
  def getLocalPath(relativePath: String): String = {
    val basePath = System.getProperty("user.dir")
    val fixedRelativePath = if (relativePath.startsWith("/")) relativePath else s"/" + relativePath
    basePath + fixedRelativePath
  }
}