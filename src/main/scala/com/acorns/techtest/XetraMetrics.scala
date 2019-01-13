package com.acorns.techtest

import com.acorns.techtest.util.SparkUtils

object XetraMetrics {
  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val jobOptions = new JobOptions(args)
    val filePath = jobOptions.filePath

    val sparkSession = SparkUtils.getSparkSession("XetraMetrics")

    val xetraMetrics = new XetraMetricsJob(filePath, sparkSession)

    xetraMetrics.showMetrics()
  }
}