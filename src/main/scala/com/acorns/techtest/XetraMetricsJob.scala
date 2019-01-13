package com.acorns.techtest

import com.acorns.techtest.util.SparkUtils

object XetraMetricsJob {
  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.getSparkSession("XetraMetrics")

    val xetraMetrics = new XetraMetrics(sparkSession)

    xetraMetrics.showMetrics()
  }
}