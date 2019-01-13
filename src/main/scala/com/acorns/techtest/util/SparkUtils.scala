package com.acorns.techtest.util

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSparkSession(jobName: String): SparkSession = {
    SparkSession.builder
      .appName(jobName)
      .master("local[4]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
  }
}