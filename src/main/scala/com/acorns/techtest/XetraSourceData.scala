package com.acorns.techtest

import com.acorns.techtest.schema.TradeActivity
import org.apache.spark.sql.{Dataset, SparkSession}

class XetraSourceData(filePath: String, sparkSession: SparkSession) {
  import sparkSession.implicits._

  def getTradeActivities(sparkSession: SparkSession): Dataset[TradeActivity] = {
    sparkSession.read
      .option("header", "true")
      .option("inferSchema", true)
      .csv(s"$filePath/*/*.csv")
      .as[TradeActivity]
  }
}
