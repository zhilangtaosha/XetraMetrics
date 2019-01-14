package com.acorns.techtest.biggestwinner

import com.acorns.techtest.DataFrameUAT
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}

class DailyBiggestWinnersUAT(sparkSession: SparkSession,
                             controlDataFrame: DataFrame,
                             dataFrame1: DataFrame,
                             dataFrame2: DataFrame) extends DataFrameUAT(
  sparkSession,
  controlDataFrame,
  dataFrame1,
  dataFrame2) {

  override protected def getGroupedDataFrame(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy("uniqueIdentifier", "Date", "SecurityID", "Description", "PercentChange")
      .agg(count("*").as("rowCount"))
      .where("rowCount != 3")
  }

  override protected def joinDataFrames(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
    dataFrame1.as("df1")
      .join(
        dataFrame2,
        Seq("uniqueIdentifier", "Date", "SecurityID", "Description", "PercentChange"),
        "inner"
      )
      .select(
        "df1.*"
      )
  }
}