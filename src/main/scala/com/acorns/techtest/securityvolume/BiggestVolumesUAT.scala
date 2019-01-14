package com.acorns.techtest

import java.util.StringJoiner

import com.acorns.techtest.util.CustomStringUtils
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

class BiggestVolumesUAT(sparkSession: SparkSession,
                        controlDataFrame: DataFrame,
                        dataFrame1: DataFrame) {

  def getComparisonSamples(label1: String): String = {
    val stringJoiner = new StringJoiner("\n")

    controlDataFrame.cache()
    dataFrame1.cache()

    val label1Count = dataFrame1.count()
    stringJoiner.add(s"Found $label1Count records by $label1.")
    stringJoiner.add("")

    val unionedDataFrames =
      controlDataFrame.withColumn("DataFrameName", lit("control")).toDF()
        .union(dataFrame1.withColumn("DataFrameName", lit(label1)).toDF())

    unionedDataFrames.cache()

    val groupedDataFrame = getGroupedDataFrame(unionedDataFrames)

    groupedDataFrame.cache()
    val diffRecords = groupedDataFrame.count()

    stringJoiner.add(s"There are $diffRecords unique records between the $label1 and control datasets.")

    if (diffRecords > 0) {
      stringJoiner.add("")
      stringJoiner.add(CustomStringUtils.getSampleTitle("DIFF"))

      joinDataFrames(unionedDataFrames, groupedDataFrame)
        .sort("uniqueIdentifier", "DataFrameName")
        .limit(10)
        .collect()
        .foreach{row =>
          stringJoiner.add(row.toString)
        }

      stringJoiner.add(CustomStringUtils.get30Dashes)
    }

    controlDataFrame.unpersist()
    dataFrame1.unpersist()
    unionedDataFrames.unpersist()
    groupedDataFrame.unpersist()

    stringJoiner.toString
  }

  private def getGroupedDataFrame(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy("uniqueIdentifier", "SecurityID", "Description", "ImpliedVolume")
      .agg(count("*").as("rowCount"))
      .where("rowCount != 2")
  }

  private def joinDataFrames(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
    dataFrame1.as("df1")
      .join(
        dataFrame2,
        Seq("uniqueIdentifier", "SecurityID", "Description", "ImpliedVolume"),
        "inner"
      )
      .select(
        "df1.*"
      )
  }
}